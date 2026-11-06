package sym

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
)

const (
	// ChangedChannel is the Postgres channel migration 0013 notifies on every symbol_revision
	// commit, with payload "<venue>/<category>/<revision>" (E22-D009).
	ChangedChannel = "sym_changed"

	// DefaultWatchBuffer is how many changes one watch may fall behind before it is closed.
	DefaultWatchBuffer = 256

	// catchUpSQL runs on the listener's own connection after LISTEN.
	catchUpSQL = `SELECT venue, category, revision FROM symbol_revision ORDER BY venue, category`

	defaultMinBackoff = 100 * time.Millisecond
	defaultMaxBackoff = 5 * time.Second
)

// ErrSlowWatcher ends a watch whose buffer filled up. The client opens a new watch and gets a
// new snapshot; a gRPC stream ends with ResourceExhausted (E22-D009).
var ErrSlowWatcher = errors.New("sym: watch fell behind")

// HubOptions tunes a Hub. The zero value is the production setting.
type HubOptions struct {
	Log zerolog.Logger
	// Buffer is each watch's queue. Zero selects DefaultWatchBuffer.
	Buffer int
	// MinBackoff and MaxBackoff bound the wait before the listener reconnects. Zero selects
	// 100ms and 5s.
	MinBackoff, MaxBackoff time.Duration
}

// Hub hears every revision commit, from any replica, and fans it out to the watches (E22-D009).
//
// It holds one connection of its own, outside the Store's pool, with LISTEN sym_changed. It
// keeps the last revision it passed on for each table and drops anything not newer. After each
// (re)connect it reads symbol_revision once and passes on every table that moved while it was
// away. A watch that falls Buffer changes behind ends with ErrSlowWatcher; the others do not wait.
type Hub struct {
	store      *Store
	log        zerolog.Logger
	buffer     int
	minBackoff time.Duration
	maxBackoff time.Duration

	mu     sync.Mutex
	last   map[tableKey]int64
	subs   map[*subscription]struct{}
	closed bool
}

type tableKey struct {
	venue    string
	category string
}

// NewHub builds a Hub over st. Run starts it.
func NewHub(st *Store, opt HubOptions) *Hub {
	h := &Hub{
		store:      st,
		log:        opt.Log,
		buffer:     opt.Buffer,
		minBackoff: opt.MinBackoff,
		maxBackoff: opt.MaxBackoff,
		last:       map[tableKey]int64{},
		subs:       map[*subscription]struct{}{},
	}
	if h.buffer <= 0 {
		h.buffer = DefaultWatchBuffer
	}
	if h.minBackoff <= 0 {
		h.minBackoff = defaultMinBackoff
	}
	if h.maxBackoff < h.minBackoff {
		h.maxBackoff = max(defaultMaxBackoff, h.minBackoff)
	}
	return h
}

// Run listens until ctx ends, reconnecting with backoff. Then every watch ends with an
// unavailable error and later watches are refused. It returns nil.
func (h *Hub) Run(ctx context.Context) error {
	defer h.close()
	backoff := h.minBackoff
	for {
		heard, err := h.listen(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if heard {
			backoff = h.minBackoff
		}
		h.log.Warn().Err(err).Dur("retry", backoff).Msg("sym change listener")
		if wait(ctx, backoff) != nil {
			return nil
		}
		backoff = min(backoff*2, h.maxBackoff)
	}
}

// listen is one connection. heard reports that LISTEN and the catch-up read both took, so the
// backoff starts over. A catch-up that fails keeps backing off (E22-D015).
func (h *Hub) listen(ctx context.Context) (heard bool, err error) {
	conn, err := h.store.listenConn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, "LISTEN "+ChangedChannel); err != nil {
		return false, err
	}
	// After LISTEN, so a commit is in this read or in a notification, never in neither.
	if err := h.catchUp(ctx, conn); err != nil {
		return false, err
	}
	h.log.Info().Msg("sym change listener up")
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return true, err
		}
		venue, category, rev, ok := parseChanged(n.Payload)
		if !ok {
			h.log.Warn().Str("payload", n.Payload).Msg("sym change payload")
			continue
		}
		h.publish(venue, category, rev)
	}
}

func (h *Hub) catchUp(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, catchUpSQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var venue, category string
		var rev int64
		if err := rows.Scan(&venue, &category, &rev); err != nil {
			return err
		}
		h.publish(venue, category, rev)
	}
	return rows.Err()
}

func parseChanged(payload string) (venue, category string, rev int64, ok bool) {
	parts := strings.Split(payload, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return "", "", 0, false
	}
	rev, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || rev < 1 {
		return "", "", 0, false
	}
	return parts[0], parts[1], rev, true
}

func (h *Hub) publish(venue, category string, rev int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := tableKey{venue, category}
	if h.closed || rev <= h.last[key] {
		return
	}
	h.last[key] = rev
	for s := range h.subs {
		if !s.filter.match(venue, category) {
			continue
		}
		select {
		case s.ch <- &symv1.Changed{Venue: venue, Category: category, Revision: rev}:
		default:
			delete(h.subs, s)
			s.end(ErrSlowWatcher)
		}
	}
}

func (h *Hub) subscribe(f WatchFilter) (*subscription, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, false
	}
	s := &subscription{filter: f, ch: make(chan *symv1.Changed, h.buffer), done: make(chan struct{})}
	h.subs[s] = struct{}{}
	return s, true
}

func (h *Hub) unsubscribe(s *subscription) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

func (h *Hub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for s := range h.subs {
		s.end(hubStopped())
	}
	clear(h.subs)
}

func hubStopped() error { return unavailable("change stream stopped", nil) }

// subscription is one watch's queue. ch is never closed; done is, once, with err.
type subscription struct {
	filter WatchFilter
	ch     chan *symv1.Changed
	once   sync.Once
	done   chan struct{}
	err    error
}

func (s *subscription) end(err error) {
	s.once.Do(func() {
		s.err = err
		close(s.done)
	})
}
