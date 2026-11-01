package latencyexp

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/sdk/eventlog"
)

// pollEvery is how often the directory is listed. The writer flushes about
// every 100 ms, so a finished frame is visible on the next tick or the one
// after.
const pollEvery = 200 * time.Millisecond

// Exporter tails session event logs under Data and serves /metrics.
// Poll is not safe to call from two goroutines. Text is safe beside Poll.
type Exporter struct {
	data string
	log  zerolog.Logger

	mu     sync.Mutex
	met    reg
	cur    map[string]*cursor
	sess   map[string]*session
	logged map[string]struct{}
}

// New checks that data is the STS data directory. sessions/ may be absent; it
// appears when the agent creates the first session.
func New(data string, log zerolog.Logger) (*Exporter, error) {
	info, err := os.Stat(data)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New(data + " is not a directory")
	}
	return &Exporter{
		data: data,
		log:  log,
		cur:  map[string]*cursor{},
		sess: map[string]*session{},
	}, nil
}

// Text is the Prometheus exposition, including families that have no samples yet.
func (e *Exporter) Text() []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.met.text()
}

// Poll reads every session event log once. A session without an eventlog
// directory is skipped (E17-D005).
func (e *Exporter) Poll() {
	root := filepath.Join(e.data, "sessions")
	ents, err := os.ReadDir(root)
	if err != nil {
		if !os.IsNotExist(err) {
			e.logOnce(root, err)
		}
		return
	}
	for _, ent := range ents {
		if !ent.IsDir() {
			continue
		}
		elog := filepath.Join(root, ent.Name(), "volume", eventlog.Dir)
		info, err := os.Stat(elog)
		if err != nil || !info.IsDir() {
			continue
		}
		e.pollDir(ent.Name(), elog)
	}
}

func (e *Exporter) pollDir(dirName, elog string) {
	files, err := eventlog.Files(elog)
	if err != nil {
		if !os.IsNotExist(err) {
			e.logOnce(elog, err)
		}
		return
	}
	seen := map[string]bool{}
	for i, f := range files {
		seen[f.Path] = true
		cur := e.cur[f.Path]
		if cur == nil {
			cur = &cursor{}
			e.cur[f.Path] = cur
		}
		if cur.done {
			continue
		}
		e.readFile(dirName, cur, f.Path, i+1 < len(files))
	}
	for path := range e.cur {
		if filepath.Dir(path) == elog && !seen[path] {
			delete(e.cur, path)
		}
	}
}

// readFile consumes the complete frames that were not read yet. A short frame
// at the end of the file still being written stays unread until the next poll.
// A later file means this one has rotated, so a short tail is abandoned and the
// missing rec_seq is counted when the next frame arrives.
func (e *Exporter) readFile(dirName string, cur *cursor, path string, successor bool) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			cur.done = true
			return
		}
		e.logOnce(path, err)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		e.logOnce(path, err)
		return
	}
	if info.Size() < cur.off {
		cur.done = true
		e.logOnce(path, errShrunk)
		return
	}
	if info.Size() == cur.off {
		if successor {
			cur.done = true
		}
		return
	}
	buf := make([]byte, info.Size()-cur.off)
	n, err := f.ReadAt(buf, cur.off)
	if n == 0 {
		if err != nil && !errors.Is(err, os.ErrClosed) && !errors.Is(err, os.ErrNotExist) {
			e.logOnce(path, err)
		}
		return
	}
	buf = buf[:n]

	header := cur.header
	payload := buf
	var headerN int
	if cur.off == 0 {
		hn, herr := peekHeader(buf)
		if errors.Is(herr, errNeedMore) {
			if successor {
				cur.done = true
				e.logOnce(path, errTorn)
			}
			return
		}
		if herr != nil {
			cur.done = true
			e.logOnce(path, herr)
			return
		}
		header = buf[:hn]
		payload = buf[hn:]
		headerN = hn
	}
	frames, consumed, status := scanFrames(payload)
	if status == scanBad {
		e.logOnce(path, eventlog.ErrFrame)
	}
	if status == scanShort && successor && consumed < len(payload) {
		e.logOnce(path, errTorn)
	}
	h, recs, err := readRecords(header, payload[:consumed])
	if err != nil {
		cur.done = true
		e.logOnce(path, err)
		return
	}
	id := h.SessionID
	if id == "" {
		id = dirName
	}
	s := e.sess[id]
	if s == nil {
		s = &session{}
		e.sess[id] = s
	}
	ps := s.collect(id, h.Incarnation, frames, recs)
	e.mu.Lock()
	e.met.add(ps)
	e.mu.Unlock()
	if cur.off == 0 {
		cur.header = append([]byte(nil), header...)
	}
	cur.off += int64(headerN + consumed)
	switch status {
	case scanBad:
		cur.done = true
	case scanShort:
		if successor {
			cur.done = true
		}
	default:
		if successor && cur.off >= info.Size() {
			cur.done = true
		}
	}
}

func (e *Exporter) logOnce(path string, err error) {
	if err == nil {
		return
	}
	key := path + "\x00" + err.Error()
	if _, ok := e.logged[key]; ok {
		return
	}
	if e.logged == nil {
		e.logged = map[string]struct{}{}
	}
	e.logged[key] = struct{}{}
	e.log.Warn().Err(err).Str("path", path).Msg("event log")
}

// Serve listens on ln and polls until ctx is cancelled. The listener is already
// bound by the caller, after CheckAddr. A cancelled ctx is a clean shutdown.
func (e *Exporter) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           e.handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	e.Poll()
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	var serveErr error
	for serveErr == nil {
		select {
		case <-ctx.Done():
			serveErr = ctx.Err()
		case err := <-errCh:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErr = err
			} else {
				serveErr = http.ErrServerClosed
			}
		case <-tick.C:
			e.Poll()
		}
	}
	shut, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(shut)
	if errors.Is(serveErr, context.Canceled) {
		// Shutdown makes Serve return. The send is buffered; if it never
		// arrives, the shutdown deadline ends the wait.
		select {
		case <-errCh:
		case <-shut.Done():
		}
		return nil
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	return serveErr
}

func (e *Exporter) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", e.serveMetrics)
	mux.HandleFunc("HEAD /metrics", e.serveMetrics)
	return mux
}

func (e *Exporter) serveMetrics(w http.ResponseWriter, r *http.Request) {
	body := e.Text()
	w.Header().Set("Content-Type", metricsType)
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}
