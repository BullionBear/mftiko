package conn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

// Instruments is one venue's active symbol table. Get with includeInactive false reads that
// table. The conn worker loads it at Init and follows sym.v1.changed.{venue}; the agent does not.
type Instruments interface {
	Get(ctx context.Context, t symbol.UniversalTicker, includeInactive bool) (*symbol.Info, error)
}

// Config is one conn worker. Venue, Route and Instance identify it. N, the connection index, is
// not used here: the caller has already chosen which atoms this process owns.
type Config struct {
	Venue    string
	Route    string
	Instance string
	// URL is the websocket to dial. Empty asks Market.Endpoint for the route's default.
	URL string
	// Market supplies the session and, when URL is empty, the endpoint. Concrete venues stay in
	// cmd; this package only sees the interface.
	Market exchange.Market
	// Publish sends one md.f message. It runs on the read goroutine, inside Decode, so it must
	// return promptly and must not call back into the Conn: the read is what answers a venue
	// ping, and the call holds the worker's lock. The bytes stay valid after it returns.
	Publish func(subject string, msg []byte) error
	// Now is the clock written into md_ts_ns. Nil means time.Now. It is not the reconnect or
	// rate-limit clock.
	Now func() time.Time
	// Epoch is the last conn_epoch already used on md.v1.f. The next dial uses Epoch+1. Zero
	// means none has been used, so the first connection is 1. A failed dial does not consume
	// a number. Report, when set, is how the number reaches the shim before the dial.
	Epoch uint64
	// Report sends one mdctl.Report. Dial waits until the call that carries the next
	// conn_epoch returns. That report, and every later one, is a full snapshot: conn_epoch
	// plus every feed's FeedStatus. A peer that reads the latest report therefore has the
	// number before the connection is up, and md.f for that connection is published only
	// after. A dial that fails leaves Epoch unchanged, so the next attempt reports the same
	// number. Nil reports nothing. Report must not call back into the Conn.
	Report func(*mdctlv1.Report) error
	// RetryAfter is the wait after a failed dial. The next failure waits twice as long, up to
	// RetryMax. Zero means 1s and a cap of 30s. Negative means do not wait, which tests use so a
	// reconnect is immediate. A dial that follows a connection which came up does not wait,
	// whatever the previous failure had grown to.
	RetryAfter time.Duration
	RetryMax   time.Duration
	// REST and Doer run SnapshotREST depth in this process. Nil is left alone until a book asks
	// for a snapshot. The call does not go through md.fetch (D402). Depth asks for BookSync.Depth()
	// (D453).
	REST exchange.PublicREST
	Doer exchange.Doer
	// BufferMax and BufferAge cap the updates kept while a snapshot is outstanding (D424). Zero
	// means DefaultBufferMax and DefaultBufferAge. Hitting either drops the buffer and makes the
	// in-flight snapshot stale.
	BufferMax int
	BufferAge time.Duration
	// RateHold is the wait after a 429 or 418 that has neither Retry-After nor ResetAt (D424).
	// Zero means DefaultRateHold. Negative is rejected by Serve. It is not an Init field.
	RateHold time.Duration
	// BookNow is the clock for buffer age, snapshot backoff and rate holds. Nil means time.Now.
	// It is not the md_ts_ns clock and not the dial clock.
	BookNow func() time.Time
	// BookAfter fires on the book clock. Nil means time.After. Tests pass a fake so the waits
	// above do not sleep.
	BookAfter func(time.Duration) <-chan time.Time
	// Symbols is this venue's active table. Nil uses exch_ticker and price_tick from the assignment,
	// which is what tests do. A running worker passes the table loaded at Init.
	Symbols Instruments
	// TapeWindow, TapeMaxEvents and TapeBudget bound each feed's ring and the worker's tape
	// memory (D458). Zero means 10 minutes, 100000 events and 256 MiB. The budget counts
	// chunks, slots and the chunk-pointer index, not only message bytes.
	TapeWindow    time.Duration
	TapeMaxEvents int
	TapeBudget    int64
	// Subscribe registers one md.v1.view responder. The handler runs off the read goroutine and
	// must not call back into the Conn. Nil records tape and does not serve md.v1.view. The
	// returned function removes the interest.
	Subscribe func(subject string, handler func(reply string, req []byte)) (func(), error)
	// Log is optional. Nil discards.
	Log *zerolog.Logger
}

// Conn is one market-data connection. SetDesired and Run are safe to call from different
// goroutines. The Sink methods are not: Decode calls them with the worker's lock held.
type Conn struct {
	venue, route, inst string
	url                string
	market             exchange.Market
	pub                func(string, []byte) error
	now                func() time.Time
	retryAfter         time.Duration
	retryMax           time.Duration
	log                zerolog.Logger
	rest               exchange.PublicREST
	doer               exchange.Doer
	bufferMax          int
	bufferAge          time.Duration
	rateHold           time.Duration
	bookNow            func() time.Time
	bookAfter          func(time.Duration) <-chan time.Time
	symbols            Instruments
	// reconEvery is how often the feed reconcile loop re-reads the venue table. Zero is one second.
	reconEvery time.Duration

	mu sync.Mutex
	// epoch is the connection that is up, or the last one that was, or Config.Epoch before the
	// first successful dial.
	epoch uint64
	// announced is the conn_epoch handed to Report for a dial that has not landed yet.
	// A feed snapshot sent while that dial is in flight uses it, so a later report cannot
	// replace the shim's copy with an older number.
	announced uint64
	report    func(*mdctlv1.Report) error
	reportMu  sync.Mutex
	// status is the last classification of each feed. statusDirty asks for another Report.
	status      map[string]feedState
	statusDirty bool
	have        bool // a Desired has been applied; the next one must be strictly newer
	ver         commonv1.Version
	// feedMode is set once the agent sends topic.UniversalTicker instead of atom ids.
	// wantFeeds is that list. The reconcile loop derives wanted from it.
	feedMode  bool
	wantFeeds []string
	// wanted is the current desired set for this venue and route, keyed by channel.
	wanted map[string][]feed
	// exch is the venue spelling for that channel, from the venue table (D426). An assignment
	// supplies it only when this worker has no table.
	exch map[string]string
	// tick is the price tick for that channel, from the same place as exch (D433).
	tick map[string]float64
	live *link
	kick chan struct{}
	// bookWake asks the snapshot loop to look again. One slot: the loop reads the current shelves.
	bookWake chan struct{}
	// gate times Depth for every book on this worker. It outlives a reconnect: the IP does not change.
	gate depthGate
	// fetchWG is the Depth calls of the connection that is up. The link is cleared only after they
	// return, so a late response cannot land on the next connection.
	fetchWG sync.WaitGroup
	// fatal is a book the worker will not run (a channel that is not a book). stop cancels Run.
	fatal error
	stop  context.CancelFunc

	subscribe  func(subject string, handler func(reply string, req []byte)) (func(), error)
	tapeWindow int64
	tapeMax    int
	tapeBudget int64
	tape       *tapeArena
	views      map[string]*viewFeed
	viewStop   map[string]*viewSub
	viewMu     sync.Mutex
	books      map[string]*atomic.Pointer[bookSnap]
	// wasDown is set when the connection that was up has ended, until the next one is up.
	wasDown bool
	downNs  int64
}

// feed is one assigned publication: the subject is md.v1.f.{instance}.{topic}.{ticker} (D455).
// uid is the active-table UID copied when the feed is created (D461). The subject keeps the ticker.
type feed struct {
	topic, ticker string
	uid           uint32
}

// link is the connection that is up. Reconnect replaces it, which drops observed channels,
// in-flight acks and per-feed sequence numbers together. An ack that names an id this link did
// not send is from the previous connection and is ignored.
type link struct {
	session       exchange.Session
	observed      map[string]struct{}
	subInflight   map[uint64][]string
	unsubInflight map[uint64][]string
	subPending    map[string]struct{}
	unsubPending  map[string]struct{}
	// resub is a SnapshotStream channel whose backoff has elapsed. plan unsubscribes it
	// before subscribing it again (D434). The session rate limit still paces those frames.
	resub map[string]struct{}
	seq   map[string]uint64 // feed key topic.ticker, last seq handed out
	mdTs  map[string]int64  // md_ts_ns of that publish
	books map[string]*shelf
}

func newLink(s exchange.Session) *link {
	return &link{
		session:       s,
		observed:      map[string]struct{}{},
		subInflight:   map[uint64][]string{},
		unsubInflight: map[uint64][]string{},
		subPending:    map[string]struct{}{},
		unsubPending:  map[string]struct{}{},
		resub:         map[string]struct{}{},
		seq:           map[string]uint64{},
		mdTs:          map[string]int64{},
		books:         map[string]*shelf{},
	}
}

// New builds a worker. It does not dial.
func New(cfg Config) *Conn {
	log := zerolog.Nop()
	if cfg.Log != nil {
		log = *cfg.Log
	}
	log = log.With().Str("venue", cfg.Venue).Str("route", cfg.Route).Str("md", cfg.Instance).Logger()
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	bufN := cfg.BufferMax
	if bufN == 0 {
		bufN = DefaultBufferMax
	}
	bufAge := cfg.BufferAge
	if bufAge == 0 {
		bufAge = DefaultBufferAge
	}
	hold := cfg.RateHold
	if hold == 0 {
		hold = DefaultRateHold
	}
	bookNow := cfg.BookNow
	if bookNow == nil {
		bookNow = time.Now
	}
	window := int64(cfg.TapeWindow)
	if window <= 0 {
		window = defaultTapeWindow
	}
	maxEvents := cfg.TapeMaxEvents
	if maxEvents <= 0 {
		maxEvents = defaultTapeMaxEvents
	}
	budget := cfg.TapeBudget
	if budget <= 0 {
		budget = defaultTapeBudget
	}
	return &Conn{
		venue: cfg.Venue, route: cfg.Route, inst: cfg.Instance,
		url: cfg.URL, market: cfg.Market, pub: cfg.Publish, now: now,
		epoch: cfg.Epoch, report: cfg.Report, retryAfter: cfg.RetryAfter, retryMax: cfg.RetryMax,
		rest: cfg.REST, doer: cfg.Doer,
		bufferMax: bufN, bufferAge: bufAge, rateHold: hold, bookNow: bookNow, bookAfter: cfg.BookAfter,
		symbols: cfg.Symbols, subscribe: cfg.Subscribe,
		tapeWindow: window, tapeMax: maxEvents, tapeBudget: budget,
		tape:  newTapeArena(budget),
		views: map[string]*viewFeed{}, viewStop: map[string]*viewSub{},
		books: map[string]*atomic.Pointer[bookSnap]{},
		log:   log, kick: make(chan struct{}, 1), bookWake: make(chan struct{}, 1),
	}
}

// Epoch is the conn_epoch of the connection that is up, or of the last one that was. Before the
// first successful dial it is the value from Config.
func (c *Conn) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// Run dials, serves, and dials again until ctx is cancelled. A session that cannot be built, or
// an endpoint that cannot be resolved, is returned as-is: retrying will not make it valid.
func (c *Conn) Run(ctx context.Context) error {
	if err := c.resolve(); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.mu.Lock()
	c.stop = cancel
	fatal := c.fatal
	c.mu.Unlock()
	if fatal != nil {
		return fatal
	}
	defer c.closeViews()
	go c.reconcile(runCtx)
	var wait time.Duration
	for {
		if err := sleep(runCtx, wait); err != nil {
			if ferr := c.fatalErr(); ferr != nil {
				return ferr
			}
			return ctx.Err()
		}
		up, retry, err := c.once(runCtx)
		if ferr := c.fatalErr(); ferr != nil {
			return ferr
		}
		if runCtx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !retry {
			return err
		}
		wait = nextDelay(wait, up, c.retryAfter, c.retryMax)
		if !up && err != nil {
			c.log.Warn().Err(err).Dur("retry_in", wait).Msg("dial")
		}
	}
}

func (c *Conn) fatalErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fatal
}

func (c *Conn) resolve() error {
	if c.venue == "" || c.route == "" || c.inst == "" || c.market == nil || c.pub == nil {
		return errors.New("conn: venue, route, instance, market and publish are required")
	}
	if c.url != "" {
		return nil
	}
	u, err := c.market.Endpoint(c.route, "")
	if err != nil {
		return err
	}
	if u == "" {
		return errors.New("conn: endpoint is empty")
	}
	c.url = u
	return nil
}

// once dials one connection and returns whether it came up. retry is false when the error is
// permanent. The epoch that will be used is reported before Dial, and stored only after Dial
// returns, so a refused connection does not burn a number that a published event would then
// have to skip. The next attempt reports that same number again.
func (c *Conn) once(ctx context.Context) (up, retry bool, err error) {
	session, err := c.market.NewSession(c.route)
	if err != nil {
		return false, false, fmt.Errorf("conn: new session: %w", err)
	}
	next, err := c.announce()
	if err != nil {
		return false, false, err
	}
	lim := session.Limits()
	conn, err := ws.Dial(ctx, c.url, ws.Options{
		ReadLimit:   lim.ReadLimit,
		Keepalive:   lim.Keepalive,
		MaxLifetime: lim.MaxLifetime,
		Ping:        session.Ping(),
	})
	if err != nil {
		return false, true, err
	}
	defer conn.Close()

	c.mu.Lock()
	c.epoch = next
	epoch := c.epoch
	c.live = newLink(session)
	c.onUpLocked()
	c.mu.Unlock()
	c.log.Info().Uint64("conn_epoch", epoch).Msg("connected")

	wctx, cancel := context.WithCancel(ctx)
	bdone := make(chan struct{})
	go func() {
		defer close(bdone)
		c.syncBooks(wctx)
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.write(wctx, conn, newLimiter(lim.Rate))
	}()
	// Kick after the writer exists. A desired set before dial is applied here; one buffered
	// kick is enough because plan reads the current set. The book loop reads the same set.
	c.poke()
	c.wakeBook()
	err = conn.Run(ctx, c.handle)
	// The writer and any Depth must finish before live is cleared, or they could land on the
	// next connection. Depth runs off the read goroutine, so cancel is what stops it.
	cancel()
	<-done
	<-bdone
	c.fetchWG.Wait()
	c.mu.Lock()
	c.onDownLocked()
	c.live = nil
	c.mu.Unlock()
	if ctx.Err() == nil {
		c.log.Info().Err(err).Uint64("conn_epoch", epoch).Msg("connection ended")
	}
	return true, true, err
}

// announce is the conn_epoch the next dial will use. Report runs here, before Dial, and a
// failure is permanent: publishing with a number the peer was not given would hide a gap.
func (c *Conn) announce() (uint64, error) {
	c.reportMu.Lock()
	defer c.reportMu.Unlock()
	c.mu.Lock()
	next := c.epoch + 1
	c.announced = next
	rep := c.snapshot(next, mdctlv1.ConnState_CONN_STATE_CONNECTING)
	report := c.report
	c.statusDirty = false
	c.mu.Unlock()
	if report == nil {
		return next, nil
	}
	if err := report(rep); err != nil {
		return 0, fmt.Errorf("conn: report conn_epoch %d: %w", next, err)
	}
	return next, nil
}

// handle is the websocket read callback. A decode error drops that frame and leaves the
// connection up: a bad message is not a reason to burn a conn_epoch. The frame is lent until
// this returns, so Decode, and the publish it triggers, finish before then.
func (c *Conn) handle(frame []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.live == nil || c.live.session == nil {
		return nil
	}
	if err := c.live.session.Decode(frame, c); err != nil {
		c.log.Warn().Err(err).Msg("decode")
	}
	return nil
}

// write sends the frames plan builds, paced by every rate limit. It stops when ctx ends, which
// is when the connection does.
func (c *Conn) write(ctx context.Context, conn *ws.Conn, lim *limiter) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.kick:
		}
		c.drain()
		if ctx.Err() != nil {
			return
		}
		c.publishStatus()
		for _, frame := range c.plan() {
			if err := lim.wait(ctx); err != nil {
				return
			}
			if err := conn.Write(ctx, frame); err != nil {
				c.log.Debug().Err(err).Msg("write")
				return
			}
		}
	}
}

func (c *Conn) poke() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

func (c *Conn) drain() {
	for {
		select {
		case <-c.kick:
		default:
			return
		}
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
