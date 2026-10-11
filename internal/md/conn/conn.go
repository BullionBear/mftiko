package conn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

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
	// Epoch is the last conn_epoch already used on md.f. The next dial uses Epoch+1. Zero
	// means none has been used, so the first connection is 1. A failed dial does not consume
	// a number. Report, when set, is how the number reaches the shim before the dial.
	Epoch uint64
	// Report is called with the conn_epoch the next dial will use, and Dial waits until it
	// returns. A peer that reads the report before accepting the socket therefore has the
	// number before the connection is up, and md.f for that connection is published only
	// after. A dial that fails leaves Epoch unchanged, so the next attempt reports the same
	// number. Nil reports nothing. Report must not call back into the Conn.
	Report func(epoch uint64) error
	// RetryAfter is the wait after a failed dial. The next failure waits twice as long, up to
	// RetryMax. Zero means 1s and a cap of 30s. Negative means do not wait, which tests use so a
	// reconnect is immediate. A dial that follows a connection which came up does not wait,
	// whatever the previous failure had grown to.
	RetryAfter time.Duration
	RetryMax   time.Duration
	// REST and Doer run SnapshotREST depth in this process. Nil is left alone until a book asks
	// for a snapshot. The call does not go through md.fetch (D402).
	REST exchange.PublicREST
	Doer exchange.Doer
	// BookDepth is the orderbook top-N (D426). Zero means missing. Serve rejects that at init. A
	// book channel adopted with zero, or with N greater than BookSync.Depth on SnapshotREST,
	// ends Run.
	BookDepth uint32
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
	// TapeWindow, TapeEvents and TapeBytes bound the tape ring (D438). Zero means
	// DefaultTapeWindow, DefaultTapeEvents and DefaultTapeBytes. InitPayload uses the same rule.
	TapeWindow time.Duration
	TapeEvents int
	TapeBytes  int64
	// ViewListen subscribes one md.view subject. The handler runs off the websocket read, takes
	// the request bytes, and returns the reply. unlisten removes the interest. Nil leaves
	// md.view unserved; tests then call handleView directly. It must not be called with the
	// worker lock held.
	ViewListen func(subject string, handler func(req []byte) []byte) (unlisten func(), err error)
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
	bookDepth          uint32
	bufferMax          int
	bufferAge          time.Duration
	rateHold           time.Duration
	bookNow            func() time.Time
	bookAfter          func(time.Duration) <-chan time.Time

	mu sync.Mutex
	// epoch is the connection that is up, or the last one that was, or Config.Epoch before the
	// first successful dial.
	epoch  uint64
	report func(uint64) error
	have   bool // a Desired has been applied; the next one must be strictly newer
	ver    commonv1.Version
	// wanted is the current desired set for this venue and route, keyed by channel.
	wanted map[string][]feed
	// exch is the assignment's exch ticker for that channel, copied at spec load (D426).
	exch map[string]string
	// tick is the assignment's price tick for that channel, copied at spec load (D433).
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
	// fatal is a book the worker will not run (missing N, or N above Depth). stop cancels Run.
	fatal error
	stop  context.CancelFunc

	// Tape (D435–D446). owners is the feed key topic.ticker. rings is per atom. marks is the
	// latest md.f cursor of a feed and outlives a trim. bound is the md.view unlisten for a
	// subject; nil means a subscribe is in flight. The byte cap counts stored messages.
	viewListen   func(string, func([]byte) []byte) (func(), error)
	owners       map[string]feedOwner
	rings        map[string]*tapeRing
	marks        map[string]pubMark
	bound        map[string]func()
	viewsClosed  bool
	tapeWindow   time.Duration
	tapeMax      int
	tapeMaxBytes int64
	tapeBytes    int64
	tapeOrd      uint64
	// seenUp is set after the first successful dial. downNs and downFeeds are the disconnect
	// that the next successful dial turns into gaps. A failed dial leaves them alone.
	seenUp    bool
	downNs    int64
	downFeeds []string
}

// feed is one assigned publication: the subject is md.f.{instance}.{topic}.{ticker}.
type feed struct {
	topic, ticker string
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
	window := cfg.TapeWindow
	if window <= 0 {
		window = DefaultTapeWindow
	}
	events := cfg.TapeEvents
	if events <= 0 {
		events = DefaultTapeEvents
	}
	capBytes := cfg.TapeBytes
	if capBytes <= 0 {
		capBytes = DefaultTapeBytes
	}
	return &Conn{
		venue: cfg.Venue, route: cfg.Route, inst: cfg.Instance,
		url: cfg.URL, market: cfg.Market, pub: cfg.Publish, now: now,
		epoch: cfg.Epoch, report: cfg.Report, retryAfter: cfg.RetryAfter, retryMax: cfg.RetryMax,
		rest: cfg.REST, doer: cfg.Doer, bookDepth: cfg.BookDepth,
		bufferMax: bufN, bufferAge: bufAge, rateHold: hold, bookNow: bookNow, bookAfter: cfg.BookAfter,
		log: log, kick: make(chan struct{}, 1), bookWake: make(chan struct{}, 1),
		viewListen: cfg.ViewListen,
		owners:     map[string]feedOwner{},
		rings:      map[string]*tapeRing{},
		marks:      map[string]pubMark{},
		bound:      map[string]func(){},
		tapeWindow: window, tapeMax: events, tapeMaxBytes: capBytes,
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
	prev := c.epoch
	start := c.downNs
	feeds := append([]string(nil), c.downFeeds...)
	c.downFeeds = nil
	upNs := c.now().UnixNano()
	wasUp := c.seenUp
	c.epoch = next
	epoch := c.epoch
	c.live = newLink(session)
	if wasUp {
		c.openGaps(feeds, prev, start, upNs)
	}
	c.seenUp = true
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
	c.markDown()
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
	c.mu.Lock()
	next := c.epoch + 1
	report := c.report
	c.mu.Unlock()
	if report == nil {
		return next, nil
	}
	if err := report(next); err != nil {
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
