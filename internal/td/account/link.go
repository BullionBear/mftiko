package account

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

type role uint8

const (
	roleEvents role = iota // carries the private events; decides the account state
	roleOrders             // a trade stream that carries no events
)

// linkEvent is a connection coming up or going down. err is set when the link gave up: the venue
// refused the credentials. orders is set when this link carries order requests. swap is a planned
// replacement that finished with the new connection up (E06-D035).
type linkEvent struct {
	role   role
	orders bool
	up     bool
	err    error
	swap   bool
}

// codec is a PrivateStream or a TradeStream, as the link drives them.
type codec struct {
	prepare      func(now time.Time) (*exchange.Call[string], error)
	dial         func(now time.Time, token string) (*exchange.Dial, error)
	refresh      func(now time.Time) (*exchange.Call[struct{}], error)
	refreshEvery time.Duration
	opts         ws.Options
	decode       func(frame []byte, s *sink) error
	trade        exchange.TradeStream
	mu           sync.Mutex // Submit, Cancel and Decode share the codec
}

func privateCodec(s exchange.PrivateStream) *codec {
	l := s.Limits()
	return &codec{
		prepare: s.Prepare, dial: s.Dial, refresh: s.Refresh, refreshEvery: l.RefreshEvery,
		opts:   ws.Options{ReadLimit: l.ReadLimit, Keepalive: l.Keepalive, MaxLifetime: l.MaxLifetime, Ping: s.Ping()},
		decode: func(frame []byte, k *sink) error { return s.Decode(frame, k) },
	}
}

func tradeCodec(t exchange.TradeStream) *codec {
	l := t.Limits()
	cd := &codec{
		prepare: func(time.Time) (*exchange.Call[string], error) { return nil, nil },
		dial:    func(now time.Time, _ string) (*exchange.Dial, error) { return t.Dial(now) },
		refresh: func(time.Time) (*exchange.Call[struct{}], error) { return nil, nil },
		opts:    ws.Options{ReadLimit: l.ReadLimit, Keepalive: l.Keepalive, MaxLifetime: l.MaxLifetime, Ping: t.Ping()},
		trade:   t,
	}
	cd.decode = func(frame []byte, k *sink) error {
		cd.mu.Lock()
		defer cd.mu.Unlock()
		return t.Decode(frame, k)
	}
	return cd
}

// link keeps one role's connection up: it dials, waits for the codec's Ready, keeps the stream's
// credentials alive, replaces the connection ahead of its MaxLifetime, and redials after a drop
// with a growing wait. It gives up only when the venue refuses the credentials.
type link struct {
	w      *Worker
	role   role
	orders bool
	name   string
	open   func() (*codec, error)

	// cur is the connection serve is writing orders on. The link goroutine owns it.
	cur *conn
}

// conn is one websocket of a link.
type conn struct {
	ws      *ws.Conn
	codec   *codec
	sink    *sink
	born    time.Time
	done    chan struct{}
	err     error // why Run returned; read after done
	ready   chan struct{}
	heard   chan struct{} // a frame from the venue decoded; connect paces the dial frames on it
	expired chan string

	flight *safeFlight
	send   chan *wsJob // nil when this link does not take orders
	mu     sync.Mutex
	closed bool
}

func (l *link) run(ctx context.Context) {
	w := l.w
	backoff := w.retryAfter
	for {
		c, err := l.connect(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if errors.Is(err, exchange.ErrAuth) {
				w.log.Error().Str("conn", l.name).Msg("credentials refused")
				w.post(ctx, linkEvent{role: l.role, orders: l.orders, err: err})
				return
			}
			w.log.Warn().Str("conn", l.name).Err(err).Dur("retry_in", backoff).Msg("dial")
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(2*backoff, w.retryMax)
			continue
		}
		backoff = w.retryAfter
		w.log.Info().Str("conn", l.name).Msg("connected")
		l.attach(c)
		w.post(ctx, linkEvent{role: l.role, orders: l.orders, up: true})
		err = l.serve(ctx, c)
		l.finishConn(c)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, exchange.ErrAuth) {
			w.log.Error().Str("conn", l.name).Msg("credentials refused")
			w.post(ctx, linkEvent{role: l.role, orders: l.orders, err: err})
			return
		}
		w.log.Warn().Str("conn", l.name).Err(err).Msg("connection lost")
		w.post(ctx, linkEvent{role: l.role, orders: l.orders, up: false})
	}
}

// connect opens one connection and waits for the codec's Ready, or, for a Live dial, only for the
// handshake. The connection lives until ctx ends or it is closed.
func (l *link) connect(ctx context.Context) (*conn, error) {
	w := l.w
	cd, err := l.open()
	if err != nil {
		return nil, err
	}
	dctx, cancel := context.WithTimeout(ctx, w.readyTimeout)
	defer cancel()
	var token string
	if call, err := cd.prepare(w.now()); err != nil {
		return nil, err
	} else if call != nil {
		if token, err = exchange.Run(dctx, w.cfg.Doer, *call); err != nil {
			return nil, fmt.Errorf("prepare: %w", err)
		}
	}
	d, err := cd.dial(w.now(), token)
	if err != nil {
		return nil, err
	}
	o := cd.opts
	o.Net = w.cfg.Net
	wc, err := ws.Dial(dctx, d.URL, o)
	if err != nil {
		return nil, err
	}
	c := &conn{ws: wc, codec: cd, born: time.Now(), done: make(chan struct{}), ready: make(chan struct{}), heard: make(chan struct{}, 1), expired: make(chan string, 1), flight: &safeFlight{}}
	if l.orders {
		c.send = make(chan *wsJob, 32)
	}
	c.sink = &sink{ctx: ctx, w: w, c: c}
	go func() {
		c.err = wc.Run(ctx, func(frame []byte) error {
			if err := cd.decode(frame, c.sink); err != nil {
				return err
			}
			select {
			case c.heard <- struct{}{}:
			default:
			}
			return nil
		})
		close(c.done)
	}()
	// Each frame after the first goes out once the venue has answered the one before: Binance spot
	// leaves the session unauthenticated when userDataStream.subscribe is written behind
	// session.logon, before its answer. Nothing comes from these venues before the logon is answered,
	// so the next frame heard is that answer.
	for i, f := range d.Frames {
		if i > 0 {
			select {
			case <-c.heard:
			case <-c.done:
				return nil, c.err
			case <-dctx.Done():
				wc.Close()
				<-c.done
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("dial frame %d not answered within %s", i, w.readyTimeout)
			}
		}
		if err := wc.Write(dctx, f); err != nil {
			wc.Close()
			<-c.done
			return nil, fmt.Errorf("logon: %w", err)
		}
	}
	if d.Live {
		c.sink.Ready()
	}
	select {
	case <-c.ready:
		return c, nil
	case <-c.done:
		return nil, c.err
	case <-dctx.Done():
		wc.Close()
		<-c.done
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("not ready within %s", w.readyTimeout)
	}
}

// serve keeps c until it ends, and returns why. It refreshes the stream's credentials, closes the
// connection when the venue says they expired, and replaces it with a new one ahead of its
// MaxLifetime: the new connection is up before the old one closes, so the role never goes down.
// An events connection then asks for one recon, and does not report DEGRADED (E06-D035, E06-D036).
func (l *link) serve(ctx context.Context, c *conn) error {
	w := l.w
	var refresh *time.Ticker
	var refreshC <-chan time.Time
	resetRefresh := func() {
		if refresh != nil {
			refresh.Stop()
			refresh, refreshC = nil, nil
		}
		if d := c.codec.refreshEvery; d > 0 {
			refresh = time.NewTicker(d)
			refreshC = refresh.C
		}
	}
	var rotate *time.Timer
	var rotateC <-chan time.Time
	resetRotate := func() {
		if rotate != nil {
			rotate.Stop()
			rotate, rotateC = nil, nil
		}
		if d := c.codec.opts.MaxLifetime; d > 0 {
			// The transport closes the connection MaxLifetimeMargin before the venue would; the
			// replacement starts one margin earlier still.
			rotate = time.NewTimer(time.Until(c.born.Add(d - 2*ws.MaxLifetimeMargin(d))))
			rotateC = rotate.C
		}
	}
	var expire *time.Timer
	var expireC <-chan time.Time
	stopExpire := func() {
		if expire != nil {
			expire.Stop()
			expire, expireC = nil, nil
		}
	}
	armExpire := func() {
		stopExpire()
		next := c.flight.next()
		if next.IsZero() {
			return
		}
		d := time.Until(next)
		if d < 0 {
			d = 0
		}
		expire = time.NewTimer(d)
		expireC = expire.C
	}
	resetRefresh()
	resetRotate()
	defer func() {
		if refresh != nil {
			refresh.Stop()
		}
		if rotate != nil {
			rotate.Stop()
		}
		stopExpire()
	}()
	for {
		var jobs <-chan *wsJob
		if c.send != nil {
			jobs = c.send
		}
		select {
		case job := <-jobs:
			l.writeJob(ctx, c, job)
			armExpire()
		case <-expireC:
			for _, u := range c.flight.expire(w.now()) {
				l.deliver(u)
			}
			armExpire()
		case <-c.done:
			return c.err
		case reason := <-c.expired:
			c.ws.Close()
			<-c.done
			return fmt.Errorf("stream expired: %s", reason)
		case <-refreshC:
			call, err := c.codec.refresh(w.now())
			if err == nil && call != nil {
				_, err = exchange.Run(ctx, w.cfg.Doer, *call)
			}
			if errors.Is(err, exchange.ErrAuth) {
				c.ws.Close()
				<-c.done
				return err
			}
			if err != nil {
				// The stream stays up; if the venue does let it expire, it says so (Expired).
				w.log.Warn().Str("conn", l.name).Err(err).Msg("refresh")
			}
		case <-rotateC:
			rotateC = nil
			next, err := l.connect(ctx)
			switch {
			case err == nil:
				old := c
				c = next
				l.attach(c)
				l.lost(old)
				old.stopSend()
				old.ws.Close()
				<-old.done
				resetRefresh()
				resetRotate()
				armExpire()
				w.log.Info().Str("conn", l.name).Msg("replaced ahead of its lifetime")
				// The dial above does not watch the old connection. Whatever it did in that window,
				// an events swap runs one recon; an orders swap changes nothing (E06-D035, E06-D036).
				if l.role == roleEvents {
					w.post(ctx, linkEvent{role: l.role, swap: true})
				}
			case errors.Is(err, exchange.ErrAuth):
				c.ws.Close()
				<-c.done
				return err
			default:
				// The old connection closes at its lifetime and the link redials as after a drop.
				w.log.Warn().Str("conn", l.name).Err(err).Msg("replacement")
			}
		}
	}
}

// sink is what a connection's codec reports to. It is a TradeSink, so it serves both codecs; the
// answers to orders are E6-08's.
type sink struct {
	ctx       context.Context
	w         *Worker
	c         *conn
	readyOnce sync.Once
}

var _ exchange.TradeSink = (*sink)(nil)

func (s *sink) Ready() { s.readyOnce.Do(func() { close(s.c.ready) }) }

func (s *sink) Expired(reason string) {
	select {
	case s.c.expired <- reason:
	default:
	}
}

// The events go to the book, which copies them: they are lent (E03-D002).
func (s *sink) Order(e *exchange.OrderUpdate)   { s.w.book.order(s.ctx, e) }
func (s *sink) Fill(e *exchange.Fill)           { s.w.book.fill(s.ctx, e) }
func (s *sink) Balances(e []exchange.Balance)   { s.w.book.balanceEvent(s.ctx, e) }
func (s *sink) Positions(e []exchange.Position) { s.w.book.positionEvent(s.ctx, e) }

func (s *sink) Submitted(id uint64, res *exchange.OrderResult, err error) {
	meta, ok := s.c.flight.take(id)
	if !ok {
		return
	}
	var cp *exchange.OrderResult
	if res != nil {
		c := *res
		cp = &c
	}
	s.w.book.send(s.ctx, msgResult{
		kind: resultSubmit, cloid: meta.cloid, orderID: meta.orderID, res: cp, err: err,
		sent: meta.sent, window: meta.window, deadline: meta.deadline,
	})
}

func (s *sink) Canceled(id uint64, err error) {
	meta, ok := s.c.flight.take(id)
	if !ok {
		return
	}
	s.w.book.send(s.ctx, msgResult{
		kind: resultCancel, cloid: meta.cloid, orderID: meta.orderID, err: err,
		sent: meta.sent, window: meta.window, deadline: meta.deadline,
	})
}

func (s *sink) Used(u []exchange.Used) {
	if len(u) == 0 {
		return
	}
	s.w.book.send(s.ctx, msgUsage{used: append([]exchange.Used(nil), u...)})
}

// attach makes c the connection new orders are written on. The link goroutine calls it.
func (l *link) attach(c *conn) {
	l.cur = c
	if l.orders {
		l.w.setOrderConn(c)
	}
}

// finishConn gives up on whatever serve left current: unanswered writes are an unknown outcome,
// and anything still queued was not written (E06-D024).
func (l *link) finishConn(*conn) {
	if l.cur == nil {
		return
	}
	l.lost(l.cur)
	l.detach()
}

func (l *link) detach() {
	c := l.cur
	l.cur = nil
	if c == nil {
		return
	}
	l.w.clearOrderConn(c)
	c.stopSend()
}

func (l *link) lost(c *conn) {
	if c == nil || c.flight == nil {
		return
	}
	for _, u := range c.flight.lost() {
		l.deliver(u)
	}
}

func (l *link) deliver(u exchange.Unanswered[fmeta]) {
	kind := resultSubmit
	if !u.V.submit {
		kind = resultCancel
	}
	l.w.book.send(l.w.ctx, msgResult{
		kind: kind, cloid: u.V.cloid, orderID: u.V.orderID, err: u.Err,
		sent: u.V.sent, window: u.V.window, deadline: u.V.deadline,
	})
}

func (l *link) writeJob(ctx context.Context, c *conn, job *wsJob) {
	if c.codec == nil || c.codec.trade == nil {
		job.done <- errNotSent
		return
	}
	id := c.flight.add(job.meta, job.deadline)
	c.codec.mu.Lock()
	var frame []byte
	var err error
	if job.cancel {
		frame, err = c.codec.trade.Cancel(job.now, id, job.ref)
	} else {
		frame, err = c.codec.trade.Submit(job.now, id, job.req)
	}
	c.codec.mu.Unlock()
	if err != nil {
		c.flight.drop(id)
		job.done <- errNotSent
		return
	}
	if err := c.ws.Write(ctx, frame); err != nil {
		c.flight.drop(id)
		job.done <- fmt.Errorf("write order: %w", exchange.ErrUnknownOutcome)
		c.ws.Close()
		return
	}
	job.done <- nil
}

func (c *conn) push(job *wsJob) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.send == nil {
		return errNotSent
	}
	select {
	case c.send <- job:
		return nil
	default:
		return errNotSent
	}
}

func (c *conn) stopSend() {
	c.mu.Lock()
	if c.closed || c.send == nil {
		c.mu.Unlock()
		return
	}
	c.closed = true
	ch := c.send
	c.mu.Unlock()
	for {
		select {
		case job := <-ch:
			job.done <- errNotSent
		default:
			return
		}
	}
}

// wsJob is one order waiting to be written on the trade stream.
type wsJob struct {
	now      time.Time
	deadline time.Time
	cancel   bool
	req      *exchange.OrderRequest
	ref      exchange.CancelRef
	meta     fmeta
	done     chan error
}

// fmeta is what an answer, a timeout, or a dropped connection needs to update the order.
type fmeta struct {
	cloid    string
	orderID  string
	submit   bool
	sent     time.Time
	window   time.Duration
	deadline time.Time
}

// safeFlight is exchange.Inflight plus the requests given up at their deadline. A late frame still
// matches those (E03-D003); a dropped connection forgets them.
type safeFlight struct {
	mu   sync.Mutex
	in   exchange.Inflight[fmeta]
	late map[uint64]fmeta
}

func (f *safeFlight) add(v fmeta, deadline time.Time) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.in.Add(v, deadline)
}

func (f *safeFlight) take(id uint64) (fmeta, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.in.Answer(id); ok {
		return v, true
	}
	v, ok := f.late[id]
	if ok {
		delete(f.late, id)
	}
	return v, ok
}

func (f *safeFlight) drop(id uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.in.Answer(id)
	delete(f.late, id)
}

func (f *safeFlight) expire(now time.Time) []exchange.Unanswered[fmeta] {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.in.Expire(now)
	if len(out) == 0 {
		return nil
	}
	if f.late == nil {
		f.late = map[uint64]fmeta{}
	}
	for _, u := range out {
		f.late[u.ID] = u.V
	}
	return out
}

func (f *safeFlight) lost() []exchange.Unanswered[fmeta] {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.in.Lost()
	f.late = nil
	return out
}

func (f *safeFlight) next() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.in.Next()
}

// tryWS writes the order on the trade stream. errNotSent means it was not written and REST may
// send it (E06-D024). Any other error means it may have been written.
func (w *Worker) tryWS(ctx context.Context, job *sendJob, now, deadline time.Time) error {
	if job.cancel {
		var degraded bool
		if !w.book.call(ctx, func() {
			degraded = w.book.state == tdv1.AccountState_ACCOUNT_STATE_DEGRADED
		}) {
			return errNotSent
		}
		if degraded {
			return errNotSent
		}
	}
	w.mu.Lock()
	c := w.orderConn
	up := w.ordersUp
	w.mu.Unlock()
	if c == nil || !up {
		return errNotSent
	}
	wj := &wsJob{
		now: now, deadline: deadline, cancel: job.cancel, req: job.req, ref: job.ref,
		done: make(chan error, 1),
		meta: fmeta{cloid: job.cloid, submit: !job.cancel, sent: now, window: job.window, deadline: deadline},
	}
	if job.cancel {
		wj.meta.orderID = job.ref.OrderID
	}
	if err := c.push(wj); err != nil {
		return err
	}
	select {
	case err := <-wj.done:
		return err
	case <-ctx.Done():
		return exchange.ErrUnknownOutcome
	}
}

func (w *Worker) setOrderConn(c *conn) {
	w.mu.Lock()
	w.orderConn = c
	w.mu.Unlock()
}

func (w *Worker) clearOrderConn(c *conn) {
	w.mu.Lock()
	if w.orderConn == c {
		w.orderConn = nil
	}
	w.mu.Unlock()
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
