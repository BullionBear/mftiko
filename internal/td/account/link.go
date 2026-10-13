package account

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

type role uint8

const (
	roleEvents role = iota // carries the private events; decides the account state
	roleOrders             // a trade stream that carries no events
)

// linkEvent is a connection coming up or going down. err is set when the link gave up: the venue
// refused the credentials.
type linkEvent struct {
	role role
	up   bool
	err  error
}

// codec is a PrivateStream or a TradeStream, as the link drives them.
type codec struct {
	prepare      func(now time.Time) (*exchange.Call[string], error)
	dial         func(now time.Time, token string) (*exchange.Dial, error)
	refresh      func(now time.Time) (*exchange.Call[struct{}], error)
	refreshEvery time.Duration
	opts         ws.Options
	decode       func(frame []byte, s *sink) error
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
	return &codec{
		prepare: func(time.Time) (*exchange.Call[string], error) { return nil, nil },
		dial:    func(now time.Time, _ string) (*exchange.Dial, error) { return t.Dial(now) },
		refresh: func(time.Time) (*exchange.Call[struct{}], error) { return nil, nil },
		opts:    ws.Options{ReadLimit: l.ReadLimit, Keepalive: l.Keepalive, MaxLifetime: l.MaxLifetime, Ping: t.Ping()},
		decode:  func(frame []byte, k *sink) error { return t.Decode(frame, k) },
	}
}

// link keeps one role's connection up: it dials, waits for the codec's Ready, keeps the stream's
// credentials alive, replaces the connection ahead of its MaxLifetime, and redials after a drop
// with a growing wait. It gives up only when the venue refuses the credentials.
type link struct {
	w    *Worker
	role role
	name string
	open func() (*codec, error)
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
	expired chan string
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
				w.post(ctx, linkEvent{role: l.role, err: err})
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
		w.post(ctx, linkEvent{role: l.role, up: true})
		err = l.serve(ctx, c)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, exchange.ErrAuth) {
			w.log.Error().Str("conn", l.name).Msg("credentials refused")
			w.post(ctx, linkEvent{role: l.role, err: err})
			return
		}
		w.log.Warn().Str("conn", l.name).Err(err).Msg("connection lost")
		w.post(ctx, linkEvent{role: l.role, up: false})
	}
}

// connect opens one connection and waits for the codec's Ready. The connection lives until ctx ends
// or it is closed.
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
	c := &conn{ws: wc, codec: cd, born: time.Now(), done: make(chan struct{}), ready: make(chan struct{}), expired: make(chan string, 1)}
	c.sink = &sink{ctx: ctx, w: w, c: c}
	go func() {
		c.err = wc.Run(ctx, func(frame []byte) error { return cd.decode(frame, c.sink) })
		close(c.done)
	}()
	for _, f := range d.Frames {
		if err := wc.Write(dctx, f); err != nil {
			wc.Close()
			<-c.done
			return nil, fmt.Errorf("logon: %w", err)
		}
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
	resetRefresh()
	resetRotate()
	defer func() {
		if refresh != nil {
			refresh.Stop()
		}
		if rotate != nil {
			rotate.Stop()
		}
	}()
	for {
		select {
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
				old.ws.Close()
				<-old.done
				resetRefresh()
				resetRotate()
				w.log.Info().Str("conn", l.name).Msg("replaced ahead of its lifetime")
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

// The events go to the book, which copies them: they are lent (D302).
func (s *sink) Order(e *exchange.OrderUpdate)   { s.w.book.order(s.ctx, e) }
func (s *sink) Fill(e *exchange.Fill)           { s.w.book.fill(s.ctx, e) }
func (s *sink) Balances(e []exchange.Balance)   { s.w.book.balanceEvent(s.ctx, e) }
func (s *sink) Positions(e []exchange.Position) { s.w.book.positionEvent(s.ctx, e) }

func (s *sink) Submitted(uint64, *exchange.OrderResult, error) {}
func (s *sink) Canceled(uint64, error)                         {}
func (s *sink) Used([]exchange.Used)                           {}

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
