// Package ws is the websocket transport: one connection of an MD conn worker or a TD account worker,
// over github.com/coder/websocket (DECISIONS D302).
//
// Run reads every message into the connection's own buffer and lends it to the handler, which is
// where Session.Decode or PrivateStream.Decode runs: no allocation per message once the buffer has
// grown to the largest message. The read never pauses while the connection is up, because
// coder/websocket answers the venue's pings, and sees the pongs to its own, only inside a read.
//
// Next to the read, Run keeps the connection's Keepalive: it sends the codec's Ping every Interval
// (a websocket ping when the codec has none) and ends the connection with ErrIdle once nothing at all
// (no message, ping or pong) has come for Timeout. It ends a connection with ErrLifetime before the
// venue's MaxLifetime would drop it. Either way the worker reconnects and bumps its conn_epoch, as on
// any reconnect; this package never reconnects by itself.
package ws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport"
)

var (
	// ErrIdle: nothing came for Keepalive.Timeout, so the connection is taken as dead.
	ErrIdle = errors.New("ws: nothing received within the keepalive timeout")
	// ErrLifetime: the connection was closed ahead of the venue's MaxLifetime.
	ErrLifetime = errors.New("ws: connection lifetime reached")
)

// MaxLifetimeMargin is how long before MaxLifetime a connection is closed: a minute, or a tenth of
// MaxLifetime when that is shorter.
func MaxLifetimeMargin(maxLifetime time.Duration) time.Duration {
	return min(maxLifetime/10, time.Minute)
}

// initialBuffer is the read buffer's first capacity, below ReadLimit; it grows to fit the largest
// message.
const initialBuffer = 64 << 10

type Options struct {
	// Net dials the handshake, with the proxy, TLS and DNS the REST transport uses; nil is
	// transport.Direct.
	Net *transport.Net
	// Header goes with the handshake request.
	Header http.Header
	// ReadLimit is the largest message the venue sends, in bytes: a longer one ends the connection.
	// Required, because coder/websocket's default of 32 KiB is too small for some venues' messages.
	ReadLimit int64
	// Keepalive and MaxLifetime are the codec's: SessionLimits or StreamLimits.
	Keepalive   exchange.Keepalive
	MaxLifetime time.Duration
	// Ping is the codec's Ping(), sent as a text message every Keepalive.Interval. nil sends a
	// websocket ping instead.
	Ping []byte
}

// Conn is one websocket connection. Write and Close are safe to call from any goroutine; Run is
// called once.
type Conn struct {
	ws      *websocket.Conn
	o       Options
	born    time.Time
	buf     []byte
	seen    atomic.Uint64 // messages, pings and pongs received
	running atomic.Bool
	pinging atomic.Bool
	reason  atomic.Pointer[error] // why the connection was ended from this side; the first one wins
	wg      sync.WaitGroup
}

// Dial opens a websocket to url through o.Net. Call Run right away: until a read is in progress
// the venue's pings go unanswered.
func Dial(ctx context.Context, rawURL string, o Options) (*Conn, error) {
	if o.ReadLimit <= 0 {
		return nil, errors.New("ws: Options.ReadLimit is not set")
	}
	n := o.Net
	if n == nil {
		n = transport.Direct()
	}
	c := &Conn{o: o}
	ws, _, err := websocket.Dial(ctx, rawURL, &websocket.DialOptions{
		HTTPClient:     n.HTTPClient(),
		HTTPHeader:     o.Header,
		OnPingReceived: func(context.Context, []byte) bool { c.seen.Add(1); return true },
		OnPongReceived: func(context.Context, []byte) { c.seen.Add(1) },
	})
	if err != nil {
		return nil, fmt.Errorf("ws: dial: %w", redact(err))
	}
	ws.SetReadLimit(o.ReadLimit)
	c.ws = ws
	c.born = time.Now()
	c.buf = make([]byte, 0, min(o.ReadLimit, initialBuffer))
	return c, nil
}

// redact keeps the URL in a handshake error to its host: a private stream's path or query can carry
// its token (Binance's listenKey). Every error wrapping the *url.Error has the URL in its message
// already, so the message is rewritten and the *url.Error left out of the chain.
func redact(err error) error {
	var ue *url.Error
	if !errors.As(err, &ue) {
		return err
	}
	host := "the url"
	if u, perr := url.Parse(ue.URL); perr == nil {
		host = u.Scheme + "://" + u.Host
	}
	msg := strings.ReplaceAll(err.Error(), strconv.Quote(ue.URL), strconv.Quote(host))
	return &redacted{msg: strings.ReplaceAll(msg, ue.URL, host), err: ue.Err}
}

type redacted struct {
	msg string
	err error
}

func (e *redacted) Error() string { return e.msg }
func (e *redacted) Unwrap() error { return e.err }

// Run reads the connection until it ends and returns why: ctx's error, ErrIdle, ErrLifetime,
// handle's error, the error of a Close, or the read error when the venue closed or dropped it.
//
// handle gets each text or binary message. frame is lent: it is valid until handle returns, and the
// next message is read into it (D302). handle runs on the read path, so it returns promptly; an
// error from it ends the connection.
func (c *Conn) Run(ctx context.Context, handle func(frame []byte) error) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("ws: Run called twice")
	}
	done := make(chan struct{})
	c.wg.Go(func() { c.watch(ctx, done) })
	err := c.loop(handle)
	close(done)
	c.ws.CloseNow()
	c.wg.Wait()
	if r := c.reason.Load(); r != nil {
		return *r
	}
	return err
}

func (c *Conn) loop(handle func([]byte) error) error {
	for {
		frame, err := c.read()
		if err != nil {
			return fmt.Errorf("ws: read: %w", err)
		}
		c.seen.Add(1)
		if err := handle(frame); err != nil {
			c.end(err, true)
			return err
		}
	}
}

// read reads one message into c.buf.
func (c *Conn) read() ([]byte, error) {
	// A context without Done: for any other, coder/websocket arms a context.AfterFunc per frame,
	// which allocates. watch ends the connection instead.
	_, r, err := c.ws.Reader(context.Background())
	if err != nil {
		return nil, err
	}
	b := c.buf[:0]
	for {
		if len(b) == cap(b) {
			b = slices.Grow(b, cap(b))
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err == io.EOF {
			c.buf = b
			return b, nil
		}
		if err != nil {
			c.buf = b
			return nil, err
		}
	}
}

// watch ends the connection when ctx ends, when nothing comes for Keepalive.Timeout and ahead of
// MaxLifetime, and pings every Keepalive.Interval, until the read loop stops.
func (c *Conn) watch(ctx context.Context, done <-chan struct{}) {
	var lifetime, idle, ping <-chan time.Time
	if d := c.o.MaxLifetime; d > 0 {
		t := time.NewTimer(time.Until(c.born.Add(d - MaxLifetimeMargin(d))))
		defer t.Stop()
		lifetime = t.C
	}
	timeout := c.o.Keepalive.Timeout
	if timeout > 0 {
		// Nothing seen across ticks spanning timeout is dead: noticed between timeout and
		// timeout plus two ticks after the last message.
		t := time.NewTicker(max(timeout/8, time.Millisecond))
		defer t.Stop()
		idle = t.C
	}
	if d := c.o.Keepalive.Interval; d > 0 {
		t := time.NewTicker(d)
		defer t.Stop()
		ping = t.C
	}
	seen, at := c.seen.Load(), time.Now()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			c.end(ctx.Err(), true)
			return
		case <-lifetime:
			c.end(ErrLifetime, true)
			return
		case now := <-idle:
			if n := c.seen.Load(); n != seen {
				seen, at = n, now
			} else if now.Sub(at) >= timeout {
				c.end(ErrIdle, false)
				return
			}
		case <-ping:
			c.ping()
		}
	}
}

// ping sends one keepalive, unless the last is still out. A failed write ends the connection; a
// missing pong is the idle check's to judge.
func (c *Conn) ping() {
	if !c.pinging.CompareAndSwap(false, true) {
		return
	}
	c.wg.Go(func() {
		defer c.pinging.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), c.o.Keepalive.Interval)
		defer cancel()
		if c.o.Ping == nil {
			_ = c.ws.Ping(ctx)
			return
		}
		if err := c.ws.Write(ctx, websocket.MessageText, c.o.Ping); err != nil {
			c.end(fmt.Errorf("ws: ping: %w", err), false)
		}
	})
}

// Write sends frame as one text message: a subscribe request, a login. Safe to call concurrently
// with Run and with itself.
func (c *Conn) Write(ctx context.Context, frame []byte) error {
	return c.ws.Write(ctx, websocket.MessageText, frame)
}

// Close ends the connection with a normal closure; a Run in progress returns net.ErrClosed.
func (c *Conn) Close() error {
	c.end(net.ErrClosed, true)
	return nil
}

// end records why this side ends the connection and closes it: with the close handshake, or at
// once when the peer is taken as dead.
func (c *Conn) end(reason error, handshake bool) {
	if !c.reason.CompareAndSwap(nil, &reason) {
		return
	}
	if handshake {
		c.ws.Close(websocket.StatusNormalClosure, "")
	} else {
		c.ws.CloseNow()
	}
}
