package ws

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/BullionBear/mftiko/exchange"
)

// serve runs fn on the server side of each websocket accepted, with opts.
func serve(t *testing.T, opts *websocket.AcceptOptions, fn func(ctx context.Context, c *websocket.Conn)) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, opts)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		fn(r.Context(), c)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func dial(t *testing.T, url string, o Options) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, url, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// closeCode reads until the client closes and reports the close status it sent.
func closeCode(ctx context.Context, c *websocket.Conn) websocket.StatusCode {
	for {
		if _, _, err := c.Read(ctx); err != nil {
			return websocket.CloseStatus(err)
		}
	}
}

// stream writes msg every few milliseconds until the client closes, and reports the close status it
// sent. It reads all along, as a venue does, so the client's close handshake completes.
func stream(ctx context.Context, c *websocket.Conn, msg []byte) websocket.StatusCode {
	got := make(chan websocket.StatusCode, 1)
	go func() { got <- closeCode(ctx, c) }()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case s := <-got:
			return s
		case <-tick.C:
			if c.Write(ctx, websocket.MessageText, msg) != nil {
				return <-got
			}
		}
	}
}

func within(t *testing.T, what string, el, lo, hi time.Duration) {
	t.Helper()
	if el < lo || el > hi {
		t.Errorf("%s after %v, want between %v and %v", what, el, lo, hi)
	}
}

// Messages arrive whole and in order, text and binary, one larger than the first buffer; Write
// reaches the server; a canceled context ends Run with a normal closure.
func TestMessages(t *testing.T) {
	big := bytes.Repeat([]byte("0123456789"), 20_000) // 200 KB, past initialBuffer
	sent := [][]byte{[]byte(`{"e":"trade"}`), big, {0, 1, 2}, []byte("last")}
	code := make(chan websocket.StatusCode, 1)
	got := make(chan string, 1)
	url := serve(t, nil, func(ctx context.Context, c *websocket.Conn) {
		for i, m := range sent {
			typ := websocket.MessageText
			if i == 2 {
				typ = websocket.MessageBinary
			}
			if err := c.Write(ctx, typ, m); err != nil {
				t.Error(err)
				return
			}
		}
		_, sub, err := c.Read(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		got <- string(sub)
		code <- closeCode(ctx, c)
	})
	c := dial(t, url, Options{ReadLimit: 1 << 20})
	ctx, cancel := context.WithCancel(context.Background())
	var recv [][]byte
	err := c.Run(ctx, func(frame []byte) error {
		recv = append(recv, bytes.Clone(frame))
		if len(recv) == len(sent) {
			if err := c.Write(ctx, []byte(`{"op":"subscribe"}`)); err != nil {
				t.Error(err)
			}
			go func() { <-got; cancel() }()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if len(recv) != len(sent) {
		t.Fatalf("got %d messages, want %d", len(recv), len(sent))
	}
	for i := range sent {
		if !bytes.Equal(recv[i], sent[i]) {
			t.Errorf("message %d: %d bytes, want %d", i, len(recv[i]), len(sent[i]))
		}
	}
	if s := <-code; s != websocket.StatusNormalClosure {
		t.Errorf("server saw close %v", s)
	}
}

// ReadLimit is the venue's: a message up to it passes, one past it ends the connection.
func TestReadLimit(t *testing.T) {
	url := serve(t, nil, func(ctx context.Context, c *websocket.Conn) {
		c.Write(ctx, websocket.MessageText, bytes.Repeat([]byte("a"), 1000))
		c.Write(ctx, websocket.MessageText, bytes.Repeat([]byte("b"), 1001))
		closeCode(ctx, c)
	})
	c := dial(t, url, Options{ReadLimit: 1000})
	n := 0
	err := c.Run(context.Background(), func(frame []byte) error { n++; return nil })
	if n != 1 || !errors.Is(err, websocket.ErrMessageTooBig) {
		t.Fatalf("%d messages, Run = %v; want 1 and ErrMessageTooBig", n, err)
	}

	if _, err := Dial(context.Background(), url, Options{}); err == nil {
		t.Fatal("Dial without a ReadLimit")
	}
}

// A failed dial or a refused handshake shows neither the handshake's headers nor the URL's path and
// query, which can carry a listenKey.
func TestDialHidesSecrets(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusUnauthorized)
	}))
	defer srv.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := "http://" + ln.Addr().String()
	ln.Close()

	for _, base := range []string{srv.URL, closed} {
		url := "ws" + strings.TrimPrefix(base, "http") + "/ws/lk-secret?token=t0k-secret"
		_, err := Dial(context.Background(), url, Options{ReadLimit: 1 << 10, Header: http.Header{"X-Bapi-Api-Key": {"k-visible"}}})
		if err == nil {
			t.Fatalf("%s: Dial succeeded", base)
		}
		for _, secret := range []string{"k-visible", "lk-secret", "t0k-secret"} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error shows %q: %v", secret, err)
			}
		}
		if base == closed && !strings.Contains(err.Error(), strings.TrimPrefix(closed, "http://")) {
			t.Errorf("error lost the host: %v", err)
		}
	}
}

// The venue's pings are answered while Run waits for messages, and they count as traffic: a quiet
// stream that the venue pings is not idle.
func TestServerPing(t *testing.T) {
	pongs := make(chan error, 1)
	url := serve(t, nil, func(ctx context.Context, c *websocket.Conn) {
		ctx = c.CloseRead(ctx)
		for range 6 {
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := c.Ping(pctx)
			cancel()
			if err != nil {
				pongs <- err
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		pongs <- nil
		<-ctx.Done()
	})
	c := dial(t, url, Options{ReadLimit: 1024, Keepalive: exchange.Keepalive{ServerPing: true, Timeout: 200 * time.Millisecond}})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := <-pongs; err != nil {
			t.Errorf("server ping: %v", err)
		}
		cancel()
	}()
	if err := c.Run(ctx, func([]byte) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled: the pings kept it alive", err)
	}
}

// Nothing for Keepalive.Timeout: the connection is dead, and Run says so.
func TestIdle(t *testing.T) {
	gone := make(chan struct{})
	url := serve(t, nil, func(ctx context.Context, c *websocket.Conn) {
		closeCode(ctx, c)
		close(gone)
	})
	const timeout = 200 * time.Millisecond
	c := dial(t, url, Options{ReadLimit: 1024, Keepalive: exchange.Keepalive{Timeout: timeout}})
	start := time.Now()
	err := c.Run(context.Background(), func([]byte) error { return nil })
	if !errors.Is(err, ErrIdle) {
		t.Fatalf("Run = %v, want ErrIdle", err)
	}
	within(t, "ErrIdle", time.Since(start), timeout, timeout+2*time.Second)
	select {
	case <-gone:
	case <-time.After(5 * time.Second):
		t.Fatal("the server still has the connection")
	}
}

// Every Keepalive.Interval the codec's Ping goes out as a text message, or a websocket ping when it
// has none.
func TestClientPing(t *testing.T) {
	const interval = 40 * time.Millisecond
	for _, ping := range [][]byte{[]byte(`{"op":"ping"}`), nil} {
		var pings atomic.Int32
		enough := make(chan struct{})
		opts := &websocket.AcceptOptions{OnPingReceived: func(context.Context, []byte) bool {
			if pings.Add(1) == 3 {
				close(enough)
			}
			return true
		}}
		url := serve(t, opts, func(ctx context.Context, c *websocket.Conn) {
			for {
				_, m, err := c.Read(ctx)
				if err != nil {
					return
				}
				if !bytes.Equal(m, ping) {
					t.Errorf("server got %q, want the ping %q", m, ping)
				}
				if pings.Add(1) == 3 {
					close(enough)
				}
			}
		})
		c := dial(t, url, Options{ReadLimit: 1024, Ping: ping, Keepalive: exchange.Keepalive{Interval: interval}})
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-enough:
			case <-time.After(5 * time.Second):
				t.Errorf("ping %q: %d pings in 5s", ping, pings.Load())
			}
			cancel()
		}()
		if err := c.Run(ctx, func([]byte) error { return nil }); !errors.Is(err, context.Canceled) {
			t.Fatalf("ping %q: Run = %v", ping, err)
		}
	}
}

// Ahead of MaxLifetime the connection closes normally with ErrLifetime, for the worker to reconnect.
func TestMaxLifetime(t *testing.T) {
	code := make(chan websocket.StatusCode, 1)
	url := serve(t, nil, func(ctx context.Context, c *websocket.Conn) {
		code <- stream(ctx, c, []byte("tick"))
	})
	const life = 500 * time.Millisecond
	c := dial(t, url, Options{ReadLimit: 1024, MaxLifetime: life})
	err := c.Run(context.Background(), func([]byte) error { return nil })
	if !errors.Is(err, ErrLifetime) {
		t.Fatalf("Run = %v, want ErrLifetime", err)
	}
	// Closed from life less the margin, with a close handshake to finish.
	within(t, "ErrLifetime", time.Since(c.born), life-MaxLifetimeMargin(life), life+time.Second)
	if s := <-code; s != websocket.StatusNormalClosure {
		t.Errorf("server saw close %v", s)
	}
	if m := MaxLifetimeMargin(24 * time.Hour); m != time.Minute {
		t.Errorf("margin of 24h is %v", m)
	}
}

// An error from the handler ends the connection, and Run returns it; so does Close, from anywhere.
func TestEnds(t *testing.T) {
	codes := make(chan websocket.StatusCode, 2)
	url := serve(t, nil, func(ctx context.Context, c *websocket.Conn) {
		codes <- stream(ctx, c, []byte("x"))
	})
	bad := errors.New("bad frame")
	c := dial(t, url, Options{ReadLimit: 1024})
	if err := c.Run(context.Background(), func([]byte) error { return bad }); !errors.Is(err, bad) {
		t.Fatalf("Run = %v, want the handler's error", err)
	}
	if err := c.Run(context.Background(), func([]byte) error { return nil }); err == nil {
		t.Fatal("second Run")
	}

	c = dial(t, url, Options{ReadLimit: 1024})
	go func() { time.Sleep(50 * time.Millisecond); c.Close() }()
	if err := c.Run(context.Background(), func([]byte) error { return nil }); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Run = %v, want net.ErrClosed", err)
	}
	for range 2 {
		if s := <-codes; s != websocket.StatusNormalClosure {
			t.Errorf("server saw close %v", s)
		}
	}
}
