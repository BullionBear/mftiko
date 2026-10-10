package transport_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

// connectProxy is an HTTP proxy that tunnels CONNECT for user:pass and counts the tunnels.
func connectProxy(t *testing.T) (addr string, tunnels *atomic.Int32) {
	tunnels = new(atomic.Int32)
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT only", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Proxy-Authorization") != want {
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		up, err := net.Dial("tcp", r.Host)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		down, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			up.Close()
			return
		}
		tunnels.Add(1)
		io.WriteString(down, "HTTP/1.1 200 Connection established\r\n\r\n")
		go func() { io.Copy(up, brw); up.Close() }()
		io.Copy(down, up)
		down.Close()
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://"), tunnels
}

// venue is one TLS server with a REST path and a websocket path.
func venue(t *testing.T) (*httptest.Server, *x509.CertPool) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			io.WriteString(w, "rest ok")
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx := c.CloseRead(r.Context())
		c.Write(ctx, websocket.MessageText, []byte("ws ok"))
		<-ctx.Done()
	}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the handshakes TestSharedFailures refuses
	srv.StartTLS()
	t.Cleanup(srv.Close)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	return srv, roots
}

func ctx5(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// rest is one GET of base over n.
func rest(t *testing.T, n *transport.Net, base string) (string, error) {
	resp, err := fasthttp.New(n, fasthttp.Options{}).Do(ctx5(t), &exchange.HTTPRequest{Method: "GET", Base: base, Path: "/"})
	if err != nil {
		return "", err
	}
	return string(resp.Body), nil
}

// stream dials url over n and returns its first message.
func stream(t *testing.T, n *transport.Net, url string) (string, error) {
	c, err := ws.Dial(ctx5(t), url, ws.Options{Net: n, ReadLimit: 1024})
	if err != nil {
		return "", err
	}
	var first string
	err = c.Run(ctx5(t), func(frame []byte) error {
		first = string(frame)
		return io.EOF
	})
	if !errors.Is(err, io.EOF) {
		return "", err
	}
	return first, nil
}

// One Config: the REST request and the websocket go through the same proxy, with the same TLS roots.
func TestSharedProxyAndTLS(t *testing.T) {
	srv, roots := venue(t)
	proxy, tunnels := connectProxy(t)
	n, err := transport.New(transport.Config{Proxy: "http://user:pass@" + proxy, TLS: &tls.Config{RootCAs: roots}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := rest(t, n, srv.URL); err != nil || got != "rest ok" {
		t.Fatalf("REST over the proxy: %q, %v", got, err)
	}
	if got, err := stream(t, n, "wss"+strings.TrimPrefix(srv.URL, "https")+"/ws"); err != nil || got != "ws ok" {
		t.Fatalf("websocket over the proxy: %q, %v", got, err)
	}
	if n := tunnels.Load(); n != 2 {
		t.Fatalf("%d tunnels, want one per transport", n)
	}
}

// A proxy that refuses, or a certificate the roots do not vouch for, fails both alike; for REST it is
// a request that never went out.
func TestSharedFailures(t *testing.T) {
	srv, roots := venue(t)
	proxy, _ := connectProxy(t)
	wsURL := "wss" + strings.TrimPrefix(srv.URL, "https") + "/ws"
	for name, cfg := range map[string]transport.Config{
		"proxy refuses":    {Proxy: "http://user:wrong@" + proxy, TLS: &tls.Config{RootCAs: roots}},
		"untrusted":        {Proxy: "http://user:pass@" + proxy},
		"untrusted direct": {},
	} {
		n, err := transport.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rest(t, n, srv.URL); err == nil || errors.Is(err, exchange.ErrUnknownOutcome) {
			t.Errorf("%s: REST = %v, want a plain error", name, err)
		}
		if _, err := stream(t, n, wsURL); err == nil {
			t.Errorf("%s: websocket dialed", name)
		}
	}
}

// Both transports look host names up with the Config's Resolver.
func TestSharedResolver(t *testing.T) {
	var lookups atomic.Int32
	n, err := transport.New(transport.Config{Resolver: &net.Resolver{
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			lookups.Add(1)
			return nil, errors.New("no DNS here")
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rest(t, n, "http://api.venue.test"); err == nil || errors.Is(err, exchange.ErrUnknownOutcome) {
		t.Errorf("REST = %v, want a plain error", err)
	}
	afterREST := lookups.Load()
	if afterREST == 0 {
		t.Error("REST did not ask the resolver")
	}
	if _, err := stream(t, n, "ws://stream.venue.test/ws"); err == nil {
		t.Error("websocket dialed")
	}
	if lookups.Load() == afterREST {
		t.Error("the websocket did not ask the resolver")
	}
}

func TestConfig(t *testing.T) {
	for _, p := range []string{"socks5://127.0.0.1:1080", "127.0.0.1:3128", "http://", "http://[::1"} {
		if _, err := transport.New(transport.Config{Proxy: p}); err == nil {
			t.Errorf("proxy %q accepted", p)
		}
	}
	if _, err := transport.New(transport.Config{Proxy: "http://user:secret@127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	_, err := transport.New(transport.Config{Proxy: "ftp://user:secret@127.0.0.1"})
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Errorf("error %v names the password", err)
	}
}
