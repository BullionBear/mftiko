// Package transport is the network setup that the REST transport (exchange/transport/fasthttp) and
// the websocket transport (exchange/transport/ws) share (DECISIONS D302): one Config gives both the
// same proxy, TLS and DNS. Both dial through Net, so a websocket handshake (net/http, under
// github.com/coder/websocket) and a REST request (github.com/valyala/fasthttp) leave the same way.
//
// The transports belong to the workers. Adapters never see them: an adapter builds bytes, a
// transport moves them.
package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// DefaultDialTimeout bounds a dial whose context has no deadline.
const DefaultDialTimeout = 10 * time.Second

// Config is the network setup of a process's venue connections.
type Config struct {
	// Proxy is the URL of an HTTP proxy that every connection tunnels through with CONNECT, e.g.
	// http://user:pass@10.0.0.1:3128. Empty connects directly. Through a proxy, the proxy resolves
	// the venue's host name.
	Proxy string
	// TLS is the base client configuration: roots, client certificates, minimum version. nil uses
	// the system roots. Each connection sets ServerName to its host when it is empty, and drops
	// NextProtos: both transports speak HTTP/1.1 (D302).
	TLS *tls.Config
	// Resolver looks up venue host names on direct connections; nil is net.DefaultResolver.
	Resolver *net.Resolver
	// DialTimeout bounds the TCP connect, the proxy's CONNECT and the TLS handshake together, when
	// the caller's context has no deadline. 0 is DefaultDialTimeout.
	DialTimeout time.Duration
}

// Net dials venue connections as its Config says. Safe for concurrent use.
type Net struct {
	proxy     *url.URL // nil: direct
	proxyAuth string   // the Proxy-Authorization line, or ""
	tls       *tls.Config
	dialer    net.Dialer
	timeout   time.Duration
	http      *http.Client
}

// New checks c and builds the Net that both transports dial through.
func New(c Config) (*Net, error) {
	n := &Net{timeout: c.DialTimeout, dialer: net.Dialer{Resolver: c.Resolver}}
	if n.timeout <= 0 {
		n.timeout = DefaultDialTimeout
	}
	if c.Proxy != "" {
		u, err := url.Parse(c.Proxy)
		if err != nil {
			return nil, fmt.Errorf("transport: proxy: %w", err)
		}
		if u.Scheme != "http" || u.Host == "" {
			return nil, fmt.Errorf("transport: proxy %q: want http://host:port", u.Redacted())
		}
		if u.Port() == "" {
			u.Host = net.JoinHostPort(u.Hostname(), "80")
		}
		if u.User != nil {
			pass, _ := u.User.Password()
			n.proxyAuth = "Proxy-Authorization: Basic " +
				base64.StdEncoding.EncodeToString([]byte(u.User.Username()+":"+pass)) + "\r\n"
		}
		n.proxy = u
	}
	if c.TLS != nil {
		n.tls = c.TLS.Clone()
	} else {
		n.tls = &tls.Config{}
	}
	n.tls.NextProtos = nil
	if n.tls.ClientSessionCache == nil {
		n.tls.ClientSessionCache = tls.NewLRUClientSessionCache(0)
	}
	n.http = &http.Client{Transport: &http.Transport{
		DialContext:    n.DialContext,
		DialTLSContext: n.DialTLSContext,
	}}
	return n, nil
}

// Direct is the process's Net with the default Config: no proxy, the system roots and resolver.
var Direct = sync.OnceValue(func() *Net {
	n, err := New(Config{})
	if err != nil {
		panic(err)
	}
	return n
})

// DialContext opens a TCP connection to addr (host:port): through the proxy's CONNECT tunnel when
// there is one, else directly with the Resolver.
func (n *Net) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.timeout)
		defer cancel()
	}
	if n.proxy == nil {
		return n.dialer.DialContext(ctx, network, addr)
	}
	conn, err := n.dialer.DialContext(ctx, "tcp", n.proxy.Host)
	if err != nil {
		return nil, fmt.Errorf("transport: proxy %s: %w", n.proxy.Host, err)
	}
	tunnel, err := n.connect(ctx, conn, addr)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return tunnel, nil
}

// DialTLSContext is DialContext followed by the TLS handshake for addr's host.
func (n *Net) DialTLSContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.timeout)
		defer cancel()
	}
	conn, err := n.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	cfg := n.tls
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			conn.Close()
			return nil, err
		}
		cfg = cfg.Clone()
		cfg.ServerName = host
	}
	tc := tls.Client(conn, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("transport: tls %s: %w", addr, err)
	}
	return tc, nil
}

// HTTPClient is a net/http client over DialContext and DialTLSContext: what the websocket handshake
// goes through. HTTP/1.1 only.
func (n *Net) HTTPClient() *http.Client { return n.http }

// connect asks the proxy for a tunnel to addr over conn.
func (n *Net) connect(ctx context.Context, conn net.Conn, addr string) (net.Conn, error) {
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	if _, err := io.WriteString(conn, "CONNECT "+addr+" HTTP/1.1\r\nHost: "+addr+"\r\n"+n.proxyAuth+"\r\n"); err != nil {
		return nil, n.tunnelErr(ctx, addr, err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return nil, n.tunnelErr(ctx, addr, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("transport: proxy %s: CONNECT %s: %s", n.proxy.Host, addr, resp.Status)
	}
	if !stop() {
		return nil, n.tunnelErr(ctx, addr, ctx.Err())
	}
	if br.Buffered() > 0 {
		return &bufferedConn{Conn: conn, r: br}, nil
	}
	return conn, nil
}

func (n *Net) tunnelErr(ctx context.Context, addr string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		err = fmt.Errorf("%w (%w)", ctxErr, err)
	}
	return fmt.Errorf("transport: proxy %s: CONNECT %s: %w", n.proxy.Host, addr, err)
}

// bufferedConn is a tunnel whose first bytes the CONNECT reader already took off the wire.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	if c.r.Buffered() > 0 {
		return c.r.Read(p)
	}
	return c.Conn.Read(p)
}
