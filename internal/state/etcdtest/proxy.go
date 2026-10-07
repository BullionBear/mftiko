package etcdtest

import (
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
)

// Proxy forwards TCP connections to a Server. Cut drops every connection and refuses new ones until Restore, as a
// network partition between the clients behind the proxy and etcd would; etcd itself keeps running, so other
// clients can write and compact meanwhile.
type Proxy struct {
	l net.Listener

	mu     sync.Mutex
	target string
	cut    bool
	conns  map[net.Conn]struct{}
}

// NewProxy listens on a free localhost port and forwards to s. It is closed when the test ends.
func NewProxy(t testing.TB, s *Server) *Proxy {
	t.Helper()
	u, err := url.Parse(s.Endpoints()[0])
	if err != nil {
		t.Fatalf("etcdtest: proxy target: %v", err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("etcdtest: proxy listen: %v", err)
	}
	p := &Proxy{target: u.Host, l: l, conns: map[net.Conn]struct{}{}}
	go p.accept()
	t.Cleanup(func() {
		l.Close()
		p.Cut()
	})
	return p
}

// Endpoints is the proxy's client URL, to dial instead of the server's.
func (p *Proxy) Endpoints() []string { return []string{"http://" + p.l.Addr().String()} }

// Cut closes every forwarded connection; connections made until Restore are closed as soon as they are accepted.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = true
	for c := range p.conns {
		c.Close()
	}
	clear(p.conns)
}

// SetTarget sends new connections to s from now on; open ones stay where they are until cut.
func (p *Proxy) SetTarget(s *Server) {
	u, err := url.Parse(s.Endpoints()[0])
	if err != nil {
		panic(err) // Endpoints are built by this package
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = u.Host
}

// Restore lets new connections through again.
func (p *Proxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = false
}

func (p *Proxy) accept() {
	for {
		in, err := p.l.Accept()
		if err != nil {
			return // listener closed
		}
		go p.forward(in)
	}
}

func (p *Proxy) forward(in net.Conn) {
	if !p.track(in) {
		in.Close()
		return
	}
	p.mu.Lock()
	target := p.target
	p.mu.Unlock()
	out, err := net.Dial("tcp", target)
	if err != nil {
		p.untrack(in)
		in.Close()
		return
	}
	if !p.track(out) {
		p.untrack(in)
		in.Close()
		out.Close()
		return
	}
	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	go pipe(out, in)
	go pipe(in, out)
	<-done // one direction ended: end both
	in.Close()
	out.Close()
	<-done
	p.untrack(in)
	p.untrack(out)
}

// track registers c unless the proxy is cut.
func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cut {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *Proxy) untrack(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.conns, c)
}
