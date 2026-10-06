// Package etcdtest runs a throwaway single-member etcd for tests of package state. It is imported by tests only.
//
// The binary is $MFTIKO_ETCD_BIN, else etcd on PATH; a test without either is skipped. No auth yet (E1-08).
package etcdtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	readyTimeout = 20 * time.Second
	startTries   = 3 // a free port can be taken between picking it and etcd binding it
)

var seq atomic.Int64

// Server is one running etcd member. It is killed when the test that started it ends.
type Server struct {
	endpoints []string
}

// Start launches etcd with a fresh data dir and free localhost ports and waits until it serves reads. It skips the
// test if no etcd binary is found, and fails it if etcd does not come up.
func Start(t testing.TB) *Server {
	t.Helper()
	bin := binary()
	if bin == "" {
		t.Skip("etcd not found: set MFTIKO_ETCD_BIN or put etcd on PATH")
	}
	var errs []error
	for range startTries {
		s, err := start(t, bin)
		if err == nil {
			return s
		}
		errs = append(errs, err)
	}
	t.Fatalf("etcdtest: etcd did not start: %v", errors.Join(errs...))
	return nil
}

// Endpoints is the client URL of the server.
func (s *Server) Endpoints() []string { return append([]string(nil), s.endpoints...) }

// Client returns a raw client, for assertions and for simulating other writers. It is closed when the test ends.
func (s *Server) Client(t testing.TB) *clientv3.Client {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{Endpoints: s.Endpoints(), DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("etcdtest: client: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func binary() string {
	if p := os.Getenv("MFTIKO_ETCD_BIN"); p != "" {
		if p, err := exec.LookPath(p); err == nil {
			return p
		}
	}
	if p, err := exec.LookPath("etcd"); err == nil {
		return p
	}
	return ""
}

func start(t testing.TB, bin string) (*Server, error) {
	client, err := freePort()
	if err != nil {
		return nil, err
	}
	peer, err := freePort()
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("etcdtest-%d-%d", os.Getpid(), seq.Add(1))
	clientURL := "http://" + client
	peerURL := "http://" + peer

	out := &lockedBuffer{}
	cmd := exec.Command(bin,
		"--name", name,
		"--data-dir", t.TempDir(),
		"--listen-client-urls", clientURL,
		"--advertise-client-urls", clientURL,
		"--listen-peer-urls", peerURL,
		"--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", name+"="+peerURL,
		"--initial-cluster-state", "new",
		"--initial-cluster-token", name,
		"--log-level=error",
	)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("etcdtest: start %s: %w", bin, err)
	}
	exited := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(exited)
	}()
	stop := func() {
		_ = cmd.Process.Kill()
		<-exited
	}

	s := &Server{endpoints: []string{clientURL}}
	if err := waitReady(s.endpoints, exited); err != nil {
		stop()
		return nil, fmt.Errorf("%w (exit: %v)\n%s", err, waitErr, out.String())
	}
	t.Cleanup(func() {
		stop()
		if t.Failed() {
			t.Logf("etcd %s output:\n%s", name, out.String())
		}
	})
	return s, nil
}

func waitReady(endpoints []string, exited <-chan struct{}) error {
	c, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 2 * time.Second})
	if err != nil {
		return fmt.Errorf("etcdtest: client: %w", err)
	}
	defer c.Close()
	deadline := time.Now().Add(readyTimeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := c.Get(ctx, "etcdtest-ready")
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-exited:
			return errors.New("etcdtest: etcd exited before it was ready")
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("etcdtest: not ready after %s: %w", readyTimeout, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func freePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("etcdtest: free port: %w", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr, nil
}

// lockedBuffer collects etcd's output; exec copies into it from its own goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
