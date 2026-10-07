// Package etcdtest runs a throwaway single-member etcd for tests of package state. It is imported by tests only.
// A Server can be stopped and restarted on the same data and ports, or rebuilt from nothing; a Proxy in front of it
// can cut its clients off while etcd keeps running, or send them to another server.
//
// The binary is $MFTIKO_ETCD_BIN, else etcd on PATH; a test without either is skipped, unless $MFTIKO_REQUIRE_ETCD
// is set (CI sets it), in which case it fails: a run that skipped every etcd test must not look green. No auth yet
// (E1-08).
package etcdtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Environment variables read by Start.
const (
	// EnvBin is the etcd binary to run.
	EnvBin = "MFTIKO_ETCD_BIN"
	// EnvRequire, when set to anything, turns a missing binary into a test failure instead of a skip.
	EnvRequire = "MFTIKO_REQUIRE_ETCD"
)

const (
	readyTimeout = 20 * time.Second
	startTries   = 3 // a free port can be taken between picking it and etcd binding it
)

var seq atomic.Int64

// Server is one etcd member. It is killed when the test that started it ends.
type Server struct {
	endpoints []string
	bin       string
	args      []string
	dataDir   string
	out       *lockedBuffer

	mu   sync.Mutex
	stop func() // kills the running process and waits for it; nil while stopped
}

// Start launches etcd with a fresh data dir and free localhost ports and waits until it serves reads. It skips the
// test if no etcd binary is found, and fails it if etcd does not come up.
func Start(t testing.TB) *Server {
	t.Helper()
	bin := binary()
	if bin == "" {
		if os.Getenv(EnvRequire) != "" {
			t.Fatalf("etcdtest: etcd not found and %s is set: set %s or put etcd on PATH", EnvRequire, EnvBin)
		}
		t.Skipf("etcd not found: set %s or put etcd on PATH", EnvBin)
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
	if p := os.Getenv(EnvBin); p != "" {
		if p, err := exec.LookPath(p); err == nil {
			return p
		}
	}
	if p, err := exec.LookPath("etcd"); err == nil {
		return p
	}
	return ""
}

// Stop kills the member, as a crash would. Its data dir and ports are kept for Restart.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop != nil {
		s.stop()
		s.stop = nil
	}
}

// Restart kills the member if it runs and starts it again on the same data dir and ports, then waits until it
// serves reads. Clients keep their endpoints and reconnect.
func (s *Server) Restart(t testing.TB) {
	t.Helper()
	s.Stop()
	s.relaunch(t)
}

// Rebuild kills the member, deletes its data and starts it again with the same name, token and ports: the same
// cluster ID, but every key gone and revisions starting over, as a cluster rebuilt from scratch would be.
func (s *Server) Rebuild(t testing.TB) {
	t.Helper()
	s.Stop()
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		t.Fatalf("etcdtest: rebuild: %v", err)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(s.dataDir, e.Name())); err != nil {
			t.Fatalf("etcdtest: rebuild: %v", err)
		}
	}
	s.relaunch(t)
}

func (s *Server) relaunch(t testing.TB) {
	t.Helper()
	var errs []error
	for range startTries { // the old process's ports can take a moment to be free
		s.mu.Lock()
		err := s.launch()
		s.mu.Unlock()
		if err == nil {
			return
		}
		errs = append(errs, err)
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("etcdtest: etcd did not restart: %v", errors.Join(errs...))
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

	s := &Server{endpoints: []string{clientURL}, bin: bin, dataDir: t.TempDir(), out: &lockedBuffer{}}
	s.args = []string{
		"--name", name,
		"--data-dir", s.dataDir,
		"--listen-client-urls", clientURL,
		"--advertise-client-urls", clientURL,
		"--listen-peer-urls", peerURL,
		"--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", name + "=" + peerURL,
		"--initial-cluster-state", "new",
		"--initial-cluster-token", name,
		"--log-level=error",
	}
	if err := s.launch(); err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		s.Stop()
		if t.Failed() {
			t.Logf("etcd %s output:\n%s", name, s.out.String())
		}
	})
	return s, nil
}

// launch starts the process and waits until it serves reads. Called with s.mu held, or before s is shared.
func (s *Server) launch() error {
	cmd := exec.Command(s.bin, s.args...)
	cmd.Stdout = s.out
	cmd.Stderr = s.out
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("etcdtest: start %s: %w", s.bin, err)
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
	if err := waitReady(s.endpoints, exited); err != nil {
		stop()
		return fmt.Errorf("%w (exit: %v)\n%s", err, waitErr, s.out.String())
	}
	s.stop = stop
	return nil
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
