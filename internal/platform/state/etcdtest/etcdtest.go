// Package etcdtest runs a throwaway single-member etcd for tests of package state. It is imported by tests only.
// A Server can be stopped and restarted on the same data and ports, or rebuilt from nothing; a Proxy in front of it
// can cut its clients off while etcd keeps running, or send them to another server.
//
// The binary is $MFTIKO_ETCD_BIN, else etcd on PATH; a test without either is skipped, unless $MFTIKO_REQUIRE_ETCD
// is set (CI sets it), in which case it fails: a run that skipped every etcd test must not look green.
//
// Auth is on, as in production (E1-08): etcd issues JWT tokens (ES256), and a root user exists whose password is
// RootPassword. Client returns a root client; tests create the other users and roles themselves, with Password.
package etcdtest

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
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

// RootUser is etcd's root user, which enables auth and manages users and roles.
const RootUser = "root"

// DefaultTokenTTL is the lifetime of the JWT tokens etcd issues, unless Start is given TokenTTL.
const DefaultTokenTTL = 5 * time.Minute

// secret is random per test binary: every password derives from it, so none is written down anywhere.
var secret = sync.OnceValue(func() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
})

// Password is the test password of user, the same for every server of this test binary.
func Password(user string) string {
	m := hmac.New(sha256.New, secret())
	m.Write([]byte(user))
	return hex.EncodeToString(m.Sum(nil))
}

// RootPassword is the root user's password.
func RootPassword() string { return Password(RootUser) }

// signingKey is the ES256 key pair etcd signs its tokens with, PEM-encoded; one per test binary.
var signingKey = sync.OnceValues(func() (priv, pub []byte) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	der, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		panic(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	if err != nil {
		panic(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
})

// Option configures Start.
type Option func(*options)

type options struct{ tokenTTL time.Duration }

// TokenTTL sets how long the tokens etcd issues stay valid; clients must authenticate again after it.
func TokenTTL(d time.Duration) Option { return func(o *options) { o.tokenTTL = d } }

// Server is one etcd member. It is killed when the test that started it ends.
type Server struct {
	endpoints []string
	bin       string
	args      []string
	dataDir   string
	out       *lockedBuffer

	mu     sync.Mutex
	stop   func() // kills the running process and waits for it; nil while stopped
	authOn bool   // the data dir has auth enabled: readiness is checked as root
}

// Start launches etcd with a fresh data dir and free localhost ports, turns auth on and waits until it serves reads.
// It skips the test if no etcd binary is found, and fails it if etcd does not come up.
func Start(t testing.TB, opts ...Option) *Server {
	t.Helper()
	bin := binary()
	if bin == "" {
		if os.Getenv(EnvRequire) != "" {
			t.Fatalf("etcdtest: etcd not found and %s is set: set %s or put etcd on PATH", EnvRequire, EnvBin)
		}
		t.Skipf("etcd not found: set %s or put etcd on PATH", EnvBin)
	}
	o := options{tokenTTL: DefaultTokenTTL}
	for _, f := range opts {
		f(&o)
	}
	var errs []error
	for range startTries {
		s, err := start(t, bin, o)
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

// Client returns a raw root client, for assertions, for simulating other writers, and for managing users and
// roles. It is closed when the test ends.
func (s *Server) Client(t testing.TB) *clientv3.Client {
	t.Helper()
	return ClientAs(t, s.Endpoints(), RootUser)
}

// ClientAs returns a raw client authenticated as user, whose password is Password(user), at endpoints. It is
// closed when the test ends.
func ClientAs(t testing.TB, endpoints []string, user string) *clientv3.Client {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints, DialTimeout: 5 * time.Second, Username: user, Password: Password(user),
	})
	if err != nil {
		t.Fatalf("etcdtest: client as %s: %v", user, err)
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
// cluster ID, but every key gone and revisions starting over, as a cluster rebuilt from scratch would be. Auth is
// turned on again with the root user only: users and roles are gone with the data.
func (s *Server) Rebuild(t testing.TB) {
	t.Helper()
	s.Stop()
	s.mu.Lock()
	s.authOn = false
	s.mu.Unlock()
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
	if err := enableAuth(s.endpoints); err != nil {
		t.Fatalf("etcdtest: rebuild: %v", err)
	}
	s.mu.Lock()
	s.authOn = true
	s.mu.Unlock()
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

func start(t testing.TB, bin string, o options) (*Server, error) {
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
	keyDir := t.TempDir()
	priv, pub := signingKey()
	privPath, pubPath := filepath.Join(keyDir, "jwt.key"), filepath.Join(keyDir, "jwt.pub")
	if err := os.WriteFile(privPath, priv, 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(pubPath, pub, 0o600); err != nil {
		return nil, err
	}
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
		fmt.Sprintf("--auth-token=jwt,pub-key=%s,priv-key=%s,sign-method=ES256,ttl=%s", pubPath, privPath, o.tokenTTL),
	}
	if err := s.launch(); err != nil {
		return nil, err
	}
	if err := enableAuth(s.endpoints); err != nil {
		s.Stop()
		return nil, err
	}
	s.authOn = true
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
	if err := waitReady(s.endpoints, exited, s.authOn); err != nil {
		stop()
		return fmt.Errorf("%w (exit: %v)\n%s", err, waitErr, s.out.String())
	}
	s.stop = stop
	return nil
}

// enableAuth creates the root user and turns auth on, on a member that has neither.
func enableAuth(endpoints []string) error {
	c, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("etcdtest: client: %w", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	if _, err := c.UserAdd(ctx, RootUser, RootPassword()); err != nil {
		return fmt.Errorf("etcdtest: add root: %w", err)
	}
	if _, err := c.UserGrantRole(ctx, RootUser, "root"); err != nil {
		return fmt.Errorf("etcdtest: grant root: %w", err)
	}
	if _, err := c.AuthEnable(ctx); err != nil {
		return fmt.Errorf("etcdtest: enable auth: %w", err)
	}
	return nil
}

// waitReady waits until the member has a leader. With auth on, it asks as root: a client with credentials only
// comes up once the member answers, so it is retried until then.
func waitReady(endpoints []string, exited <-chan struct{}, auth bool) error {
	deadline := time.Now().Add(readyTimeout)
	for {
		err := ready(endpoints, auth)
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

func ready(endpoints []string, auth bool) error {
	cfg := clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second}
	if auth {
		cfg.Username, cfg.Password = RootUser, RootPassword()
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := c.Status(ctx, endpoints[0])
	if err != nil {
		return err
	}
	if resp.Leader == 0 {
		return errors.New("no leader yet")
	}
	return nil
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
