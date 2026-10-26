package agent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// sharedEtcd is the one etcd for this package. TestMain starts it and stops it
// after the tests. Acceptance tests take a key prefix each; they do not each
// start a server. A test that must stop, restart, or disconnect the cluster,
// or that needs a non-default token TTL, calls etcdtest.Start instead of
// openCluster and keeps that private server.
var sharedEtcd *etcdCluster

const etcdReadyTimeout = 20 * time.Second

type etcdCluster struct {
	endpoints []string
	out       *etcdOut
	stop      func()
	absent    bool
	err       error
	seq       atomic.Uint64
}

func startEtcdCluster() *etcdCluster {
	bin := etcdBinary()
	if bin == "" {
		return &etcdCluster{absent: true, stop: func() {}}
	}
	var errs []error
	for try := 1; try <= 3; try++ {
		c, err := launchEtcd(bin, try)
		if err == nil {
			return c
		}
		errs = append(errs, err)
	}
	return &etcdCluster{err: errors.Join(errs...), stop: func() {}}
}

func (c *etcdCluster) close() {
	if c == nil || c.stop == nil {
		return
	}
	c.stop()
}

func (c *etcdCluster) endpointsFor(t testing.TB) []string {
	t.Helper()
	if c == nil || c.absent {
		if os.Getenv(etcdtest.EnvRequire) != "" {
			t.Fatalf("etcdtest: etcd not found and %s is set: set %s or put etcd on PATH", etcdtest.EnvRequire, etcdtest.EnvBin)
		}
		t.Skipf("etcd not found: set %s or put etcd on PATH", etcdtest.EnvBin)
	}
	if c.err != nil {
		t.Fatalf("etcdtest: etcd did not start: %v", c.err)
	}
	return append([]string(nil), c.endpoints...)
}

// instance is a name that belongs to this call. Keys for it live under
// /mftiko/v1/sts/<name>/. The counter keeps two calls in one test, and a
// repeated -count, on distinct prefixes.
func (c *etcdCluster) instance(t testing.TB) string {
	n := c.seq.Add(1)
	var b strings.Builder
	b.WriteByte('s')
	b.WriteString(strconv.FormatUint(n, 10))
	for _, r := range strings.ToLower(t.Name()) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	s := b.String()
	if len(s) > 64 {
		s = s[:64]
	}
	return s
}

// openCluster is the shared server plus this test's instance. EnsureAuth adds
// that instance's user; the controller and api users stay, and a later test
// only grants what its own instance needs. Tests do not call t.Parallel:
// auth changes and the controller's fixed prefix are one per server.
func openCluster(t *testing.T) (string, state.Topology, *clientv3.Client) {
	t.Helper()
	eps := sharedEtcd.endpointsFor(t)
	inst := sharedEtcd.instance(t)
	if sharedEtcd.out != nil {
		mark := sharedEtcd.out.Len()
		t.Cleanup(func() {
			if t.Failed() {
				if s := sharedEtcd.out.Since(mark); s != "" {
					t.Logf("etcd output:\n%s", s)
				}
			}
		})
	}
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: eps}},
		Instances: map[string]string{inst: "tw", state.ControllerInstance: "tw"},
	}
	tables, err := state.AuthTables(topo, map[string]state.Plane{inst: state.PlaneSTS})
	if err != nil {
		t.Fatal(err)
	}
	root := etcdtest.ClientAs(t, eps, etcdtest.RootUser)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := state.EnsureAuth(ctx, root, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	return inst, topo, root
}

func etcdBinary() string {
	if p := os.Getenv(etcdtest.EnvBin); p != "" {
		if p, err := exec.LookPath(p); err == nil {
			return p
		}
	}
	if p, err := exec.LookPath("etcd"); err == nil {
		return p
	}
	return ""
}

func launchEtcd(bin string, try int) (*etcdCluster, error) {
	dir, err := os.MkdirTemp("", "sts-etcd-")
	if err != nil {
		return nil, err
	}
	data := filepath.Join(dir, "data")
	keys := filepath.Join(dir, "keys")
	for _, d := range []string{data, keys} {
		if err := os.Mkdir(d, 0o700); err != nil {
			os.RemoveAll(dir)
			return nil, err
		}
	}
	clientAddr, err := freeAddr()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	peerAddr, err := freeAddr()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	privPath, pubPath, err := writeJWT(keys)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	name := fmt.Sprintf("sts-%d-%d", os.Getpid(), try)
	clientURL := "http://" + clientAddr
	peerURL := "http://" + peerAddr
	out := &etcdOut{}
	cmd := exec.Command(bin,
		"--name", name,
		"--data-dir", data,
		"--listen-client-urls", clientURL,
		"--advertise-client-urls", clientURL,
		"--listen-peer-urls", peerURL,
		"--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", name+"="+peerURL,
		"--initial-cluster-state", "new",
		"--initial-cluster-token", name,
		"--log-level=error",
		fmt.Sprintf("--auth-token=jwt,pub-key=%s,priv-key=%s,sign-method=ES256,ttl=%s", pubPath, privPath, etcdtest.DefaultTokenTTL),
		"--bcrypt-cost=4",
	)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	stop := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			<-exited
		}
		os.RemoveAll(dir)
	}
	endpoints := []string{clientURL}
	if err := waitEtcd(endpoints, exited, false); err != nil {
		stop()
		return nil, fmt.Errorf("%w\n%s", err, out.Since(0))
	}
	if err := enableRoot(endpoints); err != nil {
		stop()
		return nil, err
	}
	return &etcdCluster{endpoints: endpoints, out: out, stop: stop}, nil
}

func writeJWT(dir string) (privPath, pubPath string, err error) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	der, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		return "", "", err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	if err != nil {
		return "", "", err
	}
	privPath = filepath.Join(dir, "jwt.key")
	pubPath = filepath.Join(dir, "jwt.pub")
	if err := os.WriteFile(privPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o600); err != nil {
		return "", "", err
	}
	return privPath, pubPath, nil
}

func enableRoot(endpoints []string) error {
	c, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("etcd client: %w", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), etcdReadyTimeout)
	defer cancel()
	if _, err := c.UserAdd(ctx, etcdtest.RootUser, etcdtest.RootPassword()); err != nil {
		return fmt.Errorf("add root: %w", err)
	}
	if _, err := c.UserGrantRole(ctx, etcdtest.RootUser, "root"); err != nil {
		return fmt.Errorf("grant root: %w", err)
	}
	if _, err := c.AuthEnable(ctx); err != nil {
		return fmt.Errorf("enable auth: %w", err)
	}
	return nil
}

func waitEtcd(endpoints []string, exited <-chan struct{}, auth bool) error {
	deadline := time.Now().Add(etcdReadyTimeout)
	var err error
	for {
		err = etcdReady(endpoints, auth)
		if err == nil {
			return nil
		}
		select {
		case <-exited:
			return errors.New("etcd exited before it was ready")
		default:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not ready after %s: %w", etcdReadyTimeout, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func etcdReady(endpoints []string, auth bool) error {
	cfg := clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second}
	if auth {
		cfg.Username, cfg.Password = etcdtest.RootUser, etcdtest.RootPassword()
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

func freeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	l.Close()
	return addr, nil
}

type etcdOut struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *etcdOut) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *etcdOut) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Len()
}

func (b *etcdOut) Since(mark int) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.b.String()
	if mark > len(s) {
		return ""
	}
	return s[mark:]
}
