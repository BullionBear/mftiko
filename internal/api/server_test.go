package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// pollInterval is how often a condition is sampled. It is not the bound.
const pollInterval = 20 * time.Millisecond

func until(t *testing.T, within time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last error
	for {
		last = cond()
		if last == nil {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("waited %s for %s; last: %v", within, what, last)
		}
		time.Sleep(pollInterval)
	}
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type running struct {
	s      *Server
	done   chan struct{}
	err    error
	cancel context.CancelFunc
}

func startRunning(t *testing.T, cfg Config, log *lockedBuf) *running {
	t.Helper()
	s, err := start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{s: s, done: make(chan struct{}), cancel: cancel}
	go func() {
		r.err = s.serve(ctx)
		close(r.done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
			if r.err != nil {
				t.Errorf("serve: %v", r.err)
			}
		case <-time.After(20 * time.Second):
			t.Errorf("serve did not return")
		}
		if t.Failed() && log != nil {
			t.Log(log.String())
		}
	})
	return r
}

func (r *running) stopped() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

type readyJSON struct {
	Regions map[string]struct {
		Available bool  `json:"available"`
		Synced    bool  `json:"synced"`
		Revision  int64 `json:"revision"`
	} `json:"regions"`
	UnavailableRegions []string `json:"unavailable_regions"`
}

func getJSON(t *testing.T, url string, out any) (int, []byte) {
	t.Helper()
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("%s: %v", b, err)
		}
	}
	return resp.StatusCode, b
}

func hasEntry(view state.View, key, val string) bool {
	e, ok := view.Entries[key]
	return ok && string(e.Value) == val
}

func closedEndpoint(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return "http://" + addr
}

func offlineConfig(t *testing.T, log zerolog.Logger, cert, key string) Config {
	t.Helper()
	deploy := deployDoc(
		map[string][]string{"tw": {closedEndpoint(t)}},
		map[string]string{"controller": "tw", "md-tw-1": "tw"},
		map[string]string{"md-tw-1": "md"},
	)
	conf := `{"listen": "127.0.0.1:0", "repositories": ["mftiko/strategies"]}`
	return loadWithin(t, deploy, conf, Input{
		EtcdPassword: "pw",
		CookieKey:    "cookie-key",
		PostgresDSN:  "postgres://mftiko@127.0.0.1:1/mftiko?sslmode=disable",
		TLSCert:      cert,
		TLSKey:       key,
		Mirror: state.MirrorOptions{
			ProbeInterval: 100 * time.Millisecond,
			MinBackoff:    20 * time.Millisecond,
			MaxBackoff:    200 * time.Millisecond,
		},
		Log: log,
	})
}

func applyAuth(t *testing.T, ctx context.Context, topo state.Topology, planes map[string]state.Plane, servers map[string]*etcdtest.Server) {
	t.Helper()
	tables, err := state.AuthTables(topo, planes)
	if err != nil {
		t.Fatal(err)
	}
	for region, srv := range servers {
		if err := state.EnsureAuth(ctx, srv.Client(t), tables[region], etcdtest.Password); err != nil {
			t.Fatalf("auth %s: %v", region, err)
		}
	}
}

func putKey(t *testing.T, ctx context.Context, srv *etcdtest.Server, key, val string) int64 {
	t.Helper()
	resp, err := srv.Client(t).Put(ctx, key, val)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return resp.Header.Revision
}

// One mirror per cluster watches the whole keyspace. Cutting one region marks it
// unavailable and leaves the process up; the other region still follows writes.
func TestMirrorFollowsCutAndDrain(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw", "jp")
	proxy := etcdtest.NewProxy(t, servers["jp"])
	topo := state.Topology{
		Regions: map[string]state.Region{
			"tw": {Endpoints: servers["tw"].Endpoints()},
			"jp": {Endpoints: proxy.Endpoints()},
		},
		Instances: map[string]string{
			state.ControllerInstance: "tw",
			"sts-tw-1":               "tw",
			"td-tw-1":                "tw",
			"td-jp-1":                "jp",
		},
	}
	planes := map[string]state.Plane{
		"sts-tw-1": state.PlaneSTS,
		"td-tw-1":  state.PlaneTD,
		"td-jp-1":  state.PlaneTD,
	}
	applyAuth(t, ctx, topo, planes, servers)

	buf := &lockedBuf{}
	dep, err := json.Marshal(deployment{Topology: topo, Planes: planes})
	if err != nil {
		t.Fatal(err)
	}
	conf, err := json.Marshal(map[string]any{
		"listen":            "127.0.0.1:0",
		"unavailable_after": "2s",
		"repositories":      []string{"mftiko/strategies"},
		"users":             []any{map[string]string{"name": "op", "token_sha256": tokenHash("op")}},
		"td_keys":           map[string]string{"td-tw-1": mustPub(t), "td-jp-1": mustPub(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(bytes.NewReader(dep), bytes.NewReader(conf), Input{
		EtcdPassword: etcdtest.Password(state.UserAPI),
		CookieKey:    "cookie-key",
		PostgresDSN:  "postgres://mftiko@127.0.0.1:1/mftiko?sslmode=disable",
		Mirror: state.MirrorOptions{
			ProbeInterval: 100 * time.Millisecond,
			MinBackoff:    20 * time.Millisecond,
			MaxBackoff:    200 * time.Millisecond,
		},
		Log: zerolog.New(buf).Level(zerolog.InfoLevel),
	})
	if err != nil {
		t.Fatal(err)
	}
	r := startRunning(t, cfg, buf)

	until(t, 30*time.Second, "both regions available", func() error {
		if r.stopped() {
			return errors.New("server exited")
		}
		code, raw := getJSON(t, "http://"+r.s.Addr()+"/readyz", nil)
		var body readyJSON
		if err := json.Unmarshal(raw, &body); err != nil {
			return err
		}
		if code != http.StatusOK || !body.Regions["tw"].Available || !body.Regions["jp"].Available ||
			len(body.UnavailableRegions) != 0 || !bytes.Contains(raw, []byte(`"unavailable_regions":[]`)) {
			return fmt.Errorf("status %d %s", code, raw)
		}
		return nil
	})
	if len(r.s.clusters) != 2 {
		t.Fatalf("%d clusters", len(r.s.clusters))
	}

	spec := state.SessionSpecKey("sts-tw-1", "abc123")
	acct := state.AccountKey("td-tw-1", 7)
	jpKey := state.AccountKey("td-jp-1", 9)
	putKey(t, ctx, servers["tw"], spec, "spec-a")
	putKey(t, ctx, servers["tw"], acct, "acct-a")
	putKey(t, ctx, servers["jp"], jpKey, "jp-old")
	until(t, 10*time.Second, "one mirror sees both prefixes, and jp sees its key", func() error {
		for _, c := range r.s.clusters {
			m := c.mirror.Load()
			if m == nil || m.Prefix() != keyPrefix {
				return fmt.Errorf("%s prefix", c.name)
			}
		}
		tw, ok := r.s.snapshot("tw")
		if !ok || !hasEntry(tw, spec, "spec-a") || !hasEntry(tw, acct, "acct-a") {
			return fmt.Errorf("tw synced %v entries %d", ok, len(tw.Entries))
		}
		jp, ok := r.s.snapshot("jp")
		if !ok || !hasEntry(jp, jpKey, "jp-old") {
			return fmt.Errorf("jp synced %v", ok)
		}
		return nil
	})

	if _, err := servers["tw"].Client(t).Delete(ctx, spec); err != nil {
		t.Fatal(err)
	}
	until(t, 10*time.Second, "the delete leaves the other prefix", func() error {
		tw, _ := r.s.snapshot("tw")
		if _, ok := tw.Entries[spec]; ok {
			return errors.New("deleted key still present")
		}
		if !hasEntry(tw, acct, "acct-a") {
			return errors.New("account key disappeared")
		}
		return nil
	})

	proxy.Cut()
	if jp, ok := r.s.snapshot("jp"); !ok || !hasEntry(jp, jpKey, "jp-old") {
		t.Fatal("jp copy was dropped at the cut")
	}
	until(t, 15*time.Second, "jp unavailable, tw still ready, process up", func() error {
		if r.stopped() {
			return errors.New("server exited")
		}
		code, raw := getJSON(t, "http://"+r.s.Addr()+"/healthz", nil)
		if code != http.StatusOK || !bytes.Contains(raw, []byte(`"ok":true`)) {
			return fmt.Errorf("healthz %d %s", code, raw)
		}
		var body readyJSON
		code, raw = getJSON(t, "http://"+r.s.Addr()+"/readyz", &body)
		if code != http.StatusOK || !body.Regions["tw"].Available || body.Regions["jp"].Available {
			return fmt.Errorf("readyz %d %s", code, raw)
		}
		if !slices.Equal(body.UnavailableRegions, []string{"jp"}) {
			return fmt.Errorf("unavailable %v", body.UnavailableRegions)
		}
		jp, ok := r.s.snapshot("jp")
		if !ok || !hasEntry(jp, jpKey, "jp-old") {
			return fmt.Errorf("jp copy lost synced %v", ok)
		}
		return nil
	})

	spec2 := state.SessionSpecKey("sts-tw-1", "def456")
	putKey(t, ctx, servers["tw"], spec2, "spec-b")
	until(t, 10*time.Second, "tw follows a write while jp is cut", func() error {
		view, ok := r.s.snapshot("tw")
		if !ok || !hasEntry(view, spec2, "spec-b") {
			return errors.New("new tw key not in the copy")
		}
		return nil
	})
	jpNew := state.AccountKey("td-jp-1", 8)
	putKey(t, ctx, servers["jp"], jpNew, "jp-new")

	proxy.Restore()
	until(t, 20*time.Second, "jp available again and caught up", func() error {
		if r.stopped() {
			return errors.New("server exited")
		}
		var body readyJSON
		code, raw := getJSON(t, "http://"+r.s.Addr()+"/readyz", &body)
		if code != http.StatusOK || !body.Regions["tw"].Available || !body.Regions["jp"].Available ||
			len(body.UnavailableRegions) != 0 {
			return fmt.Errorf("readyz %d %s", code, raw)
		}
		jp, ok := r.s.snapshot("jp")
		if !ok || !hasEntry(jp, jpNew, "jp-new") || !hasEntry(jp, jpKey, "jp-old") {
			return fmt.Errorf("jp synced %v", ok)
		}
		return nil
	})

	r.s.draining.Store(true)
	until(t, 5*time.Second, "readiness 503 while the listener still serves", func() error {
		if r.stopped() {
			return errors.New("server exited")
		}
		code, raw := getJSON(t, "http://"+r.s.Addr()+"/healthz", nil)
		if code != http.StatusOK || !bytes.Contains(raw, []byte(`"ok":true`)) {
			return fmt.Errorf("healthz %d %s", code, raw)
		}
		var body readyJSON
		code, raw = getJSON(t, "http://"+r.s.Addr()+"/readyz", &body)
		if code != http.StatusServiceUnavailable || !body.Regions["tw"].Available || !body.Regions["jp"].Available {
			return fmt.Errorf("readyz %d %s", code, raw)
		}
		return nil
	})
}

func TestPlaintextStaysUp(t *testing.T) {
	buf := &lockedBuf{}
	cfg := offlineConfig(t, zerolog.New(buf).Level(zerolog.InfoLevel), "", "")
	r := startRunning(t, cfg, buf)
	resp, err := http.Get("http://" + r.s.Addr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 1 || !bytes.Contains(body, []byte(`"ok":true`)) {
		t.Fatalf("%d %s %q", resp.StatusCode, resp.Proto, body)
	}
	until(t, 8*time.Second, "stays up after a failed dial", func() error {
		if r.stopped() {
			return errors.New("exited")
		}
		if strings.Count(buf.String(), "mirror dial") < 1 {
			return errors.New("waiting for the dial to fail")
		}
		var ready readyJSON
		code, raw := getJSON(t, "http://"+r.s.Addr()+"/readyz", &ready)
		if code != http.StatusServiceUnavailable || ready.Regions["tw"].Available || ready.Regions["tw"].Synced {
			return fmt.Errorf("readyz %d %s", code, raw)
		}
		if !slices.Equal(ready.UnavailableRegions, []string{"tw"}) {
			return fmt.Errorf("unavailable %v", ready.UnavailableRegions)
		}
		return nil
	})
}

func TestHTTPS2(t *testing.T) {
	cert, key := writeCert(t)
	cfg := offlineConfig(t, zerolog.Nop(), cert, key)
	r := startRunning(t, cfg, nil)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	resp, err := client.Get("https://" + r.s.Addr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 {
		t.Fatalf("%d %s", resp.StatusCode, resp.Proto)
	}
	if resp.TLS == nil || !resp.TLS.HandshakeComplete {
		t.Fatal("response was not TLS")
	}
}

func TestDrainWaitsThenCloses(t *testing.T) {
	ln := listenLocal(t)
	entered := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	s := &Server{
		cfg: Config{ShutdownTimeout: 200 * time.Millisecond},
		log: zerolog.Nop(),
		ln:  ln,
		http: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			once.Do(func() { close(entered) })
			<-r.Context().Done()
			w.WriteHeader(http.StatusNoContent)
			close(finished)
		})},
	}
	serveDone := make(chan struct{})
	go func() {
		_ = s.http.Serve(ln)
		close(serveDone)
	}()
	t.Cleanup(func() {
		s.drain()
		select {
		case <-serveDone:
		case <-time.After(2 * time.Second):
			t.Error("serve did not return")
		}
	})
	clientDone := make(chan struct{})
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String() + "/block")
		if err == nil {
			resp.Body.Close()
		}
		close(clientDone)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	started := time.Now()
	s.drain()
	if elapsed := time.Since(started); elapsed < 150*time.Millisecond {
		t.Fatalf("drain returned in %s, under the shutdown limit", elapsed)
	}
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("handler still blocked after drain")
	}
	select {
	case <-clientDone:
	case <-time.After(2 * time.Second):
		t.Fatal("client still blocked after drain")
	}
	assertRefused(t, ln.Addr().String())
}

func TestDrainReturnsWhenHandlerFinishes(t *testing.T) {
	ln := listenLocal(t)
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	s := &Server{
		cfg: Config{ShutdownTimeout: 5 * time.Second},
		log: zerolog.Nop(),
		ln:  ln,
		http: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			once.Do(func() { close(entered) })
			<-release
			w.WriteHeader(http.StatusNoContent)
		})},
	}
	serveDone := make(chan struct{})
	go func() {
		_ = s.http.Serve(ln)
		close(serveDone)
	}()
	t.Cleanup(func() {
		letGo()
		s.drain()
		select {
		case <-serveDone:
		case <-time.After(2 * time.Second):
			t.Error("serve did not return")
		}
	})
	status := make(chan int, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String() + "/block")
		if err != nil {
			status <- 0
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	drained := make(chan time.Duration, 1)
	go func() {
		started := time.Now()
		s.drain()
		drained <- time.Since(started)
	}()
	letGo()
	select {
	case elapsed := <-drained:
		if elapsed >= s.cfg.ShutdownTimeout {
			t.Fatalf("drain hit the shutdown limit: %s", elapsed)
		}
	case <-time.After(s.cfg.ShutdownTimeout):
		t.Fatal("drain did not return before the limit")
	}
	select {
	case code := <-status:
		if code != http.StatusNoContent {
			t.Fatalf("client status %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not finish")
	}
	assertRefused(t, ln.Addr().String())
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func assertRefused(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Fatal("listener accepted a connection after drain")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("dial timed out; listener may still be up")
	}
}

func writeCert(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}
