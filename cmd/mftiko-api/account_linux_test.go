//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
	"github.com/BullionBear/mftiko/internal/api"
	platagent "github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/internal/td/account"
	tdagent "github.com/BullionBear/mftiko/internal/td/agent"
)

const (
	envTDWorker  = "MFTIKO_TD_TEST_WORKER" // "1": this process is the account worker
	envTDJournal = "MFTIKO_TD_JOURNAL"     // where that worker appends its pid
	envTDShim    = "MFTIKO_TEST_SHIM"      // "1": this process is the shim
	readyInst    = "td-1"
	readyAPIKey  = "test-key" // the fake venue accepts only this key
	readySecret  = "secret-ready-9f3c1a7e-b04d"
	readyToken   = "tok-ready-0123456789abcdef0123456789ab"
)

func init() {
	testMainExtra = func() (int, bool) {
		switch {
		case os.Getenv(envTDWorker) == "1":
			return runAccountWorker(), true
		case os.Getenv(envTDShim) == "1":
			return shim.Run(logger.NewWithWriter("mftiko-shim", os.Stderr)), true
		default:
			return 0, false
		}
	}
}

// runAccountWorker is the real account worker against a fake venue. The TD agent re-execs this
// test binary; the private stream is served in this process.
func runAccountWorker() int {
	if p := os.Getenv(envTDJournal); p != "" {
		if f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err == nil {
			_ = json.NewEncoder(f).Encode(map[string]any{"event": "start", "pid": os.Getpid()})
			f.Close()
		}
	}
	v := exchangetest.NewAccountVenue(exchangetest.AccountOptions{Key: readyAPIKey})
	srv := httptest.NewServer(v.Handler())
	defer srv.Close()
	kind := v.Kind("Fake")
	open := kind.Account
	kind.Account = func(cfg exchange.AccountConfig) (exchange.Account, error) {
		cfg.StreamEndpoint = "ws" + strings.TrimPrefix(srv.URL, "http")
		return open(cfg)
	}
	log := logger.New("mftiko-td-account")
	fd := os.NewFile(3, "ctl")
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	return account.Serve(ctx, fd, account.ServeConfig{
		Kinds: func(n string) (exchange.AccountKind, bool) { return kind, n == "Fake" },
		Doer:  v,
		Connect: func(context.Context, *tdctlv1.NatsCredentials, string) (*account.Env, error) {
			return &account.Env{Bus: nopBus{}, Instruments: anyInstrument{}}, nil
		},
		Log:        &log,
		RetryAfter: 50 * time.Millisecond, RetryMax: 200 * time.Millisecond,
	})
}

type nopBus struct{}

func (nopBus) Publish(string, []byte) error { return nil }
func (nopBus) Respond(string, func([]byte) []byte) (func(), error) {
	return func() {}, nil
}

type anyInstrument struct{}

func (anyInstrument) Reverse(context.Context, symbol.ExchKey, bool) (*symbol.Info, error) {
	return &symbol.Info{UID: 7, ContractSize: 1}, nil
}

func (anyInstrument) GetByUID(context.Context, uint32, bool) (*symbol.Info, error) {
	return &symbol.Info{UID: 7, ContractSize: 1}, nil
}

type fakeKinds map[string]exchange.AccountKind

func (k fakeKinds) AccountKind(n string) (exchange.AccountKind, bool) {
	x, ok := k[n]
	return x, ok
}

// TestAPISealedAccountReachesReady is E10-D067: the API seals an account, the TD agent opens it with
// the instance private key and checks api_id and kind, and the account worker reaches READY on the
// fake venue. The API response, both logs, and the agent work directory contain neither the
// credential plaintext nor the sealed blob.
func TestAPISealedAccountReachesReady(t *testing.T) {
	ctx := t.Context()
	s := etcdtest.Start(t)
	topo := state.Topology{
		Regions:   map[string]state.Region{"tw": {Endpoints: s.Endpoints()}},
		Instances: map[string]string{readyInst: "tw", state.ControllerInstance: "tw"},
	}
	planes := map[string]state.Plane{readyInst: state.PlaneTD}
	tables, err := state.AuthTables(topo, planes)
	if err != nil {
		t.Fatal(err)
	}
	root := s.Client(t)
	if err := state.EnsureAuth(ctx, root, tables["tw"], etcdtest.Password); err != nil {
		t.Fatal(err)
	}
	key, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	dep, err := json.Marshal(struct {
		state.Topology
		Planes map[string]state.Plane `json:"planes"`
	}{Topology: topo, Planes: planes})
	if err != nil {
		t.Fatal(err)
	}
	conf, err := json.Marshal(map[string]any{
		"listen":            "127.0.0.1:0",
		"unavailable_after": "2s",
		"shutdown_timeout":  "1s",
		"repositories":      []string{"mftiko/strategies"},
		"users":             []any{map[string]string{"name": "op", "token_sha256": tokenHash(readyToken)}},
		"td_keys":           map[string]string{readyInst: key.Public().String()},
	})
	if err != nil {
		t.Fatal(err)
	}
	apiLog := &safeBuf{}
	cfg, err := api.Load(bytes.NewReader(dep), bytes.NewReader(conf), api.Input{
		EtcdPassword: etcdtest.Password(state.UserAPI),
		CookieKey:    "cookie-key",
		PostgresDSN:  "postgres://mftiko@127.0.0.1:1/mftiko?sslmode=disable",
		Mirror: state.MirrorOptions{
			ProbeInterval: 50 * time.Millisecond,
			MinBackoff:    20 * time.Millisecond,
			MaxBackoff:    200 * time.Millisecond,
		},
		Log: logger.NewWithWriter("mftiko-api", apiLog).Level(zerolog.InfoLevel),
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Kinds = []api.Kind{{
		Name: "Fake", Label: "Fake", Trades: []string{"Fake"}, KeyTypes: []string{"hmac"},
	}}
	apiCtx, stopAPI := context.WithCancel(context.Background())
	apiErr := make(chan error, 1)
	go func() { apiErr <- api.Run(apiCtx, cfg) }()
	t.Cleanup(func() {
		stopAPI()
		select {
		case err := <-apiErr:
			if err != nil && !t.Failed() {
				t.Errorf("api: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("api did not stop")
		}
		if t.Failed() {
			t.Logf("api log:\n%s", apiLog.String())
		}
	})
	var addr string
	until(t, 30*time.Second, "the API listens", func() error {
		select {
		case err := <-apiErr:
			apiErr <- err
			return fmt.Errorf("api exited: %v\n%s", err, apiLog.String())
		default:
		}
		got, ok := listenAddr(apiLog.String())
		if !ok {
			return errors.New("no listen line")
		}
		addr = got
		return nil
	})
	base := "http://" + addr
	until(t, 30*time.Second, "the region is ready", func() error {
		var body readyJSON
		code, raw := getJSON(t, base+"/readyz", &body)
		if code != http.StatusOK || len(body.UnavailableRegions) != 0 {
			return fmt.Errorf("status %d %s", code, raw)
		}
		return nil
	})

	createBody := `{"name":"desk","kind":"Fake","td_instance":"` + readyInst + `","credentials":{"key_type":"hmac","key":"` + readyAPIKey + `","secret":"` + readySecret + `"}}`
	code, raw := httpDo(t, http.MethodPost, base+"/api/v1/accounts", readyToken, createBody)
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, raw)
	}
	var created struct {
		APIID int64 `json:"api_id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.APIID < 1 {
		t.Fatalf("created %s %v", raw, err)
	}
	bodies := []string{string(raw)}

	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wd, err := os.MkdirTemp("/tmp", "td-api-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(wd) })
	journal := filepath.Join(t.TempDir(), "starts")
	agentLog := &safeBuf{}
	log := logger.NewWithWriter("mftiko-td-agent", agentLog).Level(zerolog.InfoLevel)
	var mu sync.Mutex
	var pids []int
	agentCfg := tdagent.Config{
		Topology: topo, Instance: readyInst, Dial: state.AuthDial(readyInst, etcdtest.Password(readyInst)),
		Term:      state.TermOptions{ID: t.Name()},
		WorkDir:   wd,
		Shim:      self,
		Account:   self,
		ShimEnv:   []string{envTDShim + "=1"},
		WorkerEnv: []string{envTDWorker + "=1", envTDJournal + "=" + journal},
		Kinds: fakeKinds{"Fake": {Name: "Fake", Trades: []string{"Fake"}, Account: func(exchange.AccountConfig) (exchange.Account, error) {
			return nil, nil
		}}},
		Endpoints: map[string]tdagent.Endpoint{"Fake": {Stream: "ws://replaced-by-the-worker"}},
		Open: func(sealed []byte, id int64, kind string) error {
			_, err := sealbox.OpenCredentials(key, sealed, id, kind)
			return err
		},
		Unseal: func(sealed []byte, id int64, kind string) (sealbox.Credentials, error) {
			return sealbox.OpenCredentials(key, sealed, id, kind)
		},
		User:   func(int64) (string, string, string, error) { return "jwt", "seed", "_INBOX_td_42", nil },
		Policy: tdagent.Policy{Backoff: platagent.Backoff{Min: 50 * time.Millisecond, Max: 200 * time.Millisecond}, Beat: 10 * time.Second, GiveUpAfter: 2},
		Linger: 500 * time.Millisecond, Resync: 25 * time.Millisecond, StaleAfter: 5 * time.Second,
		Mirror:  state.MirrorOptions{ProbeInterval: 50 * time.Millisecond, Log: &log},
		Release: "test",
		Log:     log,
	}
	agentCfg.Started = func(pid int) {
		mu.Lock()
		pids = append(pids, pid)
		mu.Unlock()
	}
	agentCtx, stopAgent := context.WithCancel(context.Background())
	agentDone := make(chan error, 1)
	go func() { agentDone <- tdagent.Run(agentCtx, agentCfg) }()
	t.Cleanup(func() {
		mu.Lock()
		got := append([]int(nil), pids...)
		mu.Unlock()
		for _, pid := range append(got, journalPIDs(journal)...) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		stopAgent()
		select {
		case err := <-agentDone:
			if err != nil && !errors.Is(err, context.Canceled) && !t.Failed() {
				t.Errorf("agent: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Error("agent did not stop")
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", agentLog.String())
		}
	})

	demand, err := proto.Marshal(&statev1.DemandEntry{Target: &statev1.DemandEntry_ApiId{ApiId: created.APIID}})
	if err != nil {
		t.Fatal(err)
	}
	putCtx, cancelPut := context.WithTimeout(ctx, 5*time.Second)
	_, err = root.Put(putCtx, state.TDDemandKey(readyInst, "s1", created.APIID), string(demand))
	cancelPut()
	if err != nil {
		t.Fatal(err)
	}

	until(t, 45*time.Second, "ack READY", func() error {
		getCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := root.Get(getCtx, state.TDAckKey(readyInst, "s1", created.APIID))
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return errors.New("no ack")
		}
		st := &statev1.EntryStatus{}
		if err := proto.Unmarshal(resp.Kvs[0].Value, st); err != nil {
			return err
		}
		if st.GetState() == statev1.EntryState_ENTRY_STATE_READY && st.GetReason() == "" {
			return nil
		}
		return fmt.Errorf("state %s reason %q", st.GetState(), st.GetReason())
	})

	getCtx, cancelGet := context.WithTimeout(ctx, 5*time.Second)
	stored, err := root.Get(getCtx, state.AccountKey(readyInst, created.APIID))
	cancelGet()
	if err != nil || len(stored.Kvs) != 1 {
		t.Fatalf("stored account: %v", err)
	}
	rec := &statev1.Account{}
	if err := proto.Unmarshal(stored.Kvs[0].Value, rec); err != nil {
		t.Fatal(err)
	}
	opened, err := sealbox.OpenCredentials(key, rec.GetSealedCredentials(), created.APIID, "Fake")
	if err != nil || opened.Key != readyAPIKey || opened.Secret != readySecret || opened.KeyType != "hmac" {
		t.Fatalf("opened %+v %v", opened, err)
	}

	code, got := httpDo(t, http.MethodGet, fmt.Sprintf("%s/api/v1/accounts/%d", base, created.APIID), readyToken, "")
	if code != http.StatusOK {
		t.Fatalf("get %d %s", code, got)
	}
	bodies = append(bodies, string(got))
	code, listed := httpDo(t, http.MethodGet, base+"/api/v1/accounts", readyToken, "")
	if code != http.StatusOK {
		t.Fatalf("list %d %s", code, listed)
	}
	bodies = append(bodies, string(listed))

	visible := strings.Join(append(bodies, apiLog.String(), agentLog.String(), treeText(wd)), "\n")
	for _, plain := range []string{readyAPIKey, readySecret} {
		if strings.Contains(visible, plain) {
			t.Fatal("API response, log, or work directory contains credential plaintext")
		}
	}
	for _, enc := range sealedEncodings(rec.GetSealedCredentials()) {
		if strings.Contains(visible, enc) {
			t.Fatal("API response, log, or work directory contains sealed credentials")
		}
	}
	scanCtx, cancelScan := context.WithTimeout(ctx, 5*time.Second)
	dump, err := root.Get(scanCtx, state.Root+"/", clientv3.WithPrefix())
	cancelScan()
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range dump.Kvs {
		blob := string(kv.Key) + "\n" + string(kv.Value)
		if strings.Contains(blob, readySecret) || strings.Contains(blob, readyAPIKey) {
			t.Fatalf("etcd %s contains credential plaintext", kv.Key)
		}
	}
}

func httpDo(t *testing.T, method, url, token, body string) (int, []byte) {
	t.Helper()
	var r io.Reader
	if body != "" || method != http.MethodGet {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if method != http.MethodGet {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b
}

func sealedEncodings(sealed []byte) []string {
	return []string{
		string(sealed),
		base64.StdEncoding.EncodeToString(sealed),
		base64.RawStdEncoding.EncodeToString(sealed),
		base64.URLEncoding.EncodeToString(sealed),
		base64.RawURLEncoding.EncodeToString(sealed),
		hex.EncodeToString(sealed),
	}
}

func treeText(root string) string {
	var b strings.Builder
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		b.WriteString(path)
		b.WriteByte('\n')
		b.Write(raw)
		b.WriteByte('\n')
		return nil
	})
	return b.String()
}

func journalPIDs(path string) []int {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []int
	dec := json.NewDecoder(f)
	for {
		var line struct {
			PID int `json:"pid"`
		}
		if err := dec.Decode(&line); err != nil {
			return out
		}
		if line.PID > 0 {
			out = append(out, line.PID)
		}
	}
}
