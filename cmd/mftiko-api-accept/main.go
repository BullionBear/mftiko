// Command mftiko-api-accept drives the end-to-end acceptance of E10 against a running stack (E10-19,
// epic #12): accounts, sessions and their status through mftiko-api only, with the controller, the
// three agents and two API replicas behind it. deploy/accept/README.md is the runbook. It is a dev
// tool: it gives every etcd user one password and writes the API token next to its config.
//
//	mftiko-api-accept setup    -out DIR                    etcd users in both regions, auth on, deployment and API JSON
//	mftiko-api-accept main     -creds FILE                 account, session, RUNNING, log on disk, end, W5, audit
//	mftiko-api-accept ticker                               an unknown ticker ends REJECTED with its reason
//	mftiko-api-accept invalid                              bad specs are refused and nothing is written
//	mftiko-api-accept absent   create|resume               writes while the controller is gone, then it catches up
//	mftiko-api-accept region   before|down|up              one region's etcd stops; the other keeps serving
//	mftiko-api-accept replicas                             the same writes on two replicas at once
//	mftiko-api-accept cli      main|faults|inputs|region STEP  the same, with the mftiko CLI as the only client (E11-09, E15-07)
//
// The flows talk to replica a (-api) and replica b (-api-b). Every request they send is checked
// against the audit table at the end of the flow. Every subcommand exits 1 when its check fails.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

var (
	etcdURL  = flag.String("etcd", envOr("MFTIKO_ETCD", "http://127.0.0.1:2379"), "etcd endpoint of region accept (default $MFTIKO_ETCD)")
	etcd2URL = flag.String("etcd2", envOr("MFTIKO_ETCD2", "http://etcd2:2379"), "etcd endpoint of region accept2 (default $MFTIKO_ETCD2)")
	password = flag.String("etcd-password", os.Getenv("MFTIKO_ETCD_PASSWORD"), "password of root and of the users setup creates (default $MFTIKO_ETCD_PASSWORD)")
	apiA     = flag.String("api", envOr("MFTIKO_API", "http://127.0.0.1:8080"), "API replica a (default $MFTIKO_API)")
	apiB     = flag.String("api-b", envOr("MFTIKO_API_B", "http://127.0.0.1:8081"), "API replica b (default $MFTIKO_API_B)")
	tokenAt  = flag.String("token", envOr("MFTIKO_API_TOKEN_FILE", "/work/e2e/token"), "file holding the API token (default $MFTIKO_API_TOKEN_FILE)")
	dbURL    = flag.String("database", os.Getenv("DATABASE_URL"), "Postgres DSN, for the audit check (default $DATABASE_URL)")
	stateDir = flag.String("state", "/work/e2e", "where two-step flows keep the session they made")
)

// The deployment every flow assumes; setup writes it.
const (
	region      = "accept"
	region2     = "accept2"
	mdInst      = "dev"
	tdInst      = "td1"
	stsInst     = "sts1"
	sts2Inst    = "sts2"
	strategyID  = "hello"
	strategyTag = "accept"
	// strategyRepository is the pack path accept.sh pushes (registry:5000/strategies/hello).
	strategyRepository = "registry:5000/strategies/" + strategyID
	userName           = "accept"
)

var commands = map[string]func() error{
	"setup": setup, "main": mainFlow, "ticker": tickerFlow, "invalid": invalidFlow,
	"absent": absentFlow, "region": regionFlow, "replicas": replicasFlow, "cli": cliFlow,
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	name := os.Args[1]
	if name == "-version" || name == "version" {
		fmt.Println(buildinfo.String())
		return
	}
	run, ok := commands[name]
	if !ok {
		usage()
	}
	os.Args = append(os.Args[:1], os.Args[2:]...)
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-api-accept "+name+":", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mftiko-api-accept setup|main|ticker|invalid|absent|region|replicas [flags]")
	os.Exit(2)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// stamp is the wall clock every printed line starts with.
func stamp() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func say(format string, args ...any) { fmt.Printf(stamp()+" "+format+"\n", args...) }

// sent is one request a flow made, as the server named it (X-Request-Id, E10-D053).
type sent struct {
	id, method, path string
	status           int
}

// recorder is the HTTP client of both replicas. It keeps every request's id for the audit check.
type recorder struct {
	hc   *http.Client
	mu   sync.Mutex
	sent []sent
}

func (r *recorder) Do(req *http.Request) (*http.Response, error) {
	resp, err := r.hc.Do(req)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.sent = append(r.sent, sent{id: resp.Header.Get("X-Request-Id"), method: req.Method, path: req.URL.Path, status: resp.StatusCode})
	r.mu.Unlock()
	return resp, nil
}

func (r *recorder) all() []sent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sent(nil), r.sent...)
}

// flow is what every API flow uses: a client per replica, sharing one recorder.
type flow struct {
	rec  *recorder
	a, b *apiv1.ClientWithResponses
	ctx  context.Context
}

func newFlow(ctx context.Context) (*flow, error) {
	raw, err := os.ReadFile(*tokenAt)
	if err != nil {
		return nil, fmt.Errorf("token: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	rec := &recorder{hc: &http.Client{Timeout: 20 * time.Second}}
	auth := apiv1.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	})
	a, err := apiv1.NewClientWithResponses(strings.TrimRight(*apiA, "/")+"/api/v1", apiv1.WithHTTPClient(rec), auth)
	if err != nil {
		return nil, err
	}
	b, err := apiv1.NewClientWithResponses(strings.TrimRight(*apiB, "/")+"/api/v1", apiv1.WithHTTPClient(rec), auth)
	if err != nil {
		return nil, err
	}
	return &flow{rec: rec, a: a, b: b, ctx: ctx}, nil
}

// until polls check every second until it returns nil, and fails with its last error after d, or
// at once when check returns stop(err).
func until(ctx context.Context, d time.Duration, what string, check func() error) error {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		err := check()
		if err == nil {
			return nil
		}
		if isStop(err) {
			return fmt.Errorf("%s: %w", what, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: not within %s: %w", what, d, err)
		case <-tick.C:
		}
	}
}

// statusOf names a response for an error message.
func statusOf(resp *http.Response, body []byte) error {
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// remember and recall keep a session id between the steps of absent and region.
func remember(name, id string) error {
	if err := os.MkdirAll(*stateDir, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*stateDir, name), []byte(id+"\n"), 0o600)
}

func recall(name string) (string, error) {
	b, err := os.ReadFile(filepath.Join(*stateDir, name))
	if err != nil {
		return "", fmt.Errorf("no %s from the earlier step: %w", name, err)
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", errors.New(name + " is empty")
	}
	return id, nil
}
