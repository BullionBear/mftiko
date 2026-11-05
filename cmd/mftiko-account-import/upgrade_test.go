package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/api"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/pkg/logger"
)

const upgradeToken = "tok-upgrade-0123456789abcdef0123456789"

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

// TestUpgradeRehearsal is the runbook (docs/runbook.md §2.10) on an embedded deployment: a pre-E21
// account on two TD instances and a closed session that traded on it. After export, import and an
// API start on the new schema with the raised td_replica_grace:
//   - /accounts has the same api_id with the operator's uid, and the API logs no orphan etcd account;
//   - /session-records?api_id= still finds the old session (provenance is kept);
//   - td-1 kept its key, so its replica and etcd bytes are left alone;
//   - td-2's published key differs from the one the account was sealed to, so the API opens the
//     imported AEAD and reseals to the new key: that is the path every later session create takes;
//   - the etcd accounts survive a few reconciles, so a rollback still has them.
//
// Restarted with the default grace, the API then drops both replicas (E21-D078).
func TestUpgradeRehearsal(t *testing.T) {
	env := startEtcd(t)
	pg := startPG(t)
	k1, k2old, k2new, transfer := mustKey(t), mustKey(t), mustKey(t), mustKey(t)
	const id = int64(41)
	s1 := putOld(t, env.root, inst1, id, "scylla", "BinanceUM", k1.Public(), 0)
	putOld(t, env.root, inst2, id, "scylla", "BinanceUM", k2old.Public(), 0)
	ctx := t.Context()
	for inst, k := range map[string]*sealbox.PrivateKey{inst1: k1, inst2: k2new} {
		if _, err := env.root.Put(ctx, state.PubkeyKey(inst), k.Public().String()); err != nil {
			t.Fatal(err)
		}
	}
	// The old session, as an E10 API wrote it, closed long ago; 0012 filled api_ids.
	spec, err := proto.Marshal(&sessionv1.SessionSpec{
		Name:     "hello-scylla",
		Accounts: []*sessionv1.AccountDecl{{Name: "scylla", ApiId: id, TdInstance: inst1, AlgoIds: []uint32{1}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(spec)
	old := time.Now().Add(-72 * time.Hour).UTC()
	if _, err := pg.conn.Exec(ctx, `
INSERT INTO session_records (id, session_id, sts_instance, spec_bytes, spec_sha256, state, desired_state, created_at, ended_at, closed_at, api_ids)
VALUES ($1::uuid, 'ae9113', 'sts-jp-1', $2, $3, 'closed', 'ended', $4, $4, $4, session_spec_api_ids($2))`,
		uuid.NewString(), spec, sum[:], old); err != nil {
		t.Fatal(err)
	}

	f1, _ := runExport(t, env, inst1, k1, transfer.Public())
	f2, _ := runExport(t, env, inst2, k2old, transfer.Public())
	tkey := filepath.Join(t.TempDir(), "transfer.key")
	if err := sealbox.WriteKeyFile(tkey, transfer); err != nil {
		t.Fatal(err)
	}
	if out, err := runImport(t, pg, tkey, map[string]string{strconv.FormatInt(id, 10): "uid-77"}, true, f1, f2); err != nil {
		t.Fatalf("import: %v\n%s", err, out)
	}

	// The runbook raises td_replica_grace for the upgrade window (E21-D078).
	base, apiLog, stopAPI := startAPI(t, env, pg, "8760h")

	var accounts struct {
		Items []struct {
			APIID    int64  `json:"api_id"`
			Name     string `json:"name"`
			UID      string `json:"exch_account_uid"`
			Exchange string `json:"exchange"`
		} `json:"items"`
	}
	getAuthed(t, base+"/api/v1/accounts", &accounts)
	if len(accounts.Items) != 1 || accounts.Items[0].APIID != id || accounts.Items[0].UID != "uid-77" || accounts.Items[0].Exchange != "binance" {
		t.Fatalf("accounts %+v", accounts.Items)
	}
	var records struct {
		Items []struct {
			SessionID string `json:"session_id"`
		} `json:"items"`
	}
	getAuthed(t, base+"/api/v1/session-records?api_id="+strconv.FormatInt(id, 10), &records)
	if len(records.Items) != 1 || records.Items[0].SessionID != "ae9113" {
		t.Fatalf("session records %+v", records.Items)
	}

	pollUntil(t, 30*time.Second, "td-2 resealed to its new key", func() error {
		resp, err := env.root.Get(ctx, state.AccountKey(inst2, id))
		if err != nil || len(resp.Kvs) != 1 {
			return fmt.Errorf("get: %v", err)
		}
		var rec statev1.Account
		if err := proto.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
			return err
		}
		got, err := sealbox.OpenCredentials(k2new, rec.SealedCredentials, id, "BinanceUM")
		if err != nil {
			return err
		}
		if got != creds {
			return fmt.Errorf("resealed credentials differ")
		}
		return nil
	})
	// Let a few reconcile passes run, expiry included.
	time.Sleep(5 * reconcileEvery)
	var rep1 []byte
	if err := pg.conn.QueryRow(ctx, `SELECT sealed_credentials FROM account_replicas WHERE td_instance = $1 AND api_id = $2`, inst1, id).Scan(&rep1); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rep1, s1) {
		t.Fatal("td-1 replica was rewritten although its key did not change")
	}
	if !etcdHas(t, env, inst1, id) {
		t.Fatal("td-1 etcd account is gone within the raised grace; a rollback would lose it")
	}
	logs := apiLog.String()
	if strings.Contains(logs, "etcd account has no replica") {
		t.Fatalf("API warned about an imported account:\n%s", logs)
	}
	noSecret(t, "API log", []byte(logs))

	// With the default grace the old session ended long ago, so the replica goes at once.
	// That is why the runbook raises it until a rollback is ruled out.
	stopAPI()
	startAPI(t, env, pg, "")
	pollUntil(t, 30*time.Second, "unreferenced replicas expire under the default grace", func() error {
		if etcdHas(t, env, inst1, id) || etcdHas(t, env, inst2, id) {
			return fmt.Errorf("etcd accounts are still there")
		}
		var n int
		if err := pg.conn.QueryRow(ctx, `SELECT count(*) FROM account_replicas`).Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("%d replica rows", n)
		}
		return nil
	})
}

func etcdHas(t *testing.T, env etcdEnv, inst string, id int64) bool {
	t.Helper()
	resp, err := env.root.Get(t.Context(), state.AccountKey(inst, id))
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Kvs) == 1
}

const reconcileEvery = 200 * time.Millisecond

func startAPI(t *testing.T, env etcdEnv, pg pgEnv, grace string) (string, *lockedBuf, func()) {
	t.Helper()
	dep, err := json.Marshal(struct {
		state.Topology
		Planes map[string]state.Plane `json:"planes"`
	}{Topology: env.topo, Planes: map[string]state.Plane{inst1: state.PlaneTD, inst2: state.PlaneTD}})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(upgradeToken))
	fields := map[string]any{
		"listen":            "127.0.0.1:0",
		"unavailable_after": "2s",
		"shutdown_timeout":  "1s",
		"allow":             []string{"*"},
		"users":             []any{map[string]string{"name": "op", "token_sha256": hex.EncodeToString(sum[:])}},
	}
	if grace != "" {
		fields["td_replica_grace"] = grace
	}
	conf, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	apiLog := &lockedBuf{}
	cfg, err := api.Load(bytes.NewReader(dep), bytes.NewReader(conf), api.Input{
		EtcdPassword: etcdtest.Password(state.UserAPI),
		CookieKey:    "cookie-key",
		AccountKeys:  testKeys,
		PostgresDSN:  pg.dsn,
		Mirror: state.MirrorOptions{
			ProbeInterval: 50 * time.Millisecond,
			MinBackoff:    20 * time.Millisecond,
			MaxBackoff:    200 * time.Millisecond,
		},
		Log: logger.NewWithWriter("mftiko-api", buildinfo.Version, buildinfo.CommitHash, apiLog).Level(zerolog.InfoLevel),
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Kinds = []api.Kind{{Name: "BinanceUM", Label: "Binance UM", Trades: []string{"BinanceUM"}, KeyTypes: []string{"hmac", "ed25519"}, Exchange: "binance"}}
	cfg.ReconcileEvery = reconcileEvery
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- api.Run(runCtx, cfg) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Error("api did not stop")
			}
			if t.Failed() {
				t.Logf("api log:\n%s", apiLog.String())
			}
		})
	}
	t.Cleanup(stop)
	var addr string
	pollUntil(t, 30*time.Second, "the API listens", func() error {
		for _, line := range strings.Split(apiLog.String(), "\n") {
			var ev struct {
				Addr string `json:"addr"`
				Msg  string `json:"message"`
			}
			if json.Unmarshal([]byte(line), &ev) == nil && ev.Msg == "listen" && ev.Addr != "" {
				addr = ev.Addr
				return nil
			}
		}
		return fmt.Errorf("no listen line")
	})
	base := "http://" + addr
	pollUntil(t, 30*time.Second, "the API is ready", func() error {
		resp, err := http.Get(base + "/readyz")
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("readyz %d", resp.StatusCode)
		}
		return nil
	})
	return base, apiLog, stop
}

func getAuthed(t *testing.T, url string, out any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+upgradeToken)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, b)
	}
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatalf("%s: %v", b, err)
	}
}

func pollUntil(t *testing.T, within time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited %s for %s: %v", within, what, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
