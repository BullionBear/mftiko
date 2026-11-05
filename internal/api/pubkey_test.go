package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestPubkeyUnavailable(t *testing.T) {
	f := newAccountFix(t, startAccountsDB(t))
	if _, err := f.env.servers["tw"].Client(t).Delete(t.Context(), state.PubkeyKey("td-tw-1")); err != nil {
		t.Fatal(err)
	}
	until(t, 10*time.Second, "pubkey gone", func() error {
		f.srv.pubMu.Lock()
		defer f.srv.pubMu.Unlock()
		sl := f.srv.pubs["td-tw-1"]
		if sl == nil || sl.state != pubMissing {
			return errors.New("not yet")
		}
		return nil
	})

	code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("n", "Binance", "td-tw-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	err := f.srv.ensureSessionReplicas(t.Context(), &sessionv1.SessionSpec{
		Accounts: []*sessionv1.AccountDecl{{Name: "a", ApiId: created.ApiId, TdInstance: "td-tw-1"}},
	})
	var missing *pubkeyUnavailable
	if !errors.As(err, &missing) || missing.inst != "td-tw-1" {
		t.Fatalf("project: %v", err)
	}
	if n := f.replicaCount(t, created.ApiId); n != 0 {
		t.Fatalf("replica rows %d", n)
	}
	if f.env.get(t, "tw", state.AccountKey("td-tw-1", created.ApiId)) != nil {
		t.Fatal("missing pubkey wrote etcd")
	}
	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", withField(accountBody("n2", "Binance", "td-tw-1", "hmac", "k", "s", "", nil), "td_instance", "td-tw-1"))
	if code != http.StatusBadRequest || fieldReason(t, body, "/td_instance") != "is not accepted" {
		t.Fatalf("td_instance %d %s", code, body)
	}
}

func TestPubkeyUnreadable(t *testing.T) {
	f := newAccountFix(t, startAccountsDB(t))
	code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("desk", "Binance", "td-tw-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, body)
	}
	want := f.keys["td-tw-1"].Public().SHA256()
	if got := f.fingerprint(t, "td-tw-1"); !bytes.Equal(got, want[:]) {
		t.Fatalf("fingerprint %x", got)
	}
	key := state.PubkeyKey("td-tw-1")
	if _, err := f.env.servers["tw"].Client(t).Put(t.Context(), key, "zz"); err != nil {
		t.Fatal(err)
	}
	until(t, 10*time.Second, "unreadable pubkey", func() error {
		f.srv.pubMu.Lock()
		defer f.srv.pubMu.Unlock()
		sl := f.srv.pubs["td-tw-1"]
		if sl == nil || sl.state != pubBad {
			return errors.New("not yet")
		}
		return nil
	})
	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("desk2", "Binance", "td-tw-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create while pubkey is unreadable %d %s", code, body)
	}
	var second apiv1.Account
	if err := json.Unmarshal(body, &second); err != nil {
		t.Fatal(err)
	}
	err := f.srv.ensureSessionReplicas(t.Context(), &sessionv1.SessionSpec{
		Accounts: []*sessionv1.AccountDecl{{Name: "a", ApiId: second.ApiId, TdInstance: "td-tw-1"}},
	})
	var missing *pubkeyUnavailable
	if !errors.As(err, &missing) || missing.inst != "td-tw-1" {
		t.Fatalf("project: %v", err)
	}
	if n := f.replicaCount(t, second.ApiId); n != 0 {
		t.Fatalf("replica rows %d", n)
	}
	if got := f.fingerprint(t, "td-tw-1"); !bytes.Equal(got, want[:]) {
		t.Fatalf("fingerprint changed to %x", got)
	}
	if log := f.log.String(); !strings.Contains(log, "td pubkey is unreadable") || strings.Contains(log, "zz") {
		t.Fatalf("log %s", log)
	}
}

func TestPubkeyFromEtcd(t *testing.T) {
	dsn := startAccountsDB(t)
	env := newWriteEnv(t)
	etcdKey, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	const inst = "td-tw-1"
	keyName := state.PubkeyKey(inst)
	if _, err := env.servers["tw"].Client(t).Put(t.Context(), keyName, etcdKey.Public().String()); err != nil {
		t.Fatal(err)
	}
	f := startAccountFix(t, dsn, env)
	until(t, 10*time.Second, "etcd pubkey cached", func() error {
		f.srv.pubMu.Lock()
		defer f.srv.pubMu.Unlock()
		sl := f.srv.pubs[inst]
		if sl == nil || sl.state != pubOK || sl.key != etcdKey.Public() {
			return errors.New("not yet")
		}
		return nil
	})
	code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("desk", "Binance", inst, "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if created.Revision != 1 {
		t.Fatalf("revision %d", created.Revision)
	}
	f.project(t, created.ApiId, inst)
	opened := f.stored(t, inst, created.ApiId)
	if _, err := sealbox.OpenCredentials(etcdKey, opened.GetSealedCredentials(), created.ApiId, "Binance"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(f.log.String(), "td pubkey differs") {
		t.Fatalf("log %s", f.log.String())
	}
	if strings.Contains(f.metrics(t), `mftiko_td_pubkey_changed_total{`) {
		t.Fatal("first adopt incremented the counter")
	}
	sum := etcdKey.Public().SHA256()
	if got := f.fingerprint(t, inst); !bytes.Equal(got, sum[:]) {
		t.Fatalf("fingerprint %x", got)
	}
	rev, updated := f.accountStamp(t, created.ApiId)

	next, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.servers["tw"].Client(t).Put(t.Context(), keyName, next.Public().String()); err != nil {
		t.Fatal(err)
	}
	until(t, 10*time.Second, "resealed to the new pubkey", func() error {
		kv := f.env.get(t, "tw", state.AccountKey(inst, created.ApiId))
		if kv == nil {
			return errors.New("missing projection")
		}
		rec := &statev1.Account{}
		if err := proto.Unmarshal(kv.Value, rec); err != nil {
			return err
		}
		_, err := sealbox.OpenCredentials(next, rec.GetSealedCredentials(), created.ApiId, "Binance")
		return err
	})
	if gotRev, gotAt := f.accountStamp(t, created.ApiId); gotRev != rev || !gotAt.Equal(updated) {
		t.Fatalf("stamp rev %d -> %d updated %s -> %s", rev, gotRev, updated, gotAt)
	}
	sum = next.Public().SHA256()
	if got := f.fingerprint(t, inst); !bytes.Equal(got, sum[:]) {
		t.Fatalf("fingerprint %x", got)
	}
	if !strings.Contains(f.log.String(), "td pubkey changed") {
		t.Fatalf("log %s", f.log.String())
	}
	sample := `mftiko_td_pubkey_changed_total{td_instance="td-tw-1"} 1`
	if !strings.Contains(f.metrics(t), sample+"\n") {
		t.Fatalf("metrics %s", f.metrics(t))
	}

	if _, err := env.servers["tw"].Client(t).Put(t.Context(), keyName, next.Public().String()+"\n"); err != nil {
		t.Fatal(err)
	}
	until(t, 10*time.Second, "newline cached", func() error {
		f.srv.pubMu.Lock()
		defer f.srv.pubMu.Unlock()
		sl := f.srv.pubs[inst]
		if sl == nil || sl.raw != next.Public().String()+"\n" {
			return errors.New("not yet")
		}
		return nil
	})
	if !strings.Contains(f.metrics(t), sample+"\n") || strings.Contains(f.metrics(t), `mftiko_td_pubkey_changed_total{td_instance="td-tw-1"} 2`) {
		t.Fatalf("metrics %s", f.metrics(t))
	}
	if got := f.fingerprint(t, inst); !bytes.Equal(got, sum[:]) {
		t.Fatalf("newline changed fingerprint to %x", got)
	}
}

func problemBody(t *testing.T, body []byte) apiv1.Problem {
	t.Helper()
	var p apiv1.Problem
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("%s: %v", body, err)
	}
	return p
}

func (f *accountFix) replicaCount(t *testing.T, id int64) int {
	t.Helper()
	pool, err := f.srv.db()
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM account_replicas WHERE api_id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (f *accountFix) fingerprint(t *testing.T, inst string) []byte {
	t.Helper()
	pool, err := f.srv.db()
	if err != nil {
		t.Fatal(err)
	}
	var fp []byte
	if err := pool.QueryRow(t.Context(), `SELECT fingerprint FROM td_pubkeys WHERE td_instance = $1`, inst).Scan(&fp); err != nil {
		t.Fatal(err)
	}
	return fp
}

func (f *accountFix) accountStamp(t *testing.T, id int64) (int64, time.Time) {
	t.Helper()
	pool, err := f.srv.db()
	if err != nil {
		t.Fatal(err)
	}
	var rev int64
	var updated time.Time
	if err := pool.QueryRow(t.Context(), `SELECT revision, updated_at FROM accounts WHERE api_id = $1`, id).Scan(&rev, &updated); err != nil {
		t.Fatal(err)
	}
	return rev, updated
}

func (f *accountFix) metrics(t *testing.T) string {
	t.Helper()
	resp, err := http.Get(f.base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics %d %s", resp.StatusCode, b)
	}
	return string(b)
}
