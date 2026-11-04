package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/migrations"
)

func testKinds() []Kind {
	return []Kind{
		{Name: "Binance", Label: "Binance Spot", Trades: []string{"Binance"}, KeyTypes: []string{"hmac", "ed25519"}, Exchange: "binance"},
		{Name: "BinanceUM", Label: "Binance USDⓈ-M", Trades: []string{"BinanceUM"}, KeyTypes: []string{"hmac", "ed25519"}, Exchange: "binance"},
		{Name: "BinancePM", Label: "Binance Portfolio Margin", Trades: []string{"Binance", "BinanceUM", "BinanceCM"}, KeyTypes: []string{"hmac", "ed25519"}, Exchange: "binance"},
		{Name: "Okx", Label: "OKX", Trades: []string{"Okx"}, KeyTypes: []string{"hmac"}, Passphrase: true, Exchange: "okx"},
		{Name: "Bybit", Label: "Bybit", Trades: []string{"Bybit"}, KeyTypes: []string{"hmac"}, Exchange: "bybit"},
	}
}

func startAccountsDB(t *testing.T) string {
	t.Helper()
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	if _, err := migrations.Up(ctx, srv.DSN()); err != nil {
		t.Fatal(err)
	}
	return srv.DSN()
}

type accountFix struct {
	env    *writeEnv
	srv    *Server
	keys   map[string]*sealbox.PrivateKey
	base   string
	client *http.Client
	log    *lockedBuf
}

func newAccountFix(t *testing.T, dsn string) *accountFix {
	t.Helper()
	return startAccountFix(t, dsn, newWriteEnv(t))
}

func startAccountFix(t *testing.T, dsn string, env *writeEnv) *accountFix {
	t.Helper()
	keys := map[string]*sealbox.PrivateKey{}
	pubs := map[string]sealbox.PublicKey{}
	for inst := range env.cfg.TDKeys {
		k, err := sealbox.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		keys[inst] = k
		pubs[inst] = k.Public()
	}
	cfg := env.cfg
	cfg.TDKeys = pubs
	cfg.Users = []User{{Name: "ada", Token: sha256.Sum256([]byte(tokenAda))}}
	cfg.Kinds = testKinds()
	cfg.ReconcileEvery = time.Hour
	if dsn != "" {
		cfg.PostgresDSN = dsn
	}
	buf := env.log
	r := startRunning(t, cfg, buf)
	until(t, 30*time.Second, "both regions available", func() error {
		if r.stopped() {
			return errors.New("server exited")
		}
		body, _ := r.s.readiness(time.Now())
		if len(body.UnavailableRegions) != 0 {
			return fmt.Errorf("unavailable: %v", body.UnavailableRegions)
		}
		return nil
	})
	return &accountFix{
		env: env, srv: r.s, keys: keys, base: "http://" + r.s.Addr(),
		client: &http.Client{Timeout: 5 * time.Second}, log: buf,
	}
}

func (f *accountFix) call(t *testing.T, method, path, body string) (int, http.Header, []byte) {
	t.Helper()
	ct := ""
	if method != http.MethodGet {
		ct = "application/json"
	}
	return (apiCall{method: method, path: path, bearer: tokenAda, contentType: ct, body: body}).do(t, f.client, f.base)
}

func (f *accountFix) waitAccount(t *testing.T, id int64) apiv1.Account {
	t.Helper()
	var got apiv1.Account
	until(t, 10*time.Second, fmt.Sprintf("account %d visible", id), func() error {
		code, _, body := f.call(t, http.MethodGet, fmt.Sprintf("/api/v1/accounts/%d", id), "")
		if code != http.StatusOK {
			return fmt.Errorf("%d", code)
		}
		if err := json.Unmarshal(body, &got); err != nil {
			return err
		}
		return nil
	})
	return got
}

func (f *accountFix) stored(t *testing.T, inst string, id int64) *statev1.Account {
	t.Helper()
	key := state.AccountKey(inst, id)
	kv := f.env.get(t, f.env.region(t, key), key)
	if kv == nil {
		t.Fatalf("missing %s", key)
	}
	rec := &statev1.Account{}
	if err := proto.Unmarshal(kv.Value, rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

func accountBody(name, kind, inst, keyType, key, secret, pass string, cancel *int) string {
	_ = inst
	cred := map[string]any{"key_type": keyType, "key": key, "secret": secret}
	if pass != "" {
		cred["passphrase"] = pass
	}
	m := map[string]any{
		"name": name, "kind": kind, "exch_account_uid": "uid-1", "credentials": cred,
	}
	if cancel != nil {
		m["settings"] = map[string]any{"cancel_on_disconnect_s": *cancel}
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func withField(body, key string, value any) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		panic(err)
	}
	m[key] = value
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// project seals the account onto inst the way session create does.
func (f *accountFix) project(t *testing.T, id int64, inst string) {
	t.Helper()
	spec := &sessionv1.SessionSpec{
		Accounts: []*sessionv1.AccountDecl{{Name: "a", ApiId: id, TdInstance: inst}},
	}
	if err := f.srv.ensureSessionReplicas(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
}

func fieldReason(t *testing.T, body []byte, path string) string {
	t.Helper()
	var p struct {
		Errors []apiv1.FieldError `json:"errors"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("%s: %v", body, err)
	}
	for _, e := range p.Errors {
		if e.Path == path {
			return e.Reason
		}
	}
	t.Fatalf("no %s in %s", path, body)
	return ""
}

func TestTDInstanceNeedsPublicKey(t *testing.T) {
	s := &Server{cfg: Config{
		Topology: state.Topology{Instances: map[string]string{"td-tw-1": "tw", "md-tw-1": "tw"}},
		Planes:   map[string]state.Plane{"td-tw-1": state.PlaneTD, "md-tw-1": state.PlaneMD},
	}}
	s.pubs = map[string]*pubkeySlot{"td-tw-1": {state: pubMissing}}
	_, err := s.pubkeyFor(context.Background(), "td-tw-1")
	var missing *pubkeyUnavailable
	if !errors.As(err, &missing) || missing.inst != "td-tw-1" {
		t.Fatalf("missing key: %v", err)
	}
	k, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	s.cfg.TDKeys = map[string]sealbox.PublicKey{"td-tw-1": k.Public()}
	s.pubs["td-tw-1"] = &pubkeySlot{state: pubBad}
	_, err = s.pubkeyFor(context.Background(), "td-tw-1")
	if !errors.As(err, &missing) {
		t.Fatalf("unreadable etcd value fell back to td_keys: %v", err)
	}
	if _, _, fields := s.classifyInstance("/td_instance", "md-tw-1"); len(fields) != 1 || fields[0].Reason != "is not a td instance" {
		t.Fatalf("md: %+v", fields)
	}
}

func TestAccounts(t *testing.T) {
	f := newAccountFix(t, startAccountsDB(t))
	secret := "acct-secret-9f3c1a7e-b04d"
	key := "acct-key-6d2b8e01-77aa"

	code, _, body := f.call(t, http.MethodGet, "/api/v1/account-kinds", "")
	if code != http.StatusOK {
		t.Fatalf("kinds %d %s", code, body)
	}
	var kinds apiv1.AccountKindList
	if err := json.Unmarshal(body, &kinds); err != nil {
		t.Fatal(err)
	}
	byKind := map[string]apiv1.AccountKind{}
	var prev string
	for _, k := range kinds.Items {
		if k.Kind < prev {
			t.Fatalf("kinds not sorted: %s then %s", prev, k.Kind)
		}
		prev = k.Kind
		byKind[k.Kind] = k
	}
	if byKind["BinancePM"].Supported || !byKind["BinanceUM"].Supported || !byKind["Okx"].Passphrase || byKind["Bybit"].Passphrase {
		t.Fatalf("kinds %+v", byKind)
	}

	neg := -1
	huge := int(mathMaxUint32()) + 1
	bad := []struct {
		name, body, path, reason string
	}{
		{"missing credentials", `{"name":"n","kind":"Binance","exch_account_uid":"uid-1"}`, "/credentials", "required"},
		{"missing uid", `{"name":"n","kind":"Binance","credentials":{"key_type":"hmac","key":"` + key + `","secret":"` + secret + `"}}`, "/exch_account_uid", "required"},
		{"blank uid", `{"name":"n","kind":"Binance","exch_account_uid":"  ","credentials":{"key_type":"hmac","key":"` + key + `","secret":"` + secret + `"}}`, "/exch_account_uid", "required"},
		{"long uid", `{"name":"n","kind":"Binance","exch_account_uid":"` + strings.Repeat("u", 65) + `","credentials":{"key_type":"hmac","key":"` + key + `","secret":"` + secret + `"}}`, "/exch_account_uid", "must be 1 to 64 characters"},
		{"td_instance rejected", withField(accountBody("n", "Binance", "td-tw-1", "hmac", key, secret, "", nil), "td_instance", "td-tw-1"), "/td_instance", "is not accepted"},
		{"unknown kind", accountBody("n", "Nope", "td-tw-1", "hmac", key, secret, "", nil), "/kind", "is not accepted"},
		{"portfolio margin", accountBody("n", "BinancePM", "td-tw-1", "hmac", key, secret, "", nil), "/kind", "is not accepted"},
		{"key type", accountBody("n", "Bybit", "td-tw-1", "ed25519", key, secret, "", nil), "/credentials/key_type", "is not accepted"},
		{"empty key", accountBody("n", "Binance", "td-tw-1", "hmac", "", secret, "", nil), "/credentials/key", "required"},
		{"empty secret", accountBody("n", "Binance", "td-tw-1", "hmac", key, "", "", nil), "/credentials/secret", "required"},
		{"passphrase required", accountBody("n", "Okx", "td-tw-1", "hmac", key, secret, "", nil), "/credentials/passphrase", "required"},
		{"passphrase forbidden", accountBody("n", "Binance", "td-tw-1", "hmac", key, secret, "nope", nil), "/credentials/passphrase", "is not used by this account kind"},
		{"empty name", accountBody("", "Binance", "td-tw-1", "hmac", key, secret, "", nil), "/name", "must be 1 to 128 characters"},
		{"long name", accountBody(strings.Repeat("名", 129), "Binance", "td-tw-1", "hmac", key, secret, "", nil), "/name", "must be 1 to 128 characters"},
		{"cancel negative", accountBody("n", "Binance", "td-tw-1", "hmac", key, secret, "", &neg), "/settings/cancel_on_disconnect_s", "must be between 0 and 4294967295"},
		{"cancel overflow", accountBody("n", "Binance", "td-tw-1", "hmac", key, secret, "", &huge), "/settings/cancel_on_disconnect_s", "must be between 0 and 4294967295"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("%d %s", code, body)
			}
			mustProblem(t, body, http.StatusBadRequest, "validation_failed")
			if got := fieldReason(t, body, tc.path); got != tc.reason {
				t.Fatalf("reason %q", got)
			}
			if strings.Contains(string(body), secret) || strings.Contains(string(body), key) {
				t.Fatalf("error echoed a credential: %s", body)
			}
		})
	}

	f.srv.ids = apiSeq(2, 10, 11, 12)
	code, hdr, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("desk", "Binance", "td-tw-1", "hmac", key, secret, "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if created.ApiId != 2 || created.Revision != 1 || hdr.Get("Location") != "/api/v1/accounts/2" {
		t.Fatalf("created %+v location %s", created, hdr.Get("Location"))
	}
	if created.Exchange == nil || *created.Exchange != "binance" || created.ExchAccountUid == nil || *created.ExchAccountUid != "uid-1" || created.DeletedAt != nil {
		t.Fatalf("created fields %+v", created)
	}
	if strings.Contains(string(body), secret) || strings.Contains(string(body), key) ||
		bytes.Contains(body, []byte("fingerprint")) || bytes.Contains(body, []byte("credentials")) ||
		bytes.Contains(body, []byte("td_instance")) || bytes.Contains(body, []byte(`"region"`)) ||
		!bytes.Contains(body, []byte(`"referenced_by":[]`)) || !bytes.Contains(body, []byte(`"cancel_on_disconnect_s":0`)) {
		t.Fatalf("create body %s", body)
	}
	if f.env.get(t, "tw", state.AccountKey("td-tw-1", 2)) != nil {
		t.Fatal("account create wrote etcd")
	}
	f.project(t, 2, "td-tw-1")
	rec := f.stored(t, "td-tw-1", 2)
	if !rec.GetEnabled() {
		t.Fatal("enabled was not set")
	}
	opened, err := sealbox.OpenCredentials(f.keys["td-tw-1"], rec.GetSealedCredentials(), 2, "Binance")
	if err != nil || opened.Key != key || opened.Secret != secret || opened.KeyType != "hmac" {
		t.Fatalf("opened %+v %v", opened, err)
	}
	if strings.Contains(f.log.String(), secret) || strings.Contains(f.log.String(), key) {
		t.Fatal("log has a credential")
	}

	f.waitAccount(t, 2)
	pass := "acct-pass-c91e4f20-okx"
	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("desk", "Okx", "td-jp-1", "hmac", key, secret, pass, nil))
	if code != http.StatusConflict {
		t.Fatalf("duplicate name %d %s", code, body)
	}
	mustProblem(t, body, http.StatusConflict, "conflict")
	if got := fieldReason(t, body, "/name"); got != "already in use" || strings.Contains(string(body), pass) {
		t.Fatalf("duplicate %s", body)
	}

	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("other", "Bybit", "td-tw-1", "hmac", key, secret, "", nil))
	if code != http.StatusCreated {
		t.Fatalf("second %d %s", code, body)
	}
	var second apiv1.Account
	if err := json.Unmarshal(body, &second); err != nil || second.ApiId != 10 {
		t.Fatalf("second %+v %v", second, err)
	}
	f.waitAccount(t, second.ApiId)
	var page apiv1.AccountList
	code, _, body = f.call(t, http.MethodGet, "/api/v1/accounts?limit=1", "")
	if code != http.StatusOK || json.Unmarshal(body, &page) != nil || len(page.Items) != 1 || page.Items[0].ApiId != 2 || page.NextCursor == nil {
		t.Fatalf("page %d %s", code, body)
	}
	if !page.Freshness.ControllerAbsent || len(page.Freshness.UnavailableRegions) != 0 {
		t.Fatalf("freshness %+v", page.Freshness)
	}
	code, _, body = f.call(t, http.MethodGet, "/api/v1/accounts?limit=1&cursor="+*page.NextCursor, "")
	page = apiv1.AccountList{}
	if code != http.StatusOK || json.Unmarshal(body, &page) != nil || len(page.Items) != 1 || page.Items[0].ApiId != 10 || page.NextCursor != nil {
		t.Fatalf("next page %d %s", code, body)
	}
	code, _, body = f.call(t, http.MethodDelete, fmt.Sprintf("/api/v1/accounts/10?revision=%d", second.Revision+1), "")
	if code != http.StatusConflict || fieldReason(t, body, "/revision") != "does not match" {
		t.Fatalf("stale delete %d %s", code, body)
	}
	if f.waitAccount(t, 10).ApiId != 10 {
		t.Fatal("stale delete removed the account")
	}
	code, _, body = f.call(t, http.MethodGet, "/api/v1/accounts?td_instance=md-tw-1", "")
	if code != http.StatusBadRequest || fieldReason(t, body, "/td_instance") != "is not accepted" {
		t.Fatalf("filter %d %s", code, body)
	}

	sealed := append([]byte(nil), rec.GetSealedCredentials()...)
	code, _, body = f.call(t, http.MethodPut, "/api/v1/accounts/2", fmt.Sprintf(`{"revision":%d,"name":"desk-2"}`, created.Revision))
	if code != http.StatusOK {
		t.Fatalf("rename %d %s", code, body)
	}
	var renamed apiv1.Account
	if err := json.Unmarshal(body, &renamed); err != nil || renamed.Name != "desk-2" || renamed.Revision == created.Revision {
		t.Fatalf("renamed %+v %v", renamed, err)
	}
	rec = f.stored(t, "td-tw-1", 2)
	if !bytes.Equal(rec.GetSealedCredentials(), sealed) {
		t.Fatal("omitted credentials were resealed")
	}
	code, _, body = f.call(t, http.MethodPut, "/api/v1/accounts/2", fmt.Sprintf(`{"revision":%d,"name":"desk-2"}`, renamed.Revision))
	if code != http.StatusOK {
		t.Fatalf("noop %d %s", code, body)
	}
	var noop apiv1.Account
	if err := json.Unmarshal(body, &noop); err != nil || noop.Revision != renamed.Revision {
		t.Fatalf("noop wrote: %+v %v", noop, err)
	}
	code, _, body = f.call(t, http.MethodPut, "/api/v1/accounts/2", fmt.Sprintf(`{"revision":%d,"exch_account_uid":"uid-2"}`, noop.Revision))
	if code != http.StatusBadRequest || fieldReason(t, body, "/exch_account_uid") != "cannot be changed" {
		t.Fatalf("uid change %d %s", code, body)
	}
	code, _, body = f.call(t, http.MethodPut, "/api/v1/accounts/2", fmt.Sprintf(`{"revision":%d,"exch_account_uid":" uid-1 "}`, noop.Revision))
	if code != http.StatusOK {
		t.Fatalf("uid same %d %s", code, body)
	}
	var sameUID apiv1.Account
	if err := json.Unmarshal(body, &sameUID); err != nil || sameUID.Revision != noop.Revision || sameUID.ExchAccountUid == nil || *sameUID.ExchAccountUid != "uid-1" {
		t.Fatalf("trimmed uid %+v %v", sameUID, err)
	}
	code, _, body = f.call(t, http.MethodPut, "/api/v1/accounts/2", fmt.Sprintf(`{"revision":%d,"name":"desk-2"}`, renamed.Revision-1))
	if code != http.StatusConflict || fieldReason(t, body, "/revision") != "does not match" {
		t.Fatalf("conflict %d %s", code, body)
	}

	newSecret := "acct-secret-replaced-88aa"
	code, _, body = f.call(t, http.MethodPut, "/api/v1/accounts/2", fmt.Sprintf(
		`{"revision":%d,"credentials":{"key_type":"ed25519","key":"%s","secret":"%s"}}`, renamed.Revision, key, newSecret))
	if code != http.StatusOK {
		t.Fatalf("reseal %d %s", code, body)
	}
	var resealed apiv1.Account
	if err := json.Unmarshal(body, &resealed); err != nil || resealed.Name != "desk-2" || resealed.Revision == renamed.Revision {
		t.Fatalf("reseal response %+v %v", resealed, err)
	}
	rec = f.stored(t, "td-tw-1", 2)
	if bytes.Equal(rec.GetSealedCredentials(), sealed) {
		t.Fatal("credentials were not resealed")
	}
	opened, err = sealbox.OpenCredentials(f.keys["td-tw-1"], rec.GetSealedCredentials(), 2, "Binance")
	if err != nil || opened.Secret != newSecret || opened.KeyType != "ed25519" {
		t.Fatalf("reopened %+v %v", opened, err)
	}
	if strings.Contains(string(body), newSecret) || strings.Contains(f.log.String(), newSecret) {
		t.Fatal("reseal leaked")
	}

	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("referenced", "BinanceUM", "td-tw-1", "hmac", key, secret, "", nil))
	if code != http.StatusCreated {
		t.Fatalf("referenced create %d %s", code, body)
	}
	var refAcct apiv1.Account
	if err := json.Unmarshal(body, &refAcct); err != nil {
		t.Fatal(err)
	}
	specRaw, err := proto.Marshal(&sessionv1.SessionSpec{
		Name: "s", Accounts: []*sessionv1.AccountDecl{{Name: "a", ApiId: refAcct.ApiId, TdInstance: "td-tw-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	specKey := state.SessionSpecKey("sts-tw-1", "abc123")
	putKey(t, t.Context(), f.env.servers["tw"], specKey, string(specRaw))
	seen(t, f.srv, "tw", specKey)
	code, _, body = f.call(t, http.MethodDelete, fmt.Sprintf("/api/v1/accounts/%d?revision=%d", refAcct.ApiId, refAcct.Revision), "")
	if code != http.StatusConflict {
		t.Fatalf("delete referenced %d %s", code, body)
	}
	mustProblem(t, body, http.StatusConflict, "referenced")
	got := f.waitAccount(t, refAcct.ApiId)
	if len(got.ReferencedBy) != 1 || got.ReferencedBy[0] != "abc123" {
		t.Fatalf("referenced_by %+v", got.ReferencedBy)
	}
	statusRaw, err := proto.Marshal(&statev1.SessionStatus{Phase: statev1.Phase_PHASE_DONE})
	if err != nil {
		t.Fatal(err)
	}
	statusKey := state.SessionStatusKey("sts-tw-1", "abc123")
	putKey(t, t.Context(), f.env.servers["tw"], statusKey, string(statusRaw))
	seen(t, f.srv, "tw", statusKey)
	current := f.waitAccount(t, refAcct.ApiId)
	if len(current.ReferencedBy) != 0 {
		t.Fatalf("terminal still referenced: %+v", current.ReferencedBy)
	}
	code, _, body = f.call(t, http.MethodDelete, fmt.Sprintf("/api/v1/accounts/%d?revision=%d", refAcct.ApiId, current.Revision), "")
	if code != http.StatusNoContent {
		t.Fatalf("delete %d %s", code, body)
	}
	until(t, 10*time.Second, "account gone", func() error {
		code, _, body := f.call(t, http.MethodGet, fmt.Sprintf("/api/v1/accounts/%d", refAcct.ApiId), "")
		if code == http.StatusNotFound {
			mustProblem(t, body, http.StatusNotFound, "not_found")
			return nil
		}
		return fmt.Errorf("%d", code)
	})
	code, _, body = f.call(t, http.MethodGet, fmt.Sprintf("/api/v1/accounts/%d?include_deleted=true", refAcct.ApiId), "")
	var deleted apiv1.Account
	if code != http.StatusOK || json.Unmarshal(body, &deleted) != nil || deleted.DeletedAt == nil || deleted.ApiId != refAcct.ApiId {
		t.Fatalf("include_deleted %d %s", code, body)
	}
	code, _, body = f.call(t, http.MethodGet, "/api/v1/accounts?include_deleted=true", "")
	page = apiv1.AccountList{}
	if code != http.StatusOK || json.Unmarshal(body, &page) != nil {
		t.Fatalf("list deleted %d %s", code, body)
	}
	var sawDeleted bool
	for _, item := range page.Items {
		if item.ApiId == refAcct.ApiId {
			sawDeleted = item.DeletedAt != nil
		}
	}
	if !sawDeleted {
		t.Fatalf("deleted account missing from list %s", body)
	}
	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("referenced", "BinanceUM", "td-tw-1", "hmac", key, secret, "", nil))
	if code != http.StatusCreated {
		t.Fatalf("reuse name %d %s", code, body)
	}
	var reused apiv1.Account
	if err := json.Unmarshal(body, &reused); err != nil || reused.ApiId == refAcct.ApiId || reused.Name != "referenced" {
		t.Fatalf("reused %+v %v", reused, err)
	}

	putKey(t, t.Context(), f.env.servers["tw"], state.AgentKey(state.PlaneCtl, state.ControllerInstance), "agent")
	seen(t, f.srv, "tw", state.AgentKey(state.PlaneCtl, state.ControllerInstance))
	code, _, body = f.call(t, http.MethodGet, "/api/v1/accounts?limit=1", "")
	if code != http.StatusOK || json.Unmarshal(body, &page) != nil || page.Freshness.ControllerAbsent {
		t.Fatalf("controller present %+v %s", page.Freshness, body)
	}

	code, _, body = f.call(t, http.MethodGet, "/api/v1/accounts/999999", "")
	if code != http.StatusNotFound {
		t.Fatalf("missing %d %s", code, body)
	}
}

func mathMaxUint32() uint64 { return 4294967295 }

func TestAccountRegionDown(t *testing.T) {
	f := newAccountFix(t, startAccountsDB(t))
	f.srv.ids = apiSeq(4, 5, 6)
	code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("jp", "Binance", "td-jp-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create jp %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	f.waitAccount(t, created.ApiId)
	f.project(t, created.ApiId, "td-jp-1")
	f.env.proxy.Cut()
	until(t, 15*time.Second, "jp unavailable", func() error {
		got, _ := f.srv.readiness(time.Now())
		if len(got.UnavailableRegions) != 1 || got.UnavailableRegions[0] != "jp" {
			return fmt.Errorf("%v", got.UnavailableRegions)
		}
		return nil
	})
	code, _, body = f.call(t, http.MethodGet, fmt.Sprintf("/api/v1/accounts/%d", created.ApiId), "")
	var got apiv1.Account
	if code != http.StatusOK || json.Unmarshal(body, &got) != nil || got.ApiId != created.ApiId || got.Name != "jp" {
		t.Fatalf("get down %d %s", code, body)
	}
	code, _, body = f.call(t, http.MethodPut, fmt.Sprintf("/api/v1/accounts/%d", created.ApiId), fmt.Sprintf(`{"revision":%d,"name":"jp-2"}`, created.Revision))
	if code != http.StatusServiceUnavailable {
		t.Fatalf("update down %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "region_unavailable")
	if again := f.waitAccount(t, created.ApiId); again.Name != "jp" || again.Revision != created.Revision {
		t.Fatalf("down update wrote %+v", again)
	}
	code, _, body = f.call(t, http.MethodGet, "/api/v1/accounts?td_instance=td-jp-1", "")
	if code != http.StatusBadRequest || fieldReason(t, body, "/td_instance") != "is not accepted" {
		t.Fatalf("filter down %d %s", code, body)
	}
	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("again", "Binance", "td-jp-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create while a replica region is down %d %s", code, body)
	}
	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("tw-still", "Binance", "td-tw-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create tw %d %s", code, body)
	}
	var tw apiv1.Account
	if err := json.Unmarshal(body, &tw); err != nil || tw.ApiId == created.ApiId {
		t.Fatalf("tw %+v %v", tw, err)
	}
}

func TestAccountLeak(t *testing.T) {
	conn, dsn := applyAccountDB(t)
	f := newAccountFix(t, dsn)
	key := "acct-key-6d2b8e01-77aa"
	secret := "acct-secret-9f3c1a7e-b04d"
	pass := "acct-pass-c91e4f20-okx"
	code, hdr, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("okx-desk", "Okx", "td-tw-1", "hmac", key, secret, pass, nil))
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	f.project(t, created.ApiId, "td-tw-1")
	row := waitAuditID(t, conn, hdr.Get(headerRequestID))
	rec := f.stored(t, "td-tw-1", created.ApiId)
	var aead, sealedCol []byte
	if err := conn.QueryRow(t.Context(), `
SELECT a.credentials, r.sealed_credentials
FROM accounts a
JOIN account_replicas r ON r.api_id = a.api_id
WHERE a.api_id = $1 AND r.td_instance = 'td-tw-1'`, created.ApiId).Scan(&aead, &sealedCol); err != nil {
		t.Fatal(err)
	}
	plain, err := f.srv.cfg.AccountKeys.Open(created.ApiId, "Okx", aead)
	if err != nil {
		t.Fatal(err)
	}
	var creds statev1.AccountCredentials
	if err := proto.Unmarshal(plain, &creds); err != nil || creds.GetSecret() != secret || creds.GetKey() != key || creds.GetPassphrase() != pass {
		t.Fatal("stored aead did not open to the credential")
	}
	clear(plain)
	if !bytes.Equal(sealedCol, rec.GetSealedCredentials()) {
		t.Fatal("etcd sealbox is not the stored blob")
	}
	opened, err := sealbox.OpenCredentials(f.keys["td-tw-1"], rec.GetSealedCredentials(), created.ApiId, "Okx")
	if err != nil || opened.Key != key || opened.Secret != secret || opened.Passphrase != pass {
		t.Fatalf("opened %+v %v", opened, err)
	}
	if got := f.waitAccount(t, created.ApiId); got.Name != "okx-desk" {
		t.Fatalf("visible %+v", got)
	}
	code, _, gotBody := f.call(t, http.MethodGet, fmt.Sprintf("/api/v1/accounts/%d", created.ApiId), "")
	if code != http.StatusOK {
		t.Fatalf("get %d %s", code, gotBody)
	}
	code, _, listBody := f.call(t, http.MethodGet, "/api/v1/accounts", "")
	if code != http.StatusOK {
		t.Fatalf("list %d %s", code, listBody)
	}
	badCode, _, badBody := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("okx-desk", "Okx", "td-tw-1", "hmac", key, secret, "", nil))
	if badCode != http.StatusBadRequest {
		t.Fatalf("bad %d %s", badCode, badBody)
	}
	plains := []string{key, secret, pass}
	cipher := append(sealedEncodings(rec.GetSealedCredentials()), sealedEncodings(aead)...)
	visible := []string{string(body), string(gotBody), string(listBody), string(badBody), f.log.String()}
	auditBlob := auditBlob(t, conn)
	for _, plain := range plains {
		for _, v := range visible {
			if strings.Contains(v, plain) {
				t.Fatalf("response or log contains credential plaintext")
			}
		}
		if strings.Contains(auditBlob, plain) {
			t.Fatal("audit contains credential plaintext")
		}
	}
	for _, enc := range cipher {
		for _, v := range append(visible, auditBlob) {
			if strings.Contains(v, enc) {
				t.Fatal("response, log, or audit contains sealed credentials")
			}
		}
	}
	if row.Body == nil || strings.Contains(*row.Body, secret) || strings.Contains(*row.Body, "credentials") {
		t.Fatalf("audit body %v", row.Body)
	}
	ctx := t.Context()
	for region, srv := range f.env.servers {
		resp, err := srv.Client(t).Get(ctx, state.Root+"/", clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		for _, kv := range resp.Kvs {
			blob := string(kv.Key) + "\n" + string(kv.Value)
			for _, plain := range plains {
				if strings.Contains(blob, plain) {
					t.Fatalf("etcd %s %s contains credential plaintext", region, kv.Key)
				}
			}
		}
	}
}

func applyAccountDB(t *testing.T) (*pgx.Conn, string) {
	t.Helper()
	dsn := startAccountsDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn, dsn
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

func auditBlob(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var b strings.Builder
	for _, row := range queryAudits(t, conn) {
		b.WriteString(row.Query)
		b.WriteString(row.Detail)
		b.WriteString(stringOr(row.Body))
	}
	return b.String()
}

func TestAccountProjection(t *testing.T) {
	f := newAccountFix(t, startAccountsDB(t))
	f.srv.ids = apiSeq(4)
	code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("desk", "Binance", "td-jp-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	f.project(t, created.ApiId, "td-jp-1")
	key := state.AccountKey("td-jp-1", created.ApiId)
	before := f.env.get(t, "jp", key)
	if before == nil {
		t.Fatal("missing projection")
	}
	ctx := t.Context()
	if _, err := f.env.servers["jp"].Client(t).Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	f.srv.reconcileOnce(context.Background())
	got := f.env.get(t, "jp", key)
	if got == nil || !bytes.Equal(got.Value, before.Value) {
		t.Fatal("reconcile rewrote the sealed projection")
	}

	code, _, body = f.call(t, http.MethodDelete, fmt.Sprintf("/api/v1/accounts/%d?revision=%d", created.ApiId, created.Revision), "")
	if code != http.StatusNoContent {
		t.Fatalf("delete %d %s", code, body)
	}
	if f.env.get(t, "jp", key) != nil {
		t.Fatal("soft delete left the etcd account")
	}
	putKey(t, ctx, f.env.servers["jp"], key, "stale")
	f.srv.reconcileOnce(context.Background())
	if f.env.get(t, "jp", key) != nil {
		t.Fatal("reconcile left a soft-deleted account in etcd")
	}
}

func TestAccountOrphanStays(t *testing.T) {
	f := newAccountFix(t, startAccountsDB(t))
	ctx := t.Context()
	orphan := state.AccountKey("td-jp-1", 99)
	putKey(t, ctx, f.env.servers["jp"], orphan, "leftover")
	seen(t, f.srv, "jp", orphan)
	f.srv.reconcileOnce(context.Background())
	if f.env.get(t, "jp", orphan) == nil {
		t.Fatal("reconcile deleted an etcd account that has no postgres row")
	}
	log := f.log.String()
	if !strings.Contains(log, "etcd account has no replica") || !strings.Contains(log, `"api_id":99`) {
		t.Fatalf("orphan log = %s", log)
	}
	if strings.Contains(log, "leftover") {
		t.Fatal("orphan log included the value")
	}
}

func TestAccountEtcdFailureThenReconcile(t *testing.T) {
	f := newAccountFix(t, startAccountsDB(t))
	f.srv.ids = apiSeq(6)
	code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("desk", "Binance", "td-jp-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	key := state.AccountKey("td-jp-1", created.ApiId)
	if f.env.get(t, "jp", key) != nil {
		t.Fatal("account create wrote etcd")
	}
	f.srv.hook = func(step string) error {
		if step == "account-before-etcd" {
			return errRegionUnavailable
		}
		return nil
	}
	spec := &sessionv1.SessionSpec{
		Accounts: []*sessionv1.AccountDecl{{Name: "a", ApiId: created.ApiId, TdInstance: "td-jp-1"}},
	}
	if err := f.srv.ensureSessionReplicas(t.Context(), spec); !errors.Is(err, errRegionUnavailable) {
		t.Fatalf("publish: %v", err)
	}
	if f.env.get(t, "jp", key) != nil {
		t.Fatal("failed publish still wrote etcd")
	}
	f.srv.hook = nil
	f.srv.reconcileOnce(context.Background())
	got := f.env.get(t, "jp", key)
	if got == nil {
		t.Fatal("reconcile did not restore the projection")
	}
	rec := &statev1.Account{}
	if err := proto.Unmarshal(got.Value, rec); err != nil || rec.GetName() != "desk" {
		t.Fatalf("restored %v %v", rec, err)
	}
}

func TestAccountIDCollisionRetries(t *testing.T) {
	dsn := startAccountsDB(t)
	f := newAccountFix(t, dsn)
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	if _, err := conn.Exec(ctx, `
		INSERT INTO accounts (
			api_id, kind, name, credentials, key_version, settings, enabled,
			exch_account_uid, created_at, updated_at, revision
		) VALUES (
			6, 'Binance', 'taken', decode(repeat('ab', 44), 'hex'), 1,
			'{"cancel_on_disconnect_s":0}', true, 'uid-9', now(), now(), 1)`); err != nil {
		t.Fatal(err)
	}
	putKey(t, ctx, f.env.servers["jp"], state.AccountKey("td-jp-1", 8), "garbage")
	f.srv.ids = apiSeq(6, 8)
	code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("desk", "Binance", "td-jp-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil || created.ApiId != 8 {
		t.Fatalf("created %+v %v", created, err)
	}
	if got := f.env.get(t, "jp", state.AccountKey("td-jp-1", 8)); got == nil || string(got.Value) != "garbage" {
		t.Fatal("account create replaced the colliding etcd key")
	}
	f.project(t, created.ApiId, "td-jp-1")
	got := f.env.get(t, "jp", state.AccountKey("td-jp-1", 8))
	if got == nil {
		t.Fatal("missing projection")
	}
	rec := &statev1.Account{}
	if err := proto.Unmarshal(got.Value, rec); err != nil || rec.GetName() != "desk" {
		t.Fatal("colliding etcd key was not replaced")
	}
}

func TestAccountCursorRejectsTag(t *testing.T) {
	f := newAccountFix(t, "")
	cursor := base64.RawURLEncoding.EncodeToString([]byte("latest"))
	code, _, body := f.call(t, http.MethodGet, "/api/v1/accounts?cursor="+cursor, "")
	if code != http.StatusBadRequest || fieldReason(t, body, "/cursor") != "is not a cursor from this API" {
		t.Fatalf("cursor %d %s", code, body)
	}
}
