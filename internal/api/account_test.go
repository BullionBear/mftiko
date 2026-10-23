package api

import (
	"bytes"
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
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func testKinds() []Kind {
	return []Kind{
		{Name: "Binance", Label: "Binance Spot", Trades: []string{"Binance"}, KeyTypes: []string{"hmac", "ed25519"}},
		{Name: "BinanceUM", Label: "Binance USDⓈ-M", Trades: []string{"BinanceUM"}, KeyTypes: []string{"hmac", "ed25519"}},
		{Name: "BinancePM", Label: "Binance Portfolio Margin", Trades: []string{"Binance", "BinanceUM", "BinanceCM"}, KeyTypes: []string{"hmac", "ed25519"}},
		{Name: "Okx", Label: "OKX", Trades: []string{"Okx"}, KeyTypes: []string{"hmac"}, Passphrase: true},
		{Name: "Bybit", Label: "Bybit", Trades: []string{"Bybit"}, KeyTypes: []string{"hmac"}},
	}
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
	env := newWriteEnv(t)
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
	cred := map[string]any{"key_type": keyType, "key": key, "secret": secret}
	if pass != "" {
		cred["passphrase"] = pass
	}
	m := map[string]any{"name": name, "kind": kind, "td_instance": inst, "credentials": cred}
	if cancel != nil {
		m["settings"] = map[string]any{"cancel_on_disconnect_s": *cancel}
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
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
	if _, fields := s.checkTDInstance("td-tw-1"); len(fields) != 1 || fields[0].Path != "/td_instance" || fields[0].Reason != "has no public key" {
		t.Fatalf("missing key: %+v", fields)
	}
	if _, _, fields := s.classifyInstance("md-tw-1"); len(fields) != 1 || fields[0].Reason != "is not a td instance" {
		t.Fatalf("md: %+v", fields)
	}
}

func TestAccounts(t *testing.T) {
	f := newAccountFix(t, "")
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
		{"missing credentials", `{"name":"n","kind":"Binance","td_instance":"td-tw-1"}`, "/credentials", "required"},
		{"unknown kind", accountBody("n", "Nope", "td-tw-1", "hmac", key, secret, "", nil), "/kind", "is not accepted"},
		{"portfolio margin", accountBody("n", "BinancePM", "td-tw-1", "hmac", key, secret, "", nil), "/kind", "is not accepted"},
		{"key type", accountBody("n", "Bybit", "td-tw-1", "ed25519", key, secret, "", nil), "/credentials/key_type", "is not accepted"},
		{"empty key", accountBody("n", "Binance", "td-tw-1", "hmac", "", secret, "", nil), "/credentials/key", "required"},
		{"empty secret", accountBody("n", "Binance", "td-tw-1", "hmac", key, "", "", nil), "/credentials/secret", "required"},
		{"passphrase required", accountBody("n", "Okx", "td-tw-1", "hmac", key, secret, "", nil), "/credentials/passphrase", "required"},
		{"passphrase forbidden", accountBody("n", "Binance", "td-tw-1", "hmac", key, secret, "nope", nil), "/credentials/passphrase", "is not used by this account kind"},
		{"unknown instance", accountBody("n", "Binance", "nope", "hmac", key, secret, "", nil), "/td_instance", "is not in the topology"},
		{"not a name", accountBody("n", "Binance", "TD", "hmac", key, secret, "", nil), "/td_instance", "is not an instance name"},
		{"md plane", accountBody("n", "Binance", "md-tw-1", "hmac", key, secret, "", nil), "/td_instance", "is not a td instance"},
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

	f.srv.ids = apiSeq(2, 10, 11)
	code, hdr, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("desk", "Binance", "td-tw-1", "hmac", key, secret, "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if created.ApiId != 2 || hdr.Get("Location") != "/api/v1/accounts/2" {
		t.Fatalf("created %+v location %s", created, hdr.Get("Location"))
	}
	if strings.Contains(string(body), secret) || strings.Contains(string(body), key) ||
		bytes.Contains(body, []byte("fingerprint")) || bytes.Contains(body, []byte("credentials")) ||
		!bytes.Contains(body, []byte(`"referenced_by":[]`)) || !bytes.Contains(body, []byte(`"cancel_on_disconnect_s":0`)) {
		t.Fatalf("create body %s", body)
	}
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
	if code != http.StatusBadRequest || fieldReason(t, body, "/td_instance") != "is not a td instance" {
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

	// A root write sets the backfill fields the API does not expose. A later name change keeps them.
	rec.Settings = &statev1.AccountSettings{BackfillIntervalS: 9, BackfillRequestedAtNs: 42, CancelOnDisconnectS: rec.GetSettings().GetCancelOnDisconnectS()}
	raw, err := proto.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	backRev := putKey(t, t.Context(), f.env.servers["tw"], state.AccountKey("td-tw-1", 2), string(raw))
	var back apiv1.Account
	until(t, 10*time.Second, "backfill revision", func() error {
		code, _, body := f.call(t, http.MethodGet, "/api/v1/accounts/2", "")
		if code != http.StatusOK {
			return fmt.Errorf("%d", code)
		}
		if err := json.Unmarshal(body, &back); err != nil {
			return err
		}
		if back.Revision < backRev {
			return errors.New("mirror has not reached the backfill write")
		}
		return nil
	})
	code, _, body = f.call(t, http.MethodPut, "/api/v1/accounts/2", fmt.Sprintf(`{"revision":%d,"name":"desk-3"}`, back.Revision))
	if code != http.StatusOK {
		t.Fatalf("keep backfill %d %s", code, body)
	}
	rec = f.stored(t, "td-tw-1", 2)
	if rec.GetName() != "desk-3" || rec.GetSettings().GetBackfillIntervalS() != 9 || rec.GetSettings().GetBackfillRequestedAtNs() != 42 {
		t.Fatalf("backfill lost: %+v", rec.GetSettings())
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
	f := newAccountFix(t, "")
	f.srv.ids = apiSeq(4)
	code, _, body := f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("jp", "Binance", "td-jp-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create jp %d %s", code, body)
	}
	var created apiv1.Account
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	f.waitAccount(t, created.ApiId)
	f.env.proxy.Cut()
	until(t, 15*time.Second, "jp unavailable", func() error {
		got, _ := f.srv.readiness(time.Now())
		if len(got.UnavailableRegions) != 1 || got.UnavailableRegions[0] != "jp" {
			return fmt.Errorf("%v", got.UnavailableRegions)
		}
		return nil
	})
	code, _, body = f.call(t, http.MethodGet, fmt.Sprintf("/api/v1/accounts/%d", created.ApiId), "")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("get down %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "region_unavailable")
	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("again", "Binance", "td-jp-1", "hmac", "k", "s", "", nil))
	if code != http.StatusServiceUnavailable {
		t.Fatalf("create down %d %s", code, body)
	}
	code, _, body = f.call(t, http.MethodGet, "/api/v1/accounts?td_instance=td-jp-1", "")
	var page apiv1.AccountList
	if code != http.StatusOK || json.Unmarshal(body, &page) != nil || len(page.Items) != 0 || len(page.Freshness.UnavailableRegions) != 1 {
		t.Fatalf("filter down %d %s", code, body)
	}
	code, _, body = f.call(t, http.MethodPost, "/api/v1/accounts", accountBody("tw-still", "Binance", "td-tw-1", "hmac", "k", "s", "", nil))
	if code != http.StatusCreated {
		t.Fatalf("create tw %d %s", code, body)
	}
}

func TestAccountLeak(t *testing.T) {
	conn, dsn := applyAuditDB(t)
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
	row := waitAuditID(t, conn, hdr.Get(headerRequestID))
	rec := f.stored(t, "td-tw-1", created.ApiId)
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
	cipher := sealedEncodings(rec.GetSealedCredentials())
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

func TestAccountCursorRejectsTag(t *testing.T) {
	f := newAccountFix(t, "")
	cursor := base64.RawURLEncoding.EncodeToString([]byte("latest"))
	code, _, body := f.call(t, http.MethodGet, "/api/v1/accounts?cursor="+cursor, "")
	if code != http.StatusBadRequest || fieldReason(t, body, "/cursor") != "is not a cursor from this API" {
		t.Fatalf("cursor %d %s", code, body)
	}
}
