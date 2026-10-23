package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/migrations"
)

var (
	stratDig = "sha256:" + strings.Repeat("ab", 32)
	dataDig  = "sha256:" + strings.Repeat("cd", 32)
)

type sessionEnv struct {
	e     *writeEnv
	s     *Server
	pg    *pgtest.Server
	clock *testClock
	cat   *fakeCatalog
}

type sessionOpt struct {
	clock       *testClock
	unavailable time.Duration
	dsn         string
}

func testCatalog() *fakeCatalog {
	return &fakeCatalog{
		digest: map[string]string{
			catKey("mftiko/strategies", "v1"): stratDig,
		},
		manifest: map[string]artifact.ManifestInfo{
			catKey("mftiko/strategies", stratDig): strategyManifest(stratDig, artifact.KindStrategy, artifact.SDK{}),
		},
		data: map[string]artifact.DataInfo{
			catKey("mftiko/data", dataDig): {Digest: dataDig, Config: artifact.DataConfig{Schema: artifact.DataSchema}},
		},
		readErr: map[string]error{},
		dataErr: map[string]error{},
	}
}

func applySchema(t *testing.T, dsn string) {
	t.Helper()
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, name := range []string{"0003_audit.up.sql", "0004_session_records.up.sql"} {
		b, err := migrations.FS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := pgtest.ExecSQL(ctx, conn, string(b)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func newSessionEnv(t *testing.T, opt sessionOpt) *sessionEnv {
	t.Helper()
	var pg *pgtest.Server
	dsn := opt.dsn
	if dsn == "" {
		pg = pgtest.Start(t)
		dsn = pg.DSN()
		applySchema(t, dsn)
	}
	e := newWriteEnv(t)
	e.cfg.PostgresDSN = dsn
	e.cfg.Users = []User{{Name: "ada", Token: sha256.Sum256([]byte(tokenAda))}}
	e.cfg.ReconcileEvery = time.Hour
	if opt.clock != nil {
		e.cfg.Now = opt.clock.Now
	}
	if opt.unavailable > 0 {
		e.cfg.UnavailableAfter = opt.unavailable
	}
	s := e.replica(t)
	cat := testCatalog()
	s.catalog = cat
	// Address is set after start so useCatalog does not dial. The response prefixes it (D1060).
	s.cfg.Registry.Address = "registry.example"
	se := &sessionEnv{e: e, s: s, pg: pg, clock: opt.clock, cat: cat}
	if pg != nil {
		for _, id := range []int64{42, 43} {
			key := state.AccountKey("td-tw-1", id)
			putKey(t, t.Context(), e.servers["tw"], key, "acct")
			seen(t, s, "tw", key)
		}
	}
	return se
}

func (se *sessionEnv) call(t *testing.T, method, path, body string, hdr http.Header) (int, http.Header, []byte) {
	t.Helper()
	c := apiCall{method: method, path: path, bearer: tokenAda, body: body, header: hdr}
	if body != "" {
		c.contentType = "application/json"
	}
	client := &http.Client{Timeout: 20 * time.Second}
	return c.do(t, client, "http://"+se.s.Addr())
}

func (se *sessionEnv) post(t *testing.T, body string, hdr http.Header) (int, http.Header, []byte) {
	t.Helper()
	return se.call(t, http.MethodPost, "/api/v1/sessions", body, hdr)
}

func minBody(sts string) string {
	return fmt.Sprintf(`{"name":"n","sts_instance":%q,"strategy":{"id":"strategies","ref":%q}}`, sts, stratDig)
}

func keyHeader(k string) http.Header {
	h := make(http.Header)
	h.Set("Idempotency-Key", k)
	return h
}

func mustSession(t *testing.T, code int, hdr http.Header, body []byte) apiv1.Session {
	t.Helper()
	if code != http.StatusCreated {
		t.Fatalf("status %d body %s", code, body)
	}
	var sess apiv1.Session
	if err := json.Unmarshal(body, &sess); err != nil {
		t.Fatalf("%s: %v", body, err)
	}
	if hdr.Get("Location") != "/api/v1/sessions/"+sess.Id {
		t.Fatalf("location %q id %s", hdr.Get("Location"), sess.Id)
	}
	if len(sess.Id) != 6 {
		t.Fatalf("id %q", sess.Id)
	}
	return sess
}

func hasPath(t *testing.T, body []byte, path string) {
	t.Helper()
	mustProblem(t, body, http.StatusBadRequest, "validation_failed")
	var p apiv1.Problem
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Errors == nil {
		t.Fatalf("no errors in %s", body)
	}
	for _, f := range *p.Errors {
		if f.Path == path {
			return
		}
	}
	t.Fatalf("no %s in %s", path, body)
}

func (se *sessionEnv) rows(t *testing.T) []sessionRecord {
	t.Helper()
	pool, err := se.s.db()
	if err != nil {
		t.Fatal(err)
	}
	rs, err := pool.Query(t.Context(), `SELECT `+recordCols+` FROM session_records ORDER BY created_at, id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var out []sessionRecord
	for rs.Next() {
		rec, err := scanRecord(rs)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rec)
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (se *sessionEnv) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	pool, err := se.s.db()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func (se *sessionEnv) spec(t *testing.T, sts, id string) []byte {
	t.Helper()
	region := "tw"
	if strings.Contains(sts, "jp") {
		region = "jp"
	}
	kv := se.e.get(t, region, state.SessionSpecKey(sts, id))
	if kv == nil {
		return nil
	}
	return kv.Value
}

func (se *sessionEnv) specCount(t *testing.T, sts string) int {
	t.Helper()
	region := "tw"
	if strings.Contains(sts, "jp") {
		region = "jp"
	}
	resp, err := se.e.servers[region].Client(t).Get(t.Context(), state.SessionSpecPrefix(sts), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	return len(resp.Kvs)
}

func (se *sessionEnv) waitValue(t *testing.T, region, key string, val []byte) {
	t.Helper()
	until(t, 10*time.Second, "mirror has "+key, func() error {
		view, _ := se.s.snapshot(region)
		entry, ok := view.Entries[key]
		if !ok || !bytes.Equal(entry.Value, val) {
			return errors.New("not the value yet")
		}
		return nil
	})
}

func (se *sessionEnv) putSpec(t *testing.T, sts, id string, spec *sessionv1.SessionSpec) []byte {
	t.Helper()
	raw, err := proto.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	region := "tw"
	if strings.Contains(sts, "jp") {
		region = "jp"
	}
	putKey(t, t.Context(), se.e.servers[region], state.SessionSpecKey(sts, id), string(raw))
	se.waitValue(t, region, state.SessionSpecKey(sts, id), raw)
	return raw
}

// afterMirror runs fn while the reconcile loop waits, then polls until the mirror kick finishes
// one pass. An extra reconcileOnce would count the same spec again (D1070).
func (se *sessionEnv) afterMirror(t *testing.T, fn func()) {
	t.Helper()
	pass := se.withReconcilePaused(t, fn)
	until(t, 10*time.Second, "reconcile after mirror change", func() error {
		if se.s.reconcilePasses.Load() <= pass {
			return errors.New("waiting")
		}
		return nil
	})
}

func (se *sessionEnv) withReconcilePaused(t *testing.T, fn func()) uint64 {
	t.Helper()
	se.s.reconcileMu.Lock()
	defer se.s.reconcileMu.Unlock()
	fn()
	return se.s.reconcilePasses.Load()
}

func (se *sessionEnv) dropSpec(t *testing.T, sts, id string) {
	t.Helper()
	region := "tw"
	if strings.Contains(sts, "jp") {
		region = "jp"
	}
	key := state.SessionSpecKey(sts, id)
	se.afterMirror(t, func() {
		if _, err := se.e.servers[region].Client(t).Delete(t.Context(), key); err != nil {
			t.Fatal(err)
		}
		until(t, 10*time.Second, "mirror dropped "+key, func() error {
			view, _ := se.s.snapshot(region)
			if _, ok := view.Entries[key]; ok {
				return errors.New("still present")
			}
			return nil
		})
	})
}

func TestSessionCreateRoundTrip(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	code, hdr, body := se.post(t, minBody("sts-tw-1"), nil)
	minimal := mustSession(t, code, hdr, body)
	if minimal.Region != "tw" || minimal.StsInstance != "sts-tw-1" || minimal.CreatedBy == nil || *minimal.CreatedBy != "ada" {
		t.Fatalf("identity %+v", minimal)
	}
	if minimal.CreatedAt == nil || minimal.Spec.DesiredState != apiv1.DesiredStateRunning {
		t.Fatalf("times %+v", minimal)
	}
	if string(minimal.Spec.Params) != "{}" || minimal.Spec.Strategy.Name != "hello" || minimal.Spec.Strategy.Digest != stratDig {
		t.Fatalf("spec %+v", minimal.Spec)
	}
	if minimal.Spec.Strategy.Repository != "registry.example/mftiko/strategies" || minimal.Spec.Strategy.Id == nil || *minimal.Spec.Strategy.Id != "strategies" {
		t.Fatalf("strategy %+v", minimal.Spec.Strategy)
	}
	if minimal.Spec.Restart.Mode == nil || *minimal.Spec.Restart.Mode != apiv1.OnFailure || minimal.Spec.Restart.MaxRestarts != nil {
		t.Fatalf("restart %+v", minimal.Spec.Restart)
	}
	if len(minimal.Dependencies) != 0 || len(minimal.Spec.Feeds) != 0 || len(minimal.Spec.Accounts) != 0 {
		t.Fatalf("empty slices %+v", minimal)
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	specJSON := raw["spec"].(map[string]any)
	if _, ok := specJSON["restart"].(map[string]any)["max_restarts"]; ok {
		t.Fatalf("omitted restart numbers were emitted: %s", body)
	}
	if _, ok := raw["created_by"]; !ok {
		t.Fatal("created_by omitted")
	}
	kv := se.e.get(t, "tw", state.SessionSpecKey("sts-tw-1", minimal.Id))
	if kv == nil || kv.CreateRevision != minimal.Revision || kv.ModRevision != minimal.Revision {
		t.Fatalf("etcd revision %+v response %d", kv, minimal.Revision)
	}
	var stored sessionv1.SessionSpec
	if err := proto.Unmarshal(kv.Value, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.GetStrategy().GetRepository() != "mftiko/strategies" || len(stored.GetParams()) != 0 || stored.GetRestart() != nil {
		t.Fatalf("stored %v", &stored)
	}
	rows := se.rows(t)
	if len(rows) != 1 || rows[0].State != recCreated || rows[0].SessionID != minimal.Id || !bytes.Equal(rows[0].SHA, digestSpec(kv.Value)) {
		t.Fatalf("row %+v", rows)
	}

	full := fmt.Sprintf(`{
		"name":"full","sts_instance":"sts-tw-1",
		"strategy":{"id":"strategies","ref":"v1"},
		"labels":{"team":"desk"},
		"params":{"z":1,"a":"x"},
		"feeds":[{"feed":"not-a-ticker","md_instance":"md-tw-1"}],
		"accounts":[{"name":"main","api_id":42,"algo_ids":[1,2]}],
		"inputs":[{"name":"model","digest":%q,"repository":"mftiko/data"}],
		"restart":{"mode":"never","max_restarts":0,"window_s":3},
		"limits":{"memory_mb":0,"gomaxprocs":1,"volume_mb":2,"hook_timeout_s":0},
		"deps_timeout_s":5,"start_timeout_s":6,"recv_window_ms":7,"grace_ms":8
	}`, dataDig)
	code, hdr, body = se.post(t, full, nil)
	got := mustSession(t, code, hdr, body)
	if string(got.Spec.Params) != `{"z":1,"a":"x"}` || got.Spec.Labels["team"] != "desk" {
		t.Fatalf("params %s labels %v", got.Spec.Params, got.Spec.Labels)
	}
	if len(got.Spec.Feeds) != 1 || got.Spec.Feeds[0].Feed != "not-a-ticker" || got.Spec.Feeds[0].MdInstance != "md-tw-1" {
		t.Fatalf("feeds %+v", got.Spec.Feeds)
	}
	if len(got.Spec.Accounts) != 1 || got.Spec.Accounts[0].TdInstance != "td-tw-1" || got.Spec.Accounts[0].ApiId != 42 {
		t.Fatalf("accounts %+v", got.Spec.Accounts)
	}
	if len(got.Spec.Inputs) != 1 || got.Spec.Inputs[0].Repository != "mftiko/data" || got.Spec.Inputs[0].Digest != dataDig {
		t.Fatalf("inputs %+v", got.Spec.Inputs)
	}
	if got.Spec.Restart.Mode == nil || *got.Spec.Restart.Mode != apiv1.Never || got.Spec.Restart.MaxRestarts == nil || *got.Spec.Restart.MaxRestarts != 0 || got.Spec.Restart.WindowS == nil || *got.Spec.Restart.WindowS != 3 {
		t.Fatalf("restart %+v", got.Spec.Restart)
	}
	if got.Spec.Limits.MemoryMb == nil || *got.Spec.Limits.MemoryMb != 0 || got.Spec.Limits.Gomaxprocs == nil || *got.Spec.Limits.Gomaxprocs != 1 {
		t.Fatalf("limits %+v", got.Spec.Limits)
	}
	if got.Spec.DepsTimeoutS != 5 || got.Spec.StartTimeoutS != 6 || got.Spec.RecvWindowMs != 7 || got.Spec.GraceMs != 8 {
		t.Fatalf("numbers %+v", got.Spec)
	}
	if got.Spec.Strategy.Digest != stratDig {
		t.Fatalf("tag did not resolve: %+v", got.Spec.Strategy)
	}
	fullKV := se.e.get(t, "tw", state.SessionSpecKey("sts-tw-1", got.Id))
	if err := proto.Unmarshal(fullKV.Value, &stored); err != nil {
		t.Fatal(err)
	}
	if string(stored.GetParams()) != `{"z":1,"a":"x"}` || stored.GetStrategy().GetCode() != stratDig || stored.GetAccounts()[0].GetTdInstance() != "td-tw-1" {
		t.Fatalf("stored full %v", &stored)
	}

	stRaw, err := proto.Marshal(&statev1.SessionStatus{
		Phase: statev1.Phase_PHASE_RUNNING, Incarnation: 2, RestartCount: 1, UpdatedAtNs: 9,
		Conditions: []*commonv1.Condition{{Type: "ready", Status: true, Reason: "up", Message: "ok", AtNs: 5}},
		LastExit:   &commonv1.ExitInfo{Code: 0, AtNs: 3, Outcome: commonv1.Outcome_OUTCOME_DONE, Reason: "ok", OnStopCompleted: true},
		StateOut:   &statev1.StateSnapshot{Digest: dataDig, Repository: "mftiko/data", Clean: true, Incarnation: 2, AtNs: 4, SizeBytes: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	demRaw, err := proto.Marshal(&statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, Start: true,
		IssuedAtNs: 1_700_000_000_000_000_000, UpdatedAtNs: 1_700_000_000_000_000_100,
		Bindings: []*statev1.Binding{{
			Target: &statev1.Binding_Feed{Feed: "not-a-ticker"}, Instance: "md-tw-1",
			Status: &statev1.EntryStatus{State: statev1.EntryState_ENTRY_STATE_READY, Reason: "up", Subject: "md.v1.f.md-tw-1.x", UpdatedAtNs: 11},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	putKey(t, t.Context(), se.e.servers["tw"], state.SessionStatusKey("sts-tw-1", got.Id), string(stRaw))
	putKey(t, t.Context(), se.e.servers["tw"], state.SessionDemandKey("sts-tw-1", got.Id), string(demRaw))
	seen(t, se.s, "tw", state.SessionStatusKey("sts-tw-1", got.Id))
	seen(t, se.s, "tw", state.SessionDemandKey("sts-tw-1", got.Id))
	code, _, body = se.call(t, http.MethodPost, "/api/v1/sessions/"+got.Id+"/end", `{}`, nil)
	if code != http.StatusOK {
		t.Fatalf("end %d %s", code, body)
	}
	var detail apiv1.SessionDetail
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Spec.DesiredState != apiv1.DesiredStateEnded || detail.Status == nil || detail.Status.Phase != apiv1.PhaseRunning {
		t.Fatalf("detail %+v", detail)
	}
	if detail.Status.LastExit == nil || detail.Status.LastExit.Outcome != apiv1.OutcomeDone || detail.Status.StateOut == nil || detail.Status.StateOut.SizeBytes != 8 {
		t.Fatalf("status %+v", detail.Status)
	}
	if detail.Assignment == nil || !detail.Assignment.Start || len(detail.Dependencies) != 1 || detail.Dependencies[0].Kind != apiv1.DependencyKindFeed {
		t.Fatalf("assignment %+v deps %+v", detail.Assignment, detail.Dependencies)
	}
	if !detail.Freshness.ControllerAbsent || len(detail.Freshness.UnavailableRegions) != 0 {
		t.Fatalf("freshness %+v", detail.Freshness)
	}

	code, _, body = se.call(t, http.MethodGet, "/api/v1/sessions", "", nil)
	if code != http.StatusOK {
		t.Fatalf("list %d %s", code, body)
	}
	var listed apiv1.SessionList
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, item := range listed.Items {
		if item.Id != got.Id {
			continue
		}
		found = true
		if item.Status == nil || item.Status.Phase != apiv1.PhaseRunning || item.CreatedBy == nil || *item.CreatedBy != "ada" {
			t.Fatalf("listed %+v", item)
		}
	}
	if !found {
		t.Fatalf("list missing %s", got.Id)
	}
	code, _, body = se.call(t, http.MethodGet, "/api/v1/sessions/"+got.Id, "", nil)
	if code != http.StatusOK {
		t.Fatalf("get %d %s", code, body)
	}
	var one apiv1.SessionDetail
	if err := json.Unmarshal(body, &one); err != nil {
		t.Fatal(err)
	}
	if one.Id != got.Id || one.Status == nil || one.Status.Phase != apiv1.PhaseRunning {
		t.Fatalf("get %+v", one)
	}
	code, _, body = se.call(t, http.MethodGet, "/api/v1/session-records", "", nil)
	if code != http.StatusOK {
		t.Fatalf("records %d %s", code, body)
	}
	var recs apiv1.SessionRecordList
	if err := json.Unmarshal(body, &recs); err != nil {
		t.Fatal(err)
	}
	var row *apiv1.SessionRecord
	for i := range recs.Items {
		if recs.Items[i].SessionId == got.Id {
			row = &recs.Items[i]
			break
		}
	}
	if row == nil || row.State != apiv1.SessionRecordStateCreated || row.CreatedBy == nil || *row.CreatedBy != "ada" || row.Final != nil || row.EndedRequestedAt == nil {
		t.Fatalf("record %+v", recs.Items)
	}
	// The session is still RUNNING, so immediate delete refuses it (D1084).
	code, _, body = se.call(t, http.MethodDelete, "/api/v1/sessions/"+got.Id, "{}", nil)
	if code != http.StatusConflict {
		t.Fatalf("delete running %d %s", code, body)
	}
	mustProblem(t, body, http.StatusConflict, "not_terminal")
	if se.spec(t, "sts-tw-1", got.Id) == nil {
		t.Fatal("delete removed the spec")
	}
}

func TestSessionValidation(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	cases := []struct {
		name, body, path string
	}{
		{"empty", `{}`, "/name"},
		{"sts plane", `{"name":"n","sts_instance":"md-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"}}`, "/sts_instance"},
		{"unknown strategy", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"nope","ref":"` + stratDig + `"}}`, "/strategy/id"},
		{"bad ref", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"sha256:zz"}}`, "/strategy/ref"},
		{"selectors", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"selectors":[]}`, "/selectors"},
		{"params array", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"params":[1]}`, "/params"},
		{"dup feed", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"feeds":[{"feed":"a","md_instance":"md-tw-1"},{"feed":"a","md_instance":"md-tw-1"}]}`, "/feeds/1/feed"},
		{"md plane", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"feeds":[{"feed":"a","md_instance":"td-tw-1"}]}`, "/feeds/0/md_instance"},
		{"md missing", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"feeds":[{"feed":"a","md_instance":"md-nope"}]}`, "/feeds/0/md_instance"},
		{"dup api", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"accounts":[{"name":"a","api_id":42,"algo_ids":[1]},{"name":"b","api_id":42,"algo_ids":[2]}]}`, "/accounts/1/api_id"},
		{"algo zero", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"accounts":[{"name":"a","api_id":42,"algo_ids":[0]}]}`, "/accounts/0/algo_ids"},
		{"algo high", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"accounts":[{"name":"a","api_id":42,"algo_ids":[1024]}]}`, "/accounts/0/algo_ids"},
		{"dup algo", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"accounts":[{"name":"a","api_id":42,"algo_ids":[1,1]}]}`, "/accounts/0/algo_ids"},
		{"bad label", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"labels":{" bad":"x"}}`, "/labels/ bad"},
		{"bad input", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"inputs":[{"name":"Bad","digest":"` + dataDig + `","repository":"mftiko/data"}]}`, "/inputs/0/name"},
		{"dup input", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"inputs":[{"name":"m","digest":"` + dataDig + `","repository":"mftiko/data"},{"name":"m","digest":"` + dataDig + `","repository":"mftiko/data"}]}`, "/inputs/1/name"},
		{"u32", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"deps_timeout_s":4294967296}`, "/deps_timeout_s"},
		{"negative", `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"grace_ms":-1}`, "/grace_ms"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, body := se.post(t, tc.body, nil)
			if code != http.StatusBadRequest {
				t.Fatalf("%d %s", code, body)
			}
			hasPath(t, body, tc.path)
		})
	}
	if n := len(se.rows(t)); n != 0 {
		t.Fatalf("validation wrote %d rows", n)
	}

	info := se.cat.manifest[catKey("mftiko/strategies", stratDig)]
	info.Manifest.Kind = "conn"
	se.cat.manifest[catKey("mftiko/strategies", stratDig)] = info
	code, _, body := se.post(t, minBody("sts-tw-1"), nil)
	if code != http.StatusBadRequest {
		t.Fatalf("kind %d %s", code, body)
	}
	hasPath(t, body, "/strategy/ref")
	info.Manifest.Kind = artifact.KindStrategy
	se.cat.manifest[catKey("mftiko/strategies", stratDig)] = info

	se.cat.data[catKey("mftiko/data", dataDig)] = artifact.DataInfo{Digest: dataDig, Config: artifact.DataConfig{Schema: "other"}}
	inputBody := fmt.Sprintf(`{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":%q},"inputs":[{"name":"m","digest":%q,"repository":"mftiko/data"}]}`, stratDig, dataDig)
	code, _, body = se.post(t, inputBody, nil)
	if code != http.StatusBadRequest {
		t.Fatalf("input %d %s", code, body)
	}
	hasPath(t, body, "/inputs/0/digest")
	se.cat.data[catKey("mftiko/data", dataDig)] = artifact.DataInfo{Digest: dataDig, Config: artifact.DataConfig{Schema: artifact.DataSchema}}

	se.cat.readErr[catKey("mftiko/strategies", stratDig)] = &artifact.Error{Reason: artifact.ReasonUnavailable, Err: errors.New("down")}
	code, _, body = se.post(t, minBody("sts-tw-1"), nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("registry %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "registry_unavailable")
	delete(se.cat.readErr, catKey("mftiko/strategies", stratDig))
	if se.specCount(t, "sts-tw-1") != 0 {
		t.Fatal("registry failure wrote a spec")
	}

	code, _, body = se.post(t, `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"`+stratDig+`"},"accounts":[{"name":"a","api_id":99,"algo_ids":[1]}]}`, nil)
	if code != http.StatusBadRequest {
		t.Fatalf("account %d %s", code, body)
	}
	hasPath(t, body, "/accounts/0/api_id")

	acct := `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"` + stratDig + `"},"accounts":[{"name":"a","api_id":42,"algo_ids":[7]}]}`
	code, hdr, body := se.post(t, acct, nil)
	first := mustSession(t, code, hdr, body)
	seen(t, se.s, "tw", state.SessionSpecKey("sts-tw-1", first.Id))
	code, _, body = se.post(t, acct, nil)
	if code != http.StatusConflict {
		t.Fatalf("overlap %d %s", code, body)
	}
	mustProblem(t, body, http.StatusConflict, "algo_overlap")
	st, err := proto.Marshal(&statev1.SessionStatus{Phase: statev1.Phase_PHASE_DONE, Reason: "ended_by_spec"})
	if err != nil {
		t.Fatal(err)
	}
	putKey(t, t.Context(), se.e.servers["tw"], state.SessionStatusKey("sts-tw-1", first.Id), string(st))
	seen(t, se.s, "tw", state.SessionStatusKey("sts-tw-1", first.Id))
	code, _, body = se.post(t, acct, nil)
	if code != http.StatusCreated {
		t.Fatalf("terminal overlap %d %s", code, body)
	}

	both := state.AccountKey("td-jp-1", 77)
	putKey(t, t.Context(), se.e.servers["jp"], both, "acct")
	putKey(t, t.Context(), se.e.servers["tw"], state.AccountKey("td-tw-1", 77), "acct")
	seen(t, se.s, "jp", both)
	seen(t, se.s, "tw", state.AccountKey("td-tw-1", 77))
	code, _, body = se.post(t, `{"name":"n","sts_instance":"sts-tw-1","strategy":{"id":"strategies","ref":"`+stratDig+`"},"accounts":[{"name":"a","api_id":77,"algo_ids":[4]}]}`, nil)
	lex := mustSession(t, code, nilHdr(t, code, body), body)
	if lex.Spec.Accounts[0].TdInstance != "td-jp-1" {
		t.Fatalf("td %s", lex.Spec.Accounts[0].TdInstance)
	}
}

func nilHdr(t *testing.T, code int, body []byte) http.Header {
	t.Helper()
	if code != http.StatusCreated {
		t.Fatalf("%d %s", code, body)
	}
	h := make(http.Header)
	var sess apiv1.Session
	if err := json.Unmarshal(body, &sess); err != nil {
		t.Fatal(err)
	}
	h.Set("Location", "/api/v1/sessions/"+sess.Id)
	return h
}

func TestSessionIdempotency(t *testing.T) {
	clock := &testClock{}
	clock.Set(time.Now().UTC())
	se := newSessionEnv(t, sessionOpt{clock: clock, unavailable: 48 * time.Hour})
	body := minBody("sts-tw-1")
	code, hdr, raw := se.post(t, body, keyHeader("same-body-key"))
	first := mustSession(t, code, hdr, raw)
	code, hdr, raw = se.post(t, body, keyHeader("same-body-key"))
	again := mustSession(t, code, hdr, raw)
	if again.Id != first.Id {
		t.Fatalf("replay %s then %s", first.Id, again.Id)
	}
	spaced := strings.Replace(body, `"name":"n"`, `"name": "n"`, 1)
	code, hdr, raw = se.post(t, spaced, keyHeader("same-body-key"))
	if mustSession(t, code, hdr, raw).Id != first.Id {
		t.Fatal("whitespace was a different body")
	}
	code, _, raw = se.post(t, strings.Replace(body, `"name":"n"`, `"name":"other"`, 1), keyHeader("same-body-key"))
	if code != http.StatusConflict {
		t.Fatalf("different body %d %s", code, raw)
	}
	mustProblem(t, raw, http.StatusConflict, "conflict")
	if strings.Contains(se.e.log.String(), "same-body-key") {
		t.Fatalf("log kept the key: %s", se.e.log.String())
	}
	if n := len(se.rows(t)); n != 1 {
		t.Fatalf("rows %d", n)
	}

	p1 := strings.Replace(body, `"name":"n"`, `"name":"p","params":{"z":1,"a":2}`, 1)
	p2 := strings.Replace(body, `"name":"n"`, `"name":"p","params":{"a":2,"z":1}`, 1)
	code, hdr, raw = se.post(t, p1, keyHeader("order"))
	ordered := mustSession(t, code, hdr, raw)
	code, _, raw = se.post(t, p2, keyHeader("order"))
	if code != http.StatusConflict {
		t.Fatalf("key order %d %s", code, raw)
	}
	compact := strings.Replace(p1, `{"z":1,"a":2}`, `{ "z" : 1, "a" : 2 }`, 1)
	code, hdr, raw = se.post(t, compact, keyHeader("order"))
	if mustSession(t, code, hdr, raw).Id != ordered.Id {
		t.Fatal("compact whitespace was a different body")
	}

	// A newline cannot be a header value on the wire. The handler rejects it with the same check.
	if validIdempotencyKey("") || validIdempotencyKey(" ") || validIdempotencyKey(strings.Repeat("a", 256)) || validIdempotencyKey("ok\n") {
		t.Fatal("invalid idempotency key accepted")
	}
	if !validIdempotencyKey("a") || !validIdempotencyKey(strings.Repeat("a", 255)) {
		t.Fatal("valid idempotency key rejected")
	}
	for _, bad := range []string{"", " ", strings.Repeat("a", 256)} {
		code, _, raw = se.post(t, body, keyHeader(bad))
		if code != http.StatusBadRequest {
			t.Fatalf("key %q: %d %s", bad, code, raw)
		}
		hasPath(t, raw, "Idempotency-Key")
	}

	se.s.hook = func(step string) error {
		if step == "before-etcd" {
			return errors.New("stop")
		}
		return nil
	}
	code, _, raw = se.post(t, body, keyHeader("pend"))
	if code != http.StatusInternalServerError {
		t.Fatalf("pending hook %d %s", code, raw)
	}
	pend := se.rows(t)
	var pendingID string
	for _, row := range pend {
		if row.IdempotencyKey != nil && *row.IdempotencyKey == "pend" {
			pendingID = row.SessionID
			if row.State != recPending || se.spec(t, "sts-tw-1", row.SessionID) != nil {
				t.Fatalf("pending row %+v", row)
			}
		}
	}
	if pendingID == "" {
		t.Fatal("no pending row")
	}
	se.s.hook = nil
	code, hdr, raw = se.post(t, body, keyHeader("pend"))
	if mustSession(t, code, hdr, raw).Id != pendingID {
		t.Fatalf("finish replay body %s", raw)
	}
	if se.spec(t, "sts-tw-1", pendingID) == nil {
		t.Fatal("replay did not write the spec")
	}

	code, _, raw = se.call(t, http.MethodPost, "/api/v1/sessions/"+first.Id+"/end", `{}`, nil)
	if code != http.StatusOK {
		t.Fatalf("end %d %s", code, raw)
	}
	code, hdr, raw = se.post(t, body, keyHeader("same-body-key"))
	ended := mustSession(t, code, hdr, raw)
	if ended.Id != first.Id || ended.Spec.DesiredState != apiv1.DesiredStateEnded {
		t.Fatalf("ended replay %+v", ended.Spec.DesiredState)
	}

	se.exec(t, `UPDATE session_records SET state = 'closed' WHERE session_id = $1 AND idempotency_key = 'order'`, ordered.Id)
	if _, err := se.e.servers["tw"].Client(t).Delete(t.Context(), state.SessionSpecKey("sts-tw-1", ordered.Id)); err != nil {
		t.Fatal(err)
	}
	code, _, raw = se.post(t, p1, keyHeader("order"))
	if code != http.StatusConflict {
		t.Fatalf("closed replay %d %s", code, raw)
	}
	var closedN int
	for _, row := range se.rows(t) {
		if row.SessionID == ordered.Id {
			closedN++
		}
	}
	if closedN != 1 || se.spec(t, "sts-tw-1", ordered.Id) != nil {
		t.Fatalf("closed replay wrote another session (%d)", closedN)
	}

	code, hdr, raw = se.post(t, body, keyHeader("gone"))
	gone := mustSession(t, code, hdr, raw)
	if _, err := se.e.servers["tw"].Client(t).Delete(t.Context(), state.SessionSpecKey("sts-tw-1", gone.Id)); err != nil {
		t.Fatal(err)
	}
	code, _, raw = se.post(t, body, keyHeader("gone"))
	if code != http.StatusConflict {
		t.Fatalf("deleted spec replay %d %s", code, raw)
	}
	if se.spec(t, "sts-tw-1", gone.Id) != nil {
		t.Fatal("conflict recreated the spec")
	}

	var created time.Time
	if err := se.s.audit.pool.QueryRow(t.Context(), `SELECT created_at FROM session_records WHERE session_id = $1 AND idempotency_key = 'same-body-key'`, first.Id).Scan(&created); err != nil {
		t.Fatal(err)
	}
	clock.Set(created.Add(idempotencyTTL - time.Millisecond))
	se.s.reconcileOnce(t.Context())
	code, hdr, raw = se.post(t, body, keyHeader("same-body-key"))
	if mustSession(t, code, hdr, raw).Id != first.Id {
		t.Fatal("key expired early")
	}
	clock.Set(created.Add(idempotencyTTL))
	se.s.reconcileOnce(t.Context())
	var key *string
	if err := se.s.audit.pool.QueryRow(t.Context(), `SELECT idempotency_key FROM session_records WHERE session_id = $1 AND state = 'created' AND created_at = $2`, first.Id, created).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != nil {
		t.Fatalf("key still %q", *key)
	}
	code, hdr, raw = se.post(t, body, keyHeader("same-body-key"))
	fresh := mustSession(t, code, hdr, raw)
	if fresh.Id == first.Id {
		t.Fatal("expired key replayed the old session")
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	ids := make([]string, 2)
	codes := make([]int, 2)
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			code, _, raw := se.post(t, minBody("sts-tw-1"), keyHeader("race"))
			codes[i] = code
			var sess apiv1.Session
			_ = json.Unmarshal(raw, &sess)
			ids[i] = sess.Id
		}(i)
	}
	close(start)
	wg.Wait()
	if codes[0] != http.StatusCreated || codes[1] != http.StatusCreated || ids[0] == "" || ids[0] != ids[1] {
		t.Fatalf("concurrent %v %v", codes, ids)
	}
}

func TestSessionCreateFailures(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	se.s.reconcileMu.Lock()
	se.s.hook = func(step string) error {
		if step == "before-etcd" {
			return errors.New("stop")
		}
		return nil
	}
	code, _, body := se.post(t, minBody("sts-tw-1"), nil)
	se.s.reconcileMu.Unlock()
	if code != http.StatusInternalServerError {
		t.Fatalf("before %d %s", code, body)
	}
	rows := se.rows(t)
	if len(rows) != 1 || rows[0].State != recPending || se.spec(t, "sts-tw-1", rows[0].SessionID) != nil {
		t.Fatalf("before row %+v", rows)
	}

	se.s.reconcileMu.Lock()
	se.s.hook = func(step string) error {
		if step == "after-etcd" {
			return errors.New("stop")
		}
		return nil
	}
	code, _, body = se.post(t, minBody("sts-tw-1"), nil)
	rows = se.rows(t)
	var held string
	for _, row := range rows {
		if row.State == recPending && se.spec(t, "sts-tw-1", row.SessionID) != nil {
			held = row.SessionID
		}
	}
	if code != http.StatusInternalServerError || held == "" || se.spec(t, "sts-tw-1", held) == nil {
		se.s.reconcileMu.Unlock()
		t.Fatalf("after %d %s held %s", code, body, held)
	}
	se.s.hook = nil
	se.s.reconcileMu.Unlock()
	seen(t, se.s, "tw", state.SessionSpecKey("sts-tw-1", held))
	se.s.reconcileOnce(t.Context())
	var promoted sessionRecord
	for _, row := range se.rows(t) {
		if row.SessionID == held {
			promoted = row
		}
	}
	if promoted.State != recCreated || promoted.CreateRevision == nil {
		t.Fatalf("promoted %+v", promoted)
	}

	se.s.hook = func(step string) error {
		if step == "before-etcd" {
			return errRegionUnavailable
		}
		return nil
	}
	code, _, body = se.post(t, minBody("sts-tw-1"), nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("hook 503 %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "region_unavailable")
	se.s.hook = nil

	se.s.hook = func(step string) error {
		if step == "before-etcd" {
			se.e.proxy.Cut()
		}
		return nil
	}
	code, _, body = se.post(t, minBody("sts-jp-1"), nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("cut during create %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "region_unavailable")
	var jpPending bool
	for _, row := range se.rows(t) {
		if row.STS == "sts-jp-1" {
			jpPending = true
			if se.spec(t, "sts-jp-1", row.SessionID) != nil {
				t.Fatal("cut wrote a spec")
			}
		}
	}
	if !jpPending {
		t.Fatal("cut dropped the pending row")
	}
	se.s.hook = nil
	se.e.proxy.Restore()
	until(t, 15*time.Second, "jp back", func() error {
		if ready, _ := se.s.readiness(time.Now()); len(ready.UnavailableRegions) != 0 {
			return fmt.Errorf("%v", ready.UnavailableRegions)
		}
		return nil
	})

	se.e.proxy.Cut()
	until(t, 15*time.Second, "jp down", func() error {
		ready, _ := se.s.readiness(time.Now())
		if len(ready.UnavailableRegions) == 0 {
			return errors.New("still up")
		}
		return nil
	})
	before := len(se.rows(t))
	code, _, body = se.post(t, minBody("sts-jp-1"), nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("down before create %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "region_unavailable")
	if len(se.rows(t)) != before || se.specCount(t, "sts-jp-1") != 0 {
		t.Fatal("unavailable region left a row or a spec")
	}

	dead := newSessionEnv(t, sessionOpt{dsn: deadDSN})
	code, _, body = dead.post(t, minBody("sts-tw-1"), nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("postgres %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "storage_unavailable")
	if dead.specCount(t, "sts-tw-1") != 0 {
		t.Fatal("postgres down wrote etcd")
	}
}

func TestSessionEnd(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	code, hdr, body := se.post(t, minBody("sts-tw-1"), nil)
	sess := mustSession(t, code, hdr, body)
	code, _, body = se.call(t, http.MethodPost, "/api/v1/sessions/"+sess.Id+"/end", `{}`, nil)
	if code != http.StatusOK {
		t.Fatalf("end %d %s", code, body)
	}
	var first apiv1.SessionDetail
	if err := json.Unmarshal(body, &first); err != nil {
		t.Fatal(err)
	}
	if first.Spec.DesiredState != apiv1.DesiredStateEnded {
		t.Fatalf("desired %s", first.Spec.DesiredState)
	}
	rows := se.rows(t)
	if len(rows) != 1 || rows[0].Desired != desEnded || rows[0].EndedAt == nil || rows[0].EndReason == nil || *rows[0].EndReason != endReasonSpec {
		t.Fatalf("row %+v", rows[0])
	}
	endedAt := *rows[0].EndedAt
	code, _, body = se.call(t, http.MethodPost, "/api/v1/sessions/"+sess.Id+"/end", `{}`, nil)
	if code != http.StatusOK {
		t.Fatalf("second end %d %s", code, body)
	}
	rows = se.rows(t)
	if rows[0].EndedAt == nil || !rows[0].EndedAt.Equal(endedAt) {
		t.Fatalf("ended_at moved %v want %v", rows[0].EndedAt, endedAt)
	}
	var stored sessionv1.SessionSpec
	if err := proto.Unmarshal(se.spec(t, "sts-tw-1", sess.Id), &stored); err != nil || stored.GetDesiredState() != sessionv1.DesiredState_DESIRED_STATE_ENDED {
		t.Fatalf("etcd %+v %v", stored.GetDesiredState(), err)
	}

	code, _, body = se.call(t, http.MethodPost, "/api/v1/sessions/abc123/end", `{}`, nil)
	if code != http.StatusNotFound {
		t.Fatalf("missing %d %s", code, body)
	}

	direct := &sessionv1.SessionSpec{Name: "direct", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING}
	// Hold reconcile so the mirror kick cannot warn before the end path does (D1072).
	se.s.reconcileMu.Lock()
	se.putSpec(t, "sts-tw-1", "ffffff", direct)
	before := se.s.missingRecord.Load()
	code, _, body = se.call(t, http.MethodPost, "/api/v1/sessions/ffffff/end", `{}`, nil)
	if code != http.StatusOK {
		se.s.reconcileMu.Unlock()
		t.Fatalf("direct end %d %s", code, body)
	}
	if se.s.missingRecord.Load() != before+1 {
		se.s.reconcileMu.Unlock()
		t.Fatalf("missing counter %d", se.s.missingRecord.Load())
	}
	se.s.reconcileMu.Unlock()
	if !strings.Contains(se.e.log.String(), "session spec has no session_records row") {
		t.Fatalf("log %s", se.e.log.String())
	}
	for _, row := range se.rows(t) {
		if row.SessionID == "ffffff" {
			t.Fatal("direct spec inserted a row")
		}
	}
	if err := proto.Unmarshal(se.spec(t, "sts-tw-1", "ffffff"), &stored); err != nil || stored.GetDesiredState() != sessionv1.DesiredState_DESIRED_STATE_ENDED {
		t.Fatal("direct spec was not ended")
	}

	code, hdr, body = se.post(t, minBody("sts-tw-1"), nil)
	term := mustSession(t, code, hdr, body)
	st, err := proto.Marshal(&statev1.SessionStatus{Phase: statev1.Phase_PHASE_FAILED, Reason: "crashed"})
	if err != nil {
		t.Fatal(err)
	}
	putKey(t, t.Context(), se.e.servers["tw"], state.SessionStatusKey("sts-tw-1", term.Id), string(st))
	seen(t, se.s, "tw", state.SessionStatusKey("sts-tw-1", term.Id))
	code, _, body = se.call(t, http.MethodPost, "/api/v1/sessions/"+term.Id+"/end", `{}`, nil)
	if code != http.StatusOK {
		t.Fatalf("terminal end %d %s", code, body)
	}

	putKey(t, t.Context(), se.e.servers["tw"], state.AgentKey(state.PlaneCtl, state.ControllerInstance), "agent")
	seen(t, se.s, "tw", state.AgentKey(state.PlaneCtl, state.ControllerInstance))
	code, _, body = se.call(t, http.MethodPost, "/api/v1/sessions/"+sess.Id+"/end", `{}`, nil)
	var detail apiv1.SessionDetail
	if err := json.Unmarshal(body, &detail); err != nil || code != http.StatusOK || detail.Freshness.ControllerAbsent {
		t.Fatalf("fresh %d %+v %s", code, detail.Freshness, body)
	}

	code, hdr, body = se.post(t, minBody("sts-tw-1"), nil)
	closed := mustSession(t, code, hdr, body)
	se.s.audit.pool.Close()
	code, _, body = se.call(t, http.MethodPost, "/api/v1/sessions/"+closed.Id+"/end", `{}`, nil)
	if code != http.StatusOK {
		t.Fatalf("pool closed %d %s", code, body)
	}
	if err := proto.Unmarshal(se.spec(t, "sts-tw-1", closed.Id), &stored); err != nil || stored.GetDesiredState() != sessionv1.DesiredState_DESIRED_STATE_ENDED {
		t.Fatal("pool close blocked the etcd end")
	}
}

func TestSessionReconcile(t *testing.T) {
	clock := &testClock{}
	clock.Set(time.Now().UTC())
	se := newSessionEnv(t, sessionOpt{clock: clock, unavailable: 48 * time.Hour})

	direct := &sessionv1.SessionSpec{Name: "only-etcd", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING}
	before := se.s.missingRecord.Load()
	se.afterMirror(t, func() { se.putSpec(t, "sts-tw-1", "eeeeee", direct) })
	if se.s.missingRecord.Load() <= before || !strings.Contains(se.e.log.String(), "session spec has no session_records row") {
		t.Fatalf("missing %d log %s", se.s.missingRecord.Load(), se.e.log.String())
	}
	for _, row := range se.rows(t) {
		if row.SessionID == "eeeeee" {
			t.Fatal("reconcile inserted a row")
		}
	}
	// Every later pass re-counts a spec that is still missing, so drop it before the next delta.
	se.dropSpec(t, "sts-tw-1", "eeeeee")

	bad := []byte{0x0a, 0x05, 0x01}
	if proto.Unmarshal(bad, &sessionv1.SessionSpec{}) == nil {
		t.Fatal("fixture decodes")
	}
	miss := se.s.missingRecord.Load()
	se.afterMirror(t, func() {
		putKey(t, t.Context(), se.e.servers["tw"], state.SessionSpecKey("sts-tw-1", "dddddd"), string(bad))
		se.waitValue(t, "tw", state.SessionSpecKey("sts-tw-1", "dddddd"), bad)
	})
	if se.s.missingRecord.Load() <= miss || !strings.Contains(se.e.log.String(), "session spec could not be decoded") {
		t.Fatalf("decode missing %d", se.s.missingRecord.Load())
	}
	se.dropSpec(t, "sts-tw-1", "dddddd")

	code, hdr, body := se.post(t, minBody("sts-tw-1"), nil)
	sess := mustSession(t, code, hdr, body)
	orig := append([]byte(nil), se.spec(t, "sts-tw-1", sess.Id)...)
	var spec sessionv1.SessionSpec
	if err := proto.Unmarshal(orig, &spec); err != nil {
		t.Fatal(err)
	}
	spec.Name = "changed"
	spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
	changed, err := proto.Marshal(&spec)
	if err != nil {
		t.Fatal(err)
	}
	hash := se.s.hashMismatch.Load()
	se.afterMirror(t, func() {
		putKey(t, t.Context(), se.e.servers["tw"], state.SessionSpecKey("sts-tw-1", sess.Id), string(changed))
		se.waitValue(t, "tw", state.SessionSpecKey("sts-tw-1", sess.Id), changed)
	})
	if se.s.hashMismatch.Load() <= hash || !strings.Contains(se.e.log.String(), "session spec hash does not match session_records") {
		t.Fatalf("hash %d", se.s.hashMismatch.Load())
	}
	var row sessionRecord
	for _, item := range se.rows(t) {
		if item.SessionID == sess.Id {
			row = item
		}
	}
	if !bytes.Equal(row.Spec, orig) || row.Desired != desEnded || row.EndReason == nil || *row.EndReason != endReasonSpec {
		t.Fatalf("mismatch row desired %s bytes changed %v", row.Desired, !bytes.Equal(row.Spec, orig))
	}

	code, hdr, body = se.post(t, minBody("sts-tw-1"), nil)
	only := mustSession(t, code, hdr, body)
	cur := se.spec(t, "sts-tw-1", only.Id)
	if err := proto.Unmarshal(cur, &spec); err != nil {
		t.Fatal(err)
	}
	spec.DesiredState = sessionv1.DesiredState_DESIRED_STATE_ENDED
	ended, err := proto.Marshal(&spec)
	if err != nil {
		t.Fatal(err)
	}
	se.dropSpec(t, "sts-tw-1", sess.Id)
	hash = se.s.hashMismatch.Load()
	se.afterMirror(t, func() {
		putKey(t, t.Context(), se.e.servers["tw"], state.SessionSpecKey("sts-tw-1", only.Id), string(ended))
		se.waitValue(t, "tw", state.SessionSpecKey("sts-tw-1", only.Id), ended)
	})
	if se.s.hashMismatch.Load() != hash {
		t.Fatal("desired_state change counted as a hash mismatch")
	}
	for _, item := range se.rows(t) {
		if item.SessionID == only.Id && (!bytes.Equal(item.Spec, ended) || item.Desired != desEnded) {
			t.Fatalf("desired sync %+v", item.Desired)
		}
	}

	miss = se.s.missingRecord.Load()
	se.afterMirror(t, func() {
		putKey(t, t.Context(), se.e.servers["tw"], state.SessionSpecKey("sts-tw-1", only.Id), string(bad))
		se.waitValue(t, "tw", state.SessionSpecKey("sts-tw-1", only.Id), bad)
	})
	if se.s.missingRecord.Load() != miss {
		t.Fatal("paired undecodable spec incremented the missing counter")
	}

	se.s.ids = sessionSeq(t, "aaaaaa")
	se.s.hook = func(step string) error {
		if step == "before-etcd" {
			return errors.New("stop")
		}
		return nil
	}
	code, _, body = se.post(t, minBody("sts-tw-1"), nil)
	if code != http.StatusInternalServerError {
		t.Fatalf("hook %d %s", code, body)
	}
	var created time.Time
	if err := se.s.audit.pool.QueryRow(t.Context(), `SELECT created_at FROM session_records WHERE session_id = 'aaaaaa' AND state = 'pending'`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	clock.Set(created.Add(pendingTimeout))
	se.s.reconcileOnce(t.Context())
	var got string
	if err := se.s.audit.pool.QueryRow(t.Context(), `SELECT state FROM session_records WHERE session_id = 'aaaaaa'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != recPending {
		t.Fatalf("exactly the timeout abandoned the row: %s", got)
	}
	clock.Set(created.Add(pendingTimeout + time.Millisecond))
	se.s.reconcileOnce(t.Context())
	if err := se.s.audit.pool.QueryRow(t.Context(), `SELECT state FROM session_records WHERE session_id = 'aaaaaa'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != recAbandoned {
		t.Fatalf("state %s", got)
	}
	var abandoned sessionRecord
	for _, item := range se.rows(t) {
		if item.SessionID == "aaaaaa" {
			abandoned = item
		}
	}
	var revived sessionv1.SessionSpec
	if err := proto.Unmarshal(abandoned.Spec, &revived); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	se.afterMirror(t, func() { raw = se.putSpec(t, "sts-tw-1", "aaaaaa", &revived) })
	kv := se.e.get(t, "tw", state.SessionSpecKey("sts-tw-1", "aaaaaa"))
	for _, item := range se.rows(t) {
		if item.ID == abandoned.ID {
			if item.State != recCreated || item.CreateRevision == nil || *item.CreateRevision != kv.CreateRevision || !bytes.Equal(item.Spec, raw) {
				t.Fatalf("revived %+v rev %d", item.State, kv.CreateRevision)
			}
		}
	}

	se.s.ids = sessionSeq(t, "bbbbbb")
	code, _, _ = se.post(t, strings.Replace(minBody("sts-tw-1"), `"name":"n"`, `"name":"other"`, 1), nil)
	if code != http.StatusInternalServerError {
		t.Fatalf("second pending %d", code)
	}
	se.exec(t, `UPDATE session_records SET state = 'abandoned' WHERE session_id = 'bbbbbb'`)
	code, _, _ = se.post(t, minBody("sts-tw-1"), nil)
	if code != http.StatusInternalServerError {
		t.Fatalf("third pending %d", code)
	}
	var older sessionRecord
	for _, item := range se.rows(t) {
		if item.SessionID == "bbbbbb" && item.State == recAbandoned {
			older = item
		}
	}
	if err := proto.Unmarshal(older.Spec, &revived); err != nil {
		t.Fatal(err)
	}
	se.afterMirror(t, func() { se.putSpec(t, "sts-tw-1", "bbbbbb", &revived) })
	if !strings.Contains(se.e.log.String(), "session record revive collided") {
		t.Fatalf("log %s", se.e.log.String())
	}
	n := 0
	for _, item := range se.rows(t) {
		if item.SessionID == "bbbbbb" {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("collision rows %d", n)
	}
}

func TestSessionAbandonSkipsDownRegion(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	se.s.hook = func(step string) error {
		if step == "before-etcd" {
			return errors.New("stop")
		}
		return nil
	}
	code, _, body := se.post(t, minBody("sts-jp-1"), nil)
	if code != http.StatusInternalServerError {
		t.Fatalf("%d %s", code, body)
	}
	se.s.hook = nil
	se.exec(t, `UPDATE session_records SET created_at = now() - interval '3 minutes' WHERE sts_instance = 'sts-jp-1' AND state = 'pending'`)
	se.e.proxy.Cut()
	until(t, 15*time.Second, "jp unavailable", func() error {
		ready, _ := se.s.readiness(time.Now())
		for _, name := range ready.UnavailableRegions {
			if name == "jp" {
				return nil
			}
		}
		return fmt.Errorf("%v", ready.UnavailableRegions)
	})
	se.s.reconcileOnce(t.Context())
	var got string
	if err := se.s.audit.pool.QueryRow(t.Context(), `SELECT state FROM session_records WHERE sts_instance = 'sts-jp-1'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != recPending {
		t.Fatalf("down region abandoned the row: %s", got)
	}
}

func TestSessionReconcileReplicas(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	other := se.e.replica(t)
	other.catalog = se.cat
	code, hdr, body := se.post(t, minBody("sts-tw-1"), nil)
	sess := mustSession(t, code, hdr, body)
	seen(t, other, "tw", state.SessionSpecKey("sts-tw-1", sess.Id))
	se.s.reconcileOnce(t.Context())
	other.reconcileOnce(t.Context())
	if n := len(se.rows(t)); n != 1 {
		t.Fatalf("rows %d", n)
	}
	if se.s.hashMismatch.Load() != 0 || se.s.missingRecord.Load() != 0 || other.missingRecord.Load() != 0 || other.hashMismatch.Load() != 0 {
		t.Fatalf("counters missing %d/%d hash %d/%d", se.s.missingRecord.Load(), other.missingRecord.Load(), se.s.hashMismatch.Load(), other.hashMismatch.Load())
	}
}
