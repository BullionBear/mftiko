package api

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/migrations"
)

func TestRetireInstance(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{unavailable: 2 * time.Second})
	ctx := t.Context()
	agent := state.AgentKey(state.PlaneSTS, "sts-tw-1")
	retired := state.RetiredKey("sts-tw-1")
	putKey(t, ctx, se.e.servers["tw"], agent, "rec")

	code, _, body := se.call(t, http.MethodPost, "/api/v1/instances/sts-tw-1/retire", "", nil)
	if code != http.StatusConflict {
		t.Fatalf("present %d %s", code, body)
	}
	mustProblem(t, body, http.StatusConflict, "conflict")
	if !strings.Contains(string(body), "agent record is present") {
		t.Fatalf("detail %s", body)
	}
	if se.e.get(t, "tw", retired) != nil {
		t.Fatal("conflict wrote the marker")
	}
	if n := se.retiredCount(t); n != 0 {
		t.Fatalf("conflict wrote %d rows", n)
	}

	if _, err := se.e.servers["tw"].Client(t).Delete(ctx, agent); err != nil {
		t.Fatal(err)
	}
	code, _, body = se.call(t, http.MethodPost, "/api/v1/instances/sts-tw-1/retire", "", nil)
	if code != http.StatusNoContent || len(body) != 0 {
		t.Fatalf("retire %d %s", code, body)
	}
	if kv := se.e.get(t, "tw", retired); kv == nil || string(kv.Value) != "sts-tw-1" {
		t.Fatalf("marker %+v", kv)
	}
	se.wantRetired(t, "sts-tw-1", "sts", "tw", "ada")
	code, _, body = se.call(t, http.MethodPost, "/api/v1/instances/sts-tw-1/retire", "", nil)
	if code != http.StatusNoContent {
		t.Fatalf("repeat %d %s", code, body)
	}
	if n := se.retiredCount(t); n != 1 {
		t.Fatalf("rows %d", n)
	}

	se.exec(t, `DROP TABLE retired_instances`)
	code, _, body = se.call(t, http.MethodPost, "/api/v1/instances/md-tw-1/retire", "", nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("storage %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "storage_unavailable")
	if kv := se.e.get(t, "tw", state.RetiredKey("md-tw-1")); kv == nil || string(kv.Value) != "md-tw-1" {
		t.Fatalf("marker after storage failure %+v", kv)
	}
	sql, err := migrations.FS.ReadFile("0006_retired_instances.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	se.exec(t, string(sql))
	code, _, body = se.call(t, http.MethodPost, "/api/v1/instances/md-tw-1/retire", "", nil)
	if code != http.StatusNoContent {
		t.Fatalf("retry md %d %s", code, body)
	}
	se.wantRetired(t, "md-tw-1", "md", "tw", "ada")
	code, _, body = se.call(t, http.MethodPost, "/api/v1/instances/td-tw-1/retire", "", nil)
	if code != http.StatusNoContent {
		t.Fatalf("td %d %s", code, body)
	}
	se.wantRetired(t, "td-tw-1", "td", "tw", "ada")

	for _, name := range []string{"nope", "controller"} {
		code, _, body = se.call(t, http.MethodPost, "/api/v1/instances/"+name+"/retire", "", nil)
		if code != http.StatusNotFound {
			t.Fatalf("%s %d %s", name, code, body)
		}
		mustProblem(t, body, http.StatusNotFound, "not_found")
	}

	se.e.proxy.Cut()
	until(t, 15*time.Second, "jp down", func() error {
		ready, _ := se.s.readiness(time.Now())
		if len(ready.UnavailableRegions) == 0 {
			return errStillUp
		}
		return nil
	})
	code, _, body = se.call(t, http.MethodPost, "/api/v1/instances/sts-jp-1/retire", "", nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("region %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, "region_unavailable")
	if se.e.get(t, "jp", state.RetiredKey("sts-jp-1")) != nil {
		t.Fatal("unavailable region wrote the marker")
	}
}

func TestRetireReleasesAlgo(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	acct := `{"name":"n","sts_instance":"sts-tw-1","strategy":{"repository":"registry.example/mftiko/strategies","ref":"` + stratDig + `"},"accounts":[{"name":"a","api_id":42,"algo_ids":[7]}]}`
	code, hdr, body := se.post(t, acct, nil)
	first := mustSession(t, code, hdr, body)
	seen(t, se.s, "tw", state.SessionSpecKey("sts-tw-1", first.Id))
	code, _, body = se.post(t, acct, nil)
	if code != http.StatusConflict {
		t.Fatalf("overlap %d %s", code, body)
	}
	mustProblem(t, body, http.StatusConflict, "algo_overlap")

	code, _, body = se.call(t, http.MethodPost, "/api/v1/instances/sts-tw-1/retire", "", nil)
	if code != http.StatusNoContent {
		t.Fatalf("retire %d %s", code, body)
	}
	seen(t, se.s, "tw", state.RetiredKey("sts-tw-1"))
	code, _, body = se.post(t, acct, nil)
	if code != http.StatusCreated {
		t.Fatalf("after retire %d %s", code, body)
	}
}

func (se *sessionEnv) retiredCount(t *testing.T) int {
	t.Helper()
	pool, err := se.s.db()
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM retired_instances`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (se *sessionEnv) wantRetired(t *testing.T, inst, plane, region, by string) {
	t.Helper()
	pool, err := se.s.db()
	if err != nil {
		t.Fatal(err)
	}
	var gotPlane, gotRegion, gotBy string
	if err := pool.QueryRow(t.Context(), `
		SELECT plane, region, retired_by FROM retired_instances WHERE instance = $1`, inst).
		Scan(&gotPlane, &gotRegion, &gotBy); err != nil {
		t.Fatal(err)
	}
	if gotPlane != plane || gotRegion != region || gotBy != by {
		t.Fatalf("row %s %s %s, want %s %s %s", gotPlane, gotRegion, gotBy, plane, region, by)
	}
}

var errStillUp = errString("still up")

type errString string

func (e errString) Error() string { return string(e) }
