package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/cli/apitest"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

const (
	digA = "sha256:0000000000000000000000000000000000000000000000000000000000000001"
	digB = "sha256:0000000000000000000000000000000000000000000000000000000000000002"
)

func TestStrategyList(t *testing.T) {
	cat := &apitest.Catalog{Strategies: []apiv1.Strategy{
		{Id: "alpha", Repository: "example.invalid/alpha", Latest: &apiv1.StrategyTag{Tag: "latest", Digest: digA}},
		{Id: "beta", Repository: "example.invalid/beta"},
	}}
	srv, home := startCat(t, cat)
	got := runQuery(t, home, "strategy", "list")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if got.err != "" {
		t.Fatalf("stderr %q", got.err)
	}
	if !strings.Contains(got.out, "alpha") || !strings.Contains(got.out, "latest") || !strings.Contains(got.out, digA[:12]) {
		t.Fatalf("table %q", got.out)
	}
	if strings.Contains(got.out, digA[12:]) {
		t.Fatalf("table shows more than 12 digest characters: %q", got.out)
	}
	if !strings.Contains(got.out, "beta") || !strings.Contains(got.out, "example.invalid/beta") {
		t.Fatalf("missing strategy: %q", got.out)
	}
	if strings.Contains(got.out, "warning") {
		t.Fatalf("stdout %q", got.out)
	}
	call := srv.Calls()[0]
	if call.Operation != "ListStrategies" || call.Method != "GET" || call.Path != "/api/v1/strategies" {
		t.Fatalf("call %+v", call)
	}
	if !strings.Contains(call.RawQuery, "limit=1000") || strings.Contains(call.RawQuery, "cursor=") {
		t.Fatalf("query %q", call.RawQuery)
	}
	if call.Authorization != "Bearer "+tokenA || call.ContentType != "" || call.Cookie != "" {
		t.Fatalf("headers %+v", call)
	}
	if call.UserAgent != "mftiko-cli/"+buildinfo.Version {
		t.Fatalf("user agent %q", call.UserAgent)
	}

	got = runQuery(t, home, "-o", "json", "strategy", "list")
	if got.code != ExitOK {
		t.Fatalf("json exit %d stderr %q", got.code, got.err)
	}
	if !bytes.Equal([]byte(got.out), srv.LastBody()) {
		t.Fatalf("stdout is not the API body\n%s\n%s", got.out, srv.LastBody())
	}
	if !strings.Contains(got.out, digA) || strings.Contains(got.out, "next_cursor") || strings.Contains(got.out, "unavailable_regions") {
		t.Fatalf("json %s", got.out)
	}

	over := apitest.Start(apitest.Option{
		Token: tokenB, RequestID: "req-q", Catalog: cat,
	})
	t.Cleanup(over.Close)
	writeConfig(t, home, configYAML(over.URL(), true, tokenA))
	got = runAt(context.Background(), home, map[string]string{"MFTIKO_CONTEXT": "dev", "MFTIKO_TOKEN": tokenB}, strings.NewReader(""), "strategy", "list")
	noLeak(t, tokenA, got.out+got.err)
	noLeak(t, tokenB, got.out+got.err)
	if got.code != ExitOK || over.Calls()[0].Authorization != "Bearer "+tokenB {
		t.Fatalf("override %+v auth %q", got, over.Calls()[0].Authorization)
	}

	bad := apitest.Start(apitest.Option{Token: tokenA, RequestID: "req-q", Catalog: cat})
	t.Cleanup(bad.Close)
	writeConfig(t, home, configYAML(bad.URL(), true, tokenB))
	got = runQuery(t, home, "strategy", "list")
	if got.code != ExitUnauthenticated || !strings.Contains(got.err, "unauthenticated (11):") {
		t.Fatalf("bad token %+v", got)
	}
	if len(bad.Calls()) != 1 || bad.Calls()[0].Operation != "ListStrategies" {
		t.Fatalf("calls %+v", bad.Calls())
	}
}

func TestStrategyPaging(t *testing.T) {
	strategies := make([]apiv1.Strategy, 5)
	for i := range strategies {
		strategies[i] = apiv1.Strategy{Id: string(rune('a' + i)), Repository: "example.invalid/" + string(rune('a'+i))}
	}
	cat := &apitest.Catalog{Strategies: strategies, PageSize: 2}
	srv, home := startCat(t, cat)

	got := runQuery(t, home, "-o", "json", "strategy", "list")
	if got.code != ExitOK {
		t.Fatalf("all exit %d stderr %q", got.code, got.err)
	}
	var merged struct {
		Items      []apiv1.Strategy `json:"items"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal([]byte(got.out), &merged); err != nil {
		t.Fatal(err)
	}
	if len(merged.Items) != 5 || merged.NextCursor != nil || merged.Items[0].Id != "a" || merged.Items[4].Id != "e" {
		t.Fatalf("merged %+v", merged.Items)
	}
	if strings.Contains(got.out, "next_cursor") {
		t.Fatalf("merged json kept a cursor: %s", got.out)
	}
	calls := srv.Calls()
	if len(calls) != 3 {
		t.Fatalf("calls %d", len(calls))
	}
	if !strings.Contains(calls[0].RawQuery, "limit=1000") || strings.Contains(calls[0].RawQuery, "cursor=") {
		t.Fatalf("first query %q", calls[0].RawQuery)
	}
	if !strings.Contains(calls[1].RawQuery, "cursor=2") || !strings.Contains(calls[2].RawQuery, "cursor=4") {
		t.Fatalf("queries %q %q", calls[1].RawQuery, calls[2].RawQuery)
	}

	one := &apitest.Catalog{Strategies: strategies, PageSize: 2}
	srv, home = startCat(t, one)
	got = runQuery(t, home, "strategy", "list", "--limit", "1", "-o", "json")
	if got.code != ExitOK {
		t.Fatalf("limit 1 exit %d stderr %q", got.code, got.err)
	}
	if err := json.Unmarshal([]byte(got.out), &merged); err != nil {
		t.Fatal(err)
	}
	if len(merged.Items) != 1 || merged.Items[0].Id != "a" || len(srv.Calls()) != 1 {
		t.Fatalf("limit 1 items %d calls %d", len(merged.Items), len(srv.Calls()))
	}

	three := &apitest.Catalog{Strategies: strategies, PageSize: 2}
	srv, home = startCat(t, three)
	got = runQuery(t, home, "--limit", "3", "-o", "json", "strategy", "list")
	if got.code != ExitOK {
		t.Fatalf("limit 3 exit %d stderr %q", got.code, got.err)
	}
	if err := json.Unmarshal([]byte(got.out), &merged); err != nil {
		t.Fatal(err)
	}
	if len(merged.Items) != 3 || merged.Items[2].Id != "c" || len(srv.Calls()) != 2 {
		t.Fatalf("limit 3 items %+v calls %d", merged.Items, len(srv.Calls()))
	}

	page := &apitest.Catalog{Strategies: strategies, PageSize: 2}
	srv, home = startCat(t, page)
	got = runQuery(t, home, "-o", "json", "--cursor", "2", "strategy", "list")
	if got.code != ExitOK {
		t.Fatalf("cursor exit %d stderr %q", got.code, got.err)
	}
	if !bytes.Equal([]byte(got.out), srv.LastBody()) {
		t.Fatalf("cursor json is not the page\n%s\n%s", got.out, srv.LastBody())
	}
	if !strings.Contains(got.out, "next_cursor") || len(srv.Calls()) != 1 || !strings.Contains(srv.Calls()[0].RawQuery, "cursor=2") {
		t.Fatalf("cursor page %s calls %+v", got.out, srv.Calls())
	}

	// E11-D010 supplement: --limit together with --cursor is a usage error (exit 1).
	both := &apitest.Catalog{Strategies: strategies, PageSize: 2}
	srv, home = startCat(t, both)
	for _, args := range [][]string{
		{"strategy", "list", "--limit", "2", "--cursor", "2"},
		{"strategy", "versions", "alpha", "--cursor", "2", "--limit", "2"},
	} {
		got = runQuery(t, home, args...)
		if got.code != ExitUsage || got.out != "" || len(srv.Calls()) != 0 || !strings.Contains(got.err, "-limit and -cursor cannot both be set") {
			t.Fatalf("both %v %+v calls %d", args, got, len(srv.Calls()))
		}
	}

	detail := "registry is not configured"
	fail := &apiv1.Problem{
		Type: "about:blank", Title: "Registry Unavailable", Status: http.StatusServiceUnavailable,
		Code: apiv1.ErrorCodeRegistryUnavailable, Detail: &detail,
	}
	broken := &apitest.Catalog{Strategies: strategies, PageSize: 2, FailAt: 2, Fail: fail}
	srv, home = startCat(t, broken)
	got = runQuery(t, home, "strategy", "list")
	if got.code != ExitRegistryUnavailable || got.out != "" || len(srv.Calls()) != 2 {
		t.Fatalf("mid-page %+v calls %d", got, len(srv.Calls()))
	}
	if !strings.Contains(got.err, "registry_unavailable (19):") || !strings.Contains(got.err, "registry connection problem\n") {
		t.Fatalf("stderr %q", got.err)
	}
	if strings.Contains(got.err, "warning") {
		t.Fatalf("warning on failure: %q", got.err)
	}

	loop1 := []byte("{\"items\":[{\"id\":\"a\",\"repository\":\"example.invalid/a\"}],\"next_cursor\":\"loop\"}\n")
	loop2 := []byte("{\"items\":[{\"id\":\"b\",\"repository\":\"example.invalid/b\"}],\"next_cursor\":\"loop\"}\n")
	loops := &apitest.Catalog{StrategyPages: map[string]apitest.RawPage{
		"":     {Body: loop1},
		"loop": {Body: loop2},
	}}
	srv, home = startCat(t, loops)
	got = runQuery(t, home, "strategy", "list")
	if got.code != ExitNetwork || got.out != "" || !strings.Contains(got.err, "pagination cursor repeated") || strings.Contains(got.err, "loop") {
		t.Fatalf("repeat %+v", got)
	}
	if len(srv.Calls()) != 2 {
		t.Fatalf("repeat calls %d", len(srv.Calls()))
	}

	badPage := &apitest.Catalog{StrategyPages: map[string]apitest.RawPage{
		"": {Body: []byte("{\"nope\":true}\n")},
	}}
	_, home = startCat(t, badPage)
	got = runQuery(t, home, "strategy", "list")
	if got.code != ExitUsage || got.out != "" || !strings.Contains(got.err, "response is not a list") {
		t.Fatalf("not a list %+v", got)
	}

	nullItems := &apitest.Catalog{StrategyPages: map[string]apitest.RawPage{
		"": {Body: []byte("{\"items\":null}\n")},
	}}
	_, home = startCat(t, nullItems)
	got = runQuery(t, home, "-o", "json", "strategy", "list")
	if got.code != ExitUsage || got.out != "" {
		t.Fatalf("null items %+v", got)
	}

	loud := &apitest.Catalog{Strategies: strategies[:3], PageSize: 2}
	_, home = startCat(t, loud)
	got = runQuery(t, home, "-v", "strategy", "list")
	if got.code != ExitOK || strings.Count(got.err, "X-Request-Id: req-q\n") != 2 {
		t.Fatalf("verbose stderr %q", got.err)
	}
}

func TestStrategyVersions(t *testing.T) {
	cat := &apitest.Catalog{
		PageSize: 1,
		Versions: map[string][]apiv1.StrategyTag{
			"alpha": {
				{Tag: "v1", Digest: digA},
				{Tag: "latest", Digest: digA},
				{Tag: "v2", Digest: digB},
			},
		},
	}
	srv, home := startCat(t, cat)
	got := runQuery(t, home, "strategy", "versions", "alpha")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if len(srv.Calls()) != 3 || srv.Calls()[0].Path != "/api/v1/strategies/alpha/versions" {
		t.Fatalf("calls %+v", srv.Calls())
	}
	for _, line := range strings.Split(strings.TrimRight(got.out, "\n"), "\n") {
		if strings.Contains(line, digA) && !strings.HasSuffix(line, "  latest") {
			t.Fatalf("unmarked %q in %q", line, got.out)
		}
		if strings.Contains(line, digB) && strings.Contains(line, "latest") {
			t.Fatalf("v2 marked %q", line)
		}
	}
	if !strings.Contains(got.out, digA) || !strings.Contains(got.out, digB) {
		t.Fatalf("digests missing: %q", got.out)
	}

	got = runQuery(t, home, "-o", "json", "strategy", "versions", "alpha")
	if got.code != ExitOK || !strings.Contains(got.out, digA) || strings.Contains(got.out, "next_cursor") {
		t.Fatalf("json %+v", got)
	}

	first := &apitest.Catalog{PageSize: 1, Versions: cat.Versions}
	_, home = startCat(t, first)
	got = runQuery(t, home, "--cursor", "0", "strategy", "versions", "alpha")
	if got.code != ExitOK {
		t.Fatalf("cursor exit %d stderr %q", got.code, got.err)
	}
	if strings.Contains(got.out, "latest") || !strings.Contains(got.out, digA) {
		t.Fatalf("first page marked or missing digest: %q", got.out)
	}

	_, home = startCat(t, &apitest.Catalog{})
	got = runQuery(t, home, "strategy", "versions", "missing")
	if got.code != ExitNotFound || !strings.Contains(got.err, "not_found (12):") || got.out != "" {
		t.Fatalf("missing %+v", got)
	}
}

func TestStrategyShow(t *testing.T) {
	sdk := "sdk-1"
	size := int64(0)
	ver := apiv1.StrategyVersion{
		StrategyId: "alpha", Repository: "example.invalid/alpha", Digest: digA,
		Tags: []string{"v1", "latest"}, Name: "chase", Runtime: "go", Os: "linux", Arch: "amd64",
		Sdk: &sdk, Contracts: []string{"c4", "c6"}, SizeBytes: &size,
	}
	cat := &apitest.Catalog{Manifests: map[string]apiv1.StrategyVersion{
		"alpha\x00v1":      ver,
		"alpha\x00" + digA: ver,
	}}
	srv, home := startCat(t, cat)
	for _, ref := range []string{"v1", digA} {
		got := runQuery(t, home, "strategy", "show", "alpha", ref)
		if got.code != ExitOK {
			t.Fatalf("show %s exit %d stderr %q", ref, got.code, got.err)
		}
		for _, want := range []string{digA, "v1, latest", "chase", "go", "sdk-1", "c4, c6"} {
			if !strings.Contains(got.out, want) {
				t.Fatalf("show %s missing %q in %q", ref, want, got.out)
			}
		}
		if row(t, got.out, "SIZE_BYTES") != "0" || row(t, got.out, "DIGEST") != digA {
			t.Fatalf("show %s fields %q", ref, got.out)
		}
		if strings.Contains(got.out, "warning") {
			t.Fatalf("stdout %q", got.out)
		}
	}
	if srv.Calls()[1].Operation != "GetStrategyVersion" {
		t.Fatalf("call %+v", srv.Calls()[1])
	}

	got := runQuery(t, home, "-o", "json", "strategy", "show", "alpha", "v1")
	if got.code != ExitOK || !bytes.Equal([]byte(got.out), srv.LastBody()) {
		t.Fatalf("json equal %v\n%s\n%s", got.code == ExitOK, got.out, srv.LastBody())
	}
	if !strings.Contains(got.out, digA) {
		t.Fatalf("json %s", got.out)
	}

	got = runQuery(t, home, "strategy", "show", "alpha", "missing")
	if got.code != ExitNotFound || got.out != "" || !strings.Contains(got.err, "not_found (12):") {
		t.Fatalf("missing %+v", got)
	}

	badDetail := "manifest is not a strategy artifact"
	bad := &apiv1.Problem{
		Type: "about:blank", Title: "Invalid Artifact", Status: http.StatusBadGateway,
		Code: apiv1.ErrorCodeInvalidArtifact, Detail: &badDetail,
	}
	_, home = startCat(t, &apitest.Catalog{FailAt: 1, Fail: bad, Manifests: cat.Manifests})
	got = runQuery(t, home, "strategy", "show", "alpha", "v1")
	if got.code != ExitInvalidArtifact || got.out != "" || !strings.Contains(got.err, "invalid_artifact (21):") {
		t.Fatalf("artifact %+v", got)
	}
	if strings.Contains(got.err, "registry connection problem") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestInstanceQuery(t *testing.T) {
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	host := "box-a"
	dir := "/tmp/mftiko-test"
	last := true
	present := &apiv1.AgentStatus{Epoch: 4, Release: "rel-1", StartedAt: started, Host: &host, WorkDir: &dir}
	known := &apiv1.AgentStatus{Epoch: 4, Release: "rel-1", StartedAt: started, Host: &host, WorkDir: &dir, LastKnown: &last}
	fresh := apiv1.Freshness{UnavailableRegions: []string{}, ControllerAbsent: false}
	feed := "book"
	reason := "kept"
	apiID := apiv1.APIIDValue(42)
	stage := "live"
	phase := apiv1.PhaseRunning
	demands := []apiv1.DemandStatus{{
		SessionId: "abc123", Kind: apiv1.DemandStatusKindFeed, State: apiv1.EntryStateReady, Feed: &feed, Reason: &reason,
	}}
	account := []apiv1.DemandStatus{{
		SessionId: "abc123", Kind: apiv1.DemandStatusKindAccount, State: apiv1.EntryStatePending, ApiId: &apiID,
	}}
	workers := []apiv1.Worker{{Key: "md/book", Phase: apiv1.WorkerPhaseRunning, Incarnation: 3, Restarts: 1, Stage: &stage}}
	sessions := []apiv1.InstanceSession{{SessionId: "def456", Name: "tape", Phase: &phase}}
	cat := &apitest.Catalog{
		Instances: []apiv1.Instance{
			{Name: "md1", Plane: "md", Region: "east", RegionAvailable: true, Agent: present},
			{Name: "td1", Plane: "td", Region: "west", RegionAvailable: false, Agent: known},
			{Name: "sts1", Plane: "sts", Region: "east", RegionAvailable: true},
		},
		Controller: apiv1.ControllerStatus{Present: true, HomeRegion: "east", Agent: present},
		Freshness:  fresh,
		Details: map[string]apiv1.InstanceDetail{
			"md1": {
				Name: "md1", Plane: "md", Region: "east", RegionAvailable: true, Agent: present,
				Workers: workers, Demands: &demands, Freshness: fresh,
			},
			"td1": {
				Name: "td1", Plane: "td", Region: "west", RegionAvailable: false, Agent: known,
				Workers: []apiv1.Worker{}, Demands: &account, Freshness: fresh,
			},
			"sts1": {
				Name: "sts1", Plane: "sts", Region: "east", RegionAvailable: true,
				Workers: []apiv1.Worker{}, Sessions: &sessions, Freshness: fresh,
			},
		},
	}
	srv, home := startCat(t, cat)
	got := runQuery(t, home, "instance", "list")
	if got.code != ExitOK || got.err != "" {
		t.Fatalf("list %+v", got)
	}
	md := lineContaining(got.out, "md1")
	td := lineContaining(got.out, "td1")
	sts := lineContaining(got.out, "sts1")
	if !strings.Contains(md, "yes") || !strings.Contains(md, "false") || !strings.Contains(md, "east") {
		t.Fatalf("md row %q", md)
	}
	if !strings.Contains(td, "no") || !strings.Contains(td, "true") || !strings.Contains(td, "west") {
		t.Fatalf("td row %q", td)
	}
	if !strings.Contains(sts, "no") || !strings.Contains(sts, "false") {
		t.Fatalf("sts row %q", sts)
	}
	if !strings.Contains(got.out, "CONTROLLER") || !strings.Contains(got.out, "east") {
		t.Fatalf("controller missing: %q", got.out)
	}
	if strings.Contains(got.out, "warning") {
		t.Fatalf("stdout %q", got.out)
	}
	if srv.Calls()[0].Path != "/api/v1/instances" || srv.Calls()[0].RawQuery != "" {
		t.Fatalf("list call %+v", srv.Calls()[0])
	}

	got = runQuery(t, home, "-o", "json", "instance", "list")
	if got.code != ExitOK || !bytes.Equal([]byte(got.out), srv.LastBody()) {
		t.Fatalf("json\n%s\n%s", got.out, srv.LastBody())
	}

	got = runQuery(t, home, "instance", "show", "md1")
	if got.code != ExitOK {
		t.Fatalf("show %+v", got)
	}
	if row(t, got.out, "AGENT") != "yes" || row(t, got.out, "LAST_KNOWN") != "false" {
		t.Fatalf("agent rows %q", got.out)
	}
	if row(t, got.out, "HOST") != host || row(t, got.out, "WORK_DIR") != dir {
		t.Fatalf("place %q", got.out)
	}
	local := started.In(time.Local).Format(time.RFC3339)
	if !strings.Contains(got.out, local) || !strings.Contains(got.out, "md/book") || !strings.Contains(got.out, "book") {
		t.Fatalf("show body %q want time %s", got.out, local)
	}
	if !strings.Contains(got.out, "DEMANDS") || strings.Contains(got.out, "SESSIONS") {
		t.Fatalf("sections %q", got.out)
	}

	got = runQuery(t, home, "instance", "show", "td1")
	if got.code != ExitOK || row(t, got.out, "AGENT") != "no" || row(t, got.out, "LAST_KNOWN") != "true" {
		t.Fatalf("last known %+v", got)
	}
	if row(t, got.out, "HOST") != host || row(t, got.out, "WORK_DIR") != dir || !strings.Contains(got.out, "42") {
		t.Fatalf("last known fields %q", got.out)
	}

	got = runQuery(t, home, "instance", "show", "sts1")
	if got.code != ExitOK || strings.Contains(got.out, "HOST") || !strings.Contains(got.out, "SESSIONS") || !strings.Contains(got.out, "def456") {
		t.Fatalf("sts show %q", got.out)
	}
	if !strings.Contains(got.out, "WORKERS") {
		t.Fatalf("workers section missing: %q", got.out)
	}

	got = runQuery(t, home, "-o", "json", "instance", "show", "md1")
	if got.code != ExitOK || !bytes.Equal([]byte(got.out), srv.LastBody()) || !strings.Contains(got.out, "2026-01-02T03:04:05Z") {
		t.Fatalf("show json %s", got.out)
	}

	got = runQuery(t, home, "instance", "show", "nope")
	if got.code != ExitNotFound || got.out != "" {
		t.Fatalf("missing %+v", got)
	}

	region := "region east is down"
	down := &apiv1.Problem{
		Type: "about:blank", Title: "Region Unavailable", Status: http.StatusServiceUnavailable,
		Code: apiv1.ErrorCodeRegionUnavailable, Detail: &region,
	}
	_, home = startCat(t, &apitest.Catalog{FailAt: 1, Fail: down})
	got = runQuery(t, home, "instance", "show", "md1")
	if got.code != ExitRegionUnavailable || got.out != "" || !strings.Contains(got.err, "region_unavailable (17):") {
		t.Fatalf("503 %+v", got)
	}
	if strings.Contains(got.err, "warning") || strings.Contains(got.err, "registry connection problem") {
		t.Fatalf("stderr %q", got.err)
	}
}

func TestPartialAvailability(t *testing.T) {
	base := apiv1.Instance{Name: "md1", Plane: "md", Region: "east", RegionAvailable: true}
	ctrl := apiv1.ControllerStatus{Present: true, HomeRegion: "east"}

	regions := &apitest.Catalog{
		Instances:  []apiv1.Instance{base},
		Controller: ctrl,
		Freshness:  apiv1.Freshness{UnavailableRegions: []string{"east", "west"}},
	}
	_, home := startCat(t, regions)
	got := runQuery(t, home, "instance", "list")
	if got.code != ExitOK || got.err != "mftiko: warning: unavailable regions: east, west\n" {
		t.Fatalf("regions %+v", got)
	}
	if strings.Contains(got.out, "warning") || !strings.Contains(got.out, "md1") {
		t.Fatalf("stdout %q", got.out)
	}
	if strings.Count(got.err, "mftiko: warning:") != 1 {
		t.Fatalf("warnings %q", got.err)
	}

	absent := &apitest.Catalog{
		Instances:  []apiv1.Instance{base},
		Controller: ctrl,
		Freshness:  apiv1.Freshness{ControllerAbsent: true},
	}
	_, home = startCat(t, absent)
	got = runQuery(t, home, "instance", "list")
	if got.code != ExitOK || got.err != "mftiko: warning: controller absent\n" {
		t.Fatalf("absent %+v", got)
	}

	both := &apitest.Catalog{
		Instances:  []apiv1.Instance{base},
		Controller: ctrl,
		Freshness:  apiv1.Freshness{UnavailableRegions: []string{"east", "west"}, ControllerAbsent: true},
	}
	srv, home := startCat(t, both)
	got = runQuery(t, home, "-o", "json", "instance", "list")
	if got.code != ExitOK || got.err != "mftiko: warning: unavailable regions: east, west; controller absent\n" {
		t.Fatalf("both %+v", got)
	}
	if !bytes.Equal([]byte(got.out), srv.LastBody()) || strings.Contains(got.out, "warning") {
		t.Fatalf("json stdout changed: %s", got.out)
	}

	p1 := []byte("{\"items\":[{\"id\":\"a\",\"repository\":\"example.invalid/a\"}],\"unavailable_regions\":[\"east\"],\"next_cursor\":\"p2\"}\n")
	p2 := []byte("{\"items\":[{\"id\":\"b\",\"repository\":\"example.invalid/b\"}],\"freshness\":{\"unavailable_regions\":[\"west\"],\"controller_absent\":true}}\n")
	pages := &apitest.Catalog{StrategyPages: map[string]apitest.RawPage{
		"":   {Body: p1},
		"p2": {Body: p2},
	}}
	_, home = startCat(t, pages)
	got = runQuery(t, home, "-o", "json", "-v", "strategy", "list")
	if got.code != ExitOK {
		t.Fatalf("union exit %d stderr %q stdout %s", got.code, got.err, got.out)
	}
	var body struct {
		Items              []apiv1.Strategy `json:"items"`
		UnavailableRegions []string         `json:"unavailable_regions"`
		NextCursor         *string          `json:"next_cursor"`
		ControllerAbsent   *bool            `json:"controller_absent"`
	}
	if err := json.Unmarshal([]byte(got.out), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 2 || body.Items[0].Id != "a" || body.Items[1].Id != "b" {
		t.Fatalf("items %+v", body.Items)
	}
	if len(body.UnavailableRegions) != 2 || body.UnavailableRegions[0] != "east" || body.UnavailableRegions[1] != "west" {
		t.Fatalf("regions %+v", body.UnavailableRegions)
	}
	if body.NextCursor != nil || body.ControllerAbsent != nil {
		t.Fatalf("extra fields cursor %v absent %v", body.NextCursor, body.ControllerAbsent)
	}
	if strings.Contains(got.out, "next_cursor") || strings.Contains(got.out, "controller_absent") {
		t.Fatalf("merged text %s", got.out)
	}
	warn, ids, ok := strings.Cut(got.err, "X-Request-Id:")
	if !ok || warn != "mftiko: warning: unavailable regions: east, west; controller absent\n" {
		t.Fatalf("stderr %q", got.err)
	}
	if strings.Count(got.err, "mftiko: warning:") != 1 || strings.Count(got.err, "X-Request-Id: req-q\n") != 2 {
		t.Fatalf("stderr %q", got.err)
	}
	if strings.Contains(ids, "warning") {
		t.Fatalf("ids %q", ids)
	}
}

func TestAccountKinds(t *testing.T) {
	cat := &apitest.Catalog{Kinds: []apiv1.AccountKind{
		{Kind: "FakeUM", Label: "Fake UM", Trades: []string{"fake-um"}, KeyTypes: []apiv1.KeyType{apiv1.Hmac, apiv1.Ed25519}, Passphrase: true, Supported: true},
		{Kind: "FakePM", Label: "Fake PM", Trades: []string{"fake-a", "fake-b"}, KeyTypes: []apiv1.KeyType{apiv1.Hmac}, Passphrase: false, Supported: false},
	}}
	srv, home := startCat(t, cat)
	got := runQuery(t, home, "account-kind", "list")
	if got.code != ExitOK || got.err != "" {
		t.Fatalf("list %+v", got)
	}
	if !strings.Contains(got.out, "FakeUM") || !strings.Contains(got.out, "fake-um") || !strings.Contains(got.out, "hmac, ed25519") {
		t.Fatalf("um %q", got.out)
	}
	if !strings.Contains(got.out, "FakePM") || !strings.Contains(got.out, "fake-a, fake-b") || !strings.Contains(got.out, "false") {
		t.Fatalf("pm %q", got.out)
	}
	if srv.Calls()[0].Path != "/api/v1/account-kinds" || srv.Calls()[0].RawQuery != "" {
		t.Fatalf("call %+v", srv.Calls()[0])
	}
	got = runQuery(t, home, "-o", "json", "account-kind", "list")
	if got.code != ExitOK || !bytes.Equal([]byte(got.out), srv.LastBody()) {
		t.Fatalf("json\n%s\n%s", got.out, srv.LastBody())
	}

	_, home = startCat(t, &apitest.Catalog{})
	got = runQuery(t, home, "account-kind", "list")
	if got.code != ExitOK || got.out != "KIND  LABEL  TRADES  KEY_TYPES  PASSPHRASE  SUPPORTED\n" {
		t.Fatalf("empty %q err %q", got.out, got.err)
	}
}

func TestQueryUsage(t *testing.T) {
	var in countReader
	in.r = strings.NewReader(tokenA + "\n")
	got := runReader(context.Background(), t.TempDir(), nil, &in, "login", "--limit", "1")
	if got.code != ExitUsage || in.n != 0 || !strings.Contains(got.err, "only for strategy list and strategy versions") {
		t.Fatalf("login limit %+v reads %d", got, in.n)
	}
	noLeak(t, tokenA, got.out+got.err)

	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "whoami", "--cursor", "page")
	if got.code != ExitUsage || !strings.Contains(got.err, "only for strategy list and strategy versions") {
		t.Fatalf("whoami cursor %+v", got)
	}

	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "--limit", tokenA, "strategy", "list")
	if got.code != ExitUsage {
		t.Fatalf("bad limit %d", got.code)
	}
	noLeak(t, tokenA, got.out+got.err)

	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "help", "strategy")
	if got.code != ExitOK || got.err != "" || !strings.Contains(got.out, "strategy versions") {
		t.Fatalf("help %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "help", "instance")
	if got.code != ExitOK || !strings.Contains(got.out, "instance show") {
		t.Fatalf("help instance %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "help", "account-kind")
	if got.code != ExitOK || !strings.Contains(got.out, "account-kind list") {
		t.Fatalf("help kinds %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "strategy")
	if got.code != ExitUsage || !strings.Contains(got.err, "strategy list") {
		t.Fatalf("bare %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "strategy", "bogus")
	if got.code != ExitUsage || !strings.Contains(got.err, "unknown command") || !strings.Contains(got.err, "strategy list") {
		t.Fatalf("bogus %+v", got)
	}

	cat := &apitest.Catalog{Strategies: []apiv1.Strategy{
		{Id: "a", Repository: "example.invalid/a"},
		{Id: "b", Repository: "example.invalid/b"},
	}, PageSize: 2}
	srv, home := startCat(t, cat)
	got = runQuery(t, home, "strategy", "list", "--limit", "2")
	if got.code != ExitOK || len(srv.Calls()) != 1 || !strings.Contains(got.out, "a") || !strings.Contains(got.out, "b") {
		t.Fatalf("flag after command %+v calls %d", got, len(srv.Calls()))
	}

	got = runQuery(t, home, "instance", "list", "--limit", "1")
	if got.code != ExitUsage || !strings.Contains(got.err, "only for strategy list") || len(srv.Calls()) != 1 {
		t.Fatalf("instance limit %+v calls %d", got, len(srv.Calls()))
	}
}

func startCat(t *testing.T, cat *apitest.Catalog) (*apitest.Server, string) {
	t.Helper()
	srv := apitest.Start(apitest.Option{Token: tokenA, Name: "ada", RequestID: "req-q", Catalog: cat})
	t.Cleanup(srv.Close)
	home := t.TempDir()
	writeConfig(t, home, configYAML(srv.URL(), true, tokenA))
	return srv, home
}

func runQuery(t *testing.T, home string, args ...string) result {
	t.Helper()
	got := runAt(context.Background(), home, map[string]string{"MFTIKO_CONTEXT": "dev"}, strings.NewReader(""), args...)
	noLeak(t, tokenA, got.out+got.err)
	noLeak(t, tokenB, got.out+got.err)
	noLeak(t, tokenC, got.out+got.err)
	return got
}

func lineContaining(out, needle string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	return ""
}

func row(t *testing.T, out, field string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if line == field {
			return ""
		}
		if strings.HasPrefix(line, field+" ") {
			return strings.TrimSpace(line[len(field):])
		}
	}
	t.Fatalf("row %s missing in %q", field, out)
	return ""
}
