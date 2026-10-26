package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestRenderMetrics(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	mdPrefix := state.WorkerStatusPrefix(state.PlaneMD, "md-a")
	tw := state.View{Entries: map[string]state.Entry{
		state.AgentKey(state.PlaneMD, "md-a"):                    {Value: []byte("1")},
		state.AgentKey(state.PlaneCtl, state.ControllerInstance): {Value: []byte("1")},
		mdPrefix + "a":   {Value: mustPhase(t, "RUNNING")},
		mdPrefix + "b":   {Value: mustPhase(t, "RUNNING")},
		mdPrefix + "c":   {Value: mustPhase(t, "LOST")},
		mdPrefix + "d":   {Value: mustPhase(t, "ODD")},
		mdPrefix + "e":   {Value: []byte{0xff}},
		mdPrefix + "f":   {Value: mustPhase(t, "")},
		mdPrefix + "g/h": {Value: mustPhase(t, "RUNNING")},
	}}
	jp := state.View{Entries: map[string]state.Entry{
		state.AgentKey(state.PlaneTD, "td-a"): {Value: []byte("1")},
	}}
	cfg := Config{
		UnavailableAfter: 15 * time.Second,
		Topology: state.Topology{Instances: map[string]string{
			"md-a": "tw", "sts-a": "tw", "td-a": "jp", "md-z": "eu",
			state.ControllerInstance: "tw",
		}},
		Planes: map[string]state.Plane{
			"md-a": state.PlaneMD, "sts-a": state.PlaneSTS, "td-a": state.PlaneTD,
			"md-z": state.PlaneMD, "nope": state.PlaneCtl,
		},
	}
	obs := []regionObs{
		{name: "tw", synced: true, contact: now, view: tw},
		{name: "jp", synced: false, view: jp},
	}
	got := renderMetrics(cfg, now, obs, 0, 1)
	want := "" +
		"# HELP mftiko_agent_up " + helpAgentUp + "\n" +
		"# TYPE mftiko_agent_up gauge\n" +
		"mftiko_agent_up{instance=\"md-a\",plane=\"md\",region=\"tw\"} 1\n" +
		"mftiko_agent_up{instance=\"sts-a\",plane=\"sts\",region=\"tw\"} 0\n" +
		"\n" +
		"# HELP mftiko_controller_leader " + helpLeader + "\n" +
		"# TYPE mftiko_controller_leader gauge\n" +
		"mftiko_controller_leader{instance=\"controller\",region=\"tw\"} 1\n" +
		"\n" +
		"# HELP mftiko_worker_phase " + helpPhase + "\n" +
		"# TYPE mftiko_worker_phase gauge\n" +
		"mftiko_worker_phase{instance=\"md-a\",phase=\"STARTING\",plane=\"md\",region=\"tw\"} 0\n" +
		"mftiko_worker_phase{instance=\"md-a\",phase=\"RUNNING\",plane=\"md\",region=\"tw\"} 2\n" +
		"mftiko_worker_phase{instance=\"md-a\",phase=\"STOPPING\",plane=\"md\",region=\"tw\"} 0\n" +
		"mftiko_worker_phase{instance=\"md-a\",phase=\"BACKOFF\",plane=\"md\",region=\"tw\"} 0\n" +
		"mftiko_worker_phase{instance=\"md-a\",phase=\"EXITED\",plane=\"md\",region=\"tw\"} 0\n" +
		"mftiko_worker_phase{instance=\"md-a\",phase=\"LOST\",plane=\"md\",region=\"tw\"} 1\n" +
		"mftiko_worker_phase{instance=\"md-a\",phase=\"ODD\",plane=\"md\",region=\"tw\"} 1\n" +
		"mftiko_worker_phase{instance=\"sts-a\",phase=\"STARTING\",plane=\"sts\",region=\"tw\"} 0\n" +
		"mftiko_worker_phase{instance=\"sts-a\",phase=\"RUNNING\",plane=\"sts\",region=\"tw\"} 0\n" +
		"mftiko_worker_phase{instance=\"sts-a\",phase=\"STOPPING\",plane=\"sts\",region=\"tw\"} 0\n" +
		"mftiko_worker_phase{instance=\"sts-a\",phase=\"BACKOFF\",plane=\"sts\",region=\"tw\"} 0\n" +
		"mftiko_worker_phase{instance=\"sts-a\",phase=\"EXITED\",plane=\"sts\",region=\"tw\"} 0\n" +
		"mftiko_worker_phase{instance=\"sts-a\",phase=\"LOST\",plane=\"sts\",region=\"tw\"} 0\n" +
		"\n" +
		"# HELP mftiko_worker_bus_up " + helpBus + "\n" +
		"# TYPE mftiko_worker_bus_up gauge\n" +
		"\n" +
		"# HELP mftiko_region_available " + helpRegion + "\n" +
		"# TYPE mftiko_region_available gauge\n" +
		"mftiko_region_available{region=\"jp\"} 0\n" +
		"mftiko_region_available{region=\"tw\"} 1\n" +
		"\n" +
		"# HELP mftiko_postgres_up " + helpPostgres + "\n" +
		"# TYPE mftiko_postgres_up gauge\n" +
		"mftiko_postgres_up 0\n" +
		"\n" +
		"# HELP mftiko_registry_up " + helpRegistry + "\n" +
		"# TYPE mftiko_registry_up gauge\n" +
		"mftiko_registry_up 1\n"
	if string(got) != want {
		t.Fatalf("metrics:\n%s\nwant:\n%s", got, want)
	}

	stale := []regionObs{{name: "tw", synced: true, contact: now.Add(-15*time.Second - time.Nanosecond), view: tw}}
	text := string(renderMetrics(cfg, now, stale, 0, 0))
	if !bytes.Contains([]byte(text), []byte("mftiko_agent_up{instance=\"md-a\",plane=\"md\",region=\"tw\"} 1\n")) ||
		!bytes.Contains([]byte(text), []byte("mftiko_region_available{region=\"tw\"} 0\n")) {
		t.Fatalf("stale region:\n%s", text)
	}
	fresh := []regionObs{{name: "tw", synced: true, contact: now.Add(-15 * time.Second), view: tw}}
	text = string(renderMetrics(cfg, now, fresh, 0, 0))
	if !bytes.Contains([]byte(text), []byte("mftiko_region_available{region=\"tw\"} 1\n")) {
		t.Fatalf("contact at the threshold:\n%s", text)
	}
}

func TestRenderMetricsEscapesLabels(t *testing.T) {
	phase := "a\"b\\c"
	prefix := state.WorkerStatusPrefix(state.PlaneMD, "md-a")
	cfg := Config{
		UnavailableAfter: 15 * time.Second,
		Topology:         state.Topology{Instances: map[string]string{"md-a": "tw"}},
		Planes:           map[string]state.Plane{"md-a": state.PlaneMD},
	}
	obs := []regionObs{{
		name: "tw", synced: true, contact: time.Unix(1_700_000_000, 0),
		view: state.View{Entries: map[string]state.Entry{prefix + "a": {Value: mustPhase(t, phase)}}},
	}}
	got := string(renderMetrics(cfg, obs[0].contact, obs, 0, 0))
	want := "mftiko_worker_phase{instance=\"md-a\",phase=\"a\\\"b\\\\c\",plane=\"md\",region=\"tw\"} 1\n"
	if !bytes.Contains([]byte(got), []byte(want)) {
		t.Fatalf("escaped line missing:\n%s", got)
	}
}

func TestMetricsHTTP(t *testing.T) {
	h := testMux(t, "").routes()
	code, hdr, body := do(h, http.MethodGet, "/metrics")
	if code != http.StatusOK || hdr.Get("Content-Type") != metricsContentType || !bytes.Contains(body, []byte("mftiko_postgres_up 0\n")) {
		t.Fatalf("GET %d %q %q", code, hdr.Get("Content-Type"), body)
	}
	code, hdr, body = do(h, http.MethodGet, "/metrics/")
	if code != http.StatusOK || !bytes.Contains(body, []byte("# TYPE mftiko_registry_up gauge\n")) {
		t.Fatalf("GET /metrics/ %d %q", code, body)
	}
	code, hdr, body = do(h, http.MethodHead, "/metrics")
	if code != http.StatusOK || len(body) != 0 || hdr.Get("Content-Type") != metricsContentType {
		t.Fatalf("HEAD %d %q %q", code, hdr.Get("Content-Type"), body)
	}
	code, _, _ = do(h, http.MethodPost, "/metrics")
	if code != http.StatusMethodNotAllowed {
		t.Fatalf("POST %d", code)
	}
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestWorkerBusMetrics(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	mdPrefix := state.WorkerStatusPrefix(state.PlaneMD, "md-a")
	tdPrefix := state.WorkerStatusPrefix(state.PlaneTD, "td-a")
	cfg := Config{
		UnavailableAfter: 15 * time.Second,
		Topology: state.Topology{Instances: map[string]string{
			"md-a": "tw", "td-a": "jp",
		}},
		Planes: map[string]state.Plane{
			"md-a": state.PlaneMD, "td-a": state.PlaneTD,
		},
	}
	tw := state.View{Entries: map[string]state.Entry{
		mdPrefix + "down":  {Value: mustBus(t, "md/b", true)},
		mdPrefix + "up":    {Value: mustBus(t, "md/a", false)},
		mdPrefix + "none":  {Value: mustPhase(t, "RUNNING")},
		mdPrefix + "nokey": {Value: mustBus(t, "", false)},
		mdPrefix + "odd":   {Value: mustBus(t, "a\"b\\c", false)},
	}}
	jp := state.View{Entries: map[string]state.Entry{
		tdPrefix + "a": {Value: mustPhase(t, "RUNNING")},
	}}
	text := string(renderMetrics(cfg, now, []regionObs{
		{name: "tw", synced: true, contact: now, view: tw},
		{name: "jp", synced: false, view: jp},
	}, 0, 0))
	for _, want := range []string{
		"mftiko_worker_bus_up{instance=\"md-a\",plane=\"md\",region=\"tw\",worker=\"a\\\"b\\\\c\"} 1\n",
		"mftiko_worker_bus_up{instance=\"md-a\",plane=\"md\",region=\"tw\",worker=\"md/a\"} 1\n",
		"mftiko_worker_bus_up{instance=\"md-a\",plane=\"md\",region=\"tw\",worker=\"md/b\"} 0\n",
	} {
		if !bytes.Contains([]byte(text), []byte(want)) {
			t.Fatalf("missing %s in\n%s", want, text)
		}
	}
	if bytes.Count([]byte(text), []byte("mftiko_worker_bus_up{")) != 3 {
		t.Fatalf("series count\n%s", text)
	}
	up := strings.Index(text, "worker=\"md/a\"")
	down := strings.Index(text, "worker=\"md/b\"")
	if up < 0 || down < 0 || up > down {
		t.Fatalf("sort\n%s", text)
	}
	stale := string(renderMetrics(cfg, now, []regionObs{{
		name: "tw", synced: true, contact: now.Add(-15*time.Second - time.Nanosecond), view: tw,
	}}, 0, 0))
	if !bytes.Contains([]byte(stale), []byte("worker=\"md/a\"")) {
		t.Fatalf("stale region dropped the series\n%s", stale)
	}
	unsynced := string(renderMetrics(cfg, now, []regionObs{{
		name: "tw", synced: false, contact: now, view: tw,
	}}, 0, 0))
	if bytes.Contains([]byte(unsynced), []byte("mftiko_worker_bus_up{")) {
		t.Fatalf("unsynced region emitted a series\n%s", unsynced)
	}
}

func mustBus(t *testing.T, key string, down bool) []byte {
	t.Helper()
	b, err := proto.Marshal(&statev1.WorkerStatus{
		Phase:  "RUNNING",
		Worker: &commonv1.WorkerRef{Key: key},
		Conditions: []*commonv1.Condition{{
			Type:   condBusDisconnected,
			Status: down,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustPhase(t *testing.T, phase string) []byte {
	t.Helper()
	b, err := proto.Marshal(&statev1.WorkerStatus{Phase: phase})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
