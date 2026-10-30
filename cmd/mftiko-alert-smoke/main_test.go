package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	pathA = "/var/lib/mftiko/sts-agent/log/sts/1111111111111111.stderr"
	pathB = "/var/lib/mftiko/sts-agent/log/sts/2222222222222222.stderr"
	pathC = "/var/lib/mftiko/sts-agent/log/sts/3333333333333333.stderr"
)

var (
	r1 = session{rule: "R1", id: "aaaaaa", stderr: pathA}
	r2 = session{rule: "R2", id: "bbbbbb", stderr: pathB}
	r5 = session{rule: "R5", id: "cccccc", stderr: pathC}
)

// payload is a webhook body as the dev Alertmanager sends it, with one alert.
func payload(status string, group, labels, annotations map[string]string) string {
	b, err := json.Marshal(map[string]any{
		"receiver": "webhook", "status": status, "groupLabels": group,
		"alerts": []map[string]any{{"status": status, "labels": labels, "annotations": annotations}},
	})
	if err != nil {
		panic(err)
	}
	return string(b)
}

// r1Payload is what R1's rule makes for r1.
func r1Payload() string {
	return payload("firing",
		map[string]string{"alertname": "MftikoStrategyLogError", "instance": "sts1", "filename": pathA},
		map[string]string{"alertname": "MftikoStrategyLogError", "instance": "sts1", "region": "dev", "filename": pathA, "session_id": "aaaaaa"},
		map[string]string{"summary": "A strategy wrote a log line at warn or above.", "logs": "mftiko session logs aaaaaa --stream stderr --tail 50"})
}

func r2Payload() string {
	return payload("firing",
		map[string]string{"alertname": "MftikoStrategyLogCrash", "instance": "sts1", "filename": pathB},
		map[string]string{"alertname": "MftikoStrategyLogCrash", "instance": "sts1", "region": "dev", "filename": pathB},
		map[string]string{"summary": "A strategy panicked, hit a fatal error, or lost output.", "logs": `{filename="` + pathB + `"} |= "[mftiko-shim]"`})
}

func agentDownPayload(status string) string {
	return payload(status,
		map[string]string{"alertname": "MftikoAgentDown", "instance": "sts1"},
		map[string]string{"alertname": "MftikoAgentDown", "instance": "sts1", "region": "dev", "plane": "sts", "job": "mftiko-api"},
		map[string]string{})
}

// writeReceived writes bodies as mftiko-webhook-dev would, one line each.
func writeReceived(t *testing.T, path string, bodies ...string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, b := range bodies {
		if _, err := f.WriteString(`{"time":"2026-01-01T00:00:00Z","path":"/alertmanager","body":` + b + "}\n"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadReceived(t *testing.T) {
	path := filepath.Join(t.TempDir(), "received.jsonl")
	if ns, err := readReceived(path, 0); err != nil || len(ns) != 0 {
		t.Fatalf("missing file: %v %v", ns, err)
	}
	writeReceived(t, path, agentDownPayload("firing"), `"not a payload"`)
	from := fileSize(path)
	writeReceived(t, path, r1Payload())
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"time":"2026-01-01T00:00:00Z","body":{"status":"fir`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	all, err := readReceived(path, 0)
	if err != nil || len(all) != 2 || all[0].GroupLabels["alertname"] != "MftikoAgentDown" || all[1].GroupLabels["alertname"] != "MftikoStrategyLogError" {
		t.Fatalf("from 0: %+v %v", all, err)
	}
	after, err := readReceived(path, from)
	if err != nil || len(after) != 1 || after[0].raw != r1Payload() {
		t.Fatalf("from %d: %+v %v", from, after, err)
	}
	if again, err := readReceived(path, fileSize(path)+100); err != nil || len(again) != 2 {
		t.Fatalf("past the end, read from the start: %+v %v", again, err)
	}
	if mid, err := readReceived(path, from-5); err != nil || len(mid) != 1 || mid[0].raw != r1Payload() {
		t.Fatalf("from inside a line: %+v %v", mid, err)
	}
}

func TestLastStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "received.jsonl")
	group := agentDownWant("").group
	writeReceived(t, path, agentDownPayload("firing"), r1Payload())
	ns, _ := readReceived(path, 0)
	if got := lastStatus(ns, group); got != "firing" {
		t.Fatalf("got %q, want firing", got)
	}
	writeReceived(t, path, agentDownPayload("resolved"), r2Payload())
	ns, _ = readReceived(path, 0)
	if got := lastStatus(ns, group); got != "resolved" {
		t.Fatalf("got %q, want resolved", got)
	}
	if got := lastStatus(ns, map[string]string{"alertname": "MftikoAgentDown", "instance": "md1"}); got != "" {
		t.Fatalf("other group: got %q", got)
	}
}

func notificationOf(t *testing.T, body string) notification {
	t.Helper()
	var n notification
	if err := json.Unmarshal([]byte(body), &n); err != nil {
		t.Fatal(err)
	}
	n.raw = body
	return n
}

func TestLogWant(t *testing.T) {
	w1 := logWant(r1)
	if w1.what != "R1 MftikoStrategyLogError" || w1.labels["session_id"] != "aaaaaa" || w1.logs != "mftiko session logs aaaaaa --stream stderr --tail 50" {
		t.Errorf("R1: %+v", w1)
	}
	w2 := logWant(r2)
	if w2.what != "R2 MftikoStrategyLogCrash" || w2.noLabel != "session_id" || w2.labels["session_id"] != "" || w2.logs != `{filename="`+pathB+`"} |= "[mftiko-shim]"` {
		t.Errorf("R2: %+v", w2)
	}
	w5 := logWant(r5)
	if w5.what != "R5 MftikoStrategyOverloaded" || w5.group["filename"] != pathC || w5.labels["alertname"] != "MftikoStrategyOverloaded" {
		t.Errorf("R5: %+v", w5)
	}
}

func TestWantCheck(t *testing.T) {
	w1, w2 := logWant(r1), logWant(r2)
	for _, tc := range []struct {
		name string
		w    want
		body string
		ok   bool
		err  string
	}{
		{"R1", w1, r1Payload(), true, ""},
		{"R2", w2, r2Payload(), true, ""},
		{"agent down", agentDownWant("firing"), agentDownPayload("firing"), true, ""},
		{"another group", w1, r2Payload(), false, ""},
		{"resolved is not firing", agentDownWant("firing"), agentDownPayload("resolved"), false, ""},
		{"logs annotation", w1, strings.Replace(r1Payload(), "--tail 50", "--tail 9", 1), false, "logs annotation"},
		{"log text", w1, strings.Replace(r1Payload(), "warn or above.", "warn or above: init failed", 1), false, "log line's text"},
		{"R2 with session_id", w2, strings.Replace(r2Payload(), `"region":"dev"`, `"region":"dev","session_id":"bbbbbb"`, 1), false, "session_id"},
		{"wrong session", w1, strings.ReplaceAll(r1Payload(), "aaaaaa", "dddddd"), false, "no firing alert"},
	} {
		ok, err := tc.w.check(notificationOf(t, tc.body))
		if ok != tc.ok || (err == nil) != (tc.err == "") || (err != nil && !strings.Contains(err.Error(), tc.err)) {
			t.Errorf("%s: ok %v err %v, want ok %v err %q", tc.name, ok, err, tc.ok, tc.err)
		}
	}
}

func TestWaitReceived(t *testing.T) {
	path := filepath.Join(t.TempDir(), "received.jsonl")
	writeReceived(t, path, agentDownPayload("firing"))
	from := fileSize(path)
	writeReceived(t, path, r1Payload(), r2Payload())
	ctx := context.Background()
	if err := waitReceived(ctx, path, from, []want{logWant(r1), logWant(r2)}, time.Second); err != nil {
		t.Fatal(err)
	}
	err := waitReceived(ctx, path, from, []want{logWant(r1), agentDownWant("firing"), logWant(r5)}, 10*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), `MftikoAgentDown{instance="sts1"}, R5 MftikoStrategyOverloaded did not reach`) {
		t.Fatalf("before from, and never: %v", err)
	}
	from = fileSize(path)
	writeReceived(t, path, strings.Replace(r1Payload(), "--stream stderr", "--stream stdout", 1))
	start := time.Now()
	err = waitReceived(ctx, path, from, []want{logWant(r1)}, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "logs annotation") || time.Since(start) > 10*time.Second {
		t.Fatalf("a wrong annotation does not stop the wait: %v after %s", err, time.Since(start))
	}
}

func amAlertOf(fp, state string, by []string, labels map[string]string) amAlert {
	var a amAlert
	a.Fingerprint, a.Labels = fp, labels
	a.Status.State, a.Status.InhibitedBy = state, by
	return a
}

func TestInhibited(t *testing.T) {
	const down = "0000000000000001"
	wants := []want{logWant(r1), logWant(r2)}
	a1 := amAlertOf("000000000000000a", "suppressed", []string{down}, wants[0].labels)
	a2 := amAlertOf("000000000000000b", "suppressed", []string{down}, wants[1].labels)
	if err := inhibited([]amAlert{a2, a1}, down, wants); err != nil {
		t.Fatal(err)
	}
	active := amAlertOf("000000000000000b", "active", nil, wants[1].labels)
	if err := inhibited([]amAlert{a1, active}, down, wants); err == nil || !strings.Contains(err.Error(), "R2 MftikoStrategyLogCrash is active") {
		t.Fatalf("active: %v", err)
	}
	other := amAlertOf("000000000000000b", "suppressed", []string{"0000000000000002"}, wants[1].labels)
	if err := inhibited([]amAlert{a1, other}, down, wants); err == nil {
		t.Fatal("inhibited by another alert: no error")
	}
	if err := inhibited([]amAlert{a1}, down, wants); err == nil || !strings.Contains(err.Error(), "not in Alertmanager") {
		t.Fatalf("missing: %v", err)
	}
}

func TestGetAlertsAndWaitInhibited(t *testing.T) {
	wants := []want{logWant(r1)}
	var filters [][]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/alerts" {
			http.NotFound(w, r)
			return
		}
		f := r.URL.Query()["filter"]
		filters = append(filters, f)
		var out []amAlert
		if slices.Contains(f, `alertname="MftikoAgentDown"`) {
			out = append(out, amAlertOf("0000000000000001", "active", nil, agentDownWant("").labels))
		} else {
			out = append(out, amAlertOf("000000000000000a", "suppressed", []string{"0000000000000001"}, wants[0].labels))
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()
	if err := waitInhibited(context.Background(), srv.URL, wants, time.Second); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{`alertname="MftikoAgentDown"`, `instance="sts1"`},
		{`alertname=~"MftikoStrategy.*"`, `instance="sts1"`},
	}
	if len(filters) != 2 || !slices.Equal(filters[0], want[0]) || !slices.Equal(filters[1], want[1]) {
		t.Fatalf("filters %q, want %q", filters, want)
	}
}

func TestFollowQuery(t *testing.T) {
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[{"stream":{"filename":"` + pathB + `"},"values":[
			["1767225600000000000","[mftiko-shim] 2026-01-01T00:00:00Z worker sts/bbbbbb incarnation 1"]]}]}}`))
	}))
	defer srv.Close()
	w := logWant(r2)
	if err := followQuery(context.Background(), srv.URL, w, r2); err != nil {
		t.Fatal(err)
	}
	if query != w.logs {
		t.Fatalf("query %q, want %q", query, w.logs)
	}
	if err := followQuery(context.Background(), srv.URL, w, session{rule: "R2", id: "dddddd", stderr: pathB}); err == nil {
		t.Fatal("another session's header: no error")
	}
}

func TestHasLine(t *testing.T) {
	out := "[mftiko-shim] 2026-01-01T00:00:00Z worker sts/aaaaaa incarnation 1\n" +
		`{"level":"error","session_id":"aaaaaa","error":"x","message":"init failed"}` + "\n"
	if !hasLine(out, map[string]string{"session_id": "aaaaaa", "level": "error", "message": "init failed"}) {
		t.Error("the error line is not found")
	}
	if hasLine(out, map[string]string{"session_id": "aaaaaa", "level": "warn"}) {
		t.Error("found a warn line")
	}
}

func TestBusy(t *testing.T) {
	var v instanceView
	if err := json.Unmarshal([]byte(`{"agent":null,"sessions":[
		{"session_id":"aaaaaa","phase":"done"},{"session_id":"bbbbbb","phase":"running"},
		{"session_id":"cccccc","phase":"waiting"},{"session_id":"dddddd","phase":"restarting"}]}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.Agent != nil {
		t.Error("null agent is present")
	}
	if got := v.busy(); !slices.Equal(got, []string{"bbbbbb (running)", "dddddd (restarting)"}) {
		t.Errorf("busy %q", got)
	}
}
