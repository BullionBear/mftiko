package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/cli/apitest"
)

func TestParseSSE(t *testing.T) {
	var p sseParser
	feed := func(s string) []sseEvent {
		t.Helper()
		var out []sseEvent
		for _, line := range strings.Split(s, "\n") {
			ev, ok := p.line(line)
			if ok {
				out = append(out, ev)
			}
		}
		return out
	}
	if got := feed(": keepalive\r\n\r\n"); len(got) != 0 {
		t.Fatalf("comment dispatched %#v", got)
	}
	got := feed("id: tw=4\r\nevent: session\r\ndata: {\"a\":1}\r\ndata: {\"b\":2}\r\n\r\n")
	if len(got) != 1 || got[0].Event != "session" || got[0].ID != "tw=4" || !got[0].HasID || got[0].Data != "{\"a\":1}\n{\"b\":2}" || got[0].Resume != "tw=4" {
		t.Fatalf("event %#v", got)
	}
	got = feed("event: session\ndata: next\n\n")
	if len(got) != 1 || got[0].HasID || got[0].Resume != "tw=4" || got[0].ID != "" {
		t.Fatalf("missing id cleared resume %#v", got)
	}
	got = feed("id:\nevent: session\ndata: cleared\n\n")
	if len(got) != 1 || !got[0].HasID || got[0].ID != "" || got[0].Resume != "" {
		t.Fatalf("empty id %#v", got)
	}
	if p.resume != "" {
		t.Fatalf("parser resume %q", p.resume)
	}
}

func TestNextBackoff(t *testing.T) {
	if sseIdle != 45*time.Second {
		t.Fatalf("idle %s", sseIdle)
	}
	var prev time.Duration
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for _, d := range want {
		prev = nextBackoff(prev)
		if prev != d {
			t.Fatalf("backoff %s, want %s", prev, d)
		}
	}
}

func TestReadSSE(t *testing.T) {
	var resume string
	var n int
	got, err := readSSE(context.Background(), strings.NewReader("event: session\ndata: {}"), &resume, func(sseEvent) error {
		n++
		return nil
	})
	if n != 0 || !errors.Is(err, io.EOF) || !got {
		t.Fatalf("partial n=%d got=%v err=%v", n, got, err)
	}

	got, err = readSSE(context.Background(), strings.NewReader(": keepalive\n\n"), &resume, func(sseEvent) error {
		n++
		return nil
	})
	if n != 0 || !errors.Is(err, io.EOF) || !got {
		t.Fatalf("comment n=%d got=%v err=%v", n, got, err)
	}

	pr, pw := io.Pipe()
	t.Cleanup(func() { pr.Close(); pw.Close() })
	evc := make(chan sseEvent, 1)
	errc := make(chan error, 1)
	go func() {
		_, err := readSSE(context.Background(), pr, &resume, func(ev sseEvent) error {
			evc <- ev
			return errors.New("stop")
		})
		errc <- err
	}()
	if _, err := io.WriteString(pw, ": keepalive\n\nid: tw=1\nevent: session\ndata: {\"ok\":true}\n\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-evc:
		if ev.Event != "session" || ev.ID != "tw=1" || ev.Data != `{"ok":true}` {
			t.Fatalf("ev %#v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event was not delivered")
	}
	select {
	case err := <-errc:
		if err == nil || err.Error() != "stop" {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not return")
	}

	idle := make(chan struct{})
	prev := sseIdleC
	sseIdleC = idle
	t.Cleanup(func() { sseIdleC = prev })
	pr2, pw2 := io.Pipe()
	t.Cleanup(func() { pr2.Close(); pw2.Close() })
	errc = make(chan error, 1)
	go func() {
		_, err := readSSE(context.Background(), pr2, &resume, func(sseEvent) error { return nil })
		errc <- err
	}()
	close(idle)
	select {
	case err := <-errc:
		if !errors.Is(err, errSSEIdle) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("idle did not fire")
	}
}

func TestOpenEventsIdle(t *testing.T) {
	srv := apitest.Start(apitest.Option{
		Token: tokenA,
		Events: func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		},
	})
	t.Cleanup(srv.Close)
	api, err := Dial(Endpoint{URL: srv.URL(), Token: tokenA, InsecureHTTP: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	idle := make(chan struct{})
	close(idle)
	prev := sseIdleC
	sseIdleC = idle
	t.Cleanup(func() { sseIdleC = prev })
	_, err = api.openEvents(context.Background(), "", "")
	if !errors.Is(err, errSSEIdle) {
		t.Fatal(err)
	}
}

func TestSettleWait(t *testing.T) {
	exit := func(outcome, end, reason string, phase apiv1.Phase) apiv1.SessionDetail {
		return detailWait(phase, outcome, end, reason)
	}
	cases := []struct {
		name   string
		target string
		detail apiv1.SessionDetail
		done   bool
		code   int
		label  string
	}{
		{"running", "running", exit("", "", "", apiv1.PhaseRunning), true, 0, "running"},
		{"still pending", "running", exit("", "", "", apiv1.PhasePending), false, 0, ""},
		{"terminal before target", "running", exit("failed", "deps_failed", "", apiv1.PhaseFailed), true, 30, "failed"},
		{"target is failed", "failed", exit("failed", "", "", apiv1.PhaseFailed), true, 0, "failed"},
		{"target is done", "done", exit("failed", "", "", apiv1.PhaseDone), true, 0, "done"},
		{"outcome done", "terminal", exit("done", "", "", apiv1.PhaseDone), true, 0, "done"},
		{"init_failed", "terminal", exit("init_failed", "", "", apiv1.PhaseFailed), true, 31, "init_failed"},
		{"crashed", "terminal", exit("crashed", "", "", apiv1.PhaseFailed), true, 32, "crashed"},
		{"hung", "terminal", exit("hung", "", "", apiv1.PhaseFailed), true, 33, "hung"},
		{"unknown", "terminal", exit("unknown", "", "", apiv1.PhaseFailed), true, 34, "unknown"},
		{"deps_failed", "terminal", exit("", "deps_failed", "", apiv1.PhaseFailed), true, 35, "deps_failed"},
		{"deps_timeout", "terminal", exit("", "deps_timeout", "", apiv1.PhaseFailed), true, 36, "deps_timeout"},
		{"invalid_spec", "terminal", exit("", "invalid_spec", "", apiv1.PhaseFailed), true, 37, "invalid_spec"},
		{"outcome before end reason", "terminal", exit("failed", "deps_failed", "", apiv1.PhaseFailed), true, 30, "failed"},
		{"ended_by_spec before outcome", "terminal", exit("failed", "ended_by_spec", "", apiv1.PhaseDone), true, 0, "ended_by_spec"},
		{"phase done empty", "terminal", exit("", "", "", apiv1.PhaseDone), true, 0, "done"},
		{"reason ended_by_spec", "terminal", exit("", "", "ended_by_spec", apiv1.PhaseDone), true, 0, "ended_by_spec"},
		{"reason deps_timeout", "terminal", exit("", "", "deps_timeout", apiv1.PhaseFailed), true, 36, "deps_timeout"},
		{"phase failed empty", "terminal", exit("", "", "", apiv1.PhaseFailed), true, 30, "failed"},
		{"not terminal yet", "terminal", exit("", "", "", apiv1.PhaseStopping), false, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hit := settleWait(tc.target, tc.detail)
			if hit.done != tc.done || hit.code != tc.code || hit.name != tc.label {
				t.Fatalf("hit %+v", hit)
			}
		})
	}
}

func detailWait(phase apiv1.Phase, outcome, endReason, reason string) apiv1.SessionDetail {
	d := baseSession("abc123", phase)
	if reason != "" {
		d.Status.Reason = reason
	}
	if outcome != "" {
		d.Status.LastExit = &apiv1.Exit{Outcome: apiv1.Outcome(outcome), At: time.Unix(0, 0).UTC()}
	}
	if endReason != "" {
		er := endReason
		d.Assignment = &apiv1.Assignment{
			EndReason:    &er,
			DesiredState: apiv1.DesiredStateRunning,
			IssuedAt:     time.Unix(0, 0).UTC(),
		}
	}
	return d
}

func TestWaitUsage(t *testing.T) {
	home := t.TempDir()
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "--wait")
	if got.code != ExitUsage || !strings.Contains(got.err, "session create --wait needs a phase or terminal") || strings.Contains(got.err, "takes no arguments") {
		t.Fatalf("bare wait %+v", got)
	}
	if !strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("key missing %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "--wait", "nope")
	if got.code != ExitUsage || !strings.Contains(got.err, "needs a phase or terminal") || strings.Contains(got.err, "nope") {
		t.Fatalf("bad target %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "create", "--wait-timeout", "1s")
	if got.code != ExitUsage || !strings.Contains(got.err, "-wait-timeout requires -wait") {
		t.Fatalf("timeout alone %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "end", "--wait")
	if got.code != ExitUsage || !strings.Contains(got.err, "session end takes a session id") {
		t.Fatalf("end bool %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "end", "--wait", "running")
	if got.code != ExitUsage || !strings.Contains(got.err, "end --wait waits until terminal") {
		t.Fatalf("end phase %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "whoami", "-wait")
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -wait") {
		t.Fatalf("whoami %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "end", "--wait-timeout", "1s")
	if got.code != ExitUsage || !strings.Contains(got.err, "-wait-timeout requires -wait") {
		t.Fatalf("end timeout %+v", got)
	}
}

func TestWatchReconnectNoGap(t *testing.T) {
	installSSENoWait(t)
	var n atomic.Int32
	srv, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			if n.Add(1) == 1 {
				writeEvents(w, sseFrame("tw=4", "session", baseSession("aaaaaa", apiv1.PhasePending)))
				return
			}
			writeEvents(w, sseFrame("tw=5", "session", baseSession("bbbbbb", apiv1.PhaseRunning)))
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, errb, done := startCLI(ctx, home, "--context", "dev", "session", "watch")
	out.waitContains(t, "bbbbbb")
	cancel()
	got := waitCLI(t, done)
	if got.code != ExitOK {
		t.Fatalf("exit %d\n%s", got.code, errb.String())
	}
	if strings.Count(got.out, "aaaaaa") != 1 || strings.Count(got.out, "bbbbbb") != 1 {
		t.Fatalf("dup or gap\n%s", got.out)
	}
	var streams []apitest.Call
	for _, c := range srv.Calls() {
		if c.Operation == "StreamEvents" {
			streams = append(streams, c)
		}
	}
	if len(streams) != 2 || streams[0].LastEventID != "" || streams[1].LastEventID != "tw=4" {
		t.Fatalf("streams %+v", streams)
	}
	if streams[1].Authorization != "Bearer "+tokenA || streams[1].Cookie != "" || streams[1].ContentType != "" {
		t.Fatalf("headers %+v", streams[1])
	}
	if !strings.Contains(streams[1].RawQuery, "topics=sessions") || strings.Contains(streams[1].RawQuery, "session_id=") {
		t.Fatalf("query %s", streams[1].RawQuery)
	}
	if !strings.Contains(streams[1].UserAgent, "mftiko-cli/") {
		t.Fatalf("ua %s", streams[1].UserAgent)
	}
	noLeak(t, tokenA, got.out+got.err)
	for _, line := range strings.Split(strings.TrimRight(got.out, "\n"), "\n") {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("line %s: %v", line, err)
		}
	}
}

func TestWatchResetRelists(t *testing.T) {
	installSSENoWait(t)
	srv, home := startSess(t, apitest.Option{
		Sessions: []apiv1.SessionDetail{baseSession("abc123", apiv1.PhaseRunning)},
		Events: func(w http.ResponseWriter, r *http.Request) {
			writeEvents(w, sseFrame("", "reset", apiv1.ResetEvent{Topics: []apiv1.EventTopic{apiv1.Sessions}}))
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, _, done := startCLI(ctx, home, "--context", "dev", "session", "watch")
	waitOp(t, srv, "ListSessions")
	if countOp(srv.Calls(), "StreamEvents") != 1 {
		t.Fatalf("calls %+v", srv.Calls())
	}
	out.waitContains(t, `"event":"reset"`)
	cancel()
	got := waitCLI(t, done)
	if got.code != ExitOK || !strings.Contains(got.out, `"event":"reset"`) {
		t.Fatalf("%+v", got)
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestWatchResetGet(t *testing.T) {
	installSSENoWait(t)
	srv, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			writeEvents(w, sseFrame("", "reset", apiv1.ResetEvent{Topics: []apiv1.EventTopic{apiv1.Sessions}}))
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "session", "watch", "abc123")
	if got.code != ExitOK || !strings.Contains(got.out, "mftiko: session abc123 removed") {
		t.Fatalf("%+v", got)
	}
	if countOp(srv.Calls(), "StreamEvents") != 1 || countOp(srv.Calls(), "GetSession") != 1 {
		t.Fatalf("calls %+v", srv.Calls())
	}
	cancel()
	noLeak(t, tokenA, got.out+got.err)
}

func TestWatchTTY(t *testing.T) {
	installSSENoWait(t)
	prev := stdoutIsTerminal
	stdoutIsTerminal = func(io.Writer) bool { return true }
	t.Cleanup(func() { stdoutIsTerminal = prev })
	_, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			writeEvents(w, sseFrame("tw=1", "session", baseSession("bbbbbb", apiv1.PhasePending))+
				sseFrame("tw=2", "session", baseSession("aaaaaa", apiv1.PhaseRunning)))
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, _, done := startCLI(ctx, home, "--context", "dev", "session", "watch")
	out.waitContains(t, "\x1b[")
	text := out.String()
	if !strings.Contains(text, "ID") || !strings.Contains(text, "aaaaaa") || !strings.Contains(text, "bbbbbb") {
		t.Fatalf("frame\n%s", text)
	}
	esc := strings.LastIndex(text, "\x1b[")
	if esc < 0 || strings.Index(text[esc:], "aaaaaa") > strings.Index(text[esc:], "bbbbbb") {
		t.Fatalf("sort\n%s", text[esc:])
	}
	cancel()
	got := waitCLI(t, done)
	if got.code != ExitOK {
		t.Fatalf("%+v", got)
	}
}

func TestWatchJSONOnTerminal(t *testing.T) {
	installSSENoWait(t)
	prev := stdoutIsTerminal
	stdoutIsTerminal = func(io.Writer) bool { return true }
	t.Cleanup(func() { stdoutIsTerminal = prev })
	_, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			writeEvents(w, sseFrame("tw=1", "session", baseSession("abc123", apiv1.PhaseRunning)))
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, _, done := startCLI(ctx, home, "--context", "dev", "-o", "json", "session", "watch")
	out.waitContains(t, `"event":"session"`)
	if strings.Contains(out.String(), "\x1b[") {
		t.Fatalf("redrawn %s", out.String())
	}
	cancel()
	got := waitCLI(t, done)
	if got.code != ExitOK {
		t.Fatalf("%+v", got)
	}
}

func TestWatchRemoved(t *testing.T) {
	installSSENoWait(t)
	prev := stdoutIsTerminal
	stdoutIsTerminal = func(io.Writer) bool { return true }
	t.Cleanup(func() { stdoutIsTerminal = prev })
	known := true
	d := baseSession("abc123", apiv1.PhaseRunning)
	d.StsInstance = "sts-1"
	d.Logs = &apiv1.LogLocation{Host: "h1", Stdout: "/log/out", Stderr: "/log/err"}
	_, home := startSess(t, apitest.Option{
		Agents: map[string]apiv1.AgentStatus{"sts-1": {LastKnown: &known}},
		Events: func(w http.ResponseWriter, r *http.Request) {
			writeEvents(w, sseFrame("tw=1", "session", d)+sseFrame("", "session_removed", apiv1.SessionRemovedEvent{SessionId: "abc123"}))
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "session", "watch", "abc123")
	cancel()
	if got.code != ExitOK || !strings.Contains(got.out, "SPEC") || !strings.Contains(got.out, "mftiko: session abc123 removed") || !strings.Contains(got.out, lastKnownNote) {
		t.Fatalf("%+v", got)
	}
	if strings.Contains(got.out, `"session_removed"`) {
		t.Fatalf("ndjson %s", got.out)
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestWatchNDJSONRemoved(t *testing.T) {
	installSSENoWait(t)
	_, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			body := sseFrame("tw=1", "session", baseSession("aaaaaa", apiv1.PhasePending)) +
				sseFrame("tw=2", "session", baseSession("bbbbbb", apiv1.PhaseRunning)) +
				sseFrame("tw=3", "session_removed", apiv1.SessionRemovedEvent{SessionId: "aaaaaa"})
			writeEvents(w, body)
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, _, done := startCLI(ctx, home, "--context", "dev", "session", "watch")
	out.waitContains(t, "session_removed")
	cancel()
	got := waitCLI(t, done)
	lines := strings.Split(strings.TrimRight(got.out, "\n"), "\n")
	if got.code != ExitOK || len(lines) != 3 {
		t.Fatalf("lines %d %+v", len(lines), got)
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestCreateWait(t *testing.T) {
	cases := []struct {
		name   string
		target string
		phase  apiv1.Phase
		out    string
		end    string
		code   int
		line   string
	}{
		{"running", "running", apiv1.PhaseRunning, "", "", 0, "running (0)"},
		{"failed early", "running", apiv1.PhaseFailed, "failed", "deps_failed", ExitOutcomeFailed, "failed (30)"},
		{"done", "terminal", apiv1.PhaseDone, "done", "", 0, "done (0)"},
		{"deps", "terminal", apiv1.PhaseFailed, "", "deps_failed", ExitDepsFailed, "deps_failed (35)"},
		{"ended", "terminal", apiv1.PhaseDone, "failed", "ended_by_spec", 0, "ended_by_spec (0)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			installSSENoWait(t)
			d := detailWait(tc.phase, tc.out, tc.end, "")
			d.Id = "000001"
			_, home := startSess(t, apitest.Option{
				Events: func(w http.ResponseWriter, r *http.Request) {
					writeEvents(w, sseFrame("r1=1", "session", d))
					<-r.Context().Done()
				},
			})
			path := writeBody(t, "s.yaml", sessionYAML)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got := runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "session", "create", "-f", path, "--wait", tc.target)
			if got.code != tc.code || !strings.Contains(got.err, tc.line) || !strings.Contains(got.out, "000001") {
				t.Fatalf("%+v", got)
			}
			if tc.code == 0 && strings.Contains(got.err, "Idempotency-Key:") {
				t.Fatalf("key on success %+v", got)
			}
			if tc.code != 0 && !strings.Contains(got.err, "Idempotency-Key:") {
				t.Fatalf("key missing %+v", got)
			}
			noLeak(t, tokenA, got.out+got.err)
		})
	}
}

func TestCreateWaitHeaders(t *testing.T) {
	installSSENoWait(t)
	d := baseSession("000001", apiv1.PhaseRunning)
	srv, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			writeEvents(w, sseFrame("r1=1", "session", d))
			<-r.Context().Done()
		},
	})
	path := writeBody(t, "s.yaml", sessionYAML)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "session", "create", "-f", path, "--wait", "running")
	if got.code != ExitOK {
		t.Fatalf("%+v", got)
	}
	var stream apitest.Call
	for _, c := range srv.Calls() {
		if c.Operation == "StreamEvents" {
			stream = c
		}
	}
	if stream.Authorization != "Bearer "+tokenA || stream.Cookie != "" || stream.ContentType != "" || stream.LastEventID != "" {
		t.Fatalf("stream %+v", stream)
	}
	if !strings.Contains(stream.RawQuery, "topics=sessions") || !strings.Contains(stream.RawQuery, "session_id=000001") {
		t.Fatalf("query %s", stream.RawQuery)
	}
}

func TestWaitTimeout(t *testing.T) {
	waits := installSSEClock(t)
	srv, home := startSess(t, apitest.Option{
		Events: func(http.ResponseWriter, *http.Request) {},
	})
	path := writeBody(t, "s.yaml", sessionYAML)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "session", "create", "-f", path, "--wait", "running", "--wait-timeout", "3s")
	if got.code != ExitWaitTimeout || !strings.Contains(got.err, "等待逾時 (38)") || !strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("%+v", got)
	}
	if len(*waits) != 2 || (*waits)[0] != time.Second || (*waits)[1] != 2*time.Second {
		t.Fatalf("waits %v", *waits)
	}
	if countOp(srv.Calls(), "StreamEvents") != 2 {
		t.Fatalf("calls %+v", srv.Calls())
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestWaitDeleted(t *testing.T) {
	installSSENoWait(t)
	srv, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			writeEvents(w, sseFrame("r1=2", "session_removed", apiv1.SessionRemovedEvent{SessionId: "000001"}))
			<-r.Context().Done()
		},
	})
	path := writeBody(t, "s.yaml", sessionYAML)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "session", "create", "-f", path, "--wait", "running")
	if got.code != ExitWaitDeleted || !strings.Contains(got.err, "等待中 session 被刪除 (39)") {
		t.Fatalf("%+v", got)
	}
	if countOp(srv.Calls(), "GetSession") != 1 {
		t.Fatalf("calls %+v", srv.Calls())
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestWaitRegionThenRunning(t *testing.T) {
	installSSENoWait(t)
	release := make(chan struct{})
	srv, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			writeEvents(w, sseFrame("r1=1", "session_removed", apiv1.SessionRemovedEvent{SessionId: "000001"}))
			select {
			case <-r.Context().Done():
				return
			case <-release:
			}
			writeEvents(w, sseFrame("r1=2", "session", baseSession("000001", apiv1.PhaseRunning)))
			<-r.Context().Done()
		},
	})
	srv.SetRegionUnavailable(true)
	path := writeBody(t, "s.yaml", sessionYAML)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, errb, done := startCLI(ctx, home, "--context", "dev", "session", "create", "-f", path, "--wait", "running")
	errb.waitContains(t, "mftiko: warning: unavailable regions: r1")
	srv.SetRegionUnavailable(false)
	close(release)
	got := waitCLI(t, done)
	if got.code != ExitOK || !strings.Contains(got.err, "running (0)") || strings.Contains(got.err, "等待中 session 被刪除") {
		t.Fatalf("%+v", got)
	}
	if strings.Count(got.err, "unavailable regions") != 1 {
		t.Fatalf("warnings\n%s", got.err)
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestWaitEventsUnavailable(t *testing.T) {
	installSSENoWait(t)
	var n atomic.Int32
	srv, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			if n.Add(1) == 1 {
				w.Header().Set("Content-Type", "application/problem+json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{"type":"about:blank","title":"Region Unavailable","status":503,"code":"region_unavailable","detail":"region is unavailable"}`)
				return
			}
			writeEvents(w, sseFrame("r1=1", "session", baseSession("000001", apiv1.PhaseRunning)))
			<-r.Context().Done()
		},
	})
	path := writeBody(t, "s.yaml", sessionYAML)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "session", "create", "-f", path, "--wait", "running")
	if got.code != ExitOK || !strings.Contains(got.err, "running (0)") || strings.Count(got.err, "unavailable regions: r1") != 1 {
		t.Fatalf("%+v", got)
	}
	if got.code == ExitRegionUnavailable {
		t.Fatalf("exit 17 %+v", got)
	}
	if countOp(srv.Calls(), "StreamEvents") != 2 {
		t.Fatalf("calls %+v", srv.Calls())
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestWaitUnauthorized(t *testing.T) {
	installSSEClock(t)
	srv, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/problem+json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"type":"about:blank","title":"Unauthorized","status":401,"code":"unauthenticated","detail":"bearer token was not accepted"}`)
		},
	})
	path := writeBody(t, "s.yaml", sessionYAML)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "session", "create", "-f", path, "--wait", "running", "--wait-timeout", "3s")
	if got.code != ExitUnauthenticated || !strings.Contains(got.err, "unauthenticated (11)") {
		t.Fatalf("%+v", got)
	}
	if countOp(srv.Calls(), "StreamEvents") != 1 {
		t.Fatalf("retried %+v", srv.Calls())
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestEndWait(t *testing.T) {
	installSSENoWait(t)
	d := detailWait(apiv1.PhaseDone, "done", "", "")
	d.Id = "abc123"
	srv, home := startSess(t, apitest.Option{
		Sessions: []apiv1.SessionDetail{baseSession("abc123", apiv1.PhaseRunning)},
		Events: func(w http.ResponseWriter, r *http.Request) {
			writeEvents(w, sseFrame("tw=9", "session", d))
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "--yes", "session", "end", "abc123", "--wait")
	if got.code != ExitOK || !strings.Contains(got.err, "done (0)") || strings.Contains(got.err, "Idempotency-Key:") {
		t.Fatalf("%+v", got)
	}
	if countOp(srv.Calls(), "EndSession") != 1 || countOp(srv.Calls(), "StreamEvents") != 1 {
		t.Fatalf("calls %+v", srv.Calls())
	}
	var stream apitest.Call
	for _, c := range srv.Calls() {
		if c.Operation == "StreamEvents" {
			stream = c
		}
	}
	if !strings.Contains(stream.RawQuery, "session_id=abc123") || stream.Cookie != "" {
		t.Fatalf("stream %+v", stream)
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestWatchCancel(t *testing.T) {
	installSSENoWait(t)
	_, home := startSess(t, apitest.Option{
		Events: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	_, _, done := startCLI(ctx, home, "--context", "dev", "session", "watch")
	cancel()
	got := waitCLI(t, done)
	if got.code != ExitOK {
		t.Fatalf("%+v", got)
	}
}

func installSSENoWait(t *testing.T) {
	t.Helper()
	prev := sseRetryWait
	sseRetryWait = func(ctx context.Context, _ time.Duration) error {
		return ctx.Err()
	}
	t.Cleanup(func() { sseRetryWait = prev })
}

func installSSEClock(t *testing.T) *[]time.Duration {
	t.Helper()
	var mu sync.Mutex
	cur := time.Unix(1_700_000_000, 0).UTC()
	var waits []time.Duration
	prevNow := sseNow
	prevWait := sseRetryWait
	sseNow = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return cur
	}
	sseRetryWait = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		mu.Lock()
		waits = append(waits, d)
		cur = cur.Add(d)
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() {
		sseNow = prevNow
		sseRetryWait = prevWait
	})
	return &waits
}

type noteBuf struct {
	mu   sync.Mutex
	buf  strings.Builder
	cond *sync.Cond
}

func newNoteBuf() *noteBuf {
	n := &noteBuf{}
	n.cond = sync.NewCond(&n.mu)
	return n
}

func (n *noteBuf) Write(p []byte) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	x, err := n.buf.Write(p)
	n.cond.Broadcast()
	return x, err
}

func (n *noteBuf) String() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.buf.String()
}

func (n *noteBuf) waitContains(t *testing.T, sub string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		n.mu.Lock()
		n.cond.Broadcast()
		n.mu.Unlock()
	}()
	n.mu.Lock()
	defer n.mu.Unlock()
	for !strings.Contains(n.buf.String(), sub) {
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for %q in %q", sub, n.buf.String())
		}
		n.cond.Wait()
	}
}

func startCLI(ctx context.Context, home string, args ...string) (*noteBuf, *noteBuf, <-chan result) {
	stdout := newNoteBuf()
	stderr := newNoteBuf()
	ch := make(chan result, 1)
	go func() {
		code := Run(ctx, args, strings.NewReader(""), stdout, stderr, func(string) string { return "" }, home)
		ch <- result{code: code, out: stdout.String(), err: stderr.String()}
	}()
	return stdout, stderr, ch
}

func waitCLI(t *testing.T, done <-chan result) result {
	t.Helper()
	select {
	case got := <-done:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("command did not return")
		return result{}
	}
}

func writeEvents(w http.ResponseWriter, frame string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, frame)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func sseFrame(id, event string, data any) string {
	raw, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	var b strings.Builder
	if id != "" {
		b.WriteString("id: ")
		b.WriteString(id)
		b.WriteByte('\n')
	}
	b.WriteString("event: ")
	b.WriteString(event)
	b.WriteByte('\n')
	b.WriteString("data: ")
	b.Write(raw)
	b.WriteString("\n\n")
	return b.String()
}

func countOp(calls []apitest.Call, op string) int {
	n := 0
	for _, c := range calls {
		if c.Operation == op {
			n++
		}
	}
	return n
}

func waitOp(t *testing.T, srv *apitest.Server, op string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		calls := srv.Calls()
		if countOp(calls, op) > 0 {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for %s in %+v", op, calls)
		}
		got := make(chan []apitest.Call, 1)
		go func() { got <- srv.WaitCalls(len(calls) + 1) }()
		select {
		case <-got:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", op)
		}
	}
}
