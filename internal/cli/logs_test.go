package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/cli/apitest"
)

func TestLogStreams(t *testing.T) {
	for _, reason := range []string{"eof", "terminal", "removed"} {
		t.Run(reason, func(t *testing.T) {
			body := logSSE(
				logLine("stdout", "aaa"),
				logLine("stderr", "bbb"),
				gapLine("stdout"),
				gapLine("stderr"),
				endLine(reason),
			)
			srv, home := startLogs(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.RawQuery != "" {
					t.Errorf("query %q", r.URL.RawQuery)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, body)
			})
			got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "abcdef")
			if got.code != ExitOK {
				t.Fatalf("exit %d stderr %q", got.code, got.err)
			}
			if got.out != "aaa" {
				t.Fatalf("stdout %q", got.out)
			}
			wantErr := "bbb" + logGapLine + "\n" + logGapLine + "\n"
			if got.err != wantErr {
				t.Fatalf("stderr %q", got.err)
			}
			calls := srv.Calls()
			if len(calls) != 1 || calls[0].Operation != "GetSessionLogs" || calls[0].Path != "/api/v1/sessions/abcdef/logs" {
				t.Fatalf("calls %+v", calls)
			}
			if calls[0].Authorization != "Bearer "+tokenA || calls[0].Cookie != "" || calls[0].ContentType != "" || calls[0].LastEventID != "" {
				t.Fatalf("call %+v", calls[0])
			}
			if !strings.HasPrefix(calls[0].UserAgent, "mftiko-cli/") {
				t.Fatalf("user agent %q", calls[0].UserAgent)
			}
			noLeak(t, tokenA, got.out+got.err)
		})
	}
}

func TestLogStreamFilter(t *testing.T) {
	body := logSSE(
		logLine("stdout", "aaa"),
		logLine("stderr", "bbb"),
		gapLine("stdout"),
		gapLine("stderr"),
		endLine("eof"),
	)
	srv, home := startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "--stream", "stdout", "abcdef")
	if got.code != ExitOK {
		t.Fatalf("exit %d stderr %q", got.code, got.err)
	}
	if got.out != "aaa" {
		t.Fatalf("stdout %q", got.out)
	}
	if got.err != logGapLine+"\n" {
		t.Fatalf("stderr %q", got.err)
	}
	if strings.Contains(got.out+got.err, "bbb") {
		t.Fatalf("stderr leaked %q %q", got.out, got.err)
	}
	calls := srv.Calls()
	if len(calls) != 1 || strings.Contains(calls[0].RawQuery, "stream") {
		t.Fatalf("query %+v", calls)
	}
	noLeak(t, tokenA, got.out+got.err)

	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "--stream", "stderr", "--tail", "0", "-f", "abcdef")
	if got.code != ExitOK || got.out != "" || !strings.Contains(got.err, "bbb") || strings.Contains(got.out, "aaa") {
		t.Fatalf("stderr filter %+v", got)
	}
	if strings.Count(got.err, logGapLine) != 1 {
		t.Fatalf("gaps %q", got.err)
	}
	q := srv.Calls()[len(srv.Calls())-1].RawQuery
	if !strings.Contains(q, "tail=0") || !strings.Contains(q, "follow=true") || strings.Contains(q, "stream") {
		t.Fatalf("query %q", q)
	}
}

func TestLogResume(t *testing.T) {
	for _, follow := range []bool{false, true} {
		name := "once"
		args := []string{"--context", "dev", "session", "logs", "abcdef"}
		if follow {
			name = "follow"
			args = []string{"--context", "dev", "session", "logs", "-f", "abcdef"}
		}
		t.Run(name, func(t *testing.T) {
			waits := stubLogRetry(t)
			var mu sync.Mutex
			n := 0
			srv, home := startLogs(t, func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				n++
				step := n
				mu.Unlock()
				w.Header().Set("Content-Type", "text/event-stream")
				if step == 1 {
					if r.Header.Get("Last-Event-ID") != "" {
						t.Errorf("first resume %q", r.Header.Get("Last-Event-ID"))
					}
					_, _ = io.WriteString(w, logSSE(logLineID("stdout=1;stderr=0", "stdout", "aaa")))
					return
				}
				if r.Header.Get("Last-Event-ID") != "stdout=1;stderr=0" {
					t.Errorf("resume %q", r.Header.Get("Last-Event-ID"))
				}
				_, _ = io.WriteString(w, logSSE(endLine("eof")))
			})
			got := runAt(context.Background(), home, nil, strings.NewReader(""), args...)
			if got.code != ExitOK || got.out != "aaa" {
				t.Fatalf("got %+v", got)
			}
			if len(srv.Calls()) != 2 {
				t.Fatalf("calls %d", len(srv.Calls()))
			}
			if srv.Calls()[1].LastEventID != "stdout=1;stderr=0" {
				t.Fatalf("header %q", srv.Calls()[1].LastEventID)
			}
			if len(*waits) != 1 || (*waits)[0] != time.Second {
				t.Fatalf("waits %v", *waits)
			}
			for _, c := range srv.Calls() {
				hasFollow := strings.Contains(c.RawQuery, "follow=true")
				if hasFollow != follow || strings.Contains(c.RawQuery, "stream") {
					t.Fatalf("query %q follow %v", c.RawQuery, follow)
				}
			}
			noLeak(t, tokenA, got.out+got.err)
		})
	}
}

func TestLogReconnectExhausted(t *testing.T) {
	waits := stubLogRetry(t)
	srv, home := startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "abcdef")
	if got.code != ExitNetwork || !strings.Contains(got.err, "network (2): reconnect exhausted") {
		t.Fatalf("got %+v", got)
	}
	if len(srv.Calls()) != 7 {
		t.Fatalf("calls %d", len(srv.Calls()))
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}
	if len(*waits) != len(want) {
		t.Fatalf("waits %v", *waits)
	}
	for i := range want {
		if (*waits)[i] != want[i] {
			t.Fatalf("waits %v", *waits)
		}
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestLogByteResetsBackoff(t *testing.T) {
	waits := stubLogRetry(t)
	var mu sync.Mutex
	n := 0
	_, home := startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n++
		step := n
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if step < 7 {
			return
		}
		if step == 7 {
			_, _ = io.WriteString(w, ": keepalive\n\n")
			return
		}
		_, _ = io.WriteString(w, logSSE(endLine("terminal")))
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "abcdef")
	if got.code != ExitOK {
		t.Fatalf("got %+v", got)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, time.Second}
	if len(*waits) != len(want) {
		t.Fatalf("waits %v", *waits)
	}
	for i := range want {
		if (*waits)[i] != want[i] {
			t.Fatalf("waits %v", *waits)
		}
	}
}

func TestLogProblemDoesNotRetry(t *testing.T) {
	waits := stubLogRetry(t)
	_, home := startSess(t, apitest.Option{RequestID: "req-logs"})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "abcdef")
	if got.code != ExitLogsUnavailable || !strings.Contains(got.err, "logs_unavailable (22)") {
		t.Fatalf("got %+v", got)
	}
	if len(*waits) != 0 {
		t.Fatalf("retried %v", *waits)
	}
	noLeak(t, tokenA, got.out+got.err)

	detail := "region down"
	srv, home := startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		p := apiv1.Problem{Type: "about:blank", Title: "Region Unavailable", Status: 503, Code: apiv1.ErrorCodeRegionUnavailable, Detail: &detail}
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(p)
	})
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "-f", "abcdef")
	if got.code != ExitRegionUnavailable || len(srv.Calls()) != 1 {
		t.Fatalf("region %+v calls %d", got, len(srv.Calls()))
	}

	srv, home = startLogs(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "abcdef")
	if got.code != ExitUsage || !strings.Contains(got.err, "API 版本太舊") || strings.Contains(got.err, "not_found") {
		t.Fatalf("old api %+v", got)
	}
	if len(srv.Calls()) != 1 {
		t.Fatalf("calls %d", len(srv.Calls()))
	}

	srv, home = startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		p := apiv1.Problem{Type: "about:blank", Title: "Not Found", Status: 404, Code: apiv1.ErrorCodeNotFound}
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(p)
	})
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "abcdef")
	if got.code != ExitNotFound || strings.Contains(got.err, "API 版本太舊") || !strings.Contains(got.err, "not_found (12)") {
		t.Fatalf("missing %+v", got)
	}
	if len(srv.Calls()) != 1 {
		t.Fatalf("calls %d", len(srv.Calls()))
	}
}

func TestLogStatusRetries(t *testing.T) {
	waits := stubLogRetry(t)
	var mu sync.Mutex
	n := 0
	_, home := startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		n++
		step := n
		mu.Unlock()
		if step == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, logSSE(endLine("eof")))
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "abcdef")
	if got.code != ExitOK || len(*waits) != 1 || (*waits)[0] != time.Second {
		t.Fatalf("got %+v waits %v", got, *waits)
	}
}

func TestLogCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	var once sync.Once
	_, home := startLogs(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		once.Do(func() { close(ready) })
		<-r.Context().Done()
	})
	done := make(chan result, 1)
	go func() {
		done <- runAt(ctx, home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "-f", "abcdef")
	}()
	select {
	case <-ready:
	case got := <-done:
		t.Fatalf("exited before the stream %+v", got)
	}
	cancel()
	select {
	case got := <-done:
		if got.code != ExitOK || got.err != "" {
			t.Fatalf("got %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not finish")
	}
}

func TestLogLatest(t *testing.T) {
	older := baseSession("aaaaaa", apiv1.PhaseRunning)
	newer := baseSession("bbbbbb", apiv1.PhaseRunning)
	t0 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	older.CreatedAt = &t0
	newer.CreatedAt = &t1
	var mu sync.Mutex
	var paths []string
	logEnd := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		_, _ = io.WriteString(w, logSSE(endLine("eof")))
	}
	srv, home := startSess(t, apitest.Option{
		Token: tokenA, Name: "ada", PageSize: 1,
		Sessions: []apiv1.SessionDetail{older, newer},
		Logs:     logEnd,
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "--latest")
	if got.code != ExitOK {
		t.Fatalf("got %+v", got)
	}
	mu.Lock()
	gotPaths := append([]string(nil), paths...)
	mu.Unlock()
	if len(gotPaths) != 1 || gotPaths[0] != "/api/v1/sessions/bbbbbb/logs" {
		t.Fatalf("paths %v calls %+v", gotPaths, srv.Calls())
	}
	lists := 0
	for _, c := range srv.Calls() {
		if c.Operation == "ListSessions" {
			lists++
		}
	}
	if lists != 2 {
		t.Fatalf("pages %d %+v", lists, srv.Calls())
	}

	sameA := baseSession("aaaaaa", apiv1.PhaseRunning)
	sameB := baseSession("bbbbbb", apiv1.PhaseRunning)
	sameA.CreatedAt = &t0
	sameB.CreatedAt = &t0
	none := baseSession("bbbbbb", apiv1.PhaseRunning)
	none.CreatedAt = nil
	kept := baseSession("aaaaaa", apiv1.PhaseRunning)
	kept.CreatedAt = &t0
	paths = nil
	_, home = startSess(t, apitest.Option{
		Token: tokenA, Name: "ada",
		Sessions: []apiv1.SessionDetail{sameA, sameB},
		Logs: func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			paths = append(paths, r.URL.Path)
			mu.Unlock()
			_, _ = io.WriteString(w, logSSE(endLine("eof")))
		},
	})
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "--latest")
	mu.Lock()
	tie := append([]string(nil), paths...)
	mu.Unlock()
	if got.code != ExitOK || len(tie) != 1 || tie[0] != "/api/v1/sessions/bbbbbb/logs" {
		t.Fatalf("tie %+v %v", got, tie)
	}

	paths = nil
	_, home = startSess(t, apitest.Option{
		Token: tokenA, Name: "ada",
		Sessions: []apiv1.SessionDetail{none, kept},
		Logs: func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			paths = append(paths, r.URL.Path)
			mu.Unlock()
			_, _ = io.WriteString(w, logSSE(endLine("eof")))
		},
	})
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "--latest")
	mu.Lock()
	nilAt := append([]string(nil), paths...)
	mu.Unlock()
	if got.code != ExitOK || len(nilAt) != 1 || nilAt[0] != "/api/v1/sessions/aaaaaa/logs" {
		t.Fatalf("nil created_at %+v %v", got, nilAt)
	}

	_, home = startSess(t, apitest.Option{Token: tokenA, Name: "ada"})
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "--latest")
	if got.code != ExitNotFound || !strings.Contains(got.err, "not_found (12): no session") {
		t.Fatalf("empty %+v", got)
	}
}

func TestLogRedact(t *testing.T) {
	_, home := startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, logSSE(logLine("stdout", "aaa"+tokenA+"aaa"), endLine("eof")))
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "logs", "abcdef")
	if got.code != ExitOK || got.out != "aaa***aaa" {
		t.Fatalf("got %+v", got)
	}
	noLeak(t, tokenA, got.out+got.err)
}

func TestLogUsage(t *testing.T) {
	got := runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "logs", "--stream", tokenA, "abcdef")
	if got.code != ExitUsage || strings.Contains(got.err, tokenA) || !strings.Contains(got.err, "must be stdout or stderr") {
		t.Fatalf("stream %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "logs", "--tail=-1", "abcdef")
	if got.code != ExitUsage || strings.Contains(got.err, "-1") || !strings.Contains(got.err, "non-negative") {
		t.Fatalf("tail %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "logs")
	if got.code != ExitUsage || !strings.Contains(got.err, "takes a session id or --latest") {
		t.Fatalf("neither %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "logs", "abcdef", "--latest")
	if got.code != ExitUsage || !strings.Contains(got.err, "cannot both be set") {
		t.Fatalf("both %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "logs", "-o", "json", "abcdef")
	if got.code != ExitUsage || !strings.Contains(got.err, "-o is not used with session logs") {
		t.Fatalf("json %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "list", "--latest")
	if got.code != ExitUsage || !strings.Contains(got.err, "flag not accepted: -latest") {
		t.Fatalf("list %+v", got)
	}
	got = runAt(context.Background(), t.TempDir(), nil, strings.NewReader(""), "session", "create", "-f", "sess.yaml")
	if got.code != ExitUsage || !strings.Contains(got.err, "cannot read session file") || strings.Contains(got.err, "takes no arguments") {
		t.Fatalf("create -f %+v", got)
	}
	home := t.TempDir()
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "session", "logs", "-f")
	if got.code != ExitUsage || !strings.Contains(got.err, "takes a session id or --latest") || strings.Contains(got.err, "needs an argument") {
		t.Fatalf("-f consumed a value %+v", got)
	}
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "-f", "session", "logs", "abcdef")
	if got.code != ExitUsage || !strings.Contains(got.err, "set --context or MFTIKO_CONTEXT") {
		t.Fatalf("leading -f %+v", got)
	}
}

func TestLogExport(t *testing.T) {
	good := packLogs(t, []tarFile{
		{name: "stdout.log", body: "aaa"},
		{name: "stderr.log", body: "bbb"},
		{name: "manifest.json", body: `{"stdout":{"size":3,"sha256":"00"},"stderr":{"size":3,"sha256":"00"}}`},
	})
	dir := t.TempDir()
	out := filepath.Join(dir, "out.tar.gz")
	srv, home := startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(good)
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "export-logs", "-o", out, "abcdef")
	if got.code != ExitOK {
		t.Fatalf("got %+v", got)
	}
	file, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(file, good) {
		t.Fatalf("file %v %d", err, len(file))
	}
	noLogTemp(t, dir)
	if len(srv.Calls()) != 1 || srv.Calls()[0].Operation != "ExportSessionLogs" || srv.Calls()[0].Path != "/api/v1/sessions/abcdef/logs/export" {
		t.Fatalf("calls %+v", srv.Calls())
	}
	if srv.Calls()[0].Authorization != "Bearer "+tokenA || srv.Calls()[0].ContentType != "" || srv.Calls()[0].Cookie != "" {
		t.Fatalf("call %+v", srv.Calls()[0])
	}
	noLeak(t, tokenA, got.out+got.err)

	nested := packLogs(t, []tarFile{{name: "sub/manifest.json", body: "{}"}, {name: "stdout.log", body: "aaa"}})
	short := good[:8]
	var raw bytes.Buffer
	zw := gzip.NewWriter(&raw)
	_, _ = zw.Write([]byte("aaa"))
	_ = zw.Close()
	plain := raw.Bytes()
	noManifest := packLogs(t, []tarFile{{name: "stdout.log", body: "aaa"}, {name: "stderr.log", body: "bbb"}})
	cases := []struct {
		name string
		body []byte
		msg  string
	}{
		{"truncated gzip", short, "incomplete log export"},
		{"truncated tar", plain, "incomplete log export"},
		{"no manifest", noManifest, "log export has no manifest.json"},
		{"nested manifest", nested, "log export has no manifest.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "out.tar.gz")
			_, home := startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write(tc.body)
			})
			got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "export-logs", "-o", out, "abcdef")
			if got.code != ExitNetwork || !strings.Contains(got.err, tc.msg) {
				t.Fatalf("got %+v", got)
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatalf("file left %v", err)
			}
			noLogTemp(t, dir)
			noLeak(t, tokenA, got.out+got.err)
		})
	}
}

func TestLogExportExists(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.tar.gz")
	if err := os.WriteFile(out, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, home := startLogs(t, func(http.ResponseWriter, *http.Request) {
		t.Error("export was called")
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "export-logs", "-o", out, "abcdef")
	if got.code != ExitUsage || !strings.Contains(got.err, "file exists:") {
		t.Fatalf("got %+v", got)
	}
	file, err := os.ReadFile(out)
	if err != nil || string(file) != "keep" {
		t.Fatalf("file %q %v", file, err)
	}
	if len(srv.Calls()) != 0 {
		t.Fatalf("calls %+v", srv.Calls())
	}
	noLogTemp(t, dir)
	missing := filepath.Join(dir, "missing", "out.tar.gz")
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "export-logs", "-o", missing, "abcdef")
	if got.code != ExitUsage || !strings.Contains(got.err, "cannot create log file") || len(srv.Calls()) != 0 {
		t.Fatalf("dir %+v calls %+v", got, srv.Calls())
	}
}

func TestLogExportDefaultName(t *testing.T) {
	prev := logsNow
	logsNow = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
	t.Cleanup(func() { logsNow = prev })
	dir := t.TempDir()
	t.Chdir(dir)
	good := packLogs(t, []tarFile{{name: "manifest.json", body: "{}"}})
	_, home := startLogs(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(good)
	})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "export-logs", "abcdef")
	if got.code != ExitOK {
		t.Fatalf("got %+v", got)
	}
	name := "mftiko-abcdef-logs-20260102T030405Z.tar.gz"
	file, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil || !bytes.Equal(file, good) {
		t.Fatalf("read %v", err)
	}
	noLogTemp(t, dir)
}

func TestLogExportStatus(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.tar.gz")
	_, home := startSess(t, apitest.Option{})
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "export-logs", "-o", out, "abcdef")
	if got.code != ExitLogsUnavailable || !strings.Contains(got.err, "logs_unavailable (22)") {
		t.Fatalf("got %+v", got)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("file created")
	}
	noLogTemp(t, dir)

	_, home = startLogs(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	got = runAt(context.Background(), home, nil, strings.NewReader(""), "--context", "dev", "session", "export-logs", "-o", out, "abcdef")
	if got.code != ExitUsage || !strings.Contains(got.err, "API 版本太舊") {
		t.Fatalf("old %+v", got)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("file created")
	}
	noLogTemp(t, dir)
}

func startLogs(t *testing.T, fn func(http.ResponseWriter, *http.Request)) (*apitest.Server, string) {
	t.Helper()
	return startSess(t, apitest.Option{Logs: fn})
}

func stubLogRetry(t *testing.T) *[]time.Duration {
	t.Helper()
	prev := sseRetryWait
	var mu sync.Mutex
	waits := []time.Duration{}
	sseRetryWait = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		mu.Lock()
		waits = append(waits, d)
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { sseRetryWait = prev })
	return &waits
}

func logSSE(parts ...string) string { return strings.Join(parts, "") }

func logLine(stream, data string) string {
	return logLineID("", stream, data)
}

func logLineID(id, stream, data string) string {
	raw, err := json.Marshal(apiv1.LogEvent{Stream: apiv1.LogStream(stream), Data: data})
	if err != nil {
		panic(err)
	}
	return sseBlock(id, "log", string(raw))
}

func gapLine(stream string) string {
	raw, err := json.Marshal(apiv1.LogGapEvent{Stream: apiv1.LogStream(stream)})
	if err != nil {
		panic(err)
	}
	return sseBlock("", "gap", string(raw))
}

func endLine(reason string) string {
	raw, err := json.Marshal(apiv1.LogEndEvent{Reason: apiv1.LogEndReason(reason)})
	if err != nil {
		panic(err)
	}
	return sseBlock("", "end", string(raw))
}

func sseBlock(id, event, data string) string {
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
	b.WriteString(data)
	b.WriteString("\n\n")
	return b.String()
}

type tarFile struct {
	name string
	body string
}

func packLogs(t *testing.T, files []tarFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	for _, f := range files {
		body := []byte(f.body)
		hdr := &tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(body))}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func noLogTemp(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".mftiko-logs-") {
			t.Fatalf("temp left: %s", e.Name())
		}
	}
}
