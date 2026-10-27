package api

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	logv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/log/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestLogClient(t *testing.T) {
	if logDialTimeout != 5*time.Second || logHeaderTimeout != 10*time.Second {
		t.Fatalf("dial %s header %s", logDialTimeout, logHeaderTimeout)
	}
	c := newLogClient()
	if c.Timeout != 0 {
		t.Fatal("body deadline is set")
	}
	if err := c.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Fatal(err)
	}
	tr, ok := c.Transport.(*http.Transport)
	if !ok || tr.ResponseHeaderTimeout != logHeaderTimeout || !tr.DisableCompression || tr.Proxy == nil {
		t.Fatalf("transport %#v", c.Transport)
	}
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:9/", nil)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := tr.Proxy(req)
	if err != nil || proxy != nil {
		t.Fatalf("proxy %v %v", proxy, err)
	}
	if _, err := logURL("not-a-host", "abc123", "stdout", nil); err == nil {
		t.Fatal("bare host accepted")
	}
	if _, err := logURL("bad host:1", "abc123", "stdout", nil); err == nil {
		t.Fatal("space in host accepted")
	}
	got, err := logURL("127.0.0.1:9", "abc123", "export", nil)
	if err != nil || got != "http://127.0.0.1:9/v1/sessions/abc123/logs/export" {
		t.Fatalf("%s %v", got, err)
	}
}

func TestLogRetryAfter(t *testing.T) {
	if d := parseRetryAfter(""); d != 0 {
		t.Fatal(d)
	}
	if d := parseRetryAfter("0"); d != 0 {
		t.Fatal(d)
	}
	if d := parseRetryAfter("nope"); d != 0 {
		t.Fatal(d)
	}
	if d := parseRetryAfter("Wed, 21 Oct 2015 07:28:00 GMT"); d != 0 {
		t.Fatal(d)
	}
	if d := parseRetryAfter("1"); d != time.Second {
		t.Fatal(d)
	}
	if d := parseRetryAfter(" 9 "); d != logRetryAfterCap {
		t.Fatal(d)
	}
	for i, want := range []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond} {
		got, ok := logWaitFor(i+1, 0)
		if !ok || got != want {
			t.Fatalf("fail %d: %s %v", i+1, got, ok)
		}
	}
	if _, ok := logWaitFor(0, 0); ok {
		t.Fatal("zero fails waited")
	}
	if _, ok := logWaitFor(logMaxRetries+1, 0); ok {
		t.Fatal("budget did not end")
	}
	got, ok := logWaitFor(1, 2*time.Second)
	if !ok || got != 2*time.Second {
		t.Fatalf("capped %s %v", got, ok)
	}
}

func TestLogResume(t *testing.T) {
	got, err := parseLogResume("stdout=cursorA;stderr=cursorB")
	if err != nil || got != [2]string{"cursorA", "cursorB"} {
		t.Fatalf("%v %v", got, err)
	}
	for _, raw := range []string{
		"",
		"stdout=cursorA",
		"stderr=cursorB;stdout=cursorA",
		"stdout=cursorA;stderr=cursorB;stdout=cursorC",
		"stdout=;stderr=cursorB",
		"stdout=cursorA;stderr=has space",
		"stdout=bad=id;stderr=cursorB",
		"stdout=cursorA;stderr=pad=",
	} {
		if _, err := parseLogResume(raw); raw != "" && err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	q, fields, err := parseLogParams(apiv1.GetSessionLogsParams{})
	if err != nil || q.tail != nil || q.follow || q.resumed {
		t.Fatalf("%+v %v %v", q, fields, err)
	}
	neg := -1
	if _, _, err := parseLogParams(apiv1.GetSessionLogsParams{Tail: &neg}); err == nil {
		t.Fatal("negative tail accepted")
	}
	zero := 0
	resume := "stdout=cursorA;stderr=cursorB"
	q, _, err = parseLogParams(apiv1.GetSessionLogsParams{Tail: &zero, LastEventID: &resume})
	if err != nil || !q.resumed || q.tail != nil || q.resume != [2]string{"cursorA", "cursorB"} {
		t.Fatalf("resume did not drop tail: %+v %v", q, err)
	}
}

func TestLogFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, &logv1.Frame{Next: "cursorA", Data: []byte("aa\n"), Gap: true, Eof: true}); err != nil {
		t.Fatal(err)
	}
	buf.WriteByte(0)
	if err := writeFrame(&buf, &logv1.Frame{Next: "bad id"}); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(&buf)
	f, err := readLogFrame(r)
	if err != nil || f.empty || f.next != "cursorA" || string(f.data) != "aa\n" || !f.gap || !f.eof {
		t.Fatalf("%+v %v", f, err)
	}
	hb, err := readLogFrame(r)
	if err != nil || !hb.empty {
		t.Fatalf("heartbeat %+v %v", hb, err)
	}
	if _, err := readLogFrame(r); !errors.Is(err, errLogProtocol) {
		t.Fatal(err)
	}
	var big bytes.Buffer
	var prefix [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(prefix[:], logMaxFrame+1)
	big.Write(prefix[:n])
	if _, err := readLogFrame(bufio.NewReader(&big)); !errors.Is(err, errLogProtocol) {
		t.Fatal(err)
	}
}

func TestLogMerge(t *testing.T) {
	m := newLogMerge(false, [2]string{})
	if out := m.apply(logStdout, logFrame{next: "cursorA", data: []byte("aa\n"), eof: true}); out != nil {
		t.Fatal("sent before the other next")
	}
	out := m.apply(logStderr, logFrame{next: "cursorB", data: []byte("bb\n"), eof: true})
	if len(out) != 3 || out[0].event != "log" || out[1].event != "log" || out[2].event != "end" {
		t.Fatalf("%+v", out)
	}
	if out[0].id != "stdout=cursorA;stderr=cursorB" || out[2].id != out[0].id {
		t.Fatal(out[0].id)
	}
	var end apiv1.LogEndEvent
	if err := json.Unmarshal(out[2].data, &end); err != nil || end.Reason != apiv1.Eof {
		t.Fatalf("%s %v", out[2].data, err)
	}
	if extra := m.apply(logStdout, logFrame{next: "cursorC", data: []byte("cc\n")}); extra != nil {
		t.Fatal("data after end")
	}

	g := newLogMerge(false, [2]string{})
	if out := g.apply(logStderr, logFrame{next: "cursorB", eof: true}); out != nil {
		t.Fatal("flushed an empty side")
	}
	out = g.apply(logStdout, logFrame{next: "cursorA", gap: true, data: []byte("aa\n"), eof: true})
	if len(out) != 3 || out[0].event != "gap" || out[1].event != "log" || out[2].event != "end" {
		t.Fatalf("%+v", out)
	}
	if out[0].id != out[1].id {
		t.Fatal(out[0].id)
	}

	rm := newLogMerge(true, [2]string{})
	if out := rm.apply(logStdout, logFrame{next: "cursorA", data: []byte("aa\n"), removed: true}); out != nil {
		t.Fatal("removed before the other next")
	}
	out = rm.apply(logStderr, logFrame{next: "cursorB"})
	if len(out) != 2 || out[1].event != "end" {
		t.Fatalf("%+v", out)
	}
	if err := json.Unmarshal(out[1].data, &end); err != nil || end.Reason != apiv1.Removed {
		t.Fatalf("%s", out[1].data)
	}

	fin := newLogMerge(true, [2]string{})
	fin.apply(logStdout, logFrame{next: "cursorA", final: true})
	out = fin.apply(logStderr, logFrame{next: "cursorB", final: true})
	if len(out) != 1 || out[0].event != "end" {
		t.Fatalf("%+v", out)
	}
	if err := json.Unmarshal(out[0].data, &end); err != nil || end.Reason != apiv1.Terminal {
		t.Fatalf("%s", out[0].data)
	}
	// eof does not end a follow stream.
	hold := newLogMerge(true, [2]string{})
	hold.apply(logStdout, logFrame{next: "cursorA", eof: true})
	if out := hold.apply(logStderr, logFrame{next: "cursorB", eof: true}); out != nil {
		t.Fatal("follow ended on eof")
	}
}

func TestSessionLogs(t *testing.T) {
	tok := strings.Repeat("a", 32)
	se := newSessionEnv(t, sessionOpt{logToken: tok})
	agent := newFakeLogs(t, tok)
	waits := &waitLog{}
	waits.install(se.s)
	conn := auditConn(t, se)
	base := "http://" + se.s.Addr()

	t.Run("missing agent", func(t *testing.T) {
		waits.reset()
		id := makeLogSession(t, se, "sts-tw-1")
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", nil)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("%d %s", code, body)
		}
		mustProblem(t, body, http.StatusServiceUnavailable, auditLogsUnavailable)
		if n := len(agent.copy()); n != 0 || len(waits.copy()) != 0 {
			t.Fatalf("hits %d waits %v", n, waits.copy())
		}
	})

	t.Run("empty log addr", func(t *testing.T) {
		waits.reset()
		id := makeLogSession(t, se, "sts-tw-1")
		setLogAddr(t, se, "tw", "sts-tw-1", "")
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", nil)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("%d %s", code, body)
		}
		mustProblem(t, body, http.StatusServiceUnavailable, auditLogsUnavailable)
		if len(agent.copy()) != 0 {
			t.Fatal("dialed an empty log_addr")
		}
	})

	id := makeLogSession(t, se, "sts-tw-1")
	setLogAddr(t, se, "tw", "sts-tw-1", agent.addr())

	t.Run("tail and audit", func(t *testing.T) {
		waits.reset()
		agent.reset(frameHandler(&logv1.Frame{Next: "cursorA", Data: []byte("aa\n"), Eof: true}),
			frameHandler(&logv1.Frame{Next: "cursorB", Data: []byte("bb\n"), Eof: true}), nil)
		code, hdr, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs?tail=2", "", nil)
		if code != http.StatusOK {
			t.Fatalf("%d %s", code, body)
		}
		if hdr.Get("Content-Type") != "text/event-stream" {
			t.Fatal(hdr.Get("Content-Type"))
		}
		ev := parseSSE(t, body)
		assertLogPair(t, ev, "aa\n", "bb\n")
		assertEnd(t, ev, "eof")
		for _, e := range ev {
			if e.ID != "stdout=cursorA;stderr=cursorB" {
				t.Fatal(e.ID)
			}
		}
		for _, hit := range agent.copy() {
			if hit.auth != "Bearer "+tok {
				t.Fatal(hit.auth)
			}
			if hit.query.Get("tail") != "2" || hit.query.Get("from") != "" || hit.query.Get("cursor") != "" || hit.query.Get("follow") != "" {
				t.Fatalf("%s %v", hit.kind, hit.query)
			}
			if !strings.Contains(hit.path, "/"+id+"/logs/") {
				t.Fatal(hit.path)
			}
		}
		row := waitAuditID(t, conn, hdr.Get(headerRequestID))
		if row.Status != http.StatusOK || row.Detail != auditOK || row.SessionID == nil || *row.SessionID != id {
			t.Fatalf("%+v", row)
		}
		if strings.Contains(row.Detail+row.Query+stringOr(row.Body), tok) || strings.Contains(se.e.log.String(), tok) {
			t.Fatal("log token stored")
		}
	})

	t.Run("from start", func(t *testing.T) {
		agent.reset(frameHandler(&logv1.Frame{Next: "cursorA", Eof: true}), frameHandler(&logv1.Frame{Next: "cursorB", Eof: true}), nil)
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", nil)
		if code != http.StatusOK {
			t.Fatalf("%d %s", code, body)
		}
		ev := parseSSE(t, body)
		if len(ev) != 1 || ev[0].Event != "end" {
			t.Fatalf("%+v", ev)
		}
		for _, hit := range agent.copy() {
			if hit.query.Get("from") != "start" || hit.query.Get("tail") != "" {
				t.Fatalf("%v", hit.query)
			}
		}
	})

	t.Run("arrival order", func(t *testing.T) {
		release := make(chan struct{})
		agent.reset(func(w http.ResponseWriter, r *http.Request) {
			if err := writeFrame(w, &logv1.Frame{Next: "cursorA"}); err != nil {
				return
			}
			flushResp(w)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			_ = writeFrame(w, &logv1.Frame{Next: "cursorA", Data: []byte("aa\n"), Eof: true})
			flushResp(w)
		}, frameHandler(&logv1.Frame{Next: "cursorB", Data: []byte("bb\n"), Eof: true}), nil)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		resp := logGET(t, ctx, base, id, "", nil)
		defer resp.Body.Close()
		br := bufio.NewReader(resp.Body)
		first, err := readSSE(br)
		if err != nil {
			t.Fatal(err)
		}
		logEv, ok := first.asLog()
		if !ok || logEv.Stream != apiv1.Stderr || logEv.Data != "bb\n" || first.ID != "stdout=cursorA;stderr=cursorB" {
			t.Fatalf("%+v %+v", first, logEv)
		}
		close(release)
		rest := parseSSE(t, readRest(t, br))
		found := false
		for _, ev := range rest {
			if logEv, ok := ev.asLog(); ok && logEv.Data == "aa\n" {
				found = true
			}
		}
		if !found {
			t.Fatalf("%+v", rest)
		}
		all := append([]sseEv{first}, rest...)
		assertEnd(t, all, "eof")
	})

	t.Run("resume on another replica", func(t *testing.T) {
		agent.reset(frameHandler(&logv1.Frame{Next: "cursorC", Eof: true}), frameHandler(&logv1.Frame{Next: "cursorD", Eof: true}), nil)
		s2 := se.e.replica(t)
		waitLogVisible(t, s2, "tw", "sts-tw-1", id, agent.addr())
		hdr := http.Header{"Last-Event-ID": {"stdout=cursorA;stderr=cursorB"}}
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		resp := logGET(t, ctx, "http://"+s2.Addr(), id, "tail=9", hdr)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%d %s", resp.StatusCode, body)
		}
		for _, hit := range agent.copy() {
			if hit.query.Get("cursor") == "" || hit.query.Get("tail") != "" || hit.query.Get("from") != "" {
				t.Fatalf("%s %v", hit.kind, hit.query)
			}
		}
		byKind := map[string]string{}
		for _, hit := range agent.copy() {
			byKind[hit.kind] = hit.query.Get("cursor")
		}
		if byKind["stdout"] != "cursorA" || byKind["stderr"] != "cursorB" {
			t.Fatal(byKind)
		}
	})

	t.Run("gap", func(t *testing.T) {
		agent.reset(frameHandler(&logv1.Frame{Next: "cursorA", Gap: true, Data: []byte("aa\n"), Eof: true}),
			frameHandler(&logv1.Frame{Next: "cursorB", Eof: true}), nil)
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", nil)
		if code != http.StatusOK {
			t.Fatalf("%d %s", code, body)
		}
		ev := parseSSE(t, body)
		if len(ev) != 3 || ev[0].Event != "gap" || ev[1].Event != "log" || ev[0].ID != ev[1].ID {
			t.Fatalf("%+v", ev)
		}
		var gap apiv1.LogGapEvent
		if err := json.Unmarshal([]byte(ev[0].Data), &gap); err != nil || gap.Stream != apiv1.Stdout {
			t.Fatal(ev[0].Data)
		}
	})

	t.Run("terminal", func(t *testing.T) {
		agent.reset(frameHandler(&logv1.Frame{Next: "cursorA", Data: []byte("aa\n"), Final: true}),
			frameHandler(&logv1.Frame{Next: "cursorB", Data: []byte("bb\n"), Final: true}), nil)
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs?follow=true", "", nil)
		if code != http.StatusOK {
			t.Fatalf("%d %s", code, body)
		}
		ev := parseSSE(t, body)
		assertEnd(t, ev, "terminal")
		for _, hit := range agent.copy() {
			if hit.query.Get("follow") != "true" {
				t.Fatal(hit.query)
			}
		}
	})

	t.Run("removed", func(t *testing.T) {
		agent.reset(frameHandler(&logv1.Frame{Next: "cursorA", Data: []byte("aa\n"), Removed: true}),
			frameHandler(&logv1.Frame{Next: "cursorB"}), nil)
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs?follow=true", "", nil)
		if code != http.StatusOK {
			t.Fatalf("%d %s", code, body)
		}
		ev := parseSSE(t, body)
		assertEnd(t, ev, "removed")
		if ev[len(ev)-1].Event != "end" {
			t.Fatal("event after end")
		}
	})

	t.Run("heartbeat", func(t *testing.T) {
		agent.reset(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte{0})
			_ = writeFrame(w, &logv1.Frame{Next: "cursorA", Data: []byte("aa\n"), Eof: true})
			flushResp(w)
		}, frameHandler(&logv1.Frame{Next: "cursorB", Data: []byte("bb\n"), Eof: true}), nil)
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", nil)
		if code != http.StatusOK {
			t.Fatalf("%d %s", code, body)
		}
		ev := parseSSE(t, body)
		logs := 0
		for _, e := range ev {
			if e.Event == "log" {
				logs++
			}
			if e.Event != "log" && e.Event != "end" && e.Event != "gap" {
				t.Fatal(e.Event)
			}
		}
		if logs != 2 {
			t.Fatalf("%+v", ev)
		}
	})

	t.Run("bad resume and tail", func(t *testing.T) {
		agent.reset(frameHandler(&logv1.Frame{Next: "cursorA", Eof: true}), frameHandler(&logv1.Frame{Next: "cursorB", Eof: true}), nil)
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", http.Header{"Last-Event-ID": {"stdout=cursorA"}})
		if code != http.StatusBadRequest {
			t.Fatalf("%d %s", code, body)
		}
		mustProblem(t, body, http.StatusBadRequest, auditValidation)
		code, _, body = se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs?tail=-1", "", nil)
		if code != http.StatusBadRequest {
			t.Fatalf("%d %s", code, body)
		}
		mustProblem(t, body, http.StatusBadRequest, auditValidation)
		if len(agent.copy()) != 0 {
			t.Fatal("validation dialed the agent")
		}
	})

	t.Run("agent 401", func(t *testing.T) {
		waits.reset()
		status := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }
		agent.reset(status, status, nil)
		code, hdr, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", nil)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("%d %s", code, body)
		}
		mustProblem(t, body, http.StatusServiceUnavailable, auditLogsUnavailable)
		if n := len(agent.copy()); n < 1 || n > 2 || len(waits.copy()) != 0 {
			t.Fatalf("hits %d waits %v", n, waits.copy())
		}
		row := waitAuditID(t, conn, hdr.Get(headerRequestID))
		if row.Detail != auditLogsUnavailable || strings.Contains(row.Detail, tok) {
			t.Fatalf("%+v", row)
		}
	})

	t.Run("agent 404", func(t *testing.T) {
		waits.reset()
		status := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }
		agent.reset(status, status, nil)
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", nil)
		if code != http.StatusNotFound {
			t.Fatalf("%d %s", code, body)
		}
		mustProblem(t, body, http.StatusNotFound, auditNotFound)
		if n := len(agent.copy()); n < 1 || n > 2 || len(waits.copy()) != 0 {
			t.Fatalf("hits %d waits %v", n, waits.copy())
		}
	})

	t.Run("reconnect fails without end", func(t *testing.T) {
		waits.reset()
		var n int
		var mu sync.Mutex
		agent.reset(func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			n++
			first := n == 1
			mu.Unlock()
			if first {
				_ = writeFrame(w, &logv1.Frame{Next: "cursorA", Data: []byte("aa\n")})
				flushResp(w)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}, frameHandler(&logv1.Frame{Next: "cursorB", Data: []byte("bb\n"), Eof: true}), nil)
		code, hdr, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", nil)
		if code != http.StatusOK {
			t.Fatalf("%d %s", code, body)
		}
		ev := parseSSE(t, body)
		assertLogPair(t, ev, "aa\n", "bb\n")
		for _, e := range ev {
			if e.Event == "end" {
				t.Fatal("end after a failed reconnect")
			}
		}
		stdout := 0
		for _, hit := range agent.copy() {
			if hit.kind == "stdout" {
				stdout++
			}
		}
		if stdout != 5 {
			t.Fatalf("stdout hits %d of %v", stdout, agent.copy())
		}
		if !sameWaits(waits.copy(), 200*time.Millisecond, 400*time.Millisecond, 800*time.Millisecond) {
			t.Fatal(waits.copy())
		}
		row := waitAuditID(t, conn, hdr.Get(headerRequestID))
		if row.Status != http.StatusOK || row.Detail != auditLogsUnavailable {
			t.Fatalf("%+v", row)
		}
	})

	t.Run("reconnect new leader", func(t *testing.T) {
		next := newFakeLogs(t, tok)
		next.reset(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("cursor") != "cursorA" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = writeFrame(w, &logv1.Frame{Next: "cursorC", Data: []byte("cc\n"), Eof: true})
			flushResp(w)
		}, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}, nil)
		var stdoutN int
		var mu sync.Mutex
		agent.reset(func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			stdoutN++
			first := stdoutN == 1
			mu.Unlock()
			if first {
				_ = writeFrame(w, &logv1.Frame{Next: "cursorA", Data: []byte("aa\n")})
				flushResp(w)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}, frameHandler(&logv1.Frame{Next: "cursorB", Data: []byte("bb\n"), Eof: true}), nil)
		entered := make(chan time.Duration, 1)
		release := make(chan struct{})
		se.s.logWait = func(ctx context.Context, d time.Duration) error {
			select {
			case entered <- d:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		t.Cleanup(func() { waits.install(se.s) })
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		resp := logGET(t, ctx, base, id, "", nil)
		defer resp.Body.Close()
		var waited time.Duration
		until(t, 10*time.Second, "retry wait", func() error {
			select {
			case waited = <-entered:
				return nil
			default:
				return errors.New("not waiting")
			}
		})
		if waited != 200*time.Millisecond {
			t.Fatal(waited)
		}
		setLogAddr(t, se, "tw", "sts-tw-1", next.addr())
		close(release)
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		ev := parseSSE(t, body)
		assertLogPair(t, ev, "aa\n", "cc\n")
		assertEnd(t, ev, "eof")
		if len(next.copy()) != 1 || next.copy()[0].query.Get("cursor") != "cursorA" {
			t.Fatalf("%+v", next.copy())
		}
		waits.install(se.s)
		setLogAddr(t, se, "tw", "sts-tw-1", agent.addr())
	})

	t.Run("client closes", func(t *testing.T) {
		hold := func(w http.ResponseWriter, r *http.Request, next, data string) {
			_ = writeFrame(w, &logv1.Frame{Next: next, Data: []byte(data)})
			flushResp(w)
			<-r.Context().Done()
		}
		agent.reset(func(w http.ResponseWriter, r *http.Request) { hold(w, r, "cursorA", "aa\n") },
			func(w http.ResponseWriter, r *http.Request) { hold(w, r, "cursorB", "bb\n") }, nil)
		ctx, cancel := context.WithCancel(t.Context())
		resp := logGET(t, ctx, base, id, "follow=true", nil)
		br := bufio.NewReader(resp.Body)
		if _, err := readSSE(br); err != nil {
			cancel()
			resp.Body.Close()
			t.Fatal(err)
		}
		cancel()
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		row := waitAuditID(t, conn, resp.Header.Get(headerRequestID))
		if row.Status != http.StatusOK || row.Detail != auditClientClosed {
			t.Fatalf("%+v", row)
		}
	})

	full := sampleTar(t)
	t.Run("export", func(t *testing.T) {
		waits.reset()
		agent.reset(nil, nil, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/gzip")
			w.Header().Set("Content-Length", strconv.Itoa(len(full)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(full)
		})
		code, hdr, body, err := getExport(t, se.s.Addr(), id)
		if err != nil || code != http.StatusOK || !bytes.Equal(body, full) {
			t.Fatalf("%d %v %d", code, err, len(body))
		}
		if hdr.Get("Content-Type") != "application/gzip" || hdr.Get("Content-Length") != strconv.Itoa(len(full)) {
			t.Fatal(hdr)
		}
		names, err := tarNames(body)
		if err != nil || strings.Join(names, ",") != "stdout.log,stderr.log,manifest.json" {
			t.Fatalf("%v %v", names, err)
		}
		if len(agent.copy()) != 1 || agent.copy()[0].kind != "export" || agent.copy()[0].query.Encode() != "" {
			t.Fatal(agent.copy())
		}
		row := waitAuditID(t, conn, hdr.Get(headerRequestID))
		if row.Detail != auditOK || strings.Contains(row.Detail+stringOr(row.Body), tok) {
			t.Fatalf("%+v", row)
		}
	})

	t.Run("export disconnect", func(t *testing.T) {
		prefix := full[:len(full)/3]
		agent.reset(nil, nil, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/gzip")
			w.Header().Set("Content-Length", strconv.Itoa(len(full)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(prefix)
		})
		code, hdr, body, err := getExport(t, se.s.Addr(), id)
		if code != http.StatusOK || !errors.Is(err, io.ErrUnexpectedEOF) || !bytes.Equal(body, prefix) {
			t.Fatalf("code %d err %v len %d", code, err, len(body))
		}
		if hdr.Get("Content-Length") != strconv.Itoa(len(full)) || len(body) >= len(full) {
			t.Fatal(hdr.Get("Content-Length"))
		}
		names, nerr := tarNames(body)
		if nerr == nil && containsStr(names, "manifest.json") {
			t.Fatalf("incomplete archive had manifest: %v", names)
		}
		row := waitAuditID(t, conn, hdr.Get(headerRequestID))
		if row.Status != http.StatusOK || row.Detail != auditIncomplete {
			t.Fatalf("%+v", row)
		}
	})

	t.Run("export retry", func(t *testing.T) {
		waits.reset()
		agent.reset(nil, nil, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "9")
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		code, _, body, err := getExport(t, se.s.Addr(), id)
		if err != nil || code != http.StatusServiceUnavailable {
			t.Fatalf("%d %v %s", code, err, body)
		}
		mustProblem(t, body, http.StatusServiceUnavailable, auditLogsUnavailable)
		if len(agent.copy()) != 4 || !sameWaits(waits.copy(), 2*time.Second, 2*time.Second, 2*time.Second) {
			t.Fatalf("hits %d waits %v", len(agent.copy()), waits.copy())
		}
		waits.reset()
		agent.reset(nil, nil, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		code, _, body, err = getExport(t, se.s.Addr(), id)
		if err != nil || code != http.StatusServiceUnavailable {
			t.Fatalf("%d %v %s", code, err, body)
		}
		if len(agent.copy()) != 4 || !sameWaits(waits.copy(), 200*time.Millisecond, 400*time.Millisecond, 800*time.Millisecond) {
			t.Fatalf("hits %d waits %v", len(agent.copy()), waits.copy())
		}
		waits.reset()
		agent.reset(nil, nil, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) })
		code, _, body, err = getExport(t, se.s.Addr(), id)
		if err != nil || code != http.StatusServiceUnavailable || len(agent.copy()) != 1 || len(waits.copy()) != 0 {
			t.Fatalf("%d hits %d waits %v %s", code, len(agent.copy()), waits.copy(), body)
		}
		waits.reset()
		agent.reset(nil, nil, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) })
		code, _, body, err = getExport(t, se.s.Addr(), id)
		if err != nil || code != http.StatusNotFound || len(agent.copy()) != 1 || len(waits.copy()) != 0 {
			t.Fatalf("%d hits %d %s", code, len(agent.copy()), body)
		}
		mustProblem(t, body, http.StatusNotFound, auditNotFound)
	})

	t.Run("ignore last known", func(t *testing.T) {
		waits.reset()
		agent.reset(frameHandler(&logv1.Frame{Next: "cursorA", Eof: true}), frameHandler(&logv1.Frame{Next: "cursorB", Eof: true}), nil)
		if code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions", "", nil); code != http.StatusOK {
			t.Fatalf("list %d %s", code, body)
		}
		key := state.AgentKey(state.PlaneSTS, "sts-tw-1")
		delKey(t, se.e.servers["tw"], key)
		until(t, 10*time.Second, "agent key gone", func() error {
			view, ok := se.s.snapshot("tw")
			if !ok {
				return errors.New("no snapshot")
			}
			if _, ok := view.Entries[key]; ok {
				return errors.New("still present")
			}
			return nil
		})
		before := len(agent.copy())
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+id+"/logs", "", nil)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("%d %s", code, body)
		}
		mustProblem(t, body, http.StatusServiceUnavailable, auditLogsUnavailable)
		if len(agent.copy()) != before || len(waits.copy()) != 0 {
			t.Fatal("dialed a remembered agent")
		}
	})

	t.Run("region down", func(t *testing.T) {
		waits.reset()
		agent.reset(frameHandler(&logv1.Frame{Next: "cursorA", Eof: true}), frameHandler(&logv1.Frame{Next: "cursorB", Eof: true}), nil)
		jp := makeLogSession(t, se, "sts-jp-1")
		setLogAddr(t, se, "jp", "sts-jp-1", agent.addr())
		before := len(agent.copy())
		se.e.proxy.Cut()
		t.Cleanup(se.e.proxy.Restore)
		until(t, 15*time.Second, "jp down", func() error {
			ready, _ := se.s.readiness(time.Now())
			for _, name := range ready.UnavailableRegions {
				if name == "jp" {
					return nil
				}
			}
			return errors.New("still up")
		})
		code, _, body := se.call(t, http.MethodGet, "/api/v1/sessions/"+jp+"/logs", "", nil)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("%d %s", code, body)
		}
		mustProblem(t, body, http.StatusServiceUnavailable, auditRegionUnavailable)
		if len(agent.copy()) != before {
			t.Fatal("dialed while the region was down")
		}
	})
}

func TestSessionLogsNoToken(t *testing.T) {
	se := newSessionEnv(t, sessionOpt{})
	code, _, body := se.call(t, http.MethodGet, "/readyz", "", nil)
	if code != http.StatusOK {
		t.Fatalf("readyz %d %s", code, body)
	}
	code, _, body = se.call(t, http.MethodGet, "/api/v1/sessions", "", nil)
	if code != http.StatusOK {
		t.Fatalf("sessions %d %s", code, body)
	}
	code, _, body = se.call(t, http.MethodGet, "/api/v1/sessions/abc123/logs", "", nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("logs %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, auditLogsUnavailable)
	code, _, body = se.call(t, http.MethodGet, "/api/v1/sessions/abc123/logs/export", "", nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("export %d %s", code, body)
	}
	mustProblem(t, body, http.StatusServiceUnavailable, auditLogsUnavailable)
}

type logHit struct {
	kind  string
	path  string
	query url.Values
	auth  string
}

type fakeLogs struct {
	token  string
	srv    *httptest.Server
	mu     sync.Mutex
	hits   []logHit
	stdout func(http.ResponseWriter, *http.Request)
	stderr func(http.ResponseWriter, *http.Request)
	export func(http.ResponseWriter, *http.Request)
}

func newFakeLogs(t *testing.T, token string) *fakeLogs {
	t.Helper()
	f := &fakeLogs{token: token}
	f.srv = httptest.NewServer(f)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeLogs) addr() string { return f.srv.Listener.Addr().String() }

func (f *fakeLogs) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	kind := "other"
	switch {
	case strings.HasSuffix(r.URL.Path, "/logs/stdout"):
		kind = "stdout"
	case strings.HasSuffix(r.URL.Path, "/logs/stderr"):
		kind = "stderr"
	case strings.HasSuffix(r.URL.Path, "/logs/export"):
		kind = "export"
	}
	f.mu.Lock()
	f.hits = append(f.hits, logHit{kind: kind, path: r.URL.Path, query: r.URL.Query(), auth: r.Header.Get("Authorization")})
	var h func(http.ResponseWriter, *http.Request)
	switch kind {
	case "stdout":
		h = f.stdout
	case "stderr":
		h = f.stderr
	case "export":
		h = f.export
	}
	f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if h == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	h(w, r)
}

func (f *fakeLogs) reset(stdout, stderr, export func(http.ResponseWriter, *http.Request)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits = nil
	f.stdout, f.stderr, f.export = stdout, stderr, export
}

func (f *fakeLogs) copy() []logHit {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]logHit, len(f.hits))
	copy(out, f.hits)
	return out
}

type waitLog struct {
	mu sync.Mutex
	d  []time.Duration
}

func (w *waitLog) install(s *Server) {
	s.logWait = func(ctx context.Context, d time.Duration) error {
		w.mu.Lock()
		w.d = append(w.d, d)
		w.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
}

func (w *waitLog) reset() {
	w.mu.Lock()
	w.d = nil
	w.mu.Unlock()
}

func (w *waitLog) copy() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Duration(nil), w.d...)
}

func sameWaits(got []time.Duration, want ...time.Duration) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func frameHandler(f *logv1.Frame) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		_ = writeFrame(w, f)
		flushResp(w)
	}
}

func writeFrame(w io.Writer, f *logv1.Frame) error {
	b, err := proto.Marshal(f)
	if err != nil {
		return err
	}
	var prefix [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(prefix[:], uint64(len(b)))
	if _, err := w.Write(prefix[:n]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func flushResp(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func makeLogSession(t *testing.T, se *sessionEnv, sts string) string {
	t.Helper()
	code, hdr, body := se.post(t, minBody(sts), nil)
	return mustSession(t, code, hdr, body).Id
}

func setLogAddr(t *testing.T, se *sessionEnv, region, sts, addr string) {
	t.Helper()
	key := state.AgentKey(state.PlaneSTS, sts)
	putProto(t, se.e.servers[region], key, &statev1.AgentRecord{
		Plane: stsPlane, Instance: sts, Host: "host", WorkDir: "/var/lib/mftiko", LogAddr: addr,
	})
	until(t, 10*time.Second, "log_addr "+addr, func() error {
		view, ok := se.s.snapshot(region)
		if !ok {
			return errors.New("no snapshot")
		}
		entry, ok := view.Entries[key]
		if !ok {
			return errors.New("missing")
		}
		rec := &statev1.AgentRecord{}
		if err := proto.Unmarshal(entry.Value, rec); err != nil {
			return err
		}
		if rec.GetLogAddr() != addr {
			return fmt.Errorf("got %q", rec.GetLogAddr())
		}
		return nil
	})
}

func waitLogVisible(t *testing.T, s *Server, region, sts, id, addr string) {
	t.Helper()
	spec := state.SessionSpecKey(sts, id)
	key := state.AgentKey(state.PlaneSTS, sts)
	until(t, 10*time.Second, "replica log view", func() error {
		view, ok := s.snapshot(region)
		if !ok {
			return errors.New("no snapshot")
		}
		if _, ok := view.Entries[spec]; !ok {
			return errors.New("no spec")
		}
		entry, ok := view.Entries[key]
		if !ok {
			return errors.New("no agent")
		}
		rec := &statev1.AgentRecord{}
		if err := proto.Unmarshal(entry.Value, rec); err != nil {
			return err
		}
		if rec.GetLogAddr() != addr {
			return fmt.Errorf("addr %q", rec.GetLogAddr())
		}
		return nil
	})
}

const stsPlane = "sts"

func logGET(t *testing.T, ctx context.Context, base, id, raw string, hdr http.Header) *http.Response {
	t.Helper()
	path := base + "/api/v1/sessions/" + id + "/logs"
	if raw != "" {
		path += "?" + raw
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func getExport(t *testing.T, addr, id string) (int, http.Header, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/api/v1/sessions/"+id+"/logs/export", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b, err
}

type sseEv struct {
	ID, Event, Data string
}

func (e sseEv) asLog() (apiv1.LogEvent, bool) {
	if e.Event != "log" {
		return apiv1.LogEvent{}, false
	}
	var v apiv1.LogEvent
	if err := json.Unmarshal([]byte(e.Data), &v); err != nil {
		return apiv1.LogEvent{}, false
	}
	return v, true
}

func readSSE(r *bufio.Reader) (sseEv, error) {
	var ev sseEv
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return sseEv{}, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if ev.Event != "" || ev.Data != "" || ev.ID != "" {
				return ev, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		key, val, _ := strings.Cut(line, ":")
		val = strings.TrimPrefix(val, " ")
		switch key {
		case "id":
			ev.ID = val
		case "event":
			ev.Event = val
		case "data":
			if ev.Data != "" {
				ev.Data += "\n"
			}
			ev.Data += val
		}
	}
}

func parseSSE(t *testing.T, body []byte) []sseEv {
	t.Helper()
	return parseSSEReader(t, bufio.NewReader(bytes.NewReader(body)))
}

func parseSSEReader(t *testing.T, r *bufio.Reader) []sseEv {
	t.Helper()
	var out []sseEv
	for {
		ev, err := readSSE(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out
			}
			t.Fatal(err)
		}
		out = append(out, ev)
	}
}

func readRest(t *testing.T, r *bufio.Reader) []byte {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertLogPair(t *testing.T, ev []sseEv, a, b string) {
	t.Helper()
	got := map[string]bool{}
	for _, e := range ev {
		if logEv, ok := e.asLog(); ok {
			got[logEv.Data] = true
		}
	}
	if !got[a] || !got[b] {
		t.Fatalf("logs %v in %+v", got, ev)
	}
}

func assertEnd(t *testing.T, ev []sseEv, reason string) {
	t.Helper()
	if len(ev) == 0 || ev[len(ev)-1].Event != "end" {
		t.Fatalf("%+v", ev)
	}
	var end apiv1.LogEndEvent
	if err := json.Unmarshal([]byte(ev[len(ev)-1].Data), &end); err != nil || string(end.Reason) != reason {
		t.Fatalf("%s %v", ev[len(ev)-1].Data, err)
	}
}

func auditConn(t *testing.T, se *sessionEnv) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), se.pg.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func sampleTar(t *testing.T) []byte {
	t.Helper()
	stdout := "aa\n"
	stderr := "bb\n"
	sum := func(s string) string {
		h := sha256.Sum256([]byte(s))
		return hex.EncodeToString(h[:])
	}
	manifest := fmt.Sprintf("{\"stdout\":{\"size\":%d,\"sha256\":%q},\"stderr\":{\"size\":%d,\"sha256\":%q}}\n",
		len(stdout), sum(stdout), len(stderr), sum(stderr))
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, f := range []struct{ name, body string }{
		{"stdout.log", stdout},
		{"stderr.log", stderr},
		{"manifest.json", manifest},
	} {
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(f.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func tarNames(b []byte) ([]string, error) {
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var names []string
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return names, nil
		}
		if err != nil {
			return names, err
		}
		names = append(names, h.Name)
	}
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
