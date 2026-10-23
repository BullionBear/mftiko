package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/pgtest"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/migrations"
)

func TestRedactedBodyAndQuery(t *testing.T) {
	raw := []byte(`{"token":"sekret","name":"kept","credentials":{"secret":"s"},"nested":[{"token":"t2","ok":1}],"api_id":9223372036854775807}`)
	stored, n, truncated, parsed := redactedBody(raw, false, true)
	if truncated || n == nil || *n != int64(len(raw)) {
		t.Fatalf("stored len %d truncated %v n %v", len(stored), truncated, n)
	}
	if strings.Contains(string(stored), "sekret") || strings.Contains(string(stored), `"t2"`) || strings.Contains(string(stored), `"credentials"`) {
		t.Fatalf("redacted body %s", stored)
	}
	m, ok := parsed.(map[string]any)
	if !ok {
		t.Fatalf("parsed %T", parsed)
	}
	if _, ok := m["token"]; ok {
		t.Fatal("token key survived")
	}
	if _, ok := m["credentials"]; ok {
		t.Fatal("credentials survived")
	}
	if id, ok := int64From(m["api_id"]); !ok || id != 9223372036854775807 {
		t.Fatalf("api_id %v", m["api_id"])
	}
	nested, ok := m["nested"].([]any)
	if !ok || len(nested) != 1 {
		t.Fatalf("nested %v", m["nested"])
	}
	if _, ok := nested[0].(map[string]any)["token"]; ok {
		t.Fatal("nested token survived")
	}

	stored, n, truncated, _ = redactedBody([]byte(rawSecret), false, true)
	if stored != nil || truncated || n == nil || *n != int64(len(rawSecret)) {
		t.Fatalf("non-json stored %s n %v truncated %v", stored, n, truncated)
	}
	stored, _, truncated, _ = redactedBody([]byte(`{"a":1} trailing`), false, true)
	if stored != nil || truncated {
		t.Fatal("trailing junk was stored")
	}
	stored, n, truncated, _ = redactedBody(nil, false, true)
	if stored != nil || truncated || n == nil || *n != 0 {
		t.Fatalf("empty write stored %s n %v", stored, n)
	}
	stored, n, truncated, _ = redactedBody([]byte(`{"token":"x"}`), false, false)
	if stored != nil || n != nil || truncated {
		t.Fatal("GET body was stored")
	}
	stored, n, truncated, _ = redactedBody([]byte(credSecret), true, true)
	if stored != nil || !truncated || n == nil || *n != int64(len(credSecret)) {
		t.Fatalf("too big stored %s n %v truncated %v", stored, n, truncated)
	}

	big := append([]byte(`{"pad":"`), bytes.Repeat([]byte("a"), auditBodyLimit)...)
	big = append(big, []byte(`","name":"kept"}`)...)
	stored, _, truncated, parsed = redactedBody(big, false, true)
	if !truncated || len(stored) > auditBodyLimit {
		t.Fatalf("truncate len %d flag %v", len(stored), truncated)
	}
	var marker truncMarker
	if err := json.Unmarshal(stored, &marker); err != nil || !marker.Truncated || marker.Len <= auditBodyLimit || marker.Prefix == "" {
		t.Fatalf("marker %s err %v", stored, err)
	}
	redacted, err := json.Marshal(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(redacted), marker.Prefix) || strings.Contains(marker.Prefix, credSecret) {
		t.Fatal("prefix is not the head of the redacted JSON")
	}

	if got := auditQuery("a=1&b=2"); got != "a=1&b=2" {
		t.Fatalf("query rewritten: %s", got)
	}
	got := auditQuery("token=" + querySecret + "&a=1")
	if strings.Contains(got, querySecret) || !strings.Contains(got, "a=1") {
		t.Fatalf("token query %s", got)
	}
	if strings.Contains(auditQuery("credentials="+querySecret), querySecret) {
		t.Fatal("credentials query kept")
	}
	sid, aid := pathTargets("/api/v1/sessions/ab12cd/end")
	if sid != "ab12cd" || aid != nil {
		t.Fatalf("path session %q %v", sid, aid)
	}
	sid, aid = pathTargets("/api/v1/accounts/42")
	if sid != "" || aid == nil || *aid != 42 {
		t.Fatalf("path account %q %v", sid, aid)
	}
	sid, aid = queryTargets("session_id=cd34ef&api_id=7", "", nil)
	if sid != "cd34ef" || aid == nil || *aid != 7 {
		t.Fatalf("query targets %q %v", sid, aid)
	}
}

func TestAuditEnqueueDropsWhenFull(t *testing.T) {
	var buf lockedBuf
	a := &auditor{
		log: zerolog.New(&buf).Level(zerolog.ErrorLevel),
		ch:  make(chan auditRow, 1),
	}
	a.enqueue(auditRow{requestID: "one"})
	a.enqueue(auditRow{requestID: "two"})
	if len(a.ch) != 1 || a.dropped.Load() != 1 {
		t.Fatalf("queued %d dropped %d", len(a.ch), a.dropped.Load())
	}
	if !strings.Contains(buf.String(), "audit queue full") {
		t.Fatalf("log %s", buf.String())
	}
}

func TestAuditRows(t *testing.T) {
	conn, dsn := applyAuditDB(t)
	buf := &lockedBuf{}
	cfg := authConfig(t, zerolog.New(buf).Level(zerolog.InfoLevel), dsn, nil)
	r := startRunning(t, cfg, buf)
	base := "http://" + r.s.Addr()
	client := &http.Client{Timeout: 5 * time.Second}

	for _, path := range []string{"/healthz", "/readyz", "/not-a-page"} {
		code, _, body := (apiCall{method: http.MethodGet, path: path}).do(t, client, base)
		if path == "/healthz" && code != http.StatusOK {
			t.Fatalf("healthz %d %s", code, body)
		}
	}

	code, hdr, body := (apiCall{method: http.MethodGet, path: "/api/v1/auth/me"}).do(t, client, base)
	if code != http.StatusUnauthorized {
		t.Fatalf("anon %d %s", code, body)
	}
	anonID := hdr.Get(headerRequestID)
	anon := waitAuditID(t, conn, anonID)
	if anon.User != "" || anon.Status != http.StatusUnauthorized || anon.Body != nil || anon.BodyLen != nil {
		t.Fatalf("anon row %+v", anon)
	}
	rows := queryAudits(t, conn)
	for _, row := range rows {
		switch row.Path {
		case "/healthz", "/readyz", "/not-a-page", "/":
			t.Fatalf("non-api row %+v", row)
		}
	}

	code, hdr, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/auth/login",
		contentType: "application/json", body: `{"token":"` + tokenAda + `","extra":1}`,
	}).do(t, client, base)
	if code != http.StatusNoContent {
		t.Fatalf("login %d %s", code, body)
	}
	loginRow := waitAuditID(t, conn, hdr.Get(headerRequestID))
	var loginBody map[string]any
	// token is removed; the remaining field is kept (D1052).
	if loginRow.User != "ada" || loginRow.Status != http.StatusNoContent || loginRow.Body == nil ||
		json.Unmarshal([]byte(*loginRow.Body), &loginBody) != nil || loginBody["extra"] != float64(1) ||
		loginBody["token"] != nil || strings.Contains(*loginRow.Body, tokenAda) {
		t.Fatalf("login row %+v body %s", loginRow, stringOr(loginRow.Body))
	}

	xff := http.Header{"X-Forwarded-For": {xffSpoof}, "X-Request-Id": {"from-client"}}
	code, hdr, body = (apiCall{
		method: http.MethodGet, path: "/api/v1/sessions/ab12cd", bearer: tokenAda, header: xff,
	}).do(t, client, base)
	if code != http.StatusNotFound {
		t.Fatalf("session %d %s", code, body)
	}
	if hdr.Get(headerRequestID) == "from-client" || !requestIDHeader(hdr.Get(headerRequestID)) {
		t.Fatalf("request id %q", hdr.Get(headerRequestID))
	}
	sess := waitAuditID(t, conn, hdr.Get(headerRequestID))
	if sess.User != "ada" || sess.SessionID == nil || *sess.SessionID != "ab12cd" || sess.APIID != nil {
		t.Fatalf("session target %+v", sess)
	}
	if !strings.HasPrefix(sess.Remote, "127.0.0.1:") || strings.Contains(sess.Remote, xffSpoof) {
		t.Fatalf("remote %q", sess.Remote)
	}
	if sess.RequestID != hdr.Get(headerRequestID) {
		t.Fatalf("row id %s header %s", sess.RequestID, hdr.Get(headerRequestID))
	}

	code, hdr, _ = (apiCall{
		method: http.MethodGet,
		path:   "/api/v1/auth/me?session_id=cd34ef&api_id=7&token=" + querySecret,
		bearer: tokenAda,
	}).do(t, client, base)
	if code != http.StatusOK {
		t.Fatalf("query me %d", code)
	}
	qrow := waitAuditID(t, conn, hdr.Get(headerRequestID))
	if qrow.SessionID == nil || *qrow.SessionID != "cd34ef" || qrow.APIID == nil || *qrow.APIID != 7 {
		t.Fatalf("query targets %+v", qrow)
	}
	if strings.Contains(qrow.Query, querySecret) {
		t.Fatalf("query kept token %s", qrow.Query)
	}

	small := `{"name":"kept","token":"` + tokenAda + `","credentials":{"secret":"` + credSecret + `"}}`
	code, hdr, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/accounts", bearer: tokenAda,
		contentType: "application/json", body: small,
	}).do(t, client, base)
	if code != http.StatusBadRequest {
		t.Fatalf("small write %d %s", code, body)
	}
	smallRow := waitAuditID(t, conn, hdr.Get(headerRequestID))
	if smallRow.Body == nil || strings.Contains(*smallRow.Body, credSecret) || strings.Contains(*smallRow.Body, tokenAda) {
		t.Fatalf("small body %v", smallRow.Body)
	}
	var kept map[string]any
	if json.Unmarshal([]byte(*smallRow.Body), &kept) != nil || kept["name"] != "kept" {
		t.Fatalf("small json %v", smallRow.Body)
	}
	if _, ok := kept["credentials"]; ok {
		t.Fatal("credentials stored")
	}

	code, hdr, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/accounts", bearer: tokenAda,
		contentType: "application/json", body: rawSecret,
	}).do(t, client, base)
	if code != http.StatusBadRequest {
		t.Fatalf("raw %d %s", code, body)
	}
	rawRow := waitAuditID(t, conn, hdr.Get(headerRequestID))
	if rawRow.Body != nil || rawRow.Truncated || rawRow.BodyLen == nil || *rawRow.BodyLen != int64(len(rawSecret)) {
		t.Fatalf("raw row %+v", rawRow)
	}

	huge := `{"name":"kept","token":"` + tokenAda + `","credentials":{"secret":"` + credSecret + `"},"pad":"` + strings.Repeat("a", 70<<10) + `","session_id":"ab12cd","api_id":8}`
	code, hdr, _ = (apiCall{
		method: http.MethodPost, path: "/api/v1/sessions", bearer: tokenAda,
		contentType: "application/json", body: huge,
	}).do(t, client, base)
	if code != http.StatusBadRequest {
		t.Fatalf("huge %d", code)
	}
	hugeRow := waitAuditID(t, conn, hdr.Get(headerRequestID))
	var marker truncMarker
	if !hugeRow.Truncated || hugeRow.Body == nil || json.Unmarshal([]byte(*hugeRow.Body), &marker) != nil || !marker.Truncated {
		t.Fatalf("huge row truncated %v body %s", hugeRow.Truncated, stringOr(hugeRow.Body))
	}
	if hugeRow.SessionID == nil || *hugeRow.SessionID != "ab12cd" || hugeRow.APIID == nil || *hugeRow.APIID != 8 {
		t.Fatalf("huge targets %+v", hugeRow)
	}
	if strings.Contains(*hugeRow.Body, credSecret) || strings.Contains(*hugeRow.Body, tokenAda) {
		t.Fatalf("huge body kept a secret %s", *hugeRow.Body)
	}

	over := `{"secret":"` + credSecret + `","pad":"` + strings.Repeat("b", maxRequestBody) + `"}`
	code, hdr, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/accounts", bearer: tokenAda,
		contentType: "application/json", body: over,
	}).do(t, client, base)
	if code != http.StatusBadRequest || strings.Contains(string(body), credSecret) {
		t.Fatalf("oversize %d %s", code, body)
	}
	overRow := waitAuditID(t, conn, hdr.Get(headerRequestID))
	if overRow.User != "ada" || overRow.Body != nil || !overRow.Truncated || overRow.BodyLen == nil || *overRow.BodyLen != int64(maxRequestBody) {
		t.Fatalf("oversize row %+v", overRow)
	}
	for _, row := range queryAudits(t, conn) {
		blob := row.Query + row.Detail + row.Remote + stringOr(row.Body)
		for _, secret := range []string{tokenAda, credSecret, rawSecret, querySecret} {
			if strings.Contains(blob, secret) {
				t.Fatalf("row kept %q: %+v", secret, row)
			}
		}
	}
	log := buf.String()
	for _, secret := range []string{tokenAda, credSecret, rawSecret, querySecret} {
		if strings.Contains(log, secret) {
			t.Fatalf("log contains %q", secret)
		}
	}
}

func TestAuditSSERowAtEnd(t *testing.T) {
	conn, dsn := applyAuditDB(t)
	buf := &lockedBuf{}
	cfg := authConfig(t, zerolog.New(buf).Level(zerolog.InfoLevel), dsn, nil)
	r := startRunning(t, cfg, buf)
	base := "http://" + r.s.Addr()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/events?topics=sessions", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	type result struct {
		resp *http.Response
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		ch <- result{resp, err}
	}()
	var resp *http.Response
	until(t, 5*time.Second, "sse headers", func() error {
		select {
		case res := <-ch:
			if res.err != nil {
				return res.err
			}
			resp = res.resp
			return nil
		default:
			if resp != nil {
				return nil
			}
			return errors.New("headers not in")
		}
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("sse %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	id := resp.Header.Get(headerRequestID)
	if countAuditID(t, conn, id) != 0 {
		t.Fatal("audit row exists before the stream ended")
	}
	cancel()
	row := waitAuditID(t, conn, id)
	if row.User != "ada" || row.Status != http.StatusOK || row.Duration <= 0 || row.Body != nil {
		t.Fatalf("sse row %+v", row)
	}
	if strings.Contains(buf.String(), tokenAda) {
		t.Fatal("token in log")
	}
}

func TestAuditDeadPostgres(t *testing.T) {
	buf := &lockedBuf{}
	cfg := authConfig(t, zerolog.New(buf).Level(zerolog.InfoLevel), deadDSN, nil)
	r := startRunning(t, cfg, buf)
	base := "http://" + r.s.Addr()
	client := &http.Client{Timeout: time.Second}
	code, _, body := (apiCall{
		method: http.MethodPost, path: "/api/v1/auth/login",
		contentType: "application/json", body: `{"token":"` + tokenAda + `"}`,
	}).do(t, client, base)
	if code != http.StatusNoContent || strings.Contains(string(body), tokenAda) {
		t.Fatalf("login %d %s", code, body)
	}
	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", bearer: tokenAda}).do(t, client, base)
	if code != http.StatusOK || strings.Contains(string(body), tokenAda) {
		t.Fatalf("me %d %s", code, body)
	}
	until(t, 5*time.Second, "audit write error", func() error {
		log := buf.String()
		if strings.Contains(log, tokenAda) {
			return errors.New("token in log")
		}
		if !strings.Contains(log, "audit write") {
			return errors.New("no audit write yet")
		}
		return nil
	})
}

func TestAuditQueueFullDoesNotBlock(t *testing.T) {
	prev := auditQueueCap
	auditQueueCap = 1
	t.Cleanup(func() { auditQueueCap = prev })
	buf := &lockedBuf{}
	cfg := authConfig(t, zerolog.New(buf).Level(zerolog.InfoLevel), deadDSN, nil)
	r := startRunning(t, cfg, buf)
	hold := make(chan struct{})
	r.s.audit.hold = hold
	var once sync.Once
	release := func() { once.Do(func() { close(hold) }) }
	t.Cleanup(release)
	base := "http://" + r.s.Addr()
	client := &http.Client{Timeout: 2 * time.Second}
	call := apiCall{method: http.MethodGet, path: "/api/v1/auth/me", bearer: tokenAda}
	code, _, body := call.do(t, client, base)
	if code != http.StatusOK {
		t.Fatalf("first %d %s", code, body)
	}
	until(t, 2*time.Second, "worker blocked in insert", func() error {
		if !r.s.audit.writing.Load() {
			return errors.New("not writing")
		}
		return nil
	})
	if code, _, body = call.do(t, client, base); code != http.StatusOK {
		t.Fatalf("second %d %s", code, body)
	}
	if code, _, body = call.do(t, client, base); code != http.StatusOK {
		t.Fatalf("third %d %s", code, body)
	}
	until(t, 2*time.Second, "queue drop", func() error {
		if r.s.audit.dropped.Load() < 1 || !strings.Contains(buf.String(), "audit queue full") {
			return fmt.Errorf("dropped %d", r.s.audit.dropped.Load())
		}
		if strings.Contains(buf.String(), tokenAda) {
			return errors.New("token in log")
		}
		return nil
	})
	release()
}

func TestAuditDrainsOnShutdown(t *testing.T) {
	conn, dsn := applyAuditDB(t)
	buf := &lockedBuf{}
	cfg := authConfig(t, zerolog.New(buf).Level(zerolog.InfoLevel), dsn, nil)
	r := startRunning(t, cfg, buf)
	hold := make(chan struct{})
	r.s.audit.hold = hold
	var once sync.Once
	release := func() { once.Do(func() { close(hold) }) }
	t.Cleanup(release)
	base := "http://" + r.s.Addr()
	client := &http.Client{Timeout: 2 * time.Second}
	code, hdr, body := (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", bearer: tokenAda}).do(t, client, base)
	if code != http.StatusOK {
		t.Fatalf("me %d %s", code, body)
	}
	id := hdr.Get(headerRequestID)
	until(t, 2*time.Second, "insert held", func() error {
		if !r.s.audit.writing.Load() {
			return errors.New("not writing")
		}
		if countAuditID(t, conn, id) != 0 {
			return errors.New("row written while held")
		}
		return nil
	})
	r.cancel()
	until(t, 5*time.Second, "shutdown flushing", func() error {
		if !r.s.audit.flushing.Load() || !r.s.audit.writing.Load() {
			return fmt.Errorf("flushing %v writing %v", r.s.audit.flushing.Load(), r.s.audit.writing.Load())
		}
		if countAuditID(t, conn, id) != 0 {
			return errors.New("row written before release")
		}
		return nil
	})
	release()
	until(t, 5*time.Second, "drained row and stopped server", func() error {
		if countAuditID(t, conn, id) != 1 {
			return errors.New("row missing")
		}
		if !r.stopped() {
			return errors.New("still running")
		}
		return nil
	})
	row := waitAuditID(t, conn, id)
	if row.User != "ada" || strings.Contains(buf.String(), tokenAda) {
		t.Fatalf("row %+v", row)
	}
}

func TestAuditShutdownAbandonsHeldInsert(t *testing.T) {
	buf := &lockedBuf{}
	cfg := authConfig(t, zerolog.New(buf).Level(zerolog.InfoLevel), deadDSN, nil)
	cfg.ShutdownTimeout = 200 * time.Millisecond
	r := startRunning(t, cfg, buf)
	hold := make(chan struct{})
	r.s.audit.hold = hold
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(hold) }) })
	base := "http://" + r.s.Addr()
	client := &http.Client{Timeout: 2 * time.Second}
	code, _, body := (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", bearer: tokenAda}).do(t, client, base)
	if code != http.StatusOK {
		t.Fatalf("me %d %s", code, body)
	}
	until(t, 2*time.Second, "insert held", func() error {
		if !r.s.audit.writing.Load() {
			return errors.New("not writing")
		}
		return nil
	})
	r.cancel()
	until(t, 2*time.Second, "shutdown abandons the insert", func() error {
		if !r.stopped() {
			return errors.New("still running")
		}
		log := buf.String()
		if !strings.Contains(log, "audit write") || !strings.Contains(log, "context canceled") {
			return errors.New("missing cancel log")
		}
		if strings.Contains(log, tokenAda) {
			return errors.New("token in log")
		}
		return nil
	})
}

func TestReadyIgnoresPostgres(t *testing.T) {
	ctx := t.Context()
	servers := etcdtest.StartRegions(t, "tw")
	topo := state.Topology{
		Regions: map[string]state.Region{
			"tw": {Endpoints: servers["tw"].Endpoints()},
		},
		Instances: map[string]string{
			state.ControllerInstance: "tw",
			"md-tw-1":                "tw",
		},
	}
	planes := map[string]state.Plane{"md-tw-1": state.PlaneMD}
	applyAuth(t, ctx, topo, planes, servers)
	buf := &lockedBuf{}
	dep, err := json.Marshal(deployment{Topology: topo, Planes: planes})
	if err != nil {
		t.Fatal(err)
	}
	conf, err := json.Marshal(map[string]any{
		"listen":            "127.0.0.1:0",
		"unavailable_after": "2s",
		"repositories":      []string{"mftiko/strategies"},
		"users":             []any{map[string]string{"name": "ada", "token_sha256": tokenHash(tokenAda)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(bytes.NewReader(dep), bytes.NewReader(conf), Input{
		EtcdPassword: etcdtest.Password(state.UserAPI),
		CookieKey:    testCookieKey,
		PostgresDSN:  deadDSN,
		Mirror: state.MirrorOptions{
			ProbeInterval: 100 * time.Millisecond,
			MinBackoff:    20 * time.Millisecond,
			MaxBackoff:    200 * time.Millisecond,
		},
		Log: zerolog.New(buf).Level(zerolog.InfoLevel),
	})
	if err != nil {
		t.Fatal(err)
	}
	r := startRunning(t, cfg, buf)
	base := "http://" + r.s.Addr()
	until(t, 30*time.Second, "region ready", func() error {
		if r.stopped() {
			return errors.New("server exited")
		}
		code, raw := getJSON(t, base+"/readyz", nil)
		if code != http.StatusOK {
			return fmt.Errorf("readyz %d %s", code, raw)
		}
		return nil
	})
	secret := "bearer-SECRET-do-not-log"
	client := &http.Client{Timeout: 2 * time.Second}
	code, _, body := (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", bearer: secret}).do(t, client, base)
	if code != http.StatusUnauthorized || strings.Contains(string(body), secret) {
		t.Fatalf("me %d %s", code, body)
	}
	until(t, 5*time.Second, "audit error with readiness still up", func() error {
		if r.stopped() {
			return errors.New("server exited")
		}
		log := buf.String()
		if strings.Contains(log, secret) {
			return errors.New("secret in log")
		}
		if !strings.Contains(log, "audit write") {
			return errors.New("no audit error yet")
		}
		code, raw := getJSON(t, base+"/readyz", nil)
		if code != http.StatusOK {
			return fmt.Errorf("readyz %d %s", code, raw)
		}
		return nil
	})
}

func applyAuditDB(t *testing.T) (*pgx.Conn, string) {
	t.Helper()
	srv := pgtest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	b, err := migrations.FS.ReadFile("0003_audit.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := pgtest.ExecSQL(ctx, conn, string(b)); err != nil {
		t.Fatal(err)
	}
	return conn, srv.DSN()
}

type auditRec struct {
	User      string
	Method    string
	Path      string
	Query     string
	Status    int
	Duration  time.Duration
	RequestID string
	Remote    string
	SessionID *string
	APIID     *int64
	Body      *string
	BodyLen   *int64
	Truncated bool
	Detail    string
}

func queryAudits(t *testing.T, conn *pgx.Conn) []auditRec {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, `
		SELECT user_name, method, path, query, status, duration_ns, request_id, remote_addr,
		       session_id, api_id, body::text, body_len, body_truncated, detail
		FROM audit ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []auditRec
	for rows.Next() {
		var rec auditRec
		var dur int64
		if err := rows.Scan(&rec.User, &rec.Method, &rec.Path, &rec.Query, &rec.Status, &dur, &rec.RequestID,
			&rec.Remote, &rec.SessionID, &rec.APIID, &rec.Body, &rec.BodyLen, &rec.Truncated, &rec.Detail); err != nil {
			t.Fatal(err)
		}
		rec.Duration = time.Duration(dur)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func countAuditID(t *testing.T, conn *pgx.Conn, id string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM audit WHERE request_id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func waitAuditID(t *testing.T, conn *pgx.Conn, id string) auditRec {
	t.Helper()
	var got auditRec
	until(t, 5*time.Second, "audit row "+id, func() error {
		for _, row := range queryAudits(t, conn) {
			if row.RequestID == id {
				got = row
				return nil
			}
		}
		return errors.New("not written")
	})
	return got
}

func stringOr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
