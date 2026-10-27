package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

const (
	testCookieKey = "cookie-hmac-key"
	tokenAda      = "tok-0123456789abcdef0123456789abcdef"
	tokenBea      = "tok-fedcba9876543210fedcba9876543210"
	tokenShort    = "x"
	credSecret    = "cred-SECRET-VALUE-do-not-store"
	rawSecret     = "raw-SECRET-VALUE-do-not-store"
	querySecret   = "query-SECRET-VALUE-do-not-store"
	xffSpoof      = "203.0.113.9"
	deadDSN       = "postgres://mftiko@127.0.0.1:1/mftiko?sslmode=disable"
)

func authConfig(t *testing.T, log zerolog.Logger, dsn string, now func() time.Time) Config {
	t.Helper()
	deploy := deployDoc(
		map[string][]string{"tw": {closedEndpoint(t)}},
		map[string]string{"controller": "tw", "md-tw-1": "tw"},
		map[string]string{"md-tw-1": "md"},
	)
	conf, err := json.Marshal(map[string]any{
		"listen": "127.0.0.1:0",
		"allow":  []string{"*"},
		"users": []any{
			map[string]string{"name": "ada", "token_sha256": tokenHash(tokenAda)},
			map[string]string{"name": "bea", "token_sha256": tokenHash(tokenBea)},
			map[string]string{"name": "short", "token_sha256": tokenHash(tokenShort)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return loadWithin(t, deploy, string(conf), Input{
		EtcdPassword: "pw",
		CookieKey:    testCookieKey,
		PostgresDSN:  dsn,
		Mirror: state.MirrorOptions{
			ProbeInterval: 100 * time.Millisecond,
			MinBackoff:    20 * time.Millisecond,
			MaxBackoff:    200 * time.Millisecond,
		},
		Log: log,
		Now: now,
	})
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

type apiCall struct {
	method        string
	path          string
	bearer        string
	authorization string
	cookie        string
	body          string
	contentType   string
	header        http.Header
}

func (c apiCall) do(t *testing.T, client *http.Client, base string) (int, http.Header, []byte) {
	t.Helper()
	var rdr io.Reader
	if c.body != "" {
		rdr = strings.NewReader(c.body)
	}
	req, err := http.NewRequest(c.method, base+c.path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if c.contentType != "" {
		req.Header.Set("Content-Type", c.contentType)
	}
	switch {
	case c.authorization != "":
		req.Header.Set("Authorization", c.authorization)
	case c.bearer != "":
		req.Header.Set("Authorization", "Bearer "+c.bearer)
	}
	if c.cookie != "" {
		req.Header.Set("Cookie", cookieName+"="+c.cookie)
	}
	for k, vs := range c.header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, b
}

func mustProblem(t *testing.T, body []byte, status int, code string) {
	t.Helper()
	var p struct {
		Type   string `json:"type"`
		Code   string `json:"code"`
		Status int    `json:"status"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("%s: %v", body, err)
	}
	if p.Type != "about:blank" || p.Code != code || p.Status != status {
		t.Fatalf("problem %+v from %s", p, body)
	}
}

func sessionCookie(t *testing.T, hdr http.Header) *http.Cookie {
	t.Helper()
	resp := &http.Response{Header: hdr}
	for _, c := range resp.Cookies() {
		if c.Name == cookieName {
			return c
		}
	}
	t.Fatalf("no %s cookie in %v", cookieName, hdr.Values("Set-Cookie"))
	return nil
}

func assertSecureCookie(t *testing.T, hdr http.Header, cleared bool) *http.Cookie {
	t.Helper()
	c := sessionCookie(t, hdr)
	raw := strings.Join(hdr.Values("Set-Cookie"), "\n")
	if !c.Secure || !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/" {
		t.Fatalf("cookie flags %+v raw %q", c, raw)
	}
	if !strings.Contains(raw, "Secure") || !strings.Contains(raw, "HttpOnly") || !strings.Contains(raw, "SameSite=Strict") {
		t.Fatalf("raw cookie %q", raw)
	}
	if cleared && !strings.Contains(raw, "Max-Age=0") {
		t.Fatalf("logout cookie raw %q", raw)
	}
	return c
}

func TestBearerParsing(t *testing.T) {
	r := httptestNew(http.MethodGet, "/")
	if _, ok := bearer(r); ok {
		t.Fatal("missing Authorization counted as bearer")
	}
	r.Header.Set("Authorization", "Basic abc")
	if _, ok := bearer(r); ok {
		t.Fatal("basic counted as bearer")
	}
	r.Header.Set("Authorization", "Bearer ")
	tok, ok := bearer(r)
	if !ok || tok != "" {
		t.Fatalf("empty bearer (%q, %v)", tok, ok)
	}
	r.Header.Set("Authorization", "Bearer")
	tok, ok = bearer(r)
	if !ok || tok != "" {
		t.Fatalf("bare scheme (%q, %v)", tok, ok)
	}
	r.Header.Set("Authorization", "bearer "+tokenAda)
	tok, ok = bearer(r)
	if !ok || tok != tokenAda {
		t.Fatalf("scheme case (%q, %v)", tok, ok)
	}
}

func httptestNew(method, path string) *http.Request {
	req, err := http.NewRequest(method, path, nil)
	if err != nil {
		panic(err)
	}
	return req
}

func TestCookieRoundTrip(t *testing.T) {
	now := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	exp := now.Add(cookieTTL)
	value, err := signCookie(testCookieKey, "ada", exp)
	if err != nil {
		t.Fatal(err)
	}
	name, gotExp, ok := openCookie(testCookieKey, value)
	if !ok || name != "ada" || !gotExp.Equal(exp) {
		t.Fatalf("open (%q, %s, %v)", name, gotExp, ok)
	}
	if _, _, ok := openCookie("other-key", value); ok {
		t.Fatal("wrong key accepted")
	}
	bad := value[:len(value)-1] + "A"
	if value[len(value)-1] == 'A' {
		bad = value[:len(value)-1] + "B"
	}
	if _, _, ok := openCookie(testCookieKey, bad); ok {
		t.Fatal("tampered cookie accepted")
	}
	s := &Server{cfg: Config{CookieKey: testCookieKey, Users: []User{{Name: "ada"}}}, now: func() time.Time { return now }}
	if _, _, ok := s.verifyCookie(value); !ok {
		t.Fatal("fresh cookie rejected")
	}
	s.now = func() time.Time { return exp.Add(-time.Second) }
	if _, _, ok := s.verifyCookie(value); !ok {
		t.Fatal("cookie rejected before exp")
	}
	s.now = func() time.Time { return exp }
	if _, _, ok := s.verifyCookie(value); ok {
		t.Fatal("cookie accepted at exp")
	}
	ghost, err := signCookie(testCookieKey, "ghost", exp)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return now }
	if _, _, ok := s.verifyCookie(ghost); ok {
		t.Fatal("unknown user accepted")
	}
}

func TestAuthHTTP(t *testing.T) {
	buf := &lockedBuf{}
	cfg := authConfig(t, zerolog.New(buf).Level(zerolog.InfoLevel), deadDSN, nil)
	r := startRunning(t, cfg, buf)
	base := "http://" + r.s.Addr()
	client := &http.Client{Timeout: 2 * time.Second}

	code, hdr, body := (apiCall{method: http.MethodGet, path: "/healthz"}).do(t, client, base)
	if code != http.StatusOK || !requestIDHeader(hdr.Get(headerRequestID)) {
		t.Fatalf("healthz %d id %q body %s", code, hdr.Get(headerRequestID), body)
	}

	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me"}).do(t, client, base)
	if code != http.StatusUnauthorized {
		t.Fatalf("anon me %d %s", code, body)
	}
	mustProblem(t, body, http.StatusUnauthorized, "unauthenticated")

	bogus := "bogus-SECRET-do-not-echo"
	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", bearer: bogus}).do(t, client, base)
	if code != http.StatusUnauthorized || strings.Contains(string(body), bogus) {
		t.Fatalf("bad bearer %d %s", code, body)
	}
	mustProblem(t, body, http.StatusUnauthorized, "unauthenticated")

	fromClient := http.Header{"X-Request-Id": {"from-client"}}
	code, hdr, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", bearer: tokenAda, header: fromClient}).do(t, client, base)
	if code != http.StatusOK || !strings.Contains(string(body), `"expires_at":null`) || !strings.Contains(string(body), `"name":"ada"`) {
		t.Fatalf("bearer me %d %s", code, body)
	}
	if hdr.Get(headerRequestID) == "from-client" || !requestIDHeader(hdr.Get(headerRequestID)) {
		t.Fatalf("request id %q", hdr.Get(headerRequestID))
	}

	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/sessions", bearer: tokenAda}).do(t, client, base)
	if code != http.StatusOK {
		t.Fatalf("sessions %d %s", code, body)
	}
	var listed apiv1.SessionList
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatalf("sessions %s: %v", body, err)
	}
	twDown := false
	for _, region := range listed.Freshness.UnavailableRegions {
		if region == "tw" {
			twDown = true
		}
	}
	if len(listed.Items) != 0 || listed.Freshness.ControllerAbsent || !twDown {
		t.Fatalf("sessions %+v", listed)
	}

	code, hdr, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/auth/login",
		contentType: "application/json", body: `{"token":"` + tokenAda + `"}`,
	}).do(t, client, base)
	if code != http.StatusNoContent || strings.Contains(string(body), tokenAda) {
		t.Fatalf("login %d %s", code, body)
	}
	ck := assertSecureCookie(t, hdr, false)
	if strings.Contains(ck.Value, tokenAda) {
		t.Fatal("cookie value contains the token")
	}
	if name, _, ok := openCookie(testCookieKey, ck.Value); !ok || name != "ada" {
		t.Fatalf("issued cookie (%q, %v)", name, ok)
	}

	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", cookie: ck.Value}).do(t, client, base)
	if code != http.StatusOK || strings.Contains(string(body), `"expires_at":null`) || !strings.Contains(string(body), `"name":"ada"`) {
		t.Fatalf("cookie me %d %s", code, body)
	}
	var me struct {
		ExpiresAt *time.Time `json:"expires_at"`
	}
	if json.Unmarshal(body, &me) != nil || me.ExpiresAt == nil {
		t.Fatalf("cookie expires_at %s", body)
	}

	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", bearer: tokenBea, cookie: ck.Value}).do(t, client, base)
	if code != http.StatusOK || !strings.Contains(string(body), `"name":"bea"`) {
		t.Fatalf("bearer priority %d %s", code, body)
	}

	code, _, body = (apiCall{
		method: http.MethodGet, path: "/api/v1/auth/me", cookie: ck.Value, authorization: "Bearer " + bogus,
	}).do(t, client, base)
	if code != http.StatusUnauthorized || strings.Contains(string(body), bogus) {
		t.Fatalf("invalid bearer fell through %d %s", code, body)
	}

	code, _, body = (apiCall{
		method: http.MethodGet, path: "/api/v1/auth/me", cookie: ck.Value, authorization: "Bearer ",
	}).do(t, client, base)
	if code != http.StatusUnauthorized {
		t.Fatalf("empty bearer %d %s", code, body)
	}

	code, _, body = (apiCall{
		method: http.MethodGet, path: "/api/v1/auth/me", cookie: ck.Value, authorization: "Basic abc",
	}).do(t, client, base)
	if code != http.StatusOK || !strings.Contains(string(body), `"name":"ada"`) {
		t.Fatalf("non-bearer with cookie %d %s", code, body)
	}

	// Flip the first signature character. The last base64 character of a
	// 32-byte HMAC can hold unused bits, so changing only that character
	// does not always change the decoded MAC.
	sig := strings.LastIndex(ck.Value, ".") + 1
	repl := byte('A')
	if ck.Value[sig] == 'A' {
		repl = 'B'
	}
	tampered := ck.Value[:sig] + string(repl) + ck.Value[sig+1:]
	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", cookie: tampered}).do(t, client, base)
	if code != http.StatusUnauthorized || strings.Contains(string(body), tampered) {
		t.Fatalf("tampered %d %s", code, body)
	}

	// The Content-Type rule is for the cookie (E10-D009, E10-D110).
	code, _, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/sessions", cookie: ck.Value,
		contentType: "text/plain", body: "x",
	}).do(t, client, base)
	if code != http.StatusBadRequest || !strings.Contains(string(body), "Content-Type") {
		t.Fatalf("content type %d %s", code, body)
	}
	mustProblem(t, body, http.StatusBadRequest, "validation_failed")

	code, _, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/sessions", cookie: ck.Value,
		contentType: "application/json; charset=utf-8", body: `{}`,
	}).do(t, client, base)
	if code != http.StatusBadRequest {
		t.Fatalf("charset %d %s", code, body)
	}
	mustProblem(t, body, http.StatusBadRequest, "validation_failed")

	code, _, body = (apiCall{method: http.MethodPost, path: "/api/v1/auth/logout", cookie: ck.Value}).do(t, client, base)
	if code != http.StatusBadRequest || !strings.Contains(string(body), "Content-Type") {
		t.Fatalf("cookie write without Content-Type %d %s", code, body)
	}

	// A bearer write needs no Content-Type, with a body or without one (E10-D110).
	code, _, body = (apiCall{method: http.MethodPost, path: "/api/v1/auth/logout", bearer: tokenAda}).do(t, client, base)
	if code == http.StatusBadRequest || code >= 500 {
		t.Fatalf("bearer write without a body %d %s", code, body)
	}
	code, _, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/sessions", bearer: tokenAda,
		contentType: "text/plain", body: "x",
	}).do(t, client, base)
	if code != http.StatusBadRequest || strings.Contains(string(body), "Content-Type") {
		t.Fatalf("bearer write with text/plain reaches the handler: %d %s", code, body)
	}

	code, _, body = (apiCall{method: http.MethodPost, path: "/api/v1/sessions", contentType: "text/plain", body: "x"}).do(t, client, base)
	if code != http.StatusUnauthorized {
		t.Fatalf("anon write %d %s", code, body)
	}

	code, _, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/auth/login", contentType: "text/plain", body: `{"token":"` + tokenAda + `"}`,
	}).do(t, client, base)
	if code != http.StatusBadRequest || strings.Contains(string(body), tokenAda) {
		t.Fatalf("login content type %d %s", code, body)
	}

	code, _, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/auth/login", contentType: "application/json", body: `{}`,
	}).do(t, client, base)
	if code != http.StatusBadRequest || !strings.Contains(string(body), `"/token"`) {
		t.Fatalf("missing token %d %s", code, body)
	}

	code, hdr, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/auth/login", contentType: "application/json", body: `{"token":"` + tokenShort + `"}`,
	}).do(t, client, base)
	if code != http.StatusNoContent {
		t.Fatalf("short token %d %s", code, body)
	}
	shortCK := assertSecureCookie(t, hdr, false)

	code, hdr, body = (apiCall{
		method: http.MethodPost, path: "/api/v1/auth/logout", cookie: ck.Value,
		contentType: "application/json", body: `{}`,
	}).do(t, client, base)
	if code != http.StatusNoContent {
		t.Fatalf("logout %d %s", code, body)
	}
	assertSecureCookie(t, hdr, true)

	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me"}).do(t, client, base)
	if code != http.StatusUnauthorized {
		t.Fatalf("after logout %d %s", code, body)
	}
	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", cookie: ck.Value}).do(t, client, base)
	if code != http.StatusOK {
		t.Fatalf("resent cookie %d %s", code, body)
	}
	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/events", bearer: tokenAda}).do(t, client, base)
	if code != http.StatusBadRequest {
		t.Fatalf("events without topics %d %s", code, body)
	}
	mustProblem(t, body, http.StatusBadRequest, "validation_failed")

	if name, _, ok := openCookie(testCookieKey, shortCK.Value); !ok || name != "short" {
		t.Fatalf("short cookie (%q, %v)", name, ok)
	}
	log := buf.String()
	for _, secret := range []string{tokenAda, tokenBea, bogus, ck.Value, tampered} {
		if strings.Contains(log, secret) {
			t.Fatalf("log contains %q\n%s", secret, log)
		}
	}
}

func TestAuthCookieExpiry(t *testing.T) {
	buf := &lockedBuf{}
	clock := &testClock{t: time.Date(2026, 10, 23, 1, 0, 0, 0, time.UTC)}
	cfg := authConfig(t, zerolog.New(buf).Level(zerolog.InfoLevel), deadDSN, clock.Now)
	r := startRunning(t, cfg, buf)
	base := "http://" + r.s.Addr()
	client := &http.Client{Timeout: 2 * time.Second}
	code, hdr, body := (apiCall{
		method: http.MethodPost, path: "/api/v1/auth/login",
		contentType: "application/json", body: `{"token":"` + tokenAda + `"}`,
	}).do(t, client, base)
	if code != http.StatusNoContent {
		t.Fatalf("login %d %s", code, body)
	}
	ck := assertSecureCookie(t, hdr, false)
	_, exp, ok := openCookie(testCookieKey, ck.Value)
	if !ok || !exp.Equal(clock.Now().Add(cookieTTL)) {
		t.Fatalf("exp %s ok %v", exp, ok)
	}
	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", cookie: ck.Value}).do(t, client, base)
	if code != http.StatusOK {
		t.Fatalf("before exp %d %s", code, body)
	}
	clock.Set(exp.Add(-time.Second))
	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", cookie: ck.Value}).do(t, client, base)
	if code != http.StatusOK {
		t.Fatalf("one second before exp %d %s", code, body)
	}
	clock.Set(exp)
	code, _, body = (apiCall{method: http.MethodGet, path: "/api/v1/auth/me", cookie: ck.Value}).do(t, client, base)
	if code != http.StatusUnauthorized || strings.Contains(string(body), ck.Value) {
		t.Fatalf("at exp %d %s", code, body)
	}
	if strings.Contains(buf.String(), tokenAda) || strings.Contains(buf.String(), ck.Value) {
		t.Fatal("secret in log")
	}
}
