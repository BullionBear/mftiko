package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

func catalogSrv(allow []string, addr string, cat tagCatalog) *Server {
	return &Server{
		log: zerolog.Nop(),
		now: time.Now,
		cfg: Config{
			Users:    []User{{Name: "ada", Token: sha256.Sum256([]byte(tokenAda))}},
			Allow:    append([]string(nil), allow...),
			Registry: artifact.Registry{Address: addr, PlainHTTP: true},
		},
		catalog: cat,
	}
}

// doAuth sends the catalog requests as an authenticated caller. Every /api route, including the
// strategy catalog, requires a bearer token or cookie (E10-D009).
func doAuth(h http.Handler, method, path string) (int, http.Header, []byte) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Header(), rec.Body.Bytes()
}

func artErr(reason string, permanent bool) error {
	return &artifact.Error{Reason: reason, Permanent: permanent, Err: errors.New("secret-registry-detail")}
}

func sha(n byte) string {
	return "sha256:" + strings.Repeat(string(rune('a'+n)), 64)
}

type fakeCatalog struct {
	mu         sync.Mutex
	tags       map[string][]string
	digest     map[string]string
	manifest   map[string]artifact.ManifestInfo
	data       map[string]artifact.DataInfo
	resolveErr map[string]error
	tagsErr    map[string]error
	readErr    map[string]error
	dataErr    map[string]error
	hold       chan struct{}
	inflight   int
	max        int
	resolves   int
	tagCalls   int
	reads      int
}

func catKey(repo, ref string) string { return repo + "\x00" + ref }

func (f *fakeCatalog) Tags(ctx context.Context, repository string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tagCalls++
	if err := f.tagsErr[repository]; err != nil {
		return nil, err
	}
	tags, ok := f.tags[repository]
	if f.tags != nil && !ok {
		return nil, &artifact.Error{Reason: artifact.ReasonNotFound, Permanent: true, Err: errors.New("missing")}
	}
	return append([]string(nil), tags...), nil
}

func (f *fakeCatalog) Resolve(ctx context.Context, repository, reference string) (string, error) {
	f.mu.Lock()
	f.inflight++
	if f.inflight > f.max {
		f.max = f.inflight
	}
	f.resolves++
	err := f.resolveErr[catKey(repository, reference)]
	dig := f.digest[catKey(repository, reference)]
	hold := f.hold
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inflight--
		f.mu.Unlock()
	}()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err != nil {
		return "", err
	}
	if dig == "" {
		return "", &artifact.Error{Reason: artifact.ReasonNotFound, Permanent: true, Err: errors.New("missing")}
	}
	return dig, nil
}

func (f *fakeCatalog) ReadManifest(ctx context.Context, repository, digest string) (artifact.ManifestInfo, error) {
	if err := ctx.Err(); err != nil {
		return artifact.ManifestInfo{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	if err := f.readErr[catKey(repository, digest)]; err != nil {
		return artifact.ManifestInfo{}, err
	}
	info, ok := f.manifest[catKey(repository, digest)]
	if !ok {
		return artifact.ManifestInfo{}, &artifact.Error{Reason: artifact.ReasonNotFound, Permanent: true, Err: errors.New("missing")}
	}
	return info, nil
}

func (f *fakeCatalog) ReadData(ctx context.Context, repository, digest string) (artifact.DataInfo, error) {
	if err := ctx.Err(); err != nil {
		return artifact.DataInfo{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.dataErr[catKey(repository, digest)]; err != nil {
		return artifact.DataInfo{}, err
	}
	info, ok := f.data[catKey(repository, digest)]
	if !ok {
		return artifact.DataInfo{}, &artifact.Error{Reason: artifact.ReasonNotFound, Permanent: true, Err: errors.New("missing")}
	}
	return info, nil
}

func (f *fakeCatalog) resolved() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resolves
}

func (f *fakeCatalog) maxInflight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.max
}

func strategyManifest(digest, kind string, sdk artifact.SDK) artifact.ManifestInfo {
	return artifact.ManifestInfo{
		Digest: digest,
		Size:   42,
		Manifest: artifact.Manifest{
			Schema:    artifact.Schema,
			Kind:      kind,
			Name:      "hello",
			Runtime:   "go",
			OS:        "linux",
			Arch:      "amd64",
			SDK:       sdk,
			Contracts: []string{"worker.v1", "stsctl.v1"},
		},
	}
}

func getProblem(t *testing.T, h http.Handler, path string) (int, apiv1.Problem) {
	t.Helper()
	code, hdr, body := doAuth(h, http.MethodGet, path)
	if hdr.Get("Content-Type") != "application/problem+json" {
		t.Fatalf("%s content-type %q body %s", path, hdr.Get("Content-Type"), body)
	}
	var p apiv1.Problem
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("%s: %v body %s", path, err, body)
	}
	if p.Type != "about:blank" || p.Status != code || string(body) == "" {
		t.Fatalf("problem %+v http %d", p, code)
	}
	return code, p
}

func problemDetail(p apiv1.Problem) string {
	if p.Detail == nil {
		return ""
	}
	return *p.Detail
}

func strategyQuery(repo, extra string) string {
	u := "/api/v1/strategies?repository=" + url.QueryEscape(repo)
	if extra != "" {
		u += "&" + extra
	}
	return u
}

func TestListStrategies(t *testing.T) {
	code, p := getProblem(t, catalogSrv(nil, "", nil).routes(), "/api/v1/strategies")
	if code != http.StatusBadRequest || p.Errors == nil || (*p.Errors)[0].Path != "/repository" {
		t.Fatalf("missing query: %d %+v", code, p)
	}

	code, p = getProblem(t, catalogSrv(nil, "reg.example", &fakeCatalog{}).routes(), strategyQuery("reg.example/team/hello", ""))
	if code != http.StatusBadRequest || p.Code != apiv1.ErrorCodeValidationFailed || p.Errors == nil || (*p.Errors)[0].Reason != "is not allowed" {
		t.Fatalf("not allowed: %d %+v", code, p)
	}

	code, p = getProblem(t, catalogSrv([]string{"*"}, "", nil).routes(), strategyQuery("reg.example/team/hello", ""))
	if code != http.StatusServiceUnavailable || p.Code != apiv1.ErrorCodeRegistryUnavailable || problemDetail(p) != "registry is not configured" {
		t.Fatalf("unconfigured: %d %+v", code, p)
	}

	wrong := &fakeCatalog{tags: map[string][]string{}}
	code, p = getProblem(t, catalogSrv([]string{"*"}, "reg.example", wrong).routes(), strategyQuery("other.example/team/hello", ""))
	if code != http.StatusNotFound || p.Code != apiv1.ErrorCodeNotFound || wrong.tagCalls != 1 {
		t.Fatalf("other host: %d %+v calls %d", code, p, wrong.tagCalls)
	}

	digA := sha(0)
	f := &fakeCatalog{
		tags: map[string][]string{"reg.example/team/hello": {"v1", "latest", "a", "a", "gone"}},
		digest: map[string]string{
			catKey("reg.example/team/hello", "a"):      digA,
			catKey("reg.example/team/hello", "latest"): digA,
			catKey("reg.example/team/hello", "v1"):     digA,
		},
	}
	h := catalogSrv([]string{"reg.example/team"}, "reg.example", f).routes()
	repo := "reg.example/team/hello"
	var page apiv1.StrategyRepository
	code, _, body := doAuth(h, http.MethodGet, strategyQuery(repo, "limit=1"))
	if code != http.StatusOK {
		t.Fatalf("page: %d %s", code, body)
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if page.Repository != repo || page.NextCursor == nil || len(page.Items) != 1 || page.Items[0].Tag != "a" || page.Items[0].Digest != digA {
		t.Fatalf("first %+v", page)
	}
	if page.Latest == nil || page.Latest.Tag != "latest" || page.Latest.Digest != digA {
		t.Fatalf("latest off the page: %+v", page.Latest)
	}
	if f.resolved() != 4 {
		t.Fatalf("resolved %d, want every tag", f.resolved())
	}
	code, _, body = doAuth(h, http.MethodGet, strategyQuery(repo, "limit=2&cursor="+*page.NextCursor))
	if code != http.StatusOK {
		t.Fatalf("next: %d %s", code, body)
	}
	page = apiv1.StrategyRepository{}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if page.NextCursor != nil || len(page.Items) != 2 || page.Items[0].Tag != "latest" || page.Items[1].Tag != "v1" || page.Latest == nil || page.Latest.Digest != digA {
		t.Fatalf("last page %+v", page)
	}

	before := f.resolved()
	encoded := base64.RawURLEncoding.EncodeToString([]byte("zzz"))
	code, _, body = doAuth(h, http.MethodGet, strategyQuery(repo, "cursor="+encoded))
	if code != http.StatusOK {
		t.Fatalf("past the end: %d %s", code, body)
	}
	page = apiv1.StrategyRepository{}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 || page.Repository != repo || page.Latest == nil || f.resolved() == before {
		t.Fatalf("past the end %+v resolved %d to %d", page, before, f.resolved())
	}

	code, p = getProblem(t, h, strategyQuery("reg.example/team/missing", ""))
	if code != http.StatusNotFound || p.Code != apiv1.ErrorCodeNotFound || problemDetail(p) != "repository not found" {
		t.Fatalf("missing %d %+v", code, p)
	}

	empty := &fakeCatalog{tags: map[string][]string{"reg.example/team/empty": {}}}
	code, _, body = doAuth(catalogSrv([]string{"reg.example/team"}, "reg.example", empty).routes(), http.MethodGet, strategyQuery("reg.example/team/empty", ""))
	if code != http.StatusOK {
		t.Fatalf("empty %d %s", code, body)
	}
	page = apiv1.StrategyRepository{}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if page.Repository != "reg.example/team/empty" || len(page.Items) != 0 || page.Latest != nil {
		t.Fatalf("empty page %+v", page)
	}
}

func TestListStrategiesUnavailable(t *testing.T) {
	f := &fakeCatalog{
		tags: map[string][]string{"reg.example/team/alpha": {"latest"}},
		resolveErr: map[string]error{
			catKey("reg.example/team/alpha", "latest"): artErr(artifact.ReasonUnavailable, false),
		},
	}
	code, p := getProblem(t, catalogSrv([]string{"reg.example/team"}, "reg.example", f).routes(), strategyQuery("reg.example/team/alpha", ""))
	if code != http.StatusServiceUnavailable || p.Code != apiv1.ErrorCodeRegistryUnavailable || problemDetail(p) != "registry unavailable" {
		t.Fatalf("%d %+v", code, p)
	}
	if strings.Contains(string(mustJSON(p)), "secret-registry-detail") {
		t.Fatalf("detail leaked: %+v", p)
	}

	f.resolveErr[catKey("reg.example/team/alpha", "latest")] = artErr(artifact.ReasonUnauthorized, true)
	code, p = getProblem(t, catalogSrv([]string{"reg.example/team"}, "reg.example", f).routes(), strategyQuery("reg.example/team/alpha", ""))
	if code != http.StatusServiceUnavailable || p.Code != apiv1.ErrorCodeRegistryUnavailable || problemDetail(p) != "registry unavailable" {
		t.Fatalf("unauthorized %d %+v", code, p)
	}
	if strings.Contains(problemDetail(p), "secret-registry-detail") {
		t.Fatal(p)
	}

	f = &fakeCatalog{
		tags: map[string][]string{"reg.example/team/alpha": {"latest"}},
		resolveErr: map[string]error{
			catKey("reg.example/team/alpha", "latest"): artErr(artifact.ReasonUnavailable, true),
		},
	}
	code, p = getProblem(t, catalogSrv([]string{"reg.example/team"}, "reg.example", f).routes(), strategyQuery("reg.example/team/alpha", ""))
	if code != http.StatusInternalServerError || p.Code != apiv1.ErrorCodeInternal {
		t.Fatalf("permanent unavailable %d %+v", code, p)
	}
}

func TestCatalogValidation(t *testing.T) {
	f := &fakeCatalog{
		tags:   map[string][]string{"reg.example/team/hello": {"v1"}},
		digest: map[string]string{catKey("reg.example/team/hello", "v1"): sha(0)},
	}
	h := catalogSrv([]string{"reg.example/team"}, "reg.example", f).routes()
	code, p := getProblem(t, h, strategyQuery("reg.example/team/hello", "limit=0"))
	if code != http.StatusBadRequest || p.Code != apiv1.ErrorCodeValidationFailed || p.Errors == nil || (*p.Errors)[0].Path != "/limit" {
		t.Fatalf("limit 0: %d %+v", code, p)
	}
	code, p = getProblem(t, h, strategyQuery("reg.example/team/hello", "limit=1001"))
	if code != http.StatusBadRequest || (*p.Errors)[0].Path != "/limit" {
		t.Fatalf("limit 1001: %d %+v", code, p)
	}
	code, p = getProblem(t, h, strategyQuery("reg.example/team/hello", "limit=abc"))
	if code != http.StatusBadRequest || p.Code != apiv1.ErrorCodeValidationFailed || (*p.Errors)[0].Path != "/limit" {
		t.Fatalf("limit abc: %d %+v", code, p)
	}
	code, p = getProblem(t, h, strategyQuery("reg.example/team/hello", "cursor="+strings.Repeat("!", 4)))
	if code != http.StatusBadRequest || (*p.Errors)[0].Path != "/cursor" {
		t.Fatalf("cursor: %d %+v", code, p)
	}
	if f.resolved() != 0 || f.tagCalls != 0 {
		t.Fatal("validation called the registry")
	}

	code, _, body := doAuth(h, http.MethodGet, "/api/v1")
	if code != http.StatusNotFound {
		t.Fatalf("/api/v1 %d %s", code, body)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/strategies", nil)
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST %d", rec.Code)
	}
	if f.tagCalls != 0 {
		t.Fatal("routing called the registry")
	}
}

func TestCatalogResolveParallel(t *testing.T) {
	f := &fakeCatalog{
		tags:   map[string][]string{"reg.example/team/hello": {}},
		digest: map[string]string{},
		hold:   make(chan struct{}),
	}
	for i := 0; i < 20; i++ {
		tag := "t" + string(rune('a'+i))
		f.tags["reg.example/team/hello"] = append(f.tags["reg.example/team/hello"], tag)
		f.digest[catKey("reg.example/team/hello", tag)] = sha(0)
	}
	var once sync.Once
	closeHold := func() { once.Do(func() { close(f.hold) }) }
	t.Cleanup(closeHold)

	h := catalogSrv([]string{"reg.example/team"}, "reg.example", f).routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, strategyQuery("reg.example/team/hello", ""), nil)
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()
	until(t, 5*time.Second, "8 tag resolves in flight", func() error {
		if f.maxInflight() < tagResolveParallel {
			return errors.New("waiting")
		}
		return nil
	})
	closeHold()
	until(t, 5*time.Second, "handler to finish", func() error {
		select {
		case <-done:
			return nil
		default:
			return errors.New("still running")
		}
	})
	if f.maxInflight() > tagResolveParallel {
		t.Fatalf("in flight %d, limit %d", f.maxInflight(), tagResolveParallel)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	var page apiv1.StrategyRepository
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 20 || page.Repository != "reg.example/team/hello" {
		t.Fatalf("items %d repo %q", len(page.Items), page.Repository)
	}
}

func TestCatalogCancelWritesNothing(t *testing.T) {
	f := &fakeCatalog{hold: make(chan struct{})}
	h := catalogSrv([]string{"reg.example/team"}, "reg.example", f).routes()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, strategyQuery("reg.example/team/hello", ""), nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	rec := &markWriter{ResponseWriter: httptest.NewRecorder()}
	h.ServeHTTP(rec, req)
	if rec.wrote {
		t.Fatal("a canceled request was turned into a response")
	}
}

func TestCatalogClosedPort(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:1", 200*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Skip("127.0.0.1:1 accepted a connection")
	}
	cat, err := artifact.OpenCatalog(artifact.Registry{PlainHTTPHosts: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	s := catalogSrv([]string{"127.0.0.1:1/team"}, "127.0.0.1:1", cat)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, strategyQuery("127.0.0.1:1/team/hello", ""), nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+tokenAda)
	s.routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	var p apiv1.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.Code != apiv1.ErrorCodeRegistryUnavailable || problemDetail(p) != "registry unavailable" {
		t.Fatalf("%+v", p)
	}
	if strings.Contains(rec.Body.String(), "127.0.0.1") || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("body %s", rec.Body)
	}
}

type markWriter struct {
	http.ResponseWriter
	wrote bool
}

func (m *markWriter) WriteHeader(code int) {
	m.wrote = true
	m.ResponseWriter.WriteHeader(code)
}

func (m *markWriter) Write(b []byte) (int, error) {
	m.wrote = true
	return m.ResponseWriter.Write(b)
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
