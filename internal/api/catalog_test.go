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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

func catalogSrv(repos []string, addr string, cat tagCatalog) *Server {
	return &Server{
		log: zerolog.Nop(),
		now: time.Now,
		cfg: Config{
			Users:        []User{{Name: "ada", Token: sha256.Sum256([]byte(tokenAda))}},
			Repositories: append([]string(nil), repos...),
			Registry:     artifact.Registry{Address: addr, PlainHTTP: true},
		},
		catalog: cat,
	}
}

// doAuth sends the catalog requests as an authenticated caller. Every /api route, including the
// strategy catalog, requires a bearer token or cookie (D1009).
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
	resolveErr map[string]error
	tagsErr    map[string]error
	readErr    map[string]error
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
	return append([]string(nil), f.tags[repository]...), nil
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

func TestListStrategies(t *testing.T) {
	code, hdr, body := doAuth(catalogSrv(nil, "", nil).routes(), http.MethodGet, "/api/v1/strategies")
	if code != http.StatusOK || hdr.Get("Content-Type") != "application/json" || string(body) != "{\"items\":[]}\n" {
		t.Fatalf("empty: %d %s %s", code, hdr.Get("Content-Type"), body)
	}

	code, p := getProblem(t, catalogSrv([]string{"team/alpha"}, "", nil).routes(), "/api/v1/strategies")
	if code != http.StatusServiceUnavailable || p.Code != apiv1.ErrorCodeRegistryUnavailable || problemDetail(p) != "registry is not configured" {
		t.Fatalf("unconfigured: %d %+v", code, p)
	}

	f := &fakeCatalog{
		digest: map[string]string{
			catKey("team/alpha", "latest"): sha(0),
			catKey("team/zeta", "latest"):  sha(1),
		},
	}
	s := catalogSrv([]string{"team/zeta", "other/mid", "team/alpha"}, "reg.example", f)
	h := s.routes()
	var page apiv1.StrategyList
	code, hdr, body = doAuth(h, http.MethodGet, "/api/v1/strategies?limit=2")
	if code != http.StatusOK {
		t.Fatalf("page: %d %s", code, body)
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if page.NextCursor == nil || len(page.Items) != 2 {
		t.Fatalf("page %+v", page)
	}
	if page.Items[0].Id != "alpha" || page.Items[0].Repository != "reg.example/team/alpha" ||
		page.Items[0].Latest == nil || page.Items[0].Latest.Tag != "latest" || page.Items[0].Latest.Digest != sha(0) {
		t.Fatalf("first %+v", page.Items[0])
	}
	if page.Items[1].Id != "mid" || page.Items[1].Latest != nil {
		t.Fatalf("missing latest should be omitted: %+v", page.Items[1])
	}
	raw, err := base64.RawURLEncoding.DecodeString(*page.NextCursor)
	if err != nil || string(raw) != "mid" {
		t.Fatalf("cursor %q %v", *page.NextCursor, err)
	}
	if f.resolved() != 2 {
		t.Fatalf("resolved %d, want the page only", f.resolved())
	}
	code, _, body = doAuth(h, http.MethodGet, "/api/v1/strategies?limit=2&cursor="+*page.NextCursor)
	if code != http.StatusOK {
		t.Fatalf("next: %d %s", code, body)
	}
	page = apiv1.StrategyList{}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if page.NextCursor != nil || len(page.Items) != 1 || page.Items[0].Id != "zeta" || page.Items[0].Latest.Digest != sha(1) {
		t.Fatalf("last page %+v", page)
	}

	// A cursor past the end is a page with no items. It does not ask the registry again.
	before := f.resolved()
	encoded := base64.RawURLEncoding.EncodeToString([]byte("zzz"))
	code, _, body = doAuth(h, http.MethodGet, "/api/v1/strategies?cursor="+encoded)
	if code != http.StatusOK || string(body) != "{\"items\":[]}\n" {
		t.Fatalf("past the end: %d %s", code, body)
	}
	if f.resolved() != before {
		t.Fatal("empty page resolved a tag")
	}
}

func TestListStrategiesUnavailable(t *testing.T) {
	f := &fakeCatalog{resolveErr: map[string]error{
		catKey("team/alpha", "latest"): artErr(artifact.ReasonUnavailable, false),
	}}
	code, p := getProblem(t, catalogSrv([]string{"team/alpha", "team/beta"}, "reg.example", f).routes(), "/api/v1/strategies")
	if code != http.StatusServiceUnavailable || p.Code != apiv1.ErrorCodeRegistryUnavailable || problemDetail(p) != "registry unavailable" {
		t.Fatalf("%d %+v", code, p)
	}
	if strings.Contains(string(mustJSON(p)), "secret-registry-detail") {
		t.Fatalf("detail leaked: %+v", p)
	}

	f.resolveErr[catKey("team/alpha", "latest")] = artErr(artifact.ReasonUnauthorized, true)
	code, p = getProblem(t, catalogSrv([]string{"team/alpha"}, "reg.example", f).routes(), "/api/v1/strategies")
	if code != http.StatusInternalServerError || p.Code != apiv1.ErrorCodeInternal || problemDetail(p) != "internal error" {
		t.Fatalf("unauthorized %d %+v", code, p)
	}
	if strings.Contains(problemDetail(p), "secret-registry-detail") {
		t.Fatal(p)
	}

	f = &fakeCatalog{resolveErr: map[string]error{
		catKey("team/alpha", "latest"): artErr(artifact.ReasonUnavailable, true),
	}}
	code, p = getProblem(t, catalogSrv([]string{"team/alpha"}, "reg.example", f).routes(), "/api/v1/strategies")
	if code != http.StatusInternalServerError || p.Code != apiv1.ErrorCodeInternal {
		t.Fatalf("permanent unavailable %d %+v", code, p)
	}
}

func TestListStrategyVersions(t *testing.T) {
	digA := sha(0)
	f := &fakeCatalog{
		tags: map[string][]string{"team/hello": {"v1", "latest", "a", "a", "gone"}},
		digest: map[string]string{
			catKey("team/hello", "a"):      digA,
			catKey("team/hello", "latest"): digA,
			catKey("team/hello", "v1"):     digA,
		},
	}
	h := catalogSrv([]string{"team/hello"}, "reg.example", f).routes()
	var page apiv1.StrategyTagList
	code, _, body := doAuth(h, http.MethodGet, "/api/v1/strategies/hello/versions?limit=2")
	if code != http.StatusOK {
		t.Fatalf("%d %s", code, body)
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Items[0].Tag != "a" || page.Items[1].Tag != "latest" || page.NextCursor == nil {
		t.Fatalf("page %+v", page)
	}
	code, _, body = doAuth(h, http.MethodGet, "/api/v1/strategies/hello/versions?limit=2&cursor="+*page.NextCursor)
	page = apiv1.StrategyTagList{}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if code != http.StatusOK || len(page.Items) != 1 || page.Items[0].Tag != "v1" || page.NextCursor != nil || page.Items[0].Digest != digA {
		t.Fatalf("last %+v code %d", page, code)
	}

	f.tagsErr = map[string]error{"team/hello": artErr(artifact.ReasonUnavailable, false)}
	code, p := getProblem(t, h, "/api/v1/strategies/hello/versions")
	if code != http.StatusServiceUnavailable || p.Code != apiv1.ErrorCodeRegistryUnavailable {
		t.Fatalf("tags %d %+v", code, p)
	}

	code, p = getProblem(t, h, "/api/v1/strategies/missing/versions")
	if code != http.StatusNotFound || p.Code != apiv1.ErrorCodeNotFound || problemDetail(p) != "strategy not found" {
		t.Fatalf("unknown %d %+v", code, p)
	}
}

func TestGetStrategyVersion(t *testing.T) {
	digA := sha(0)
	digB := sha(1)
	f := &fakeCatalog{
		tags: map[string][]string{
			"team/hello": {"v1", "latest", "a", "other"},
			"team/conn":  {"w"},
			"team/lang":  {"v"},
			"team/ver":   {"v"},
			"team/none":  {"v"},
		},
		digest: map[string]string{
			catKey("team/hello", "a"):      digA,
			catKey("team/hello", "latest"): digA,
			catKey("team/hello", "v1"):     digA,
			catKey("team/hello", "other"):  digB,
			catKey("team/conn", "w"):       digB,
			catKey("team/lang", "v"):       sha(2),
			catKey("team/ver", "v"):        sha(3),
			catKey("team/none", "v"):       sha(4),
		},
		manifest: map[string]artifact.ManifestInfo{
			catKey("team/hello", digA):  strategyManifest(digA, artifact.KindStrategy, artifact.SDK{Lang: "go", Version: "1.2.3"}),
			catKey("team/conn", digB):   strategyManifest(digB, artifact.KindMDConn, artifact.SDK{}),
			catKey("team/lang", sha(2)): strategyManifest(sha(2), artifact.KindStrategy, artifact.SDK{Lang: "go"}),
			catKey("team/ver", sha(3)):  strategyManifest(sha(3), artifact.KindStrategy, artifact.SDK{Version: "9"}),
			catKey("team/none", sha(4)): strategyManifest(sha(4), artifact.KindStrategy, artifact.SDK{}),
		},
		readErr: map[string]error{},
	}
	repos := []string{"team/hello", "team/conn", "team/lang", "team/ver", "team/none"}
	h := catalogSrv(repos, "reg.example", f).routes()

	var got apiv1.StrategyVersion
	code, hdr, body := doAuth(h, http.MethodGet, "/api/v1/strategies/hello/versions/latest")
	if code != http.StatusOK || hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("tag %d %s %s", code, hdr.Get("Content-Type"), body)
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.StrategyId != "hello" || got.Repository != "reg.example/team/hello" || got.Digest != digA ||
		got.Name != "hello" || got.Runtime != "go" || got.Os != "linux" || got.Arch != "amd64" ||
		got.Sdk == nil || *got.Sdk != "go 1.2.3" || got.SizeBytes == nil || *got.SizeBytes != 42 {
		t.Fatalf("%+v", got)
	}
	if strings.Join(got.Tags, ",") != "a,latest,v1" || strings.Join(got.Contracts, ",") != "worker.v1,stsctl.v1" {
		t.Fatalf("tags %q contracts %q", got.Tags, got.Contracts)
	}
	code, _, body = doAuth(h, http.MethodGet, "/api/v1/strategies/hello/versions/"+digA)
	if code != http.StatusOK {
		t.Fatalf("digest %d %s", code, body)
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Digest != digA || strings.Join(got.Tags, ",") != "a,latest,v1" {
		t.Fatalf("by digest %+v", got)
	}

	code, p := getProblem(t, h, "/api/v1/strategies/hello/versions/no-such")
	if code != http.StatusNotFound || problemDetail(p) != "version not found" {
		t.Fatalf("tag %d %+v", code, p)
	}
	code, p = getProblem(t, h, "/api/v1/strategies/hello/versions/"+sha(9))
	if code != http.StatusNotFound || p.Code != apiv1.ErrorCodeNotFound {
		t.Fatalf("digest %d %+v", code, p)
	}
	reads := f.reads
	code, p = getProblem(t, h, "/api/v1/strategies/hello/versions/sha256:ABC")
	if code != http.StatusNotFound || problemDetail(p) != "version not found" {
		t.Fatalf("bad digest %d %+v", code, p)
	}
	if f.reads != reads {
		t.Fatalf("reads %d to %d: a bad digest must not be fetched", reads, f.reads)
	}

	code, p = getProblem(t, h, "/api/v1/strategies/conn/versions/w")
	if code != http.StatusBadGateway || p.Code != apiv1.ErrorCodeInvalidArtifact || problemDetail(p) != "manifest is not a strategy artifact" {
		t.Fatalf("kind %d %+v", code, p)
	}

	f.readErr[catKey("team/hello", digA)] = artErr(artifact.ReasonBadManifest, true)
	code, p = getProblem(t, h, "/api/v1/strategies/hello/versions/latest")
	if code != http.StatusBadGateway || p.Code != apiv1.ErrorCodeInvalidArtifact || problemDetail(p) != "manifest could not be read" {
		t.Fatalf("bad manifest %d %+v", code, p)
	}
	f.readErr[catKey("team/hello", digA)] = artErr(artifact.ReasonNotImage, true)
	code, p = getProblem(t, h, "/api/v1/strategies/hello/versions/latest")
	if code != http.StatusBadGateway || problemDetail(p) != "manifest is not a strategy artifact" {
		t.Fatalf("index %d %+v", code, p)
	}
	delete(f.readErr, catKey("team/hello", digA))

	checkSDK := func(id, want string, present bool) {
		t.Helper()
		code, _, body := doAuth(h, http.MethodGet, "/api/v1/strategies/"+id+"/versions/v")
		if code != http.StatusOK {
			t.Fatalf("%s %d %s", id, code, body)
		}
		var v apiv1.StrategyVersion
		if err := json.Unmarshal(body, &v); err != nil {
			t.Fatal(err)
		}
		if !present {
			if v.Sdk != nil {
				t.Fatalf("%s sdk %q", id, *v.Sdk)
			}
			return
		}
		if v.Sdk == nil || *v.Sdk != want {
			t.Fatalf("%s sdk %+v", id, v.Sdk)
		}
	}
	checkSDK("lang", "go", true)
	checkSDK("ver", "9", true)
	checkSDK("none", "", false)

	code, _, body = doAuth(h, http.MethodGet, "/api/v1")
	if code != http.StatusNotFound {
		t.Fatalf("/api/v1 %d %s", code, body)
	}
	code, _, _ = doAuth(h, http.MethodPost, "/api/v1/strategies")
	if code != http.StatusMethodNotAllowed {
		t.Fatalf("POST %d", code)
	}
}

func TestCatalogValidation(t *testing.T) {
	f := &fakeCatalog{
		tags:   map[string][]string{"team/hello": {"v1"}},
		digest: map[string]string{catKey("team/hello", "v1"): sha(0)},
	}
	h := catalogSrv([]string{"team/hello"}, "reg.example", f).routes()
	code, p := getProblem(t, h, "/api/v1/strategies?limit=0")
	if code != http.StatusBadRequest || p.Code != apiv1.ErrorCodeValidationFailed || p.Errors == nil || (*p.Errors)[0].Path != "/limit" {
		t.Fatalf("limit 0: %d %+v", code, p)
	}
	code, p = getProblem(t, h, "/api/v1/strategies?limit=1001")
	if code != http.StatusBadRequest || (*p.Errors)[0].Path != "/limit" {
		t.Fatalf("limit 1001: %d %+v", code, p)
	}
	code, p = getProblem(t, h, "/api/v1/strategies?limit=abc")
	if code != http.StatusBadRequest || p.Code != apiv1.ErrorCodeValidationFailed || (*p.Errors)[0].Path != "/limit" {
		t.Fatalf("limit abc: %d %+v", code, p)
	}
	code, p = getProblem(t, h, "/api/v1/strategies/hello/versions?cursor="+strings.Repeat("!", 4))
	if code != http.StatusBadRequest || (*p.Errors)[0].Path != "/cursor" {
		t.Fatalf("cursor: %d %+v", code, p)
	}
	if f.resolved() != 0 || f.tagCalls != 0 {
		t.Fatal("validation called the registry")
	}
}

func TestCatalogResolveParallel(t *testing.T) {
	f := &fakeCatalog{
		tags:   map[string][]string{"team/hello": {}},
		digest: map[string]string{},
		hold:   make(chan struct{}),
	}
	for i := 0; i < 20; i++ {
		tag := "t" + string(rune('a'+i))
		f.tags["team/hello"] = append(f.tags["team/hello"], tag)
		f.digest[catKey("team/hello", tag)] = sha(0)
	}
	var once sync.Once
	closeHold := func() { once.Do(func() { close(f.hold) }) }
	t.Cleanup(closeHold)

	h := catalogSrv([]string{"team/hello"}, "reg.example", f).routes()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/strategies/hello/versions", nil)
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
	var page apiv1.StrategyTagList
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 20 {
		t.Fatalf("items %d", len(page.Items))
	}
}

func TestCatalogCancelWritesNothing(t *testing.T) {
	f := &fakeCatalog{hold: make(chan struct{})}
	h := catalogSrv([]string{"team/hello"}, "reg.example", f).routes()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/strategies/hello/versions", nil).WithContext(ctx)
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
	cat, err := artifact.OpenCatalog(artifact.Registry{Address: "127.0.0.1:1", PlainHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	s := catalogSrv([]string{"team/hello"}, "127.0.0.1:1", cat)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/strategies/hello/versions", nil).WithContext(ctx)
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
