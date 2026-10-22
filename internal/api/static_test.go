package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func testMux(t *testing.T, dir string) *Server {
	t.Helper()
	s := &Server{now: timeNow, log: zerolog.Nop(), cfg: Config{UnavailableAfter: DefaultUnavailableAfter}}
	if dir != "" {
		root, err := os.OpenRoot(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { root.Close() })
		s.web = root
	}
	return s
}

func timeNow() time.Time { return time.Time{} }

func do(h http.Handler, method, path string) (int, http.Header, []byte) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Header(), rec.Body.Bytes()
}

func TestStaticFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("INDEX-PAGE"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app.js"), []byte("APP-JS"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "healthz"), []byte("NOT-HEALTH"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	secret := []byte("SECRET-OUTSIDE-VALUE")
	if err := os.WriteFile(outside, secret, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "leak")); err != nil {
		t.Fatal(err)
	}

	h := testMux(t, dir).routes()
	code, _, body := do(h, http.MethodGet, "/")
	if code != http.StatusOK || !bytes.Equal(body, []byte("INDEX-PAGE")) {
		t.Fatalf("index %d %q", code, body)
	}
	code, _, body = do(h, http.MethodGet, "/app.js")
	if code != http.StatusOK || !bytes.Equal(body, []byte("APP-JS")) {
		t.Fatalf("app.js %d %q", code, body)
	}
	code, _, body = do(h, http.MethodGet, "/missing")
	if code != http.StatusOK || !bytes.Equal(body, []byte("INDEX-PAGE")) {
		t.Fatalf("missing %d %q", code, body)
	}
	code, _, body = do(h, http.MethodGet, "/assets")
	if code != http.StatusOK || !bytes.Equal(body, []byte("INDEX-PAGE")) {
		t.Fatalf("directory %d %q", code, body)
	}
	code, _, body = do(h, http.MethodGet, "/apiv1")
	if code != http.StatusOK || !bytes.Equal(body, []byte("INDEX-PAGE")) {
		t.Fatalf("apiv1 %d %q", code, body)
	}
	for _, path := range []string{"/api", "/api/", "/api/v1"} {
		code, _, body = do(h, http.MethodGet, path)
		if code != http.StatusNotFound || bytes.Contains(body, []byte("INDEX-PAGE")) {
			t.Fatalf("%s %d %q", path, code, body)
		}
	}
	code, _, body = do(h, http.MethodPost, "/api/v1")
	if code != http.StatusNotFound || bytes.Contains(body, []byte("INDEX-PAGE")) {
		t.Fatalf("POST /api/v1 %d %q", code, body)
	}
	code, _, _ = do(h, http.MethodPost, "/")
	if code != http.StatusMethodNotAllowed {
		t.Fatalf("POST / %d", code)
	}
	code, hdr, body := do(h, http.MethodGet, "/healthz")
	if code != http.StatusOK || !bytes.Contains(body, []byte(`"ok":true`)) || bytes.Contains(body, []byte("NOT-HEALTH")) {
		t.Fatalf("healthz %d %q", code, body)
	}
	if hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("content-type %q", hdr.Get("Content-Type"))
	}
	code, _, body = do(h, http.MethodGet, "/leak")
	if bytes.Contains(body, secret) {
		t.Fatalf("symlink escaped: %d %q", code, body)
	}
	req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	req.URL.Path = "/../../secret"
	rec := httptest.NewRecorder()
	testMux(t, dir).routes().ServeHTTP(rec, req)
	if bytes.Contains(rec.Body.Bytes(), secret) {
		t.Fatalf("cleaned path escaped: %d %q", rec.Code, rec.Body.Bytes())
	}
}

func TestNoWebDir(t *testing.T) {
	h := testMux(t, "").routes()
	code, _, _ := do(h, http.MethodGet, "/")
	if code != http.StatusNotFound {
		t.Fatalf("GET / %d", code)
	}
	code, _, body := do(h, http.MethodGet, "/healthz")
	if code != http.StatusOK || !bytes.Contains(body, []byte(`"ok":true`)) {
		t.Fatalf("healthz %d %q", code, body)
	}
	code, _, body = do(h, http.MethodGet, "/readyz")
	if code != http.StatusServiceUnavailable || !bytes.Contains(body, []byte(`"unavailable_regions":[]`)) {
		t.Fatalf("readyz %d %q", code, body)
	}
}

func TestMissingIndex(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "app.js"), []byte("APP-JS"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := testMux(t, dir).routes()
	code, _, body := do(h, http.MethodGet, "/app.js")
	if code != http.StatusOK || !bytes.Equal(body, []byte("APP-JS")) {
		t.Fatalf("app.js %d %q", code, body)
	}
	for _, path := range []string{"/", "/missing"} {
		code, _, body = do(h, http.MethodGet, path)
		if code != http.StatusNotFound {
			t.Fatalf("%s %d %q", path, code, body)
		}
	}
}
