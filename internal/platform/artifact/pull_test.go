package artifact

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// fakeReg is an in-process OCI distribution registry. Pull talks to it over HTTP; nothing here dials out.
type fakeReg struct {
	srv     *httptest.Server
	mu      sync.Mutex
	objects map[string]regObj
	gets    map[string]int
	auths   []string
	agents  []string

	authMode string // "", "basic", "bearer"
	user     string
	pass     string
	token    string
	// status, when set, is the response for every manifest and blob GET.
	status    int
	tokenHits int

	// block, when set, holds the first GET of that digest until release is closed.
	// hits receives one value per GET of block, before the wait.
	block   string
	release chan struct{}
	hits    chan struct{}

	handlers map[string]http.HandlerFunc
}

type regObj struct {
	mediaType string
	body      []byte
}

func newFake(t *testing.T) *fakeReg {
	s := &fakeReg{
		objects: map[string]regObj{},
		gets:    map[string]int{},
		token:   "issued-token",
		user:    "alice",
		pass:    "secret",
	}
	s.srv = httptest.NewServer(s)
	t.Cleanup(s.srv.Close)
	return s
}

func (s *fakeReg) host() string {
	return strings.TrimPrefix(s.srv.URL, "http://")
}

func (s *fakeReg) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v2", "/v2/":
		w.WriteHeader(http.StatusOK)
		return
	case "/token":
		s.serveToken(w, r)
		return
	}
	kind, ref, ok := splitDistributionPath(r.URL.Path)
	if !ok || (kind != "manifests" && kind != "blobs") {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	s.gets[ref]++
	s.auths = append(s.auths, r.Header.Get("Authorization"))
	s.agents = append(s.agents, r.Header.Get("User-Agent"))
	s.mu.Unlock()

	if s.status != 0 {
		w.WriteHeader(s.status)
		return
	}
	if !s.allow(w, r) {
		return
	}
	// Every GET of the blocked digest waits. A cancelled leader's request ends on its own, and the
	// waiter that downloads the layer again waits here too: the test hangs until release is closed.
	if s.block != "" && ref == s.block {
		if s.hits != nil {
			s.hits <- struct{}{}
		}
		if s.release != nil {
			select {
			case <-s.release:
			case <-r.Context().Done():
				return
			}
		}
	}
	if h := s.handlers[ref]; h != nil {
		h(w, r)
		return
	}
	obj, ok := s.objects[ref]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", obj.mediaType)
	w.Header().Set("Docker-Content-Digest", ref)
	_, _ = w.Write(obj.body)
}

func (s *fakeReg) allow(w http.ResponseWriter, r *http.Request) bool {
	switch s.authMode {
	case "":
		return true
	case "basic":
		user, pass, ok := r.BasicAuth()
		if ok && user == s.user && pass == s.pass {
			return true
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="mftiko"`)
		w.WriteHeader(http.StatusUnauthorized)
		return false
	case "bearer":
		if r.Header.Get("Authorization") == "Bearer "+s.token {
			return true
		}
		realm := "http://" + r.Host + "/token"
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s",service="mftiko"`, realm))
		w.WriteHeader(http.StatusUnauthorized)
		return false
	default:
		w.WriteHeader(http.StatusInternalServerError)
		return false
	}
}

func (s *fakeReg) serveToken(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.tokenHits++
	s.mu.Unlock()
	user, pass, ok := r.BasicAuth()
	if !ok || user != s.user || pass != s.pass {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"token": s.token})
}

func (s *fakeReg) count(ref string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[ref]
}

func (s *fakeReg) put(mediaType string, body []byte) ocispec.Descriptor {
	d := ocispec.Descriptor{MediaType: mediaType, Digest: digest.FromBytes(body), Size: int64(len(body))}
	s.objects[d.Digest.String()] = regObj{mediaType: mediaType, body: body}
	return d
}

func (s *fakeReg) publish(man ocispec.Manifest) string {
	if man.MediaType == "" {
		man.MediaType = ocispec.MediaTypeImageManifest
	}
	if man.SchemaVersion == 0 {
		man.SchemaVersion = 2
	}
	raw, err := json.Marshal(man)
	if err != nil {
		panic(err)
	}
	return s.put(man.MediaType, raw).Digest.String()
}

func splitDistributionPath(path string) (kind, ref string, ok bool) {
	for _, k := range []string{"/manifests/", "/blobs/"} {
		if i := strings.LastIndex(path, k); i >= 0 {
			return strings.Trim(k, "/"), path[i+len(k):], true
		}
	}
	return "", "", false
}

func workerConfig(t *testing.T, m Manifest) []byte {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func testManifest() Manifest {
	return Manifest{
		Schema:     Schema,
		Kind:       KindStrategy,
		Name:       "chase",
		Runtime:    "go",
		OS:         "linux",
		Arch:       "amd64",
		Entrypoint: []string{"bin/strategy"},
		SDK:        SDK{Lang: "go", Version: "1.0.0"},
		Contracts:  []string{"stsctl.v1", "worker.v1"},
	}
}

func (s *fakeReg) image(t *testing.T, cfg Manifest, layers ...[]byte) (string, []ocispec.Descriptor) {
	t.Helper()
	config := s.put(MediaTypeConfig, workerConfig(t, cfg))
	descs := make([]ocispec.Descriptor, len(layers))
	for i, body := range layers {
		descs[i] = s.put("application/vnd.mftiko.layer.v1.tar", body)
	}
	id := s.publish(ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    config,
		Layers:    descs,
	})
	return id, descs
}

func (s *fakeReg) open(t *testing.T, creds string) (*Client, string) {
	t.Helper()
	return openAt(t, Registry{Address: s.host(), Credentials: creds, PlainHTTP: true}, t.TempDir())
}

func openAt(t *testing.T, reg Registry, data string) (*Client, string) {
	t.Helper()
	c, err := Open(reg, data)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return c, data
}

func baseReq(id string) Request {
	return Request{
		Repository: "mftiko/strategies",
		Digest:     id,
		Kind:       KindStrategy,
		OS:         "linux",
		Arch:       "amd64",
		Contracts:  []string{"worker.v1"},
	}
}

func wantReason(t *testing.T, err error, reason string, permanent bool) {
	t.Helper()
	if ReasonOf(err) != reason || Permanent(err) != permanent {
		t.Fatalf("reason %q permanent %v, err %v; want %s permanent %v", ReasonOf(err), Permanent(err), err, reason, permanent)
	}
}

func wantRequest(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrRequest) {
		t.Fatalf("got %v, want ErrRequest", err)
	}
	if ReasonOf(err) != "" {
		t.Fatalf("bad request carries reason %q", ReasonOf(err))
	}
}

func assertNoTemps(t *testing.T, data string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(data, "layers"))
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Errorf("temporary file left behind: %s", e.Name())
		}
	}
}

func TestPullStoresVerifiedLayers(t *testing.T) {
	s := newFake(t)
	id, layers := s.image(t, testManifest(), []byte("alpha-bytes"), []byte("beta-bytes"))
	c, data := s.open(t, "")
	got, err := c.Pull(context.Background(), baseReq(id))
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest != id || got.Manifest.Name != "chase" || len(got.Layers) != 2 {
		t.Fatalf("fetched %+v", got)
	}
	for i, layer := range got.Layers {
		body, err := os.ReadFile(layer.Path)
		if err != nil {
			t.Fatal(err)
		}
		want := []byte("alpha-bytes")
		if i == 1 {
			want = []byte("beta-bytes")
		}
		if !bytes.Equal(body, want) || layer.Digest != layers[i].Digest.String() {
			t.Fatalf("layer %d: path %s bytes %q digest %s", i, layer.Path, body, layer.Digest)
		}
		if layer.Path != filepath.Join(data, "layers", layer.Digest) {
			t.Fatalf("layer path %s", layer.Path)
		}
	}
	// The config blob is checked and not stored in the layer cache.
	entries, err := os.ReadDir(filepath.Join(data, "layers"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("cache has %d files, want the two layers", len(entries))
	}
	if s.agents[0] != "mftiko-artifact" {
		t.Fatalf("User-Agent %q", s.agents[0])
	}

	before := s.count(layers[0].Digest.String()) + s.count(layers[1].Digest.String())
	again, err := c.Pull(context.Background(), baseReq(id))
	if err != nil {
		t.Fatal(err)
	}
	if again.Layers[0].Path != got.Layers[0].Path {
		t.Fatalf("second pull path %s", again.Layers[0].Path)
	}
	after := s.count(layers[0].Digest.String()) + s.count(layers[1].Digest.String())
	if after != before {
		t.Fatalf("second pull downloaded layers again: gets %d, was %d", after, before)
	}
	// The manifest and the config are not the layer cache. They are fetched again.
	if s.count(id) < 2 {
		t.Fatalf("manifest gets %d, want a second fetch", s.count(id))
	}
}

func TestPullReusesSameSizeCacheWithoutHashing(t *testing.T) {
	s := newFake(t)
	body := []byte("alpha-bytes")
	id, layers := s.image(t, testManifest(), body)
	c, data := s.open(t, "")
	if _, err := c.Pull(context.Background(), baseReq(id)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(data, "layers", layers[0].Digest.String())
	corrupt := bytes.Repeat([]byte("x"), len(body))
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}
	before := s.count(layers[0].Digest.String())
	if _, err := c.Pull(context.Background(), baseReq(id)); err != nil {
		t.Fatal(err)
	}
	if s.count(layers[0].Digest.String()) != before {
		t.Fatal("same-size cache was downloaded again")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, corrupt) {
		t.Fatalf("cache bytes changed: %q", got)
	}

	// A different size is not a hit: the layer is downloaded again and replaces the file.
	if err := os.WriteFile(path, []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pull(context.Background(), baseReq(id)); err != nil {
		t.Fatal(err)
	}
	if s.count(layers[0].Digest.String()) != before+1 {
		t.Fatalf("size mismatch gets %d, want %d", s.count(layers[0].Digest.String()), before+1)
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("redownloaded bytes %q", got)
	}
}

func TestPullKeepsCachedLayerAndFetchesMissing(t *testing.T) {
	s := newFake(t)
	id, layers := s.image(t, testManifest(), []byte("one"), []byte("two"))
	c, data := s.open(t, "")
	if _, err := c.Pull(context.Background(), baseReq(id)); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(data, "layers", layers[1].Digest.String())
	if err := os.Remove(missing); err != nil {
		t.Fatal(err)
	}
	before0 := s.count(layers[0].Digest.String())
	if _, err := c.Pull(context.Background(), baseReq(id)); err != nil {
		t.Fatal(err)
	}
	if s.count(layers[0].Digest.String()) != before0 {
		t.Fatal("cached layer was downloaded")
	}
	if s.count(layers[1].Digest.String()) != 2 {
		t.Fatalf("missing layer gets %d", s.count(layers[1].Digest.String()))
	}
	if _, err := os.Stat(missing); err != nil {
		t.Fatal(err)
	}
}

func TestPullDigestMismatchLeavesNoFile(t *testing.T) {
	s := newFake(t)
	body := []byte("0123456789")
	id, layers := s.image(t, testManifest(), body)
	// The registry claims the descriptor's digest, and the body is different but the same length.
	// oras accepts the headers; the hash we compute while writing does not match.
	s.handlers = map[string]http.HandlerFunc{
		layers[0].Digest.String(): func(w http.ResponseWriter, r *http.Request) {
			lied := bytes.Repeat([]byte("x"), len(body))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Docker-Content-Digest", layers[0].Digest.String())
			w.Header().Set("Content-Length", strconv.Itoa(len(lied)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(lied)
		},
	}
	c, data := s.open(t, "")
	_, err := c.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonDigestMismatch, true)
	if _, statErr := os.Stat(filepath.Join(data, "layers", layers[0].Digest.String())); !os.IsNotExist(statErr) {
		t.Fatalf("digest file present: %v", statErr)
	}
	assertNoTemps(t, data)

	// A body shorter than the descriptor, sent without Content-Length so the client reads it.
	s.handlers[layers[0].Digest.String()] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Docker-Content-Digest", layers[0].Digest.String())
		w.WriteHeader(http.StatusOK)
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush: %v", err)
			return
		}
		_, _ = w.Write([]byte("no"))
	}
	_, err = c.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonDigestMismatch, true)
	assertNoTemps(t, data)

	// Content-Length that is not the descriptor size fails before the body is hashed.
	s.handlers[layers[0].Digest.String()] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Docker-Content-Digest", layers[0].Digest.String())
		w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
	_, err = c.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonDigestMismatch, true)
	assertNoTemps(t, data)
}

func TestPullBytesPastDeclaredSize(t *testing.T) {
	s := newFake(t)
	body := []byte("0123456789")
	id, layers := s.image(t, testManifest(), body)
	s.handlers = map[string]http.HandlerFunc{
		layers[0].Digest.String(): func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Docker-Content-Digest", layers[0].Digest.String())
			w.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Errorf("flush: %v", err)
				return
			}
			_, _ = w.Write(bytes.Repeat([]byte("x"), 64))
		},
	}
	c, data := s.open(t, "")
	_, err := c.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonTooLarge, true)
	if _, statErr := os.Stat(filepath.Join(data, "layers", layers[0].Digest.String())); !os.IsNotExist(statErr) {
		t.Fatalf("layer file present: %v", statErr)
	}
	assertNoTemps(t, data)
}

func TestPullDeclaredSizeIsConfigPlusLayers(t *testing.T) {
	s := newFake(t)
	cfg := testManifest()
	config := s.put(MediaTypeConfig, workerConfig(t, cfg))
	layer := s.put("application/vnd.mftiko.layer.v1.tar", []byte("0123456789"))
	sum := config.Size + layer.Size

	over := layer
	over.Size = layer.Size + 1
	overID := s.publish(ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    config,
		Layers:    []ocispec.Descriptor{over},
	})
	c, _ := s.open(t, "")
	req := baseReq(overID)
	req.MaxBytes = sum
	_, err := c.Pull(context.Background(), req)
	wantReason(t, err, ReasonTooLarge, true)
	if s.count(config.Digest.String()) != 0 || s.count(layer.Digest.String()) != 0 {
		t.Fatalf("oversized manifest fetched config %d layer %d", s.count(config.Digest.String()), s.count(layer.Digest.String()))
	}

	exact := s.publish(ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    config,
		Layers:    []ocispec.Descriptor{layer},
	})
	req = baseReq(exact)
	req.MaxBytes = sum
	if _, err := c.Pull(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	// The limit includes the config: one byte under the sum is too big, and nothing is fetched.
	cfgUnder := cfg
	cfgUnder.Name = "under"
	cfg2 := s.put(MediaTypeConfig, workerConfig(t, cfgUnder))
	layer2 := s.put("application/vnd.mftiko.layer.v1.tar", []byte("zzzzzzzzzz"))
	under := s.publish(ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    cfg2,
		Layers:    []ocispec.Descriptor{layer2},
	})
	req = baseReq(under)
	req.MaxBytes = cfg2.Size + layer2.Size - 1
	_, err = c.Pull(context.Background(), req)
	wantReason(t, err, ReasonTooLarge, true)
	if s.count(cfg2.Digest.String()) != 0 || s.count(layer2.Digest.String()) != 0 {
		t.Fatal("declared sum fetched a blob")
	}

	huge := layer
	huge.Size = math.MaxInt64
	overflow := s.publish(ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    config,
		Layers:    []ocispec.Descriptor{huge},
	})
	_, err = c.Pull(context.Background(), baseReq(overflow))
	wantReason(t, err, ReasonTooLarge, true)
}

func TestPullChecksConfigAndManifest(t *testing.T) {
	good := testManifest()
	layerBody := []byte("layer")

	t.Run("kind", func(t *testing.T) {
		s := newFake(t)
		m := good
		m.Kind = KindMDConn
		id, layers := s.image(t, m, layerBody)
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonWrongKind, true)
		if s.count(layers[0].Digest.String()) != 0 {
			t.Fatal("layer was fetched")
		}
	})
	t.Run("platform", func(t *testing.T) {
		s := newFake(t)
		m := good
		m.Arch = "arm64"
		id, layers := s.image(t, m, layerBody)
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonPlatformMismatch, true)
		if s.count(layers[0].Digest.String()) != 0 {
			t.Fatal("layer was fetched")
		}
	})
	t.Run("empty os and arch follow the host", func(t *testing.T) {
		s := newFake(t)
		m := good
		m.OS, m.Arch = runtime.GOOS, runtime.GOARCH
		id, _ := s.image(t, m, layerBody)
		c, _ := s.open(t, "")
		req := baseReq(id)
		req.OS, req.Arch = "", ""
		if _, err := c.Pull(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("contracts", func(t *testing.T) {
		s := newFake(t)
		id, layers := s.image(t, good, layerBody)
		c, _ := s.open(t, "")
		req := baseReq(id)
		req.Contracts = []string{"worker.v1", "missing.v1"}
		_, err := c.Pull(context.Background(), req)
		wantReason(t, err, ReasonContractsMismatch, true)
		if s.count(layers[0].Digest.String()) != 0 {
			t.Fatal("layer was fetched")
		}
		req.Contracts = []string{"worker.v1", "stsctl.v1"}
		if _, err := c.Pull(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		req.Contracts = []string{"stsctl.v1", "worker.v1"}
		if _, err := c.Pull(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		req.Contracts = nil
		if _, err := c.Pull(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		req.Contracts = []string{"worker.v10"}
		_, err = c.Pull(context.Background(), req)
		wantReason(t, err, ReasonContractsMismatch, true)
	})
	t.Run("schema", func(t *testing.T) {
		s := newFake(t)
		m := good
		m.Schema = "other"
		id, layers := s.image(t, m, layerBody)
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonBadManifest, true)
		if s.count(layers[0].Digest.String()) != 0 {
			t.Fatal("layer was fetched")
		}
	})
	t.Run("config is not json", func(t *testing.T) {
		s := newFake(t)
		config := s.put(MediaTypeConfig, []byte("not-json"))
		layer := s.put("application/vnd.mftiko.layer.v1.tar", layerBody)
		id := s.publish(ocispec.Manifest{
			Versioned: specs.Versioned{SchemaVersion: 2},
			MediaType: ocispec.MediaTypeImageManifest,
			Config:    config,
			Layers:    []ocispec.Descriptor{layer},
		})
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonBadManifest, true)
		if s.count(layer.Digest.String()) != 0 {
			t.Fatal("layer was fetched")
		}
	})
	t.Run("config media type", func(t *testing.T) {
		s := newFake(t)
		config := s.put("application/vnd.oci.image.config.v1+json", workerConfig(t, good))
		layer := s.put("application/vnd.mftiko.layer.v1.tar", layerBody)
		id := s.publish(ocispec.Manifest{
			Versioned: specs.Versioned{SchemaVersion: 2},
			MediaType: ocispec.MediaTypeImageManifest,
			Config:    config,
			Layers:    []ocispec.Descriptor{layer},
		})
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonWrongMediaType, true)
		if s.count(config.Digest.String()) != 0 || s.count(layer.Digest.String()) != 0 {
			t.Fatal("blob was fetched")
		}
	})
	t.Run("schemaVersion", func(t *testing.T) {
		s := newFake(t)
		config := s.put(MediaTypeConfig, workerConfig(t, good))
		layer := s.put("application/vnd.mftiko.layer.v1.tar", layerBody)
		man := ocispec.Manifest{
			Versioned: specs.Versioned{SchemaVersion: 2},
			MediaType: ocispec.MediaTypeImageManifest,
			Config:    config,
			Layers:    []ocispec.Descriptor{layer},
		}
		raw, err := json.Marshal(man)
		if err != nil {
			t.Fatal(err)
		}
		raw = bytes.Replace(raw, []byte(`"schemaVersion":2`), []byte(`"schemaVersion":1`), 1)
		id := s.put(ocispec.MediaTypeImageManifest, raw).Digest.String()
		c, _ := s.open(t, "")
		_, err = c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonBadManifest, true)
		if s.count(config.Digest.String()) != 0 {
			t.Fatal("config was fetched")
		}
	})
	t.Run("oci index", func(t *testing.T) {
		s := newFake(t)
		id, _ := s.image(t, good, layerBody)
		img := s.objects[id]
		var desc ocispec.Descriptor
		desc.MediaType = ocispec.MediaTypeImageManifest
		desc.Digest = digest.Digest(id)
		desc.Size = int64(len(img.body))
		idx := ocispec.Index{
			Versioned: specs.Versioned{SchemaVersion: 2},
			MediaType: ocispec.MediaTypeImageIndex,
			Manifests: []ocispec.Descriptor{desc},
		}
		raw, err := json.Marshal(idx)
		if err != nil {
			t.Fatal(err)
		}
		indexID := s.put(ocispec.MediaTypeImageIndex, raw).Digest.String()
		c, _ := s.open(t, "")
		_, err = c.Pull(context.Background(), baseReq(indexID))
		wantReason(t, err, ReasonNotImage, true)
		if s.count(id) != 0 {
			t.Fatalf("index pull fetched the child manifest %d times", s.count(id))
		}
	})
	t.Run("docker manifest list", func(t *testing.T) {
		s := newFake(t)
		raw := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[]}`)
		id := s.put(dockerMediaTypeManifestList, raw).Digest.String()
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonNotImage, true)
	})
	t.Run("not an image manifest", func(t *testing.T) {
		s := newFake(t)
		config := s.put(MediaTypeConfig, workerConfig(t, good))
		layer := s.put("application/vnd.mftiko.layer.v1.tar", layerBody)
		id := s.publish(ocispec.Manifest{
			Versioned: specs.Versioned{SchemaVersion: 2},
			MediaType: "application/vnd.docker.distribution.manifest.v2+json",
			Config:    config,
			Layers:    []ocispec.Descriptor{layer},
		})
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonNotImage, true)
		if s.count(layer.Digest.String()) != 0 {
			t.Fatal("layer was fetched")
		}
	})
	t.Run("manifest bytes are not the requested digest", func(t *testing.T) {
		s := newFake(t)
		id, layers := s.image(t, good, layerBody)
		s.handlers = map[string]http.HandlerFunc{
			id: func(w http.ResponseWriter, r *http.Request) {
				body := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`)
				w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
				w.Header().Set("Docker-Content-Digest", id)
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(body)
			},
		}
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonDigestMismatch, true)
		if s.count(layers[0].Digest.String()) != 0 {
			t.Fatal("layer was fetched")
		}
	})
	t.Run("layer descriptor is not sha256", func(t *testing.T) {
		s := newFake(t)
		config := s.put(MediaTypeConfig, workerConfig(t, good))
		sum := sha512.Sum512([]byte("layer"))
		layer := ocispec.Descriptor{
			MediaType: "application/vnd.mftiko.layer.v1.tar",
			Digest:    digest.NewDigestFromBytes(digest.SHA512, sum[:]),
			Size:      5,
		}
		id := s.publish(ocispec.Manifest{
			Versioned: specs.Versioned{SchemaVersion: 2},
			MediaType: ocispec.MediaTypeImageManifest,
			Config:    config,
			Layers:    []ocispec.Descriptor{layer},
		})
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonDigestMismatch, true)
		if s.count(layer.Digest.String()) != 0 {
			t.Fatal("layer was fetched")
		}
	})
	t.Run("negative descriptor size", func(t *testing.T) {
		s := newFake(t)
		config := s.put(MediaTypeConfig, workerConfig(t, good))
		layer := s.put("application/vnd.mftiko.layer.v1.tar", layerBody)
		layer.Size = -1
		id := s.publish(ocispec.Manifest{
			Versioned: specs.Versioned{SchemaVersion: 2},
			MediaType: ocispec.MediaTypeImageManifest,
			Config:    config,
			Layers:    []ocispec.Descriptor{layer},
		})
		c, _ := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonBadManifest, true)
	})
	t.Run("empty layer list", func(t *testing.T) {
		s := newFake(t)
		config := s.put(MediaTypeConfig, workerConfig(t, good))
		id := s.publish(ocispec.Manifest{
			Versioned: specs.Versioned{SchemaVersion: 2},
			MediaType: ocispec.MediaTypeImageManifest,
			Config:    config,
			Layers:    []ocispec.Descriptor{},
		})
		c, _ := s.open(t, "")
		got, err := c.Pull(context.Background(), baseReq(id))
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Layers) != 0 {
			t.Fatalf("layers %+v", got.Layers)
		}
	})
	t.Run("first layer stays when the second fails", func(t *testing.T) {
		s := newFake(t)
		id, layers := s.image(t, good, []byte("one"), []byte("two"))
		s.handlers = map[string]http.HandlerFunc{
			layers[1].Digest.String(): func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Docker-Content-Digest", layers[1].Digest.String())
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("zzz"))
			},
		}
		c, data := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonDigestMismatch, true)
		if _, statErr := os.Stat(filepath.Join(data, "layers", layers[0].Digest.String())); statErr != nil {
			t.Fatal(statErr)
		}
		if _, statErr := os.Stat(filepath.Join(data, "layers", layers[1].Digest.String())); !os.IsNotExist(statErr) {
			t.Fatalf("failed layer present: %v", statErr)
		}
		assertNoTemps(t, data)
	})
}

func TestPullRegistryStatuses(t *testing.T) {
	s := newFake(t)
	id, _ := s.image(t, testManifest(), []byte("layer"))
	c, data := s.open(t, "")

	s.status = http.StatusNotFound
	_, err := c.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonNotFound, true)

	s.status = http.StatusForbidden
	_, err = c.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonUnauthorized, true)
	if s.count(id) != 2 { // 404 and 403, one GET each
		t.Fatalf("manifest gets %d", s.count(id))
	}

	s.status = http.StatusInternalServerError
	before := s.count(id)
	_, err = c.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonUnavailable, false)
	if s.count(id) != before+1 {
		t.Fatalf("5xx was retried: gets %d, was %d", s.count(id), before)
	}

	s.status = http.StatusTooManyRequests
	_, err = c.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonUnavailable, false)

	s.status = 0
	delete(s.objects, id)
	_, err = c.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonNotFound, true)
	assertNoTemps(t, data)
}

func TestPullBasicAndBearer(t *testing.T) {
	body := []byte("layer-bytes")

	t.Run("no credentials", func(t *testing.T) {
		s := newFake(t)
		s.authMode = "basic"
		id, layers := s.image(t, testManifest(), body)
		c, data := s.open(t, "")
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonUnauthorized, true)
		if _, statErr := os.Stat(filepath.Join(data, "layers", layers[0].Digest.String())); !os.IsNotExist(statErr) {
			t.Fatal("layer file written")
		}
	})
	t.Run("basic username and password", func(t *testing.T) {
		s := newFake(t)
		s.authMode = "basic"
		id, layers := s.image(t, testManifest(), body)
		creds := writeDockerConfig(t, s.host(), s.user, s.pass, false)
		c, data := s.open(t, creds)
		if _, err := c.Pull(context.Background(), baseReq(id)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(mustRead(t, filepath.Join(data, "layers", layers[0].Digest.String())), body) {
			t.Fatal("layer bytes")
		}
		if s.count(layers[0].Digest.String()) != 1 {
			t.Fatalf("layer gets %d", s.count(layers[0].Digest.String()))
		}
		if !sawPrefix(s.authorizations(), "Basic ") {
			t.Fatalf("authorizations %q", s.authorizations())
		}
	})
	t.Run("basic auth field", func(t *testing.T) {
		s := newFake(t)
		s.authMode = "basic"
		id, _ := s.image(t, testManifest(), body)
		creds := writeDockerConfig(t, s.host(), s.user, s.pass, true)
		c, _ := s.open(t, creds)
		if _, err := c.Pull(context.Background(), baseReq(id)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wrong password", func(t *testing.T) {
		s := newFake(t)
		s.authMode = "basic"
		id, layers := s.image(t, testManifest(), body)
		creds := writeDockerConfig(t, s.host(), s.user, "nope", false)
		c, data := s.open(t, creds)
		_, err := c.Pull(context.Background(), baseReq(id))
		wantReason(t, err, ReasonUnauthorized, true)
		if _, statErr := os.Stat(filepath.Join(data, "layers", layers[0].Digest.String())); !os.IsNotExist(statErr) {
			t.Fatal("layer file written")
		}
	})
	t.Run("bearer token exchange", func(t *testing.T) {
		s := newFake(t)
		s.authMode = "bearer"
		id, layers := s.image(t, testManifest(), body)
		creds := writeDockerConfig(t, s.host(), s.user, s.pass, false)
		c, _ := s.open(t, creds)
		if _, err := c.Pull(context.Background(), baseReq(id)); err != nil {
			t.Fatal(err)
		}
		if s.tokenHits != 1 {
			t.Fatalf("token hits %d", s.tokenHits)
		}
		if s.count(layers[0].Digest.String()) != 1 {
			t.Fatalf("layer gets %d", s.count(layers[0].Digest.String()))
		}
		if !sawPrefix(s.authorizations(), "Bearer "+s.token) {
			t.Fatalf("authorizations %q", s.authorizations())
		}
	})
}

func (s *fakeReg) authorizations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.auths))
	copy(out, s.auths)
	return out
}

func sawPrefix(values []string, prefix string) bool {
	for _, v := range values {
		if strings.HasPrefix(v, prefix) {
			return true
		}
	}
	return false
}

func writeDockerConfig(t *testing.T, host, user, pass string, authField bool) string {
	t.Helper()
	entry := map[string]string{"username": user, "password": pass}
	if authField {
		entry = map[string]string{"auth": base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))}
	}
	raw, err := json.Marshal(map[string]any{"auths": map[string]any{host: entry}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPullUnavailable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	c, _ := openAt(t, Registry{Address: addr, PlainHTTP: true}, t.TempDir())
	_, err = c.Pull(context.Background(), baseReq(digest.FromBytes([]byte("x")).String()))
	wantReason(t, err, ReasonUnavailable, false)

	s := newFake(t)
	id, _ := s.image(t, testManifest(), []byte("layer"))
	https, _ := openAt(t, Registry{Address: s.host(), PlainHTTP: false}, t.TempDir())
	_, err = https.Pull(context.Background(), baseReq(id))
	wantReason(t, err, ReasonUnavailable, false)
}

func TestOpenDropsLeftoverTemps(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "layers")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, ".tmp-leftover")
	keep := filepath.Join(dir, "sha256:"+strings.Repeat("ab", 32))
	if err := os.WriteFile(tmp, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keep, []byte("verified"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := newFake(t)
	if _, err := Open(Registry{Address: s.host(), PlainHTTP: true}, data); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp still there: %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal(err)
	}
}

func TestPullSingleFlight(t *testing.T) {
	s := newFake(t)
	id, layers := s.image(t, testManifest(), []byte("shared-layer"))
	data := t.TempDir()
	reg := Registry{Address: s.host(), PlainHTTP: true}
	c1, _ := openAt(t, reg, data)
	c2, _ := openAt(t, reg, data)

	s.block = layers[0].Digest.String()
	s.release = make(chan struct{})
	s.hits = make(chan struct{}, 4)
	joined := make(chan struct{})
	t.Cleanup(func() {
		layerWait = nil
		select {
		case <-s.release:
		default:
			close(s.release)
		}
	})

	type result struct {
		err error
	}
	first := make(chan result, 1)
	go func() {
		_, err := c1.Pull(context.Background(), baseReq(id))
		first <- result{err}
	}()
	select {
	case <-s.hits:
	case res := <-first:
		t.Fatalf("leader: %v", res.err)
	}

	layerWait = func() { close(joined) }
	second := make(chan result, 1)
	go func() {
		_, err := c2.Pull(context.Background(), baseReq(id))
		second <- result{err}
	}()
	select {
	case <-joined:
	case <-s.hits:
		t.Fatal("second pull downloaded the layer")
	}
	select {
	case <-s.hits:
		t.Fatal("second pull downloaded the layer")
	default:
	}
	close(s.release)
	if err := (<-first).err; err != nil {
		t.Fatal(err)
	}
	if err := (<-second).err; err != nil {
		t.Fatal(err)
	}
	if s.count(layers[0].Digest.String()) != 1 {
		t.Fatalf("layer gets %d", s.count(layers[0].Digest.String()))
	}
}

func TestPullWaiterSurvivesLeaderCancel(t *testing.T) {
	s := newFake(t)
	body := []byte("shared-layer")
	id, layers := s.image(t, testManifest(), body)
	data := t.TempDir()
	reg := Registry{Address: s.host(), PlainHTTP: true}
	leader, _ := openAt(t, reg, data)
	waiter, _ := openAt(t, reg, data)

	s.block = layers[0].Digest.String()
	s.release = make(chan struct{})
	s.hits = make(chan struct{}, 4)
	joined := make(chan struct{})
	t.Cleanup(func() {
		layerWait = nil
		select {
		case <-s.release:
		default:
			close(s.release)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leaderErr := make(chan error, 1)
	go func() {
		_, err := leader.Pull(ctx, baseReq(id))
		leaderErr <- err
	}()
	select {
	case <-s.hits:
	case err := <-leaderErr:
		t.Fatalf("leader: %v", err)
	}

	layerWait = func() { close(joined) }
	waiterErr := make(chan error, 1)
	go func() {
		_, err := waiter.Pull(context.Background(), baseReq(id))
		waiterErr <- err
	}()
	select {
	case <-joined:
	case <-s.hits:
		t.Fatal("waiter downloaded the layer before the leader finished")
	}

	cancel()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader: %v", err)
	}
	// The waiter downloads the layer again, and that GET is held until release.
	close(s.release)
	if err := <-waiterErr; err != nil {
		t.Fatal(err)
	}
	if s.count(layers[0].Digest.String()) != 2 {
		t.Fatalf("layer gets %d, want the leader's cancelled GET and the waiter's", s.count(layers[0].Digest.String()))
	}
	if !bytes.Equal(mustRead(t, filepath.Join(data, "layers", layers[0].Digest.String())), body) {
		t.Fatal("layer was not cached")
	}
}

func TestPullWaiterRejectsDifferentDeclaredSize(t *testing.T) {
	s := newFake(t)
	body := []byte("0123456789")
	cfg := testManifest()
	config := s.put(MediaTypeConfig, workerConfig(t, cfg))
	layer := s.put("application/vnd.mftiko.layer.v1.tar", body)
	honest := s.publish(ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    config,
		Layers:    []ocispec.Descriptor{layer},
	})
	lied := layer
	lied.Size = layer.Size + 1
	liar := s.publish(ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    config,
		Layers:    []ocispec.Descriptor{lied},
	})
	// The liar's declared sum must stay under the limit, or it fails before the flight.
	data := t.TempDir()
	reg := Registry{Address: s.host(), PlainHTTP: true}
	leader, _ := openAt(t, reg, data)
	waiter, _ := openAt(t, reg, data)

	s.block = layer.Digest.String()
	s.release = make(chan struct{})
	s.hits = make(chan struct{}, 2)
	joined := make(chan struct{})
	t.Cleanup(func() {
		layerWait = nil
		select {
		case <-s.release:
		default:
			close(s.release)
		}
	})

	leaderErr := make(chan error, 1)
	go func() {
		_, err := leader.Pull(context.Background(), baseReq(honest))
		leaderErr <- err
	}()
	select {
	case <-s.hits:
	case err := <-leaderErr:
		t.Fatalf("leader: %v", err)
	}
	layerWait = func() { close(joined) }
	waiterErr := make(chan error, 1)
	go func() {
		_, err := waiter.Pull(context.Background(), baseReq(liar))
		waiterErr <- err
	}()
	select {
	case <-joined:
	case <-s.hits:
		t.Fatal("waiter downloaded the layer")
	}
	close(s.release)
	if err := <-leaderErr; err != nil {
		t.Fatal(err)
	}
	wantReason(t, <-waiterErr, ReasonDigestMismatch, true)
	if !bytes.Equal(mustRead(t, filepath.Join(data, "layers", layer.Digest.String())), body) {
		t.Fatal("verified layer was not kept")
	}
}

func TestPullCancelDropsTemp(t *testing.T) {
	s := newFake(t)
	id, layers := s.image(t, testManifest(), bytes.Repeat([]byte("y"), 32))
	s.handlers = map[string]http.HandlerFunc{
		layers[0].Digest.String(): func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Docker-Content-Digest", layers[0].Digest.String())
			w.WriteHeader(http.StatusOK)
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
			<-r.Context().Done()
		},
	}
	c, data := s.open(t, "")
	created := make(chan string, 1)
	layerCreated = func(name string) { created <- name }
	t.Cleanup(func() { layerCreated = nil })

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := c.Pull(ctx, baseReq(id))
		errc <- err
	}()
	name := <-created
	cancel()
	err := <-errc
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if _, statErr := os.Stat(name); !os.IsNotExist(statErr) {
		t.Fatalf("temp %s: %v", name, statErr)
	}
	if _, statErr := os.Stat(filepath.Join(data, "layers", layers[0].Digest.String())); !os.IsNotExist(statErr) {
		t.Fatalf("layer file: %v", statErr)
	}
}

func TestPullBadRequest(t *testing.T) {
	s := newFake(t)
	if _, err := Open(Registry{}, t.TempDir()); err == nil || !errors.Is(err, ErrRequest) {
		wantRequest(t, err)
	}
	for _, addr := range []string{"http://127.0.0.1:1", "127.0.0.1:1/repo", "not a host"} {
		if _, err := Open(Registry{Address: addr, PlainHTTP: true}, t.TempDir()); !errors.Is(err, ErrRequest) {
			t.Fatalf("address %q: %v", addr, err)
		}
	}
	if _, err := Open(Registry{Address: s.host(), PlainHTTP: true}, ""); !errors.Is(err, ErrRequest) {
		t.Fatal(err)
	}
	c, _ := s.open(t, "")
	id, _ := s.image(t, testManifest(), []byte("x"))
	req := baseReq(id)
	req.Repository = ""
	_, err := c.Pull(context.Background(), req)
	wantRequest(t, err)
	if s.count(id) != 0 {
		t.Fatal("empty repository contacted the registry")
	}
	req = baseReq(id)
	req.Kind = ""
	_, err = c.Pull(context.Background(), req)
	wantRequest(t, err)

	req = baseReq("sha256:abcd")
	_, err = c.Pull(context.Background(), req)
	wantReason(t, err, ReasonDigestMismatch, true)
	sum := sha512.Sum512([]byte("x"))
	req.Digest = digest.NewDigestFromBytes(digest.SHA512, sum[:]).String()
	_, err = c.Pull(context.Background(), req)
	wantReason(t, err, ReasonDigestMismatch, true)

	req = baseReq(id)
	req.MaxBytes = -1
	_, err = c.Pull(context.Background(), req)
	wantRequest(t, err)
	if s.count(id) != 0 {
		t.Fatal("bad request contacted the registry")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.Pull(ctx, baseReq(id))
	if !errors.Is(err, context.Canceled) || ReasonOf(err) != "" {
		t.Fatalf("got %v", err)
	}
}
