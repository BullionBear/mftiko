package agent

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// memRegistry is the distribution subset oras-go uses to push and pull one image.
// It is the in-process registry from the artifact tests (D965); this package cannot
// import that unexported type.
type memRegistry struct {
	srv           *httptest.Server
	mu            sync.Mutex
	blobs         map[string][]byte
	manifests     map[string][]byte
	user, pass    string
	failManifests atomic.Int32 // GET of a manifest returns 503 this many times
}

func newMemRegistry() *memRegistry {
	r := &memRegistry{blobs: map[string][]byte{}, manifests: map[string][]byte{}}
	r.srv = httptest.NewServer(r)
	return r
}

func (r *memRegistry) Close() { r.srv.Close() }

func (r *memRegistry) addr() string { return strings.TrimPrefix(r.srv.URL, "http://") }

func (r *memRegistry) repo() string { return r.addr() + "/hello" }

func (r *memRegistry) basic() string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(r.user+":"+r.pass))
}

func (r *memRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if r.user != "" && req.Header.Get("Authorization") != r.basic() {
		w.Header().Set("WWW-Authenticate", `Basic realm="registry"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	rest := strings.TrimPrefix(req.URL.Path, "/v2/")
	if req.Method == http.MethodGet && strings.Contains(rest, "/manifests/") && r.failManifests.Load() > 0 {
		r.failManifests.Add(-1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	switch {
	case strings.Contains(rest, "/blobs/uploads/"):
		r.blobUpload(w, req, rest)
	case strings.Contains(rest, "/blobs/"):
		r.blob(w, req, rest)
	case strings.Contains(rest, "/manifests/"):
		r.manifest(w, req, rest)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (r *memRegistry) blobUpload(w http.ResponseWriter, req *http.Request, rest string) {
	i := strings.Index(rest, "/blobs/uploads/")
	repo := rest[:i]
	leaf := rest[i+len("/blobs/uploads/"):]
	if req.Method == http.MethodPost && leaf == "" {
		w.Header().Set("Location", "/v2/"+repo+"/blobs/uploads/1")
		w.Header().Set("Docker-Upload-UUID", "1")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if req.Method != http.MethodPut {
		http.Error(w, "bad upload", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sum := digest.FromBytes(body).String()
	if got := req.URL.Query().Get("digest"); got != sum {
		http.Error(w, "digest", http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.blobs[sum] = body
	r.mu.Unlock()
	w.Header().Set("Docker-Content-Digest", sum)
	w.Header().Set("Location", "/v2/"+repo+"/blobs/"+sum)
	w.WriteHeader(http.StatusCreated)
}

func (r *memRegistry) blob(w http.ResponseWriter, req *http.Request, rest string) {
	leaf := rest[strings.Index(rest, "/blobs/")+len("/blobs/"):]
	r.mu.Lock()
	body, ok := r.blobs[leaf]
	r.mu.Unlock()
	if !ok || req.Method != http.MethodGet {
		http.NotFound(w, req)
		return
	}
	writeRegBody(w, "application/octet-stream", leaf, body)
}

func (r *memRegistry) manifest(w http.ResponseWriter, req *http.Request, rest string) {
	leaf := rest[strings.Index(rest, "/manifests/")+len("/manifests/"):]
	if req.Method == http.MethodPut {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sum := digest.FromBytes(body).String()
		r.mu.Lock()
		r.manifests[leaf] = body
		r.manifests[sum] = body
		r.mu.Unlock()
		w.Header().Set("Docker-Content-Digest", sum)
		w.Header().Set("Location", req.URL.Path)
		w.WriteHeader(http.StatusCreated)
		return
	}
	r.mu.Lock()
	body, ok := r.manifests[leaf]
	r.mu.Unlock()
	if !ok || req.Method != http.MethodGet {
		http.NotFound(w, req)
		return
	}
	writeRegBody(w, ocispec.MediaTypeImageManifest, digest.FromBytes(body).String(), body)
}

func writeRegBody(w http.ResponseWriter, mediaType, sum string, body []byte) {
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Docker-Content-Digest", sum)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
