// Package artifacttest is an in-process OCI registry for tests of the agents that pull worker artifacts. It is
// imported by tests only.
package artifacttest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

// Registry is the distribution subset oras-go uses to push, resolve and pull images, served over plain HTTP.
type Registry struct {
	srv       *httptest.Server
	mu        sync.Mutex
	blobs     map[string][]byte
	manifests map[string][]byte
	down      atomic.Bool
}

// New starts a registry that stops when the test ends.
func New(t testing.TB) *Registry {
	r := &Registry{blobs: map[string][]byte{}, manifests: map[string][]byte{}}
	r.srv = httptest.NewServer(r)
	t.Cleanup(r.srv.Close)
	return r
}

// Addr is host:port, for artifact.Registry.Address with PlainHTTP.
func (r *Registry) Addr() string { return strings.TrimPrefix(r.srv.URL, "http://") }

// SetDown makes every request fail with 503 while v is true, the way an unavailable registry does.
func (r *Registry) SetDown(v bool) { r.down.Store(v) }

func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if r.down.Load() {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	rest := strings.TrimPrefix(req.URL.Path, "/v2/")
	switch {
	case strings.Contains(rest, "/blobs/uploads/"):
		r.upload(w, req, rest)
	case strings.Contains(rest, "/blobs/"):
		r.get(w, req, r.blobs, rest[strings.Index(rest, "/blobs/")+len("/blobs/"):], "application/octet-stream")
	case strings.Contains(rest, "/manifests/"):
		// Key by repository. A tag in one repository is not a tag in another.
		i := strings.Index(rest, "/manifests/")
		repo, leaf := rest[:i], rest[i+len("/manifests/"):]
		if req.Method == http.MethodPut {
			body, _ := io.ReadAll(req.Body)
			sum := digest.FromBytes(body).String()
			r.mu.Lock()
			r.manifests[repo+"\x00"+leaf] = body
			r.manifests[repo+"\x00"+sum] = body
			r.mu.Unlock()
			w.Header().Set("Docker-Content-Digest", sum)
			w.WriteHeader(http.StatusCreated)
			return
		}
		r.get(w, req, r.manifests, repo+"\x00"+leaf, ocispec.MediaTypeImageManifest)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (r *Registry) upload(w http.ResponseWriter, req *http.Request, rest string) {
	repo := rest[:strings.Index(rest, "/blobs/uploads/")]
	if req.Method == http.MethodPost {
		w.Header().Set("Location", "/v2/"+repo+"/blobs/uploads/1")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	body, _ := io.ReadAll(req.Body)
	sum := digest.FromBytes(body).String()
	r.mu.Lock()
	r.blobs[sum] = body
	r.mu.Unlock()
	w.Header().Set("Docker-Content-Digest", sum)
	w.WriteHeader(http.StatusCreated)
}

func (r *Registry) get(w http.ResponseWriter, req *http.Request, m map[string][]byte, key, mediaType string) {
	r.mu.Lock()
	body, ok := m[key]
	r.mu.Unlock()
	if !ok || (req.Method != http.MethodGet && req.Method != http.MethodHead) {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if req.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

// Push packs binary as a worker of kind and pushes it to repository under tag. It returns the image digest.
func (r *Registry) Push(t testing.TB, repository, tag, kind string, binary []byte, env map[string]string) string {
	t.Helper()
	img, err := artifact.PackWorker(binary, kind, path.Base(repository), env)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := artifact.Push(ctx, img, artifact.Destination{Repository: r.Addr() + "/" + repository, Tag: tag, PlainHTTP: true}); err != nil {
		t.Fatal(err)
	}
	return img.Digest
}
