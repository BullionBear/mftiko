package artifact

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// memRegistry is the distribution subset oras-go uses to push and pull one image.
// tagPageSize, when set, pages GET /v2/<name>/tags/list. status, when set, is the status of every
// request. Both stay zero for the pull and push tests.
type memRegistry struct {
	srv         *httptest.Server
	mu          sync.Mutex
	blobs       map[string][]byte
	manifests   map[string][]byte
	tags        map[string]map[string]struct{}
	repos       map[string]struct{}
	hits        int
	blobPuts    int // completed blob uploads
	user        string
	pass        string
	tagPageSize int
	status      int
}

func newMemRegistry() *memRegistry {
	r := &memRegistry{
		blobs:     map[string][]byte{},
		manifests: map[string][]byte{},
		tags:      map[string]map[string]struct{}{},
		repos:     map[string]struct{}{},
	}
	r.srv = httptest.NewServer(r)
	return r
}

func (r *memRegistry) Close() { r.srv.Close() }

func (r *memRegistry) address() string {
	return strings.TrimPrefix(r.srv.URL, "http://")
}

func (r *memRegistry) repo() string {
	return r.address() + "/hello"
}

// store keeps a manifest the way a PUT would, without an HTTP request.
func (r *memRegistry) store(repo, ref string, body []byte) {
	sum := digest.FromBytes(body).String()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manifests[ref] = body
	r.manifests[sum] = body
	r.repos[repo] = struct{}{}
	if !strings.HasPrefix(ref, "sha256:") {
		if r.tags[repo] == nil {
			r.tags[repo] = map[string]struct{}{}
		}
		r.tags[repo][ref] = struct{}{}
	}
}

func (r *memRegistry) basic() string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(r.user+":"+r.pass))
}

func (r *memRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.hits++
	status := r.status
	r.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	if r.user != "" && req.Header.Get("Authorization") != r.basic() {
		w.Header().Set("WWW-Authenticate", `Basic realm="registry"`)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	rest := strings.TrimPrefix(req.URL.Path, "/v2/")
	switch {
	case strings.Contains(rest, "/blobs/uploads/"):
		r.blobUpload(w, req, rest)
	case strings.Contains(rest, "/blobs/"):
		r.blob(w, req, rest)
	case strings.Contains(rest, "/manifests/"):
		r.manifest(w, req, rest)
	case strings.HasSuffix(rest, "/tags/list"):
		r.tagList(w, req, rest)
	default:
		http.NotFound(w, req)
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
	r.blobPuts++
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
	if !ok || (req.Method != http.MethodGet && req.Method != http.MethodHead) {
		http.NotFound(w, req)
		return
	}
	writeBody(w, "application/octet-stream", leaf, body)
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
		repo := rest[:strings.Index(rest, "/manifests/")]
		r.mu.Lock()
		r.manifests[leaf] = body
		r.manifests[sum] = body
		r.repos[repo] = struct{}{}
		if !strings.HasPrefix(leaf, "sha256:") {
			if r.tags[repo] == nil {
				r.tags[repo] = map[string]struct{}{}
			}
			r.tags[repo][leaf] = struct{}{}
		}
		r.mu.Unlock()
		w.Header().Set("Docker-Content-Digest", sum)
		w.Header().Set("Location", req.URL.Path)
		w.WriteHeader(http.StatusCreated)
		return
	}
	r.mu.Lock()
	body, ok := r.manifests[leaf]
	r.mu.Unlock()
	if !ok || (req.Method != http.MethodGet && req.Method != http.MethodHead) {
		http.NotFound(w, req)
		return
	}
	// Resolve is a HEAD. The headers are what it checks: media type, size and digest.
	writeBody(w, ocispec.MediaTypeImageManifest, digest.FromBytes(body).String(), body)
}

func (r *memRegistry) tagList(w http.ResponseWriter, req *http.Request, rest string) {
	repo := strings.TrimSuffix(rest, "/tags/list")
	r.mu.Lock()
	_, known := r.repos[repo]
	names := make([]string, 0, len(r.tags[repo]))
	for tag := range r.tags[repo] {
		names = append(names, tag)
	}
	pageSize := r.tagPageSize
	r.mu.Unlock()
	if !known || req.Method != http.MethodGet {
		http.NotFound(w, req)
		return
	}
	sort.Strings(names)
	n := len(names)
	if pageSize > 0 {
		n = pageSize
	}
	if q := req.URL.Query().Get("n"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 {
			n = v
		}
	}
	start := 0
	if last := req.URL.Query().Get("last"); last != "" {
		start = sort.Search(len(names), func(i int) bool { return names[i] > last })
	}
	if start > len(names) {
		start = len(names)
	}
	end := start + n
	if end > len(names) {
		end = len(names)
	}
	page := names[start:end]
	if page == nil {
		page = []string{}
	}
	if end < len(names) && len(page) > 0 {
		q := url.Values{}
		q.Set("n", strconv.Itoa(n))
		q.Set("last", page[len(page)-1])
		w.Header().Set("Link", fmt.Sprintf(`</v2/%s/tags/list?%s>; rel="next"`, repo, q.Encode()))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}{Name: repo, Tags: page})
}

func writeBody(w http.ResponseWriter, mediaType, sum string, body []byte) {
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Docker-Content-Digest", sum)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
