package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"
)

func TestUsage(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"-bin", "b"},
		{"-bin", "b", "-name", "n"},
		{"-bin", "b", "-name", "n", "-repo", "127.0.0.1:5000/hello", "extra"},
		{"-h"},
	} {
		if err := run(args, io.Discard); err == nil || !strings.Contains(err.Error(), "usage:") {
			t.Errorf("%q: %v", args, err)
		}
	}
	var out bytes.Buffer
	if err := run([]string{"-version"}, &out); err != nil || !strings.Contains(out.String(), "commit") {
		t.Fatalf("version: %v %q", err, out.String())
	}
	if err := run([]string{"-bin", "b", "-name", "n", "-repo", "r/n", "-env", "NOEQUALS"}, io.Discard); err == nil {
		t.Fatal("bad env")
	}
	if err := run([]string{"-env", "A=1", "-env", "A=2", "-bin", "b", "-name", "n", "-repo", "r/r"}, io.Discard); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatal(err)
	}
}

func TestPackHello(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("hello is packed from an ELF binary")
	}
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	bin := helloBinary(t)
	reg := newCLIRegistry()
	defer reg.Close()

	args := []string{"-bin", bin, "-name", "hello", "-repo", reg.repo(), "-plain-http", "-env", "B=2", "-env", "A=1"}
	var first bytes.Buffer
	if err := run(args, &first); err != nil {
		t.Fatal(err)
	}
	var second bytes.Buffer
	swapped := []string{"-bin", bin, "-name", "hello", "-repo", reg.repo(), "-plain-http", "-tag", "dev", "-env", "A=1", "-env", "B=2"}
	if err := run(swapped, &second); err != nil {
		t.Fatal(err)
	}
	d1 := strings.TrimSpace(first.String())
	d2 := strings.TrimSpace(second.String())
	if first.String() != d1+"\n" || d1 != d2 || !strings.HasPrefix(d1, "sha256:") {
		t.Fatalf("stdout %q then %q", first.String(), second.String())
	}

	raw, err := os.ReadFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	img, err := artifact.PackStrategy(raw, "hello", map[string]string{"A": "1", "B": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if img.Digest != d1 {
		t.Fatalf("cli digest %s, pack %s", d1, img.Digest)
	}
	for _, ref := range []string{d1, "dev"} {
		body := cliFetch(t, reg.repo(), ref)
		if !bytes.Equal(body, img.Manifest) {
			t.Fatalf("pull %s is not the manifest we packed", ref)
		}
	}
}

func TestPackRejectsBeforePush(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("hello is packed from an ELF binary")
	}
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newCLIRegistry()
	defer reg.Close()
	bin := helloBinary(t)
	err := run([]string{"-bin", bin, "-name", "hello", "-repo", reg.repo(), "-plain-http", "-env", "MFTIKO_X=1"}, io.Discard)
	if !errors.Is(err, artifact.ErrBadManifest) {
		t.Fatalf("platform env: %v", err)
	}
	notELF := filepath.Join(t.TempDir(), "notelf")
	if err := os.WriteFile(notELF, []byte("not elf"), 0o755); err != nil {
		t.Fatal(err)
	}
	err = run([]string{"-bin", notELF, "-name", "hello", "-repo", reg.repo(), "-plain-http"}, io.Discard)
	if !errors.Is(err, artifact.ErrPlatform) {
		t.Fatalf("not a strategy: %v", err)
	}
	if reg.hits() != 0 {
		t.Fatalf("refused pack still contacted the registry, %d requests", reg.hits())
	}
}

var (
	helloOnce sync.Once
	helloPath string
	helloErr  error
)

func helloBinary(t *testing.T) string {
	t.Helper()
	helloOnce.Do(func() {
		dir, err := os.MkdirTemp("", "mftiko-hello")
		if err != nil {
			helloErr = err
			return
		}
		helloPath = filepath.Join(dir, "hello")
		cmd := exec.Command("go", "build", "-o", helloPath, "github.com/BullionBear/mftiko/examples/strategies/hello")
		_, file, _, ok := runtime.Caller(0)
		if !ok {
			helloErr = errors.New("no caller")
			return
		}
		cmd.Dir = filepath.Join(filepath.Dir(file), "..", "..")
		out, err := cmd.CombinedOutput()
		if err != nil {
			helloErr = fmt.Errorf("%w\n%s", err, out)
		}
	})
	if helloErr != nil {
		t.Fatal(helloErr)
	}
	return helloPath
}

func cliFetch(t *testing.T, repo, ref string) []byte {
	t.Helper()
	r, err := remote.NewRepository(repo)
	if err != nil {
		t.Fatal(err)
	}
	r.PlainHTTP = true
	r.Client = http.DefaultClient
	_, rc, err := r.FetchReference(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// cliRegistry is the distribution subset one mftiko-pack push speaks.
type cliRegistry struct {
	srv       *httptest.Server
	mu        sync.Mutex
	blobs     map[string][]byte
	manifests map[string][]byte
	n         int
}

func newCLIRegistry() *cliRegistry {
	r := &cliRegistry{blobs: map[string][]byte{}, manifests: map[string][]byte{}}
	r.srv = httptest.NewServer(r)
	return r
}

func (r *cliRegistry) Close() { r.srv.Close() }

func (r *cliRegistry) repo() string {
	return strings.TrimPrefix(r.srv.URL, "http://") + "/hello"
}

func (r *cliRegistry) hits() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

func (r *cliRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.n++
	r.mu.Unlock()
	rest := strings.TrimPrefix(req.URL.Path, "/v2/")
	switch {
	case strings.Contains(rest, "/blobs/uploads/"):
		r.upload(w, req, rest)
	case strings.Contains(rest, "/blobs/"):
		r.getBlob(w, req, rest)
	case strings.Contains(rest, "/manifests/"):
		r.manifest(w, req, rest)
	default:
		http.NotFound(w, req)
	}
}

func (r *cliRegistry) upload(w http.ResponseWriter, req *http.Request, rest string) {
	i := strings.Index(rest, "/blobs/uploads/")
	repo, leaf := rest[:i], rest[i+len("/blobs/uploads/"):]
	if req.Method == http.MethodPost && leaf == "" {
		w.Header().Set("Location", "/v2/"+repo+"/blobs/uploads/1")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sum := digest.FromBytes(body).String()
	if req.URL.Query().Get("digest") != sum {
		http.Error(w, "digest", http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.blobs[sum] = body
	r.mu.Unlock()
	w.Header().Set("Docker-Content-Digest", sum)
	w.WriteHeader(http.StatusCreated)
}

func (r *cliRegistry) getBlob(w http.ResponseWriter, req *http.Request, rest string) {
	leaf := rest[strings.Index(rest, "/blobs/")+len("/blobs/"):]
	r.mu.Lock()
	body, ok := r.blobs[leaf]
	r.mu.Unlock()
	if !ok {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", leaf)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (r *cliRegistry) manifest(w http.ResponseWriter, req *http.Request, rest string) {
	leaf := rest[strings.Index(rest, "/manifests/")+len("/manifests/"):]
	if req.Method == http.MethodPut {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sum := digest.FromBytes(body).String()
		r.mu.Lock()
		r.manifests[leaf] = append([]byte(nil), body...)
		r.manifests[sum] = r.manifests[leaf]
		r.mu.Unlock()
		w.Header().Set("Docker-Content-Digest", sum)
		w.WriteHeader(http.StatusCreated)
		return
	}
	r.mu.Lock()
	body, ok := r.manifests[leaf]
	r.mu.Unlock()
	if !ok {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
	w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
