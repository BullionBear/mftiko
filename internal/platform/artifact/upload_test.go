package artifact

import (
	"archive/tar"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
)

// uploadTree is a model directory: a nested file, an executable, and a relative symlink unpack accepts.
func uploadTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "weights"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "weights", "model.pt"), []byte("tensor bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "prep.sh"), []byte("echo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("weights/model.pt", filepath.Join(root, "latest.pt")); err != nil {
		t.Fatal(err)
	}
	return root
}

func uploadTo(t *testing.T, reg *memRegistry, root, tag string) DataUpload {
	t.Helper()
	return DataUpload{
		Root:        root,
		Destination: Destination{Repository: reg.address() + "/data/model", Tag: tag, PlainHTTP: true},
		TempDir:     t.TempDir(),
	}
}

func TestUploadDataRoundTrip(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	defer reg.Close()
	root := uploadTree(t)
	ctx := context.Background()
	first, err := UploadData(ctx, uploadTo(t, reg, root, ""))
	if err != nil {
		t.Fatal(err)
	}
	// prep.sh, latest.pt, weights, weights/model.pt.
	if first.Entries != 4 || first.Unpacked != int64(len("tensor bytes")+len("echo")) || first.Size <= 0 {
		t.Fatalf("first %+v", first)
	}
	old := time.Unix(1_000_000, 0)
	if err := os.Chtimes(filepath.Join(root, "weights", "model.pt"), old, old); err != nil {
		t.Fatal(err)
	}
	second, err := UploadData(ctx, uploadTo(t, reg, root, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	if second.Digest != first.Digest {
		t.Fatalf("mtime or tag changed the digest: %s, %s", first.Digest, second.Digest)
	}
	manifest, config, _ := fetch(t, reg.address()+"/data/model", "v1")
	if string(config) != `{"schema":"mftiko.data.v1"}` {
		t.Fatalf("config %s", config)
	}
	if got := digestOf(manifest); got != first.Digest {
		t.Fatalf("tag v1 names %s, want %s", got, first.Digest)
	}

	c, err := Open(Registry{PlainHTTPHosts: []string{reg.address()}}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.PullData(ctx, DataRequest{Repository: reg.address() + "/data/model", Digest: first.Digest})
	if err != nil {
		t.Fatal(err)
	}
	if got.Config.Source != nil {
		t.Fatalf("source %+v", got.Config.Source)
	}
	dest := filepath.Join(t.TempDir(), "model")
	if err := UnpackReadOnly(ctx, []string{got.Layer.Path}, dest, UnpackLimits{}); err != nil {
		t.Fatal(err)
	}
	// The tree is 0555/0444; a non-root TempDir cleanup cannot remove it (E09-D075).
	t.Cleanup(func() { _ = RemoveWritable(dest) })
	if string(mustRead(t, filepath.Join(dest, "latest.pt"))) != "tensor bytes" {
		t.Fatal("symlink does not reach the file")
	}
	if link, err := os.Readlink(filepath.Join(dest, "latest.pt")); err != nil || link != "weights/model.pt" {
		t.Fatalf("latest.pt link %q err %v", link, err)
	}
	// Unpacked inputs are 0444 (E09-D075); the layer keeps the execute bit (E09-D080).
	_, _, layer := fetch(t, reg.address()+"/data/model", first.Digest)
	members := readDataLayerBytes(t, layer)
	if len(members) != 4 || members[1].name != "prep.sh" || members[1].mode&0o777 != 0o755 {
		t.Fatalf("members %+v", members)
	}
}

func TestUploadDataSkipsBlobsTheRegistryHas(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	defer reg.Close()
	root := uploadTree(t)
	if _, err := UploadData(context.Background(), uploadTo(t, reg, root, "")); err != nil {
		t.Fatal(err)
	}
	reg.mu.Lock()
	after := reg.blobPuts
	reg.mu.Unlock()
	if after != 2 {
		t.Fatalf("first upload sent %d blobs, want config and layer", after)
	}
	if _, err := UploadData(context.Background(), uploadTo(t, reg, root, "again")); err != nil {
		t.Fatal(err)
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.blobPuts != after {
		t.Fatalf("second upload sent %d more blobs", reg.blobPuts-after)
	}
}

func TestUploadDataRejectsWhatUnpackRejects(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	defer reg.Close()
	root := uploadTree(t)
	outside := filepath.Join(t.TempDir(), "big.pt")
	if err := os.WriteFile(outside, []byte("elsewhere"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "abs.pt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../weights/model.pt", filepath.Join(root, "weights", "up.pt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".wh.gone"), []byte("w"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := UploadData(context.Background(), uploadTo(t, reg, root, "v1"))
	var unsupported *UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("err %v", err)
	}
	want := []string{".wh.gone", "abs.pt", "weights/up.pt"}
	if len(unsupported.Entries) != len(want) {
		t.Fatalf("entries %+v", unsupported.Entries)
	}
	for i, path := range want {
		if unsupported.Entries[i].Path != path || !strings.Contains(err.Error(), path) {
			t.Fatalf("entry %d %+v, want %s", i, unsupported.Entries[i], path)
		}
	}
	if !unsupported.Symlinks() {
		t.Fatal("two symlinks are listed")
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.blobPuts != 0 || len(reg.manifests) != 0 {
		t.Fatalf("pushed %d blobs and %d manifests", reg.blobPuts, len(reg.manifests))
	}
}

// The HuggingFace hub cache links each snapshot file to a blob two directories up.
func TestUploadDataFollowsSymlinks(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	defer reg.Close()
	cache := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cache, "blobs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(cache, "snapshots", "r1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cache, "blobs", "abc"), []byte("safetensors"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../blobs/abc", filepath.Join(cache, "snapshots", "r1", "model.safetensors")); err != nil {
		t.Fatal(err)
	}
	shared := t.TempDir()
	if err := os.WriteFile(filepath.Join(shared, "vocab.txt"), []byte("a b c"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(shared, filepath.Join(cache, "snapshots", "r1", "tok")); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(cache, "snapshots", "r1")
	ctx := context.Background()
	if _, err := UploadData(ctx, uploadTo(t, reg, snapshot, "")); err == nil {
		t.Fatal("the snapshot's symlinks were uploaded without -L")
	}
	req := uploadTo(t, reg, snapshot, "")
	req.FollowSymlinks = true
	up, err := UploadData(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if up.Entries != 3 {
		t.Fatalf("entries %d, want model.safetensors, tok, tok/vocab.txt", up.Entries)
	}
	_, _, layer := fetch(t, reg.address()+"/data/model", up.Digest)
	members := readDataLayerBytes(t, layer)
	if len(members) != 3 || members[0].name != "model.safetensors" || members[0].typ != tar.TypeReg ||
		string(members[0].body) != "safetensors" {
		t.Fatalf("members %+v", members)
	}
	if members[1].name != "tok" || members[1].typ != tar.TypeDir || members[2].name != "tok/vocab.txt" {
		t.Fatalf("members %+v", members)
	}
}

func TestUploadDataFollowRejectsLoopsAndDangling(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	defer reg.Close()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..", filepath.Join(root, "sub", "loop")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	req := uploadTo(t, reg, root, "")
	req.FollowSymlinks = true
	_, err := UploadData(context.Background(), req)
	var unsupported *UnsupportedError
	if !errors.As(err, &unsupported) || len(unsupported.Entries) != 2 {
		t.Fatalf("err %v", err)
	}
	if e := unsupported.Entries[0]; e.Path != "dangling" || !strings.Contains(e.Reason, "does not exist") {
		t.Fatalf("dangling %+v", e)
	}
	if e := unsupported.Entries[1]; e.Path != "sub/loop" || !strings.Contains(e.Reason, "loop") {
		t.Fatalf("loop %+v", e)
	}
}

func TestUploadDataLimits(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	defer reg.Close()
	root := uploadTree(t)
	ctx := context.Background()
	ok, err := UploadData(ctx, uploadTo(t, reg, root, ""))
	if err != nil {
		t.Fatal(err)
	}
	_, config, _ := fetch(t, reg.address()+"/data/model", ok.Digest)
	compressed := int64(len(config)) + ok.Size
	exact := uploadTo(t, reg, root, "")
	exact.Limits = UnpackLimits{MaxFiles: ok.Entries, MaxBytes: ok.Unpacked}
	exact.MaxBytes = compressed
	if _, err := UploadData(ctx, exact); err != nil {
		t.Fatalf("limits equal to the tree: %v", err)
	}

	reg2 := newMemRegistry()
	defer reg2.Close()
	for name, mutate := range map[string]func(*DataUpload){
		"files":      func(r *DataUpload) { r.Limits.MaxFiles = ok.Entries - 1 },
		"unpacked":   func(r *DataUpload) { r.Limits.MaxBytes = ok.Unpacked - 1 },
		"compressed": func(r *DataUpload) { r.MaxBytes = compressed - 1 },
	} {
		req := uploadTo(t, reg2, root, "")
		mutate(&req)
		if _, err := UploadData(ctx, req); !errors.Is(err, ErrUploadLimit) {
			t.Fatalf("%s: err %v", name, err)
		}
	}
	reg2.mu.Lock()
	defer reg2.mu.Unlock()
	if reg2.blobPuts != 0 || len(reg2.manifests) != 0 {
		t.Fatalf("over the limit pushed %d blobs and %d manifests", reg2.blobPuts, len(reg2.manifests))
	}
}

func TestUploadDataRequest(t *testing.T) {
	ctx := context.Background()
	if _, err := UploadData(ctx, DataUpload{Destination: Destination{Repository: "r.example/x"}}); !errors.Is(err, ErrRequest) {
		t.Fatalf("no root: %v", err)
	}
	if _, err := UploadData(ctx, DataUpload{Root: t.TempDir(), Destination: Destination{Repository: "x"}}); !errors.Is(err, ErrRequest) {
		t.Fatalf("no host: %v", err)
	}
	file := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := UploadData(ctx, DataUpload{Root: file, Destination: Destination{Repository: "r.example/x"}}); err == nil {
		t.Fatal("a file root was accepted")
	}
}

func digestOf(b []byte) string { return digest.FromBytes(b).String() }

func readDataLayerBytes(t *testing.T, layer []byte) []tarMember {
	t.Helper()
	path := filepath.Join(t.TempDir(), "layer.tar.gz")
	if err := os.WriteFile(path, layer, 0o644); err != nil {
		t.Fatal(err)
	}
	return readDataLayer(t, path)
}
