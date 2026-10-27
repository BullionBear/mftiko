package artifact

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// registry:2 is the CI service (E10-D061). MFTIKO_REGISTRY names another one; without either the
// test skips, unless MFTIKO_REQUIRE_REGISTRY is set.
func registry2(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("MFTIKO_REGISTRY")
	if addr == "" {
		addr = "127.0.0.1:5000"
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/v2/")
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return addr
		}
	}
	if os.Getenv("MFTIKO_REQUIRE_REGISTRY") != "" {
		t.Fatalf("MFTIKO_REQUIRE_REGISTRY is set and no registry answers on %s: %v", addr, err)
	}
	t.Skipf("no registry on %s; set MFTIKO_REGISTRY", addr)
	return ""
}

func TestUploadDataRegistry2(t *testing.T) {
	addr := registry2(t)
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	root := uploadTree(t)
	repo := addr + "/mftiko-test/upload"
	ctx := context.Background()
	up, err := UploadData(ctx, DataUpload{
		Root: root, Destination: Destination{Repository: repo, Tag: "e15", PlainHTTP: true}, TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	again, err := UploadData(ctx, DataUpload{
		Root: root, Destination: Destination{Repository: repo, PlainHTTP: true}, TempDir: t.TempDir(),
	})
	if err != nil || again.Digest != up.Digest {
		t.Fatalf("again %+v err %v, first %s", again, err, up.Digest)
	}
	c, err := Open(Registry{PlainHTTPHosts: []string{addr}}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.PullData(ctx, DataRequest{Repository: repo, Digest: up.Digest})
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
	if string(mustRead(t, filepath.Join(dest, "weights", "model.pt"))) != "tensor bytes" {
		t.Fatal("unpacked body")
	}
	cat, err := OpenCatalog(Registry{PlainHTTPHosts: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	info, err := cat.ReadData(ctx, repo, up.Digest)
	if err != nil || info.Digest != up.Digest {
		t.Fatalf("catalog %+v err %v", info, err)
	}
}
