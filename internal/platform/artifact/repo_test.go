package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/opencontainers/go-digest"
)

func TestUnpackImageFromRecordsRepository(t *testing.T) {
	data := t.TempDir()
	c := testClient(t, data)
	body := gzipTar(t, reg("bin/worker", 0o755, "exe"))
	layer := cacheLayer(t, data, body)
	image := digest.FromString("recorded").String()
	ctx := context.Background()

	root, err := c.UnpackImageFrom(ctx, image, "hello/app", []Layer{layer}, UnpackLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if string(mustRead(t, filepath.Join(data, "artifacts", image, repositoryName))) != "hello/app\n" {
		t.Fatal("repository record")
	}
	// A rootfs that is already complete is reused. A later unpack does not overwrite the record.
	again, err := c.UnpackImageFrom(ctx, image, "other/app", []Layer{layer}, UnpackLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if again != root || string(mustRead(t, filepath.Join(data, "artifacts", image, repositoryName))) != "hello/app\n" {
		t.Fatal("existing record was rewritten")
	}

	plain := digest.FromString("plain").String()
	if _, err := c.UnpackImage(ctx, plain, []Layer{layer}, UnpackLimits{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(data, "artifacts", plain, repositoryName)); !os.IsNotExist(err) {
		t.Fatalf("UnpackImage wrote a repository: %v", err)
	}
	reused, err := c.UnpackImageFrom(ctx, plain, "hello/app", []Layer{layer}, UnpackLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(reused), repositoryName)); !os.IsNotExist(err) {
		t.Fatalf("reuse wrote a repository: %v", err)
	}
}

func TestUnpackImageFromRejectsRepository(t *testing.T) {
	data := t.TempDir()
	c := testClient(t, data)
	body := gzipTar(t, reg("bin/worker", 0o755, "exe"))
	layer := cacheLayer(t, data, body)
	image := digest.FromString("bad-repo").String()
	for _, repo := range []string{"", " ", " hello", "hello ", "hello\n", "hello\r", "hel\x00lo"} {
		_, err := c.UnpackImageFrom(context.Background(), image, repo, []Layer{layer}, UnpackLimits{})
		if !errors.Is(err, ErrRequest) {
			t.Fatalf("repository %q: %v", repo, err)
		}
	}
	if _, err := os.Stat(filepath.Join(data, "artifacts", image)); !os.IsNotExist(err) {
		t.Fatalf("rejected repository left a rootfs: %v", err)
	}
}

func TestRootfsRepos(t *testing.T) {
	data := t.TempDir()
	c := testClient(t, data)
	if got, err := c.RootfsRepos(); err != nil || len(got) != 0 {
		t.Fatalf("empty: %v %v", got, err)
	}
	helloA := plantRootfs(t, data, "hello", "a", true)
	helloB := plantRootfs(t, data, "hello", "b", true)
	other := plantRootfs(t, data, "other", "c", true)
	plantRootfs(t, data, "", "unrecorded", false)
	if err := os.MkdirAll(filepath.Join(data, "artifacts", "not-a-digest", "rootfs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "artifacts", "not-a-digest", repositoryName), []byte("junk\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(data, "artifacts", tempPrefix+"partial", "rootfs")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		t.Fatal(err)
	}
	blank := digest.FromString("blank").String()
	if err := os.MkdirAll(filepath.Join(data, "artifacts", blank, "rootfs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "artifacts", blank, repositoryName), []byte("\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nul := digest.FromString("nul").String()
	if err := os.MkdirAll(filepath.Join(data, "artifacts", nul, "rootfs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "artifacts", nul, repositoryName), []byte("bad\x00repo\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := c.RootfsRepos()
	if err != nil {
		t.Fatal(err)
	}
	wantHello := []string{helloA, helloB}
	slices.Sort(wantHello)
	if !slices.Equal(got["hello"], wantHello) || !slices.Equal(got["other"], []string{other}) || len(got) != 2 {
		t.Fatalf("repos %v", got)
	}

	broken := digest.FromString("broken").String()
	if err := os.MkdirAll(filepath.Join(data, "artifacts", broken, "rootfs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(data, "artifacts", broken, repositoryName), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RootfsRepos(); err == nil {
		t.Fatal("unreadable repository")
	}
}

func TestDigestOfExe(t *testing.T) {
	data := filepath.Clean("/var/lib/mftiko/sts")
	dig := digest.FromString("exe").String()
	okPath := filepath.Join(data, "artifacts", dig, "rootfs", "bin", "worker")
	if got, ok := digestOfExe(data, okPath); !ok || got != dig {
		t.Fatalf("exe %q %v", got, ok)
	}
	if got, ok := digestOfExe(data, okPath+" (deleted)"); !ok || got != dig {
		t.Fatalf("deleted exe %q %v", got, ok)
	}
	for _, exe := range []string{
		filepath.Join("/elsewhere", "artifacts", dig, "rootfs", "bin", "worker"),
		filepath.Join(data, "layers", dig),
		filepath.Join(data, "artifacts", dig, "layers"),
		filepath.Join(data, "artifacts", "nope", "rootfs", "bin", "worker"),
		filepath.Join(data, "artifacts", dig, "rootfs", "..", "..", "..", "etc", "passwd"),
	} {
		if _, ok := digestOfExe(data, exe); ok {
			t.Fatalf("accepted %s", exe)
		}
	}
}

func plantRootfs(t *testing.T, data, repo, label string, record bool) string {
	t.Helper()
	dig := digest.FromString(label).String()
	dir := filepath.Join(data, "artifacts", dig)
	if err := os.MkdirAll(filepath.Join(dir, "rootfs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if record {
		if err := os.WriteFile(filepath.Join(dir, repositoryName), []byte(repo+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dig
}
