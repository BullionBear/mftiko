package artifact

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	st, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != want {
		t.Fatalf("%s mode %o, want %o", path, got, want)
	}
}

func TestMakeReadOnlyAndRemoveWritable(t *testing.T) {
	root := filepath.Join(t.TempDir(), "tree")
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "out")
	if err := os.WriteFile(outside, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := MakeReadOnly(root); err != nil {
		t.Fatal(err)
	}
	if err := MakeReadOnly(root); err != nil {
		t.Fatal(err)
	}
	assertMode(t, root, 0o555)
	assertMode(t, filepath.Join(root, "sub"), 0o555)
	assertMode(t, filepath.Join(root, "sub", "f"), 0o444)
	assertMode(t, outside, 0o644)
	if err := RemoveWritable(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("removed: %v", err)
	}
	if string(mustRead(t, outside)) != "keep" {
		t.Fatal("symlink target removed")
	}
	if err := RemoveWritable(root); err != nil {
		t.Fatalf("missing root: %v", err)
	}
}

// UnpackReadOnly returns a tree that is already read-only: dest never shows the
// extraction's permissions (D975, D996).
func TestUnpackReadOnly(t *testing.T) {
	root := t.TempDir()
	layer := filepath.Join(root, "layer.tar.gz")
	if err := os.WriteFile(layer, gzipTar(t, reg("model/note.txt", 0o644, "hello"), reg("top", 0o755, "x")), 0o644); err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(root, "inputs")
	if err := os.Mkdir(work, 0o700); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(work, "model")
	if err := UnpackReadOnly(context.Background(), []string{layer}, dest, UnpackLimits{}); err != nil {
		t.Fatal(err)
	}
	assertMode(t, dest, 0o555)
	assertMode(t, filepath.Join(dest, "model"), 0o555)
	assertMode(t, filepath.Join(dest, "model", "note.txt"), 0o444)
	assertMode(t, filepath.Join(dest, "top"), 0o444)
	if string(mustRead(t, filepath.Join(dest, "model", "note.txt"))) != "hello" {
		t.Fatal("content")
	}
	assertNoExtract(t, work)
	if err := RemoveWritable(dest); err != nil {
		t.Fatal(err)
	}
}

// A rejected read-only unpack leaves nothing behind, as Unpack does.
func TestUnpackReadOnlyRejectLeavesNothing(t *testing.T) {
	root := t.TempDir()
	layer := filepath.Join(root, "layer.tar.gz")
	if err := os.WriteFile(layer, []byte("not a gzip"), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(root, "out")
	if err := UnpackReadOnly(context.Background(), []string{layer}, dest, UnpackLimits{}); err == nil {
		t.Fatal("want an error")
	}
	if _, err := os.Lstat(dest); !os.IsNotExist(err) {
		t.Fatalf("dest: %v", err)
	}
	assertNoExtract(t, root)
}
