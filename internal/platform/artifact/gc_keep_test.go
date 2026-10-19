package artifact

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestGCKeepsListedLayers(t *testing.T) {
	data := t.TempDir()
	c := testClient(t, data)
	keepDigest := "sha256:" + strings.Repeat("aa", 32)
	layerKeep := "sha256:" + strings.Repeat("11", 32)
	orphan := "sha256:" + strings.Repeat("44", 32)
	dir := filepath.Join(data, "artifacts", keepDigest, "rootfs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "artifacts", keepDigest, layersName), []byte(layerKeep+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	layerDir := filepath.Join(data, "layers")
	if err := os.MkdirAll(layerDir, 0o755); err != nil {
		t.Fatal(err)
	}
	keepPath := filepath.Join(layerDir, layerKeep)
	orphanPath := filepath.Join(layerDir, orphan)
	if err := os.WriteFile(keepPath, []byte("ref"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := c.GC([]string{keepDigest}, []string{orphan}); err != nil {
		t.Fatal(err)
	}
	if string(mustRead(t, orphanPath)) != "orphan" {
		t.Fatal("listed layer was deleted")
	}
	if string(mustRead(t, keepPath)) != "ref" {
		t.Fatal("referenced layer was deleted")
	}
	if string(mustRead(t, filepath.Join(dir, "file"))) != "keep" {
		t.Fatal("kept rootfs was deleted")
	}

	if err := c.GC([]string{keepDigest}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("nil layer set left the orphan: %v", err)
	}
	if string(mustRead(t, keepPath)) != "ref" {
		t.Fatal("nil layer set deleted a referenced layer")
	}

	if err := c.GC(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(keepPath); !os.IsNotExist(err) {
		t.Fatalf("empty keep left the layer: %v", err)
	}
}

func TestGCBadLayerKeepDeletesNothing(t *testing.T) {
	data := t.TempDir()
	c := testClient(t, data)
	keepDigest := "sha256:" + strings.Repeat("aa", 32)
	layerKeep := "sha256:" + strings.Repeat("11", 32)
	dir := filepath.Join(data, "artifacts", keepDigest, "rootfs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "artifacts", keepDigest, layersName), []byte(layerKeep+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	layerPath := filepath.Join(data, "layers", layerKeep)
	if err := os.MkdirAll(filepath.Dir(layerPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(layerPath, []byte("ref"), 0o644); err != nil {
		t.Fatal(err)
	}
	orphan := "sha256:" + strings.Repeat("44", 32)
	orphanPath := filepath.Join(data, "layers", orphan)
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := c.GC([]string{keepDigest}, []string{"not-a-digest"})
	if err == nil || !errors.Is(err, ErrRequest) {
		t.Fatalf("bad layer keep: %v", err)
	}
	if string(mustRead(t, filepath.Join(dir, "file"))) != "keep" || string(mustRead(t, layerPath)) != "ref" || string(mustRead(t, orphanPath)) != "orphan" {
		t.Fatal("bad layer keep deleted something")
	}

	err = c.GC(nil, []string{orphan}, []string{orphan})
	if err == nil || !errors.Is(err, ErrRequest) {
		t.Fatalf("two layer sets: %v", err)
	}
	if string(mustRead(t, orphanPath)) != "orphan" || string(mustRead(t, filepath.Join(dir, "file"))) != "keep" {
		t.Fatal("two layer sets deleted something")
	}
}

func TestHoldLayersSurvivesGC(t *testing.T) {
	data := t.TempDir()
	c := testClient(t, data)
	orphan := "sha256:" + strings.Repeat("55", 32)
	path := filepath.Join(data, "layers", orphan)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("held"), 0o644); err != nil {
		t.Fatal(err)
	}
	release, err := c.HoldLayers([]string{orphan})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.GC(nil); err != nil {
			t.Errorf("gc: %v", err)
		}
	}()
	wg.Wait()
	if string(mustRead(t, path)) != "held" {
		t.Fatal("hold did not keep the layer")
	}
	release()
	release() // once
	if err := c.GC(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("released layer: %v", err)
	}
	if _, err := c.HoldLayers([]string{"nope"}); err == nil || !errors.Is(err, ErrRequest) {
		t.Fatalf("bad hold: %v", err)
	}
}
