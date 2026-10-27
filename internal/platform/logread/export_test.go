//go:build unix

package logread

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestExportOrderAndDeletion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	oldest := bytes.Repeat([]byte("o"), 20)
	middle := bytes.Repeat([]byte("m"), 20)
	live := bytes.Repeat([]byte("l"), 10)
	writeFile(t, path+".2", oldest)
	writeFile(t, path+".1", middle)
	writeFile(t, path, live)
	want := append(append(append([]byte{}, oldest...), middle...), live...)

	snap, err := Export(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if snap.Size() != int64(len(want)) {
		t.Fatalf("size %d", snap.Size())
	}
	if err := os.Remove(path + ".2"); err != nil {
		t.Fatal(err)
	}
	// The live file grows after the snapshot; those bytes stay out of it.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte("Z"), 15)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	got, err := io.ReadAll(snap)
	if err != nil {
		t.Fatal(err)
	}
	differ(t, got, want)
}

func TestExportMissing(t *testing.T) {
	snap, err := Export(context.Background(), filepath.Join(t.TempDir(), "h.stdout"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if snap.Size() != 0 {
		t.Fatal(snap.Size())
	}
	got, err := io.ReadAll(snap)
	if err != nil || len(got) != 0 {
		t.Fatalf("%q %v", got, err)
	}
}
