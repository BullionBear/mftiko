//go:build unix

package shim

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/logread"
)

// The reader has to keep up with the shim's own rotator: bytes out equal bytes
// in, including across rotations, with no gap while every generation is still
// on disk (E14-03).
func TestLogreadFollowsRotator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	const maxBytes = 256
	const keep = 5
	r, err := openRotator(path, maxBytes, keep)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var chunks [][]byte
	var want []byte
	for i := range 10 {
		b := bytes.Repeat([]byte{byte('a' + i%26)}, 100)
		chunks = append(chunks, b)
		want = append(want, b...)
	}
	if len(want) > maxBytes*keep {
		t.Fatal("the fixture rotates a file out of the window")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := logread.Open(ctx, logread.Options{Path: path, Origin: logread.OriginStart, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	writeDone := make(chan error, 1)
	go func() {
		for _, c := range chunks {
			if _, err := r.Write(c); err != nil {
				writeDone <- err
				return
			}
		}
		writeDone <- nil
	}()

	var got []byte
	gaps := 0
	for len(got) < len(want) {
		rec, err := st.Next()
		if err != nil {
			t.Fatalf("after %d/%d bytes, %d gaps: %v", len(got), len(want), gaps, err)
		}
		if rec.Gap {
			gaps++
			continue
		}
		got = append(got, rec.Data...)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if gaps != 0 {
		t.Fatalf("gaps %d", gaps)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
}

func TestLogreadRotatorKeep1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stderr")
	r, err := openRotator(path, 32, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	first := bytes.Repeat([]byte("a"), 32)
	if _, err := r.Write(first); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := logread.Open(ctx, logread.Options{Path: path, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got := readLog(t, st, len(first))
	second := bytes.Repeat([]byte("b"), 16)
	if _, err := r.Write(second); err != nil {
		t.Fatal(err)
	}
	got = append(got, readLog(t, st, len(second))...)
	if !bytes.Equal(got, append(first, second...)) {
		t.Fatalf("%q", got)
	}
}

func TestLogreadRotatorSplitsLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	r, err := openRotator(path, 8, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("hello world\n")
	if _, err := r.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := logread.Open(context.Background(), logread.Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, err := readLogAll(st)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%q", got)
	}
}

func TestLogreadExportSurvivesRotator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	r, err := openRotator(path, 32, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("q"), 32*2+10)
	if _, err := r.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	snap, err := logread.Export(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if snap.Size() != int64(len(want)) {
		t.Fatalf("size %d", snap.Size())
	}
	// Another generation drops the oldest name. The snapshot already holds that fd.
	r, err = openRotator(path, 32, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Write(bytes.Repeat([]byte("r"), 40)); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(snap)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %d want %d", len(got), len(want))
	}
}

func readLog(t *testing.T, st *logread.Stream, n int) []byte {
	t.Helper()
	var got []byte
	for len(got) < n {
		rec, err := st.Next()
		if err != nil {
			t.Fatal(err)
		}
		if rec.Gap {
			t.Fatal("gap")
		}
		got = append(got, rec.Data...)
	}
	if len(got) != n {
		t.Fatalf("got %d, want %d", len(got), n)
	}
	return got
}

func readLogAll(st *logread.Stream) ([]byte, error) {
	var got []byte
	for {
		rec, err := st.Next()
		if errors.Is(err, io.EOF) {
			return got, nil
		}
		if err != nil {
			return nil, err
		}
		if rec.Gap {
			return nil, errors.New("gap")
		}
		got = append(got, rec.Data...)
	}
}
