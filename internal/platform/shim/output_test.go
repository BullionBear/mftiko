package shim

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
)

// files reads path and its rotated files, oldest first, as the rotator left them.
func files(t *testing.T, path string) (names []string, contents [][]byte) {
	t.Helper()
	for i := MaxOutputKeep; i >= 0; i-- {
		name := path
		if i > 0 {
			name += "." + strconv.Itoa(i)
		}
		b, err := os.ReadFile(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		names, contents = append(names, filepath.Base(name)), append(contents, b)
	}
	return names, contents
}

// Rotation keeps exactly keep files, none over max_bytes, and loses only what fell off the oldest: what is kept is
// the tail of what was written, in order.
func TestRotator(t *testing.T) {
	for _, keep := range []int{1, 2, 3, 5} {
		t.Run(strconv.Itoa(keep), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "h.stdout")
			const maxBytes = 100
			r, err := openRotator(path, maxBytes, keep)
			if err != nil {
				t.Fatal(err)
			}
			var all []byte
			for i := range 400 { // writes of every size, some larger than a file
				b := bytes.Repeat([]byte{byte('a' + i%26)}, i%(3*maxBytes/2)+1)
				if n, err := r.Write(b); err != nil || n != len(b) {
					t.Fatalf("write %d: %d %v", len(b), n, err)
				}
				all = append(all, b...)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			names, contents := files(t, path)
			if len(names) != keep {
				t.Fatalf("files %v, want %d", names, keep)
			}
			var kept []byte
			for i, c := range contents {
				if len(c) > maxBytes {
					t.Errorf("%s is %d bytes, over %d", names[i], len(c), maxBytes)
				}
				if i < len(contents)-1 && len(c) != maxBytes {
					t.Errorf("%s is %d bytes: a rotated file is full", names[i], len(c))
				}
				kept = append(kept, c...)
			}
			if !bytes.HasSuffix(all, kept) {
				t.Fatal("the files are not the tail of what was written")
			}
			if want := (keep-1)*maxBytes + len(contents[len(contents)-1]); len(kept) != want {
				t.Errorf("kept %d bytes, want %d", len(kept), want)
			}
		})
	}
}

// A new incarnation appends to what the last one left, and rotation goes on from there.
func TestRotatorAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stderr")
	r, _ := openRotator(path, 10, 3)
	r.Write([]byte("0123456")) //nolint:errcheck
	r.Close()
	r, err := openRotator(path, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	r.Write([]byte("789abc")) //nolint:errcheck
	r.Close()
	_, contents := files(t, path)
	if got := string(bytes.Join(contents, nil)); got != "0123456789abc" || string(contents[0]) != "0123456789" {
		t.Fatalf("files %q", contents)
	}
}

// slowWriter blocks every write until released, as a stalled disk would.
type slowWriter struct {
	mu      sync.Mutex
	release chan struct{}
	buf     bytes.Buffer
	closed  bool
}

func (w *slowWriter) Write(p []byte) (int, error) {
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *slowWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

// Done criterion 1: however slow the file, the worker's writes never wait for it. What does not fit is dropped,
// and the file says how much and where.
func TestStreamNeverBlocks(t *testing.T) {
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	dst := &slowWriter{release: make(chan struct{})}
	s := copyStream(zerolog.Nop(), "stdout", pr, dst)

	const total = 64 << 20 // far beyond the pipe and the queue
	chunk := bytes.Repeat([]byte("x"), 64<<10)
	start := time.Now()
	for n := 0; n < total; n += len(chunk) {
		if _, err := pw.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the writer took %v with the file stalled", d)
	}
	close(dst.release) // the disk comes back, and the queue drains
	time.Sleep(200 * time.Millisecond)
	pw.Write([]byte("tail\n")) //nolint:errcheck
	pw.Close()
	select {
	case <-s.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("stream not done")
	}
	got := dst.buf.String()
	if !dst.closed {
		t.Error("file not closed")
	}
	if !strings.Contains(got, "bytes of stdout lost here") {
		t.Fatal("no note of what was dropped")
	}
	if !strings.HasSuffix(got, "tail\n") {
		t.Errorf("the file does not end with the tail: %q", got[max(0, len(got)-120):])
	}
	// The note sits where the gap is: after the queued chunks, and before whatever got through once the disk came
	// back (what was still in the pipe then, if anything, and the tail).
	note := strings.Index(got, "[mftiko-shim]")
	if note < outputQueue*1024 || strings.Count(got, "[mftiko-shim]") != 1 {
		t.Errorf("note at %d (%d notes): want one, after the queued chunks", note, strings.Count(got, "[mftiko-shim]"))
	}
	if len(got) >= total {
		t.Errorf("wrote %d bytes: nothing was dropped", len(got))
	}
}

// A file that fails (a full disk) loses what it cannot write and goes on draining; once it works again, the note
// says how much is missing.
func TestStreamWriteFails(t *testing.T) {
	pr, pw, _ := os.Pipe()
	dst := &failingWriter{fail: 2}
	s := copyStream(zerolog.Nop(), "stderr", pr, dst)
	for _, line := range []string{"one\n", "two\n", "three\n"} {
		pw.Write([]byte(line))            //nolint:errcheck
		time.Sleep(20 * time.Millisecond) // one read each
	}
	pw.Close()
	<-s.Done()
	if got := dst.buf.String(); !strings.Contains(got, "8 bytes of stderr lost here") || !strings.HasSuffix(got, "three\n") {
		t.Fatalf("wrote %q", got)
	}
}

type failingWriter struct {
	fail int
	buf  bytes.Buffer
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.fail > 0 {
		w.fail--
		return 0, errors.New("no space left on device")
	}
	return w.buf.Write(p)
}

func (w *failingWriter) Close() error { return nil }

var _ io.WriteCloser = (*failingWriter)(nil)

func TestCheckOutput(t *testing.T) {
	for _, c := range []struct {
		o  *shimv1.Output
		ok bool
	}{
		{nil, true},
		{&shimv1.Output{}, true},
		{&shimv1.Output{MaxBytes: MinOutputMaxBytes, Keep: 1}, true},
		{&shimv1.Output{MaxBytes: MinOutputMaxBytes - 1}, false},
		{&shimv1.Output{Keep: MaxOutputKeep}, true},
		{&shimv1.Output{Keep: MaxOutputKeep + 1}, false},
	} {
		if err := checkOutput(c.o); (err == nil) != c.ok || (err != nil && !errors.Is(err, ErrBadLaunch)) {
			t.Errorf("%v: %v", c.o, err)
		}
	}
	maxBytes, keep := outputLimits(&shimv1.Launch{})
	if maxBytes != DefaultOutputMaxBytes || keep != DefaultOutputKeep {
		t.Errorf("defaults %d %d", maxBytes, keep)
	}
	maxBytes, keep = outputLimits(&shimv1.Launch{Output: &shimv1.Output{MaxBytes: 1 << 20, Keep: 2}})
	if maxBytes != 1<<20 || keep != 2 {
		t.Errorf("limits %d %d", maxBytes, keep)
	}
}

func TestOutputPath(t *testing.T) {
	got := OutputPath("/var/mftiko", "md", "md/binance/spot/0", "stderr")
	if want := "/var/mftiko/log/md/" + workerHash("md/binance/spot/0") + ".stderr"; got != want {
		t.Errorf("%s, want %s", got, want)
	}
}
