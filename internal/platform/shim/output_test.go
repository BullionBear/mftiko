package shim

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
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

// Rotation keeps exactly keep files and loses only what fell off the oldest: what is kept is the tail of what was
// written, in order. A file is rotated once it reaches max_bytes, at the end of the line it is in, so every rotated
// file ends with a newline and is at most max_bytes plus the overrun (E16-D037).
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
			for i := range 400 { // lines of every length up to 1.5 files
				all = append(all, bytes.Repeat([]byte{byte('a' + i%26)}, i%(3*maxBytes/2))...)
				all = append(all, '\n')
			}
			for rest := all; len(rest) > 0; { // in writes that start and end anywhere in a line
				b := rest[:min(len(rest), 1+len(rest)%173)]
				if n, err := r.Write(b); err != nil || n != len(b) {
					t.Fatalf("write %d: %d %v", len(b), n, err)
				}
				rest = rest[len(b):]
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
				if int64(len(c)) > maxBytes+r.overrun {
					t.Errorf("%s is %d bytes, over %d", names[i], len(c), maxBytes+r.overrun)
				}
				if i < len(contents)-1 && (len(c) < maxBytes || c[len(c)-1] != '\n') {
					t.Errorf("%s is %d bytes ending %q: a rotated file is full and ends a line", names[i], len(c), c[len(c)-1:])
				}
				kept = append(kept, c...)
			}
			if !bytes.HasSuffix(all, kept) {
				t.Fatal("the files are not the tail of what was written")
			}
		})
	}
}

// A line longer than max_bytes plus the overrun is cut where the file reaches that size (E16-D037).
func TestRotatorCutsLongLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	r, err := openRotator(path, 100, 5)
	if err != nil {
		t.Fatal(err)
	}
	line := append(bytes.Repeat([]byte("x"), 500), '\n')
	if n, err := r.Write(line); err != nil || n != len(line) {
		t.Fatalf("write: %d %v", n, err)
	}
	r.Close()
	_, contents := files(t, path)
	var sizes []int
	for _, c := range contents {
		sizes = append(sizes, len(c))
	}
	if len(sizes) != 3 || sizes[0] != 200 || sizes[1] != 200 || sizes[2] != 101 {
		t.Fatalf("file sizes %v, want [200 200 101]", sizes)
	}
}

// A new incarnation appends to what the last one left, and rotation goes on from there: the file reaches max_bytes
// in the middle of a line and is rotated at its end.
func TestRotatorAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stderr")
	r, _ := openRotator(path, 10, 3)
	r.Write([]byte("0123456\n")) //nolint:errcheck
	r.Close()
	r, err := openRotator(path, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	r.Write([]byte("89abc\nde")) //nolint:errcheck
	r.Close()
	_, contents := files(t, path)
	if len(contents) != 2 || string(contents[0]) != "0123456\n89abc\n" || string(contents[1]) != "de" {
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
	if !strings.HasSuffix(got, "tail\n") {
		t.Errorf("the file does not end with the tail: %q", got[max(0, len(got)-120):])
	}
	// Every byte the worker wrote is either in the file or counted in a note at the point it went missing: the notes
	// and the data add up exactly.
	notes := noteRE.FindAllStringSubmatch(got, -1)
	if len(notes) == 0 {
		t.Fatal("nothing was dropped, or no note says so")
	}
	var lost int
	for _, m := range notes {
		n, _ := strconv.Atoi(m[1])
		lost += n
	}
	kept := strings.Count(got, "x")
	if kept+lost != total {
		t.Errorf("%d bytes kept + %d noted lost = %d, want the %d written", kept, lost, kept+lost, total)
	}
	if first := strings.Index(got, "[mftiko-shim]"); first < outputQueue*1024 {
		t.Errorf("first note at %d: before the queued chunks", first)
	}
}

var noteRE = regexp.MustCompile(`\[mftiko-shim\] (\d+) bytes of stdout lost here`)

// A file that fails (a full disk) loses what it cannot write and goes on draining; once it works again, the note
// says how much is missing, counting the worker's bytes and not its own. Failures at the very end are noted too.
func TestStreamWriteFails(t *testing.T) {
	for _, c := range []struct {
		fail int
		want string
	}{
		{2, "\n[mftiko-shim] 8 bytes of stderr lost here: the disk fell behind or failed\nthree\n"},
		{3, "\n[mftiko-shim] 14 bytes of stderr lost here: the disk fell behind or failed\n"},
	} {
		dst := &failingWriter{fail: c.fail}
		s := copyStream(zerolog.Nop(), "stderr", &lineReader{lines: []string{"one\n", "two\n", "three\n"}}, dst)
		<-s.Done()
		if got := dst.buf.String(); got != c.want {
			t.Errorf("%d failures: wrote %q, want %q", c.fail, got, c.want)
		}
	}
}

// lineReader returns one line per Read, then EOF: reads never run together, however busy the machine.
type lineReader struct{ lines []string }

func (r *lineReader) Read(p []byte) (int, error) {
	if len(r.lines) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.lines[0])
	r.lines = r.lines[1:]
	return n, nil
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
