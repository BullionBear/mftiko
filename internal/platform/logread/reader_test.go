//go:build unix

package logread

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func writeFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o640); err != nil {
		t.Fatal(err)
	}
}

func ident(t *testing.T, path string) (dev, ino uint64) {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	dev, ino, err = fileID(fi)
	if err != nil {
		t.Fatal(err)
	}
	return dev, ino
}

func differ(t *testing.T, got, want []byte) {
	t.Helper()
	if bytes.Equal(got, want) {
		return
	}
	n := min(len(got), len(want))
	i := 0
	for i < n && got[i] == want[i] {
		i++
	}
	t.Fatalf("mismatch at %d: got %d bytes, want %d", i, len(got), len(want))
}

// collect reads s to io.EOF, or, with Follow and a Pause that is always true,
// until nothing is ready.
func collect(t *testing.T, s *Stream) (data []byte, gaps int) {
	t.Helper()
	for {
		rec, err := s.Next()
		if errors.Is(err, io.EOF) || errors.Is(err, ErrPause) {
			return data, gaps
		}
		if err != nil {
			t.Fatal(err)
		}
		if rec.Gap {
			if len(rec.Data) != 0 {
				t.Fatalf("gap carries %q", rec.Data)
			}
			gaps++
			continue
		}
		if len(rec.Data) > maxChunk {
			t.Fatalf("record %d exceeds %d", len(rec.Data), maxChunk)
		}
		data = append(data, rec.Data...)
	}
}

func TestStartAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	// One line is split on the rotation boundary: "hello " | "world\n".
	writeFile(t, path+".1", []byte("hello "))
	writeFile(t, path, []byte("world\n"))
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data, gaps := collect(t, s)
	if gaps != 0 {
		t.Fatal(gaps)
	}
	differ(t, data, []byte("hello world\n"))
}

func TestMissingAndEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = s.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("missing file blocked")
	}
	s.Close()

	writeFile(t, path, nil)
	s, err = Open(ctx, Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start = time.Now()
	data, gaps := collect(t, s)
	if gaps != 0 || len(data) != 0 {
		t.Fatalf("gaps %d data %q", gaps, data)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("empty file blocked")
	}
}

func TestFollowWaitsForFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stderr")
	entered := make(chan struct{})
	var once sync.Once
	pollHook = func() { once.Do(func() { close(entered) }) }
	t.Cleanup(func() { pollHook = nil })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Open(ctx, Options{Path: path, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	type result struct {
		data []byte
		gap  bool
		err  error
	}
	got := make(chan result, 1)
	go func() {
		rec, err := s.Next()
		got <- result{data: rec.Data, gap: rec.Gap, err: err}
	}()
	select {
	case <-entered:
	case res := <-got:
		t.Fatalf("returned before the poll: %+v", res)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	writeFile(t, path, []byte("later\n"))
	select {
	case res := <-got:
		if res.err != nil || res.gap || string(res.data) != "later\n" {
			t.Fatalf("%+v", res)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestFollowCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	pollHook = func() { once.Do(cancel) }
	t.Cleanup(func() { pollHook = nil })
	s, err := Open(ctx, Options{Path: path, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Next(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestTailAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	writeFile(t, path+".2", []byte("a\nb\nc\n"))
	writeFile(t, path+".1", []byte("d\ne\nf\n"))
	writeFile(t, path, []byte("g\nh"))
	// The last line has no newline and still counts (E14-D012).
	// Lines: a b c d e f g h. tail 3 is f, g, h and crosses .1 into the live file.
	ctx := context.Background()
	for _, c := range []struct {
		n    int
		want string
	}{
		{1, "h"},
		{3, "f\ng\nh"},
		{100, "a\nb\nc\nd\ne\nf\ng\nh"},
		{0, ""},
	} {
		s, err := Open(ctx, Options{Path: path, Origin: OriginTail, Tail: c.n})
		if err != nil {
			t.Fatal(err)
		}
		data, gaps := collect(t, s)
		s.Close()
		if gaps != 0 {
			t.Fatal(gaps)
		}
		if string(data) != c.want {
			t.Fatalf("tail %d: %q", c.n, data)
		}
	}

	// A line split across the rotation: "bb" | "cc\n".
	writeFile(t, path+".1", []byte("aa\nbb"))
	writeFile(t, path, []byte("cc\ndd\n"))
	os.Remove(path + ".2")
	s, err := Open(ctx, Options{Path: path, Origin: OriginTail, Tail: 2})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := collect(t, s)
	s.Close()
	if string(data) != "bbcc\ndd\n" {
		t.Fatalf("%q", data)
	}
}

func TestTailBlockBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	var b []byte
	for i := range 3000 {
		b = append(b, []byte("line\n")...)
		_ = i
	}
	writeFile(t, path, b)
	if len(b) <= scanBlock {
		t.Fatalf("file %d did not cross a scan block", len(b))
	}
	s, err := Open(context.Background(), Options{Path: path, Origin: OriginTail, Tail: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data, gaps := collect(t, s)
	if gaps != 0 || string(data) != "line\nline\n" {
		t.Fatalf("gaps %d %q", gaps, data)
	}
}

func TestKeep1NoGap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	first := bytes.Repeat([]byte("a"), 32)
	writeFile(t, path, first)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Open(ctx, Options{Path: path, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rec, err := s.Next()
	if err != nil || rec.Gap || !bytes.Equal(rec.Data, first) {
		t.Fatalf("%+v %v", rec, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	second := bytes.Repeat([]byte("b"), 16)
	writeFile(t, path, second)
	rec, err = s.Next()
	if err != nil || rec.Gap || !bytes.Equal(rec.Data, second) {
		t.Fatalf("after rotate %+v %v", rec, err)
	}
}

func TestCursorIsResolvedStart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	body := []byte("abcdef\n")
	writeFile(t, path, body)
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	start := s.Cursor()
	if start == "" {
		t.Fatal("empty start")
	}
	rec, err := s.Next()
	if err != nil || rec.Gap || string(rec.Data) != string(body) {
		t.Fatalf("%+v %v", rec, err)
	}
	if rec.Cursor == start {
		t.Fatal("cursor did not advance")
	}
	s.Close()
	if s.Cursor() != "" {
		t.Fatal("closed stream still has a cursor")
	}
	s, err = Open(ctx, Options{Path: path, Origin: OriginCursor, Cursor: start})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Cursor() != start {
		t.Fatalf("resume at %q, start %q", s.Cursor(), start)
	}
	data, gaps := collect(t, s)
	if gaps != 0 || string(data) != string(body) {
		t.Fatalf("gaps %d %q", gaps, data)
	}
}

func TestCursorResumeAndReject(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	line := []byte("abcdefghij\n")
	var body []byte
	for len(body) < maxChunk+len(line) {
		body = append(body, line...)
	}
	writeFile(t, path, body)
	ctx := context.Background()
	s, err := Open(ctx, Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if rec.Gap || len(rec.Data) == 0 || len(rec.Data) > maxChunk || rec.Data[len(rec.Data)-1] != '\n' {
		t.Fatalf("first record %d gap %v", len(rec.Data), rec.Gap)
	}
	s.Close()
	s, err = Open(ctx, Options{Path: path, Origin: OriginCursor, Cursor: rec.Cursor})
	if err != nil {
		t.Fatal(err)
	}
	rest, gaps := collect(t, s)
	s.Close()
	if gaps != 0 {
		t.Fatal(gaps)
	}
	differ(t, append(append([]byte{}, rec.Data...), rest...), body)

	// Same inode, different bytes in the hashed window: the cursor must not stick.
	off := int64(100)
	raw := bytes.Repeat([]byte("abcd"), 40)
	writeFile(t, path, raw)
	dev, ino := ident(t, path)
	prefix := raw[off-prefixN : off]
	good := encode(dev, ino, off, fnv64(prefix))
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte("z"), prefixN), off-prefixN); err != nil {
		t.Fatal(err)
	}
	f.Close()
	s, err = Open(ctx, Options{Path: path, Origin: OriginCursor, Cursor: good})
	if err != nil {
		t.Fatal(err)
	}
	data, gaps := collect(t, s)
	s.Close()
	if gaps != 1 {
		t.Fatalf("hash mismatch gaps %d", gaps)
	}
	changed := append([]byte{}, raw...)
	copy(changed[off-prefixN:off], bytes.Repeat([]byte("z"), prefixN))
	differ(t, data, changed)

	// A different device with the same inode and a matching hash is not this file.
	cur := encode(dev+1, ino, off, fnv64(changed[off-prefixN:off]))
	s, err = Open(ctx, Options{Path: path, Origin: OriginCursor, Cursor: cur})
	if err != nil {
		t.Fatal(err)
	}
	data, gaps = collect(t, s)
	s.Close()
	if gaps != 1 {
		t.Fatal(gaps)
	}
	differ(t, data, changed)

	if _, err := Open(ctx, Options{Path: path, Origin: OriginCursor, Cursor: "not-a-cursor"}); !errors.Is(err, ErrCursor) {
		t.Fatal(err)
	}
	if _, err := Open(ctx, Options{Path: path, Origin: OriginTail, Tail: -1}); !errors.Is(err, ErrOrigin) {
		t.Fatal(err)
	}
}

func TestInodeReuse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	oldBody := bytes.Repeat([]byte("abcd"), 32) // 128 bytes
	writeFile(t, path, oldBody)
	dev, ino := ident(t, path)
	const off = 80
	cur := encode(dev, ino, off, fnv64(oldBody[off-prefixN:off]))
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	fresh := bytes.Repeat([]byte("wxyz"), 32)
	reused := false
	for range 200 {
		writeFile(t, path, fresh)
		dev2, ino2 := ident(t, path)
		if dev2 == dev && ino2 == ino {
			reused = true
			break
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	if !reused {
		t.Fatal("filesystem did not reuse the inode")
	}
	s, err := Open(context.Background(), Options{Path: path, Origin: OriginCursor, Cursor: cur})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data, gaps := collect(t, s)
	if gaps != 1 {
		t.Fatalf("gaps %d", gaps)
	}
	differ(t, data, fresh)
}

func TestCursorOnRotatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	older := []byte("0123456789abcdef")
	writeFile(t, path+".1", older)
	writeFile(t, path, []byte("XYZ"))
	dev, ino := ident(t, path+".1")
	const off = 10
	cur := encode(dev, ino, off, fnv64(older[:off]))
	s, err := Open(context.Background(), Options{Path: path, Origin: OriginCursor, Cursor: cur})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data, gaps := collect(t, s)
	if gaps != 0 {
		t.Fatal(gaps)
	}
	differ(t, data, []byte("abcdefXYZ"))
}

func TestFollowAppendAndRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	writeFile(t, path, []byte("aa"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Open(ctx, Options{Path: path, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rec, err := s.Next()
	if err != nil || rec.Gap || string(rec.Data) != "aa" {
		t.Fatalf("%+v %v", rec, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("bb")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	rec, err = s.Next()
	if err != nil || rec.Gap || string(rec.Data) != "bb" {
		t.Fatalf("append %+v %v", rec, err)
	}

	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, []byte("cc"))
	rec, err = s.Next()
	if err != nil || rec.Gap || string(rec.Data) != "cc" {
		t.Fatalf("rename %+v %v", rec, err)
	}
}

func TestFellOffGapResumes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	writeFile(t, path, []byte("gone"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Open(ctx, Options{Path: path, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rec, err := s.Next()
	if err != nil || rec.Gap || string(rec.Data) != "gone" {
		t.Fatalf("%+v %v", rec, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	writeFile(t, path+".1", []byte("old\n"))
	writeFile(t, path, []byte("new\n"))
	rec, err = s.Next()
	if err != nil || !rec.Gap || len(rec.Data) != 0 || rec.Cursor == "" {
		t.Fatalf("gap %+v %v", rec, err)
	}
	gapCursor := rec.Cursor
	var rest []byte
	for string(rest) != "old\nnew\n" {
		rec, err = s.Next()
		if err != nil || rec.Gap {
			t.Fatalf("after gap %+v %v", rec, err)
		}
		rest = append(rest, rec.Data...)
	}
	s.Close()
	s, err = Open(context.Background(), Options{Path: path, Origin: OriginCursor, Cursor: gapCursor})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	again, gaps := collect(t, s)
	if gaps != 0 || string(again) != "old\nnew\n" {
		t.Fatalf("resume gaps %d %q", gaps, again)
	}
}

// openNoWait follows path from the start. Pause is always true, so collect
// stops where a follow read would wait.
func openNoWait(t *testing.T, path string) *Stream {
	t.Helper()
	s, err := Open(context.Background(), Options{Path: path, Follow: true, Pause: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// duringList runs rotate once, inside the next listFiles, after the numbered
// names are stat'ed and before the live one (E14-D042).
func duringList(t *testing.T, rotate func()) {
	t.Helper()
	var once sync.Once
	listHook = func() { once.Do(rotate) }
	t.Cleanup(func() { listHook = nil })
}

func appendFile(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func rename(t *testing.T, from, to string) {
	t.Helper()
	if err := os.Rename(from, to); err != nil {
		t.Fatal(err)
	}
}

// The first rotation lands inside the listing: the scan finds no .1 yet, and
// path is already the new file, so our inode is under neither name. The bytes
// the writer added just before rotating are only on our descriptor. They come
// before the new file, with no gap (E14-D041).
func TestRotateDuringListFirst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	first := bytes.Repeat([]byte("a"), 64)
	late := bytes.Repeat([]byte("b"), 32)
	next := bytes.Repeat([]byte("c"), 16)
	writeFile(t, path, first)
	s := openNoWait(t, path)
	data, gaps := collect(t, s)
	if gaps != 0 {
		t.Fatal(gaps)
	}
	differ(t, data, first)

	duringList(t, func() {
		appendFile(t, path, late)
		rename(t, path, path+".1")
		writeFile(t, path, next)
	})
	data, gaps = collect(t, s)
	if gaps != 0 {
		t.Fatalf("gaps %d", gaps)
	}
	differ(t, data, append(append([]byte{}, late...), next...))
}

// With numbered files the same race shows the scan an older file and not
// ours: the rotator renamed .1 to .2 before the scan reached them, and renames
// path to .1 after the scan has passed .1. No generation fell off, so there is
// no gap, and the older file is not read again (E14-D041).
func TestRotateDuringListNumbered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	older := bytes.Repeat([]byte("o"), 48)
	first := bytes.Repeat([]byte("a"), 64)
	next := bytes.Repeat([]byte("c"), 16)
	writeFile(t, path+".1", older)
	writeFile(t, path, first)
	s := openNoWait(t, path)
	data, gaps := collect(t, s)
	if gaps != 0 {
		t.Fatal(gaps)
	}
	differ(t, data, append(append([]byte{}, older...), first...))

	rename(t, path+".1", path+".2")
	duringList(t, func() {
		rename(t, path, path+".1")
		writeFile(t, path, next)
	})
	data, gaps = collect(t, s)
	if gaps != 0 {
		t.Fatalf("gaps %d", gaps)
	}
	differ(t, data, next)
}

// keep=1 inside the listing: our inode has no name left, so this is tail -F
// without a gap, after the descriptor is read to its end (E14-D041).
func TestRemoveDuringList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	first := bytes.Repeat([]byte("a"), 64)
	late := bytes.Repeat([]byte("b"), 32)
	next := bytes.Repeat([]byte("c"), 16)
	writeFile(t, path, first)
	s := openNoWait(t, path)
	data, gaps := collect(t, s)
	if gaps != 0 {
		t.Fatal(gaps)
	}
	differ(t, data, first)

	duringList(t, func() {
		appendFile(t, path, late)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		writeFile(t, path, next)
	})
	data, gaps = collect(t, s)
	if gaps != 0 {
		t.Fatalf("gaps %d", gaps)
	}
	differ(t, data, append(append([]byte{}, late...), next...))
}

// A rotation after the listing gives the name listed for our inode to the
// next file. find hashes through the descriptor, so a shorter file under that
// name is neither an error nor a mismatch, and following goes on to it.
func TestFindAfterListRotates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	first := bytes.Repeat([]byte("a"), 100)
	next := []byte("b")
	writeFile(t, path, first)
	s := openNoWait(t, path)
	data, _ := collect(t, s)
	differ(t, data, first)

	ents, err := listFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	rename(t, path, path+".1")
	writeFile(t, path, next)
	idx, listed, err := s.find(ents)
	if err != nil || !listed || idx != 0 {
		t.Fatalf("idx %d listed %v: %v", idx, listed, err)
	}
	data, gaps := collect(t, s)
	if gaps != 0 {
		t.Fatalf("gaps %d", gaps)
	}
	differ(t, data, next)
}

func TestPollInterval(t *testing.T) {
	if pollInterval != 250*time.Millisecond {
		t.Fatal(pollInterval)
	}
}

func TestSymlinkIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	other := filepath.Join(dir, "other")
	writeFile(t, other, []byte("other-file\n"))
	if err := os.Symlink(other, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, []byte("live\n"))
	s, err := Open(context.Background(), Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data, gaps := collect(t, s)
	if gaps != 0 || string(data) != "live\n" {
		t.Fatalf("gaps %d %q", gaps, data)
	}
}

func TestChunkSplitsRune(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "h.stdout")
	head := bytes.Repeat([]byte("x"), maxChunk-1)
	body := append(append([]byte{}, head...), []byte("你yyyyyyyy")...)
	writeFile(t, path, body)
	s, err := Open(context.Background(), Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if first.Gap || !bytes.Equal(first.Data, head) {
		t.Fatalf("first len %d gap %v", len(first.Data), first.Gap)
	}
	rest, gaps := collect(t, s)
	if gaps != 0 {
		t.Fatal(gaps)
	}
	if !bytes.HasPrefix(rest, []byte("你")) {
		t.Fatalf("rest starts %q", rest[:min(8, len(rest))])
	}
	differ(t, append(append([]byte{}, first.Data...), rest...), body)
}

func TestPauseSkipsPoll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	writeFile(t, path, []byte("line\n"))
	s, err := Open(context.Background(), Options{
		Path: path, Follow: true, Pause: func() bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now()
	rec, err := s.Next()
	if err != nil || string(rec.Data) != "line\n" {
		t.Fatalf("%+v %v", rec, err)
	}
	if _, err := s.Next(); !errors.Is(err, ErrPause) {
		t.Fatal(err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("pause waited for the poll")
	}
}

func TestPrimeDoesNotConsume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	writeFile(t, path, []byte("ab\n"))
	s, err := Open(context.Background(), Options{Path: path, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Prime(); err != nil {
		t.Fatal(err)
	}
	if s.Cursor() == "" {
		t.Fatal("empty resolved start")
	}
	rec, err := s.Next()
	if err != nil || rec.Gap || string(rec.Data) != "ab\n" {
		t.Fatalf("%+v %v", rec, err)
	}
}

func TestPrimeWaitsForFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	entered := make(chan struct{})
	var once sync.Once
	pollHook = func() { once.Do(func() { close(entered) }) }
	t.Cleanup(func() { pollHook = nil })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Open(ctx, Options{Path: path, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	errc := make(chan error, 1)
	go func() { errc <- s.Prime() }()
	select {
	case <-entered:
	case err := <-errc:
		t.Fatalf("prime returned early: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	writeFile(t, path, []byte("later\n"))
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if s.Cursor() == "" {
		t.Fatal("empty resolved start")
	}
	rec, err := s.Next()
	if err != nil || rec.Gap || string(rec.Data) != "later\n" {
		t.Fatalf("%+v %v", rec, err)
	}
}

func TestPrimePause(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.stdout")
	s, err := Open(context.Background(), Options{
		Path: path, Follow: true, Pause: func() bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now()
	if err := s.Prime(); !errors.Is(err, ErrPause) {
		t.Fatal(err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("prime waited for the poll")
	}
}
