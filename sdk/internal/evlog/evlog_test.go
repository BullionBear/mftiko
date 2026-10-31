package evlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/BullionBear/mftiko/sdk/eventlog"
)

func header() eventlog.Header {
	return eventlog.Header{Incarnation: 3, SessionID: "s1", SDKVersion: "v1.0.0", SDKCommit: "abc"}
}

func TestWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	l := Open(Config{Dir: dir, Header: header()})
	l.Append(eventlog.KindInit, 0, 10, 11, 7, []byte("ini"), []byte("t"))
	l.Append(eventlog.KindMDFeed, 0, 20, 21, 0, nil, []byte("md"))
	l.Append(eventlog.KindHookEnd, eventlog.EndPanic, 0, 22, 2, nil, []byte("boom"))
	l.Close()

	fs, err := eventlog.Files(dir)
	if err != nil || len(fs) != 1 || filepath.Base(fs[0].Path) != "3-1.mel" {
		t.Fatalf("files %+v %v", fs, err)
	}
	h, recs := readFile(t, fs[0].Path)
	if h.SessionID != "s1" || h.Incarnation != 3 || h.N != 1 || h.SDKVersion != "v1.0.0" || h.SDKCommit != "abc" || h.CreatedNs == 0 {
		t.Fatalf("header %+v", h)
	}
	want := []struct {
		kind eventlog.Kind
		body string
	}{{eventlog.KindInit, "init"}, {eventlog.KindMDFeed, "md"}, {eventlog.KindHookEnd, "boom"}, {eventlog.KindEnd, "\x00\x00\x00\x00\x00\x00\x00\x00"}}
	if len(recs) != len(want) {
		t.Fatalf("records %d", len(recs))
	}
	for i, w := range want {
		if recs[i].Kind != w.kind || string(recs[i].Body) != w.body || recs[i].Seq != uint64(i+1) {
			t.Fatalf("record %d %+v", i, recs[i])
		}
	}
	if recs[0].RecvNs != 10 || recs[0].TNs != 11 || recs[0].Ref != 7 || recs[2].Flags != eventlog.EndPanic {
		t.Fatalf("fields %+v %+v", recs[0], recs[2])
	}
	if w, d := l.Stats(); w != 4 || d != 0 {
		t.Fatalf("stats %d %d", w, d)
	}
}

// Files rotate by size, and the oldest are deleted so the directory stays within keep.
func TestRotationAndRetention(t *testing.T) {
	dir := t.TempDir()
	const keep = 256 << 10
	l := Open(Config{Dir: dir, Header: header(), KeepBytes: keep})
	body := []byte(strings.Repeat("x", 10<<10))
	const n = 100
	for i := range n {
		l.Append(eventlog.KindPost, 0, int64(i), int64(i), uint64(i+1), nil, body)
		if i%10 == 9 {
			l.Flush()
		}
	}
	l.Close()

	fs, err := eventlog.Files(dir)
	if err != nil || len(fs) < 2 {
		t.Fatalf("files %+v %v", fs, err)
	}
	var total int64
	var seqs []uint64
	for i, f := range fs {
		st, err := os.Stat(f.Path)
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() > minFile {
			t.Fatalf("%s is %d bytes", f.Path, st.Size())
		}
		total += st.Size()
		if i > 0 && f.N != fs[i-1].N+1 {
			t.Fatalf("numbering %+v", fs)
		}
		_, recs := readFile(t, f.Path)
		for _, r := range recs {
			seqs = append(seqs, r.Seq)
		}
	}
	if total > keep {
		t.Fatalf("total %d over %d", total, keep)
	}
	if fs[0].N == 1 {
		t.Fatal("the oldest file was kept")
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			t.Fatalf("seq %d after %d", seqs[i], seqs[i-1])
		}
	}
	if seqs[len(seqs)-1] != n+1 {
		t.Fatalf("last seq %d", seqs[len(seqs)-1])
	}
}

// A full ring drops and counts; Append returns while the disk is stuck (E20-D007).
func TestRingFullDoesNotBlock(t *testing.T) {
	dir := t.TempDir()
	sink := &stuck{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	l := Open(Config{
		Dir: dir, Header: header(), RingBytes: 4 << 10, FlushEvery: time.Millisecond,
		Create: func(path string) (File, error) {
			f, err := createFile(path)
			if err != nil {
				return nil, err
			}
			sink.f = f
			return sink, nil
		},
	})
	body := []byte(strings.Repeat("y", 100))
	l.Append(eventlog.KindPost, 0, 1, 1, 1, nil, body)
	<-sink.entered
	const n = 1000
	done := make(chan struct{})
	go func() {
		for i := range n {
			l.Append(eventlog.KindPost, 0, int64(i), int64(i), uint64(i+2), nil, body)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Append waited for the disk")
	}
	if _, d := l.Stats(); d == 0 {
		t.Fatal("nothing was dropped")
	}
	close(sink.release)
	l.Flush()
	l.Close()
	written, dropped := l.Stats()
	if written+dropped != n+2 {
		t.Fatalf("written %d dropped %d", written, dropped)
	}
	fs, _ := eventlog.Files(dir)
	var got uint64
	var last uint64
	for _, f := range fs {
		_, recs := readFile(t, f.Path)
		for _, r := range recs {
			if r.Seq <= last {
				t.Fatalf("seq %d after %d", r.Seq, last)
			}
			last = r.Seq
			got++
		}
	}
	if got != written || last != n+2 {
		t.Fatalf("frames %d written %d last %d", got, written, last)
	}
}

func TestUnwritableDirCountsDropped(t *testing.T) {
	file := filepath.Join(t.TempDir(), "plain")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var lines []string
	for _, dir := range []string{filepath.Join(file, "eventlog"), ""} {
		lines = nil
		l := Open(Config{Dir: dir, Header: header(), Log: func(s string) {
			mu.Lock()
			lines = append(lines, s)
			mu.Unlock()
		}})
		for range 3 {
			l.Append(eventlog.KindPost, 0, 1, 1, 1, nil, nil)
			l.Flush()
		}
		l.Close()
		if w, d := l.Stats(); w != 0 || d != 4 {
			t.Fatalf("dir %q: written %d dropped %d", dir, w, d)
		}
		mu.Lock()
		if len(lines) != 1 {
			t.Fatalf("dir %q: lines %v", dir, lines)
		}
		mu.Unlock()
	}
}

func TestAppendAfterClose(t *testing.T) {
	l := Open(Config{Dir: t.TempDir(), Header: header()})
	l.Close()
	l.Append(eventlog.KindPost, 0, 1, 1, 1, nil, nil)
	l.Close()
	if w, d := l.Stats(); w != 1 || d != 1 {
		t.Fatalf("written %d dropped %d", w, d)
	}
}

// A worker that comes back with the same incarnation does not overwrite the files already there.
func TestNumbersAfterExistingFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"3-1.mel", "3-2.mel", "4-7.mel"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	l := Open(Config{Dir: dir, Header: header()})
	l.Append(eventlog.KindPost, 0, 1, 1, 1, nil, nil)
	l.Close()
	if _, err := os.Stat(filepath.Join(dir, "3-3.mel")); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	l := Open(Config{Dir: dir, Header: header()})
	var wg sync.WaitGroup
	const workers, each = 8, 500
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				l.Append(eventlog.KindPost, 0, int64(w), int64(i), uint64(w), nil, []byte("p"))
			}
		}()
	}
	wg.Wait()
	l.Close()
	fs, _ := eventlog.Files(dir)
	seen := map[uint64]bool{}
	for _, f := range fs {
		_, recs := readFile(t, f.Path)
		for _, r := range recs {
			if seen[r.Seq] {
				t.Fatalf("seq %d twice", r.Seq)
			}
			seen[r.Seq] = true
		}
	}
	if len(seen) != workers*each+1 {
		t.Fatalf("frames %d", len(seen))
	}
}

func TestMemory(t *testing.T) {
	l := Memory()
	l.Append(eventlog.KindOrderCall, eventlog.OpSubmit, 0, 5, 1, []byte("a"), []byte("b"))
	recs := l.Records()
	if len(recs) != 1 || string(recs[0].Body) != "ab" || recs[0].Seq != 1 || recs[0].TNs != 5 {
		t.Fatalf("records %+v", recs)
	}
	l.Close()
	if w, d := l.Stats(); w != 1 || d != 0 {
		t.Fatalf("stats %d %d", w, d)
	}
}

func TestOutcome(t *testing.T) {
	for _, c := range []struct {
		err   error
		flags uint16
		is    error
	}{
		{nil, eventlog.OK, nil},
		{nats.ErrNoResponders, eventlog.NoResponders, nats.ErrNoResponders},
		{fmt.Errorf("fetch: %w", nats.ErrNoResponders), eventlog.NoResponders, nats.ErrNoResponders},
		{context.DeadlineExceeded, eventlog.Timeout, context.DeadlineExceeded},
		{nats.ErrTimeout, eventlog.Timeout, nats.ErrTimeout},
		{fmt.Errorf("fetch: %w", nats.ErrTimeout), eventlog.Timeout, nats.ErrTimeout},
		{context.Canceled, eventlog.Canceled, context.Canceled},
		{errors.New("bad reply"), eventlog.Failed, nil},
	} {
		flags, text := Outcome(c.err)
		if flags != c.flags {
			t.Fatalf("%v: flags %d, want %d", c.err, flags, c.flags)
		}
		back := Err(flags, []byte(text))
		if c.err == nil {
			if back != nil || text != "" {
				t.Fatalf("nil came back as %v %q", back, text)
			}
			continue
		}
		if back.Error() != c.err.Error() {
			t.Fatalf("%v came back as %q", c.err, back.Error())
		}
		if c.is != nil && !errors.Is(back, c.is) {
			t.Fatalf("%v came back as %v, which is not %v", c.err, back, c.is)
		}
		if f2, t2 := Outcome(back); f2 != flags || t2 != text {
			t.Fatalf("%v: %d %q then %d %q", c.err, flags, text, f2, t2)
		}
	}
}

func BenchmarkAppend(b *testing.B) {
	l := Open(Config{Dir: b.TempDir(), Header: header()})
	defer l.Close()
	body := make([]byte, 64)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		l.Append(eventlog.KindMDFeed, 0, int64(i), int64(i), 0, nil, body)
	}
}

type stuck struct {
	f       File
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *stuck) Write(p []byte) (int, error) {
	s.once.Do(func() {
		s.entered <- struct{}{}
		<-s.release
	})
	return s.f.Write(p)
}

func (s *stuck) Sync() error  { return s.f.Sync() }
func (s *stuck) Close() error { return s.f.Close() }

func readFile(t *testing.T, path string) (eventlog.Header, []eventlog.Record) {
	t.Helper()
	r, err := eventlog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out []eventlog.Record
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			return r.Header(), out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rec)
	}
}
