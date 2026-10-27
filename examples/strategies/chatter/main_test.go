package main

import (
	"bytes"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/sdk"
)

// fakeCtx is the slice of Context chatter uses. The other methods are unused.
type fakeCtx struct {
	sdk.Context
	params []byte
}

func (f *fakeCtx) Params() []byte { return f.params }

// lockedBuf is a bytes.Buffer the writer goroutine and the test can share.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestChatterWritesNumberedLines(t *testing.T) {
	var out, errw lockedBuf
	c := &chatter{out: &out, errw: &errw}
	f := &fakeCtx{params: []byte(`{"lines":30,"bytes":60,"stderr_every":10}`)}
	if err := c.OnInit(f); err != nil {
		t.Fatal(err)
	}
	if err := c.OnStart(f); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for strings.Count(out.String(), "\n") < 30 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	c.OnStop(f)
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 30 {
		t.Fatalf("%d stdout lines", len(lines))
	}
	for i, l := range lines {
		n := strconv.Itoa(i + 1)
		if len(l)+1 != 60 || !strings.HasPrefix(l, "chatter "+n+" "+n+" ") {
			t.Fatalf("line %d is %q", i+1, l)
		}
	}
	if got := strings.Count(errw.String(), "\n"); got != 3 || !strings.HasPrefix(errw.String(), "chatter 10 10 ") {
		t.Fatalf("stderr %q", errw.String())
	}
}

func TestChatterStopsOnStop(t *testing.T) {
	var out lockedBuf
	c := &chatter{out: &out, errw: &lockedBuf{}}
	f := &fakeCtx{params: []byte(`{"rate":1000}`)}
	if err := c.OnInit(f); err != nil {
		t.Fatal(err)
	}
	if err := c.OnStart(f); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	c.OnStop(f)
	n := out.String()
	time.Sleep(20 * time.Millisecond)
	if out.String() != n || n == "" {
		t.Fatalf("wrote %d bytes, then %d after OnStop", len(n), len(out.String()))
	}
}

func TestChatterRejectsBadParams(t *testing.T) {
	for _, raw := range []string{`{`, `{"bytes":50}`, `{"bytes":5000}`, `{"rate":-1}`} {
		c := &chatter{out: &lockedBuf{}, errw: &lockedBuf{}}
		if err := c.OnInit(&fakeCtx{params: []byte(raw)}); err == nil {
			t.Errorf("%s was accepted", raw)
		}
		c.OnStop(&fakeCtx{})
	}
}

// A rate above one line per tick is kept by writing several lines per tick.
func TestChatterHighRate(t *testing.T) {
	var out lockedBuf
	c := &chatter{out: &out, errw: &lockedBuf{}}
	f := &fakeCtx{params: []byte(`{"rate":20000,"lines":4000}`)}
	if err := c.OnInit(f); err != nil {
		t.Fatal(err)
	}
	begin := time.Now()
	if err := c.OnStart(f); err != nil {
		t.Fatal(err)
	}
	for strings.Count(out.String(), "\n") < 4000 && time.Since(begin) < 5*time.Second {
		time.Sleep(time.Millisecond)
	}
	took := time.Since(begin)
	c.OnStop(f)
	if n := strings.Count(out.String(), "\n"); n != 4000 || took < 150*time.Millisecond || took > 2*time.Second {
		t.Fatalf("%d lines in %s, want 4000 in about 200ms", n, took)
	}
}
