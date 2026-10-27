//go:build unix

package logread

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLimiterSlots(t *testing.T) {
	l := NewLimiter(1, 0)
	if err := l.acquire(); err != nil {
		t.Fatal(err)
	}
	if err := l.acquire(); !errors.Is(err, ErrBusy) {
		t.Fatalf("second acquire: %v", err)
	}
	l.release()
	if err := l.acquire(); err != nil {
		t.Fatal(err)
	}
	l.release()
}

func TestLimiterReserve(t *testing.T) {
	l := NewLimiter(0, 1000)
	now := time.Unix(1_700_000_000, 0)
	l.now = func() time.Time { return now }
	l.asOf = now
	l.tokens = l.burst
	if d := l.reserve(400); d != 0 {
		t.Fatal(d)
	}
	if d := l.reserve(700); d != 100*time.Millisecond {
		t.Fatalf("delay %v", d)
	}
}

func TestLimiterWaitCancel(t *testing.T) {
	l := NewLimiter(0, 10)
	if err := l.WaitN(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.WaitN(ctx, 10); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestLimiterSharedByStreams(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/h.stdout"
	writeFile(t, path, []byte("a\n"))
	lim := NewLimiter(1, 0)
	ctx := context.Background()
	a, err := Open(ctx, Options{Path: path, Limit: lim})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := Open(ctx, Options{Path: path, Limit: lim}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	snap, err := Export(ctx, path, lim)
	if !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if snap != nil {
		snap.Close()
	}
	a.Close()
	b, err := Open(ctx, Options{Path: path, Limit: lim})
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
}
