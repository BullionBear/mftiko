package sym

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// countREST counts Listing calls. The other methods come from listREST.
type countREST struct {
	listREST
	n *atomic.Int32
}

func (c countREST) Listing(ctx context.Context, cat symbol.Category) ([]symbol.Listing, error) {
	if c.n != nil {
		c.n.Add(1)
	}
	return c.listREST.Listing(ctx, cat)
}

func TestStartupRefreshAgainstStore(t *testing.T) {
	srv, conn := startSym(t)
	ctx := context.Background()
	row := spotL("BTC", "USDT", "BTCUSDT")

	t.Run("empty database has an active row within a minute", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{7}})
		if has, err := st.HasActive(ctx, "Bybit", symbol.Spot); err != nil || has {
			t.Fatalf("has = %v err = %v, want empty", has, err)
		}
		if _, err := st.HasActive(ctx, "", symbol.Spot); err == nil {
			t.Fatal("empty venue was accepted")
		}
		var calls atomic.Int32
		startFeed(t, st, &calls, row)
		waitActive(t, st, "Bybit", symbol.Spot)
		if calls.Load() < 1 {
			t.Fatal("active row appeared without a refresh")
		}
	})

	t.Run("populated feed does not refresh immediately", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{7}})
		mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{row}, zerolog.Nop())
		if has, err := st.HasActive(ctx, "Bybit", symbol.Spot); err != nil || !has {
			t.Fatalf("has = %v err = %v", has, err)
		}
		var calls atomic.Int32
		r := newStartupRunner(t, st, &calls, row)
		waited := make(chan time.Duration, 1)
		r.delay = func(ctx context.Context, d time.Duration) error {
			select {
			case waited <- d:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
		runCtx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			r.Run(runCtx)
			close(done)
		}()
		select {
		case d := <-waited:
			if d != time.Hour-1 {
				t.Fatalf("first wait = %s, want the jitter draw", d)
			}
			if calls.Load() != 0 {
				t.Fatalf("refresh calls = %d, want 0", calls.Load())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("runner did not reach the jitter wait")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("runner did not stop")
		}
	})

	t.Run("terminal rows are not an active table", func(t *testing.T) {
		truncate(t, ctx, conn)
		st := openStore(t, srv.DSN(), &script{uids: []uint32{7, 8}})
		mustRefresh(t, st, symbol.Spot, 100, []symbol.Listing{row}, zerolog.Nop())
		mustRefresh(t, st, symbol.Spot, 200, nil, zerolog.Nop())
		if has, err := st.HasActive(ctx, "Bybit", symbol.Spot); err != nil || has {
			t.Fatalf("has = %v err = %v, want no active row", has, err)
		}
		var calls atomic.Int32
		startFeed(t, st, &calls, row)
		waitActive(t, st, "Bybit", symbol.Spot)
		if calls.Load() < 1 {
			t.Fatal("active row appeared without a refresh")
		}
	})
}

func startFeed(t *testing.T, st *Store, calls *atomic.Int32, row symbol.Listing) {
	t.Helper()
	r := newStartupRunner(t, st, calls, row)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("runner did not stop")
		}
	})
}

func newStartupRunner(t *testing.T, st *Store, calls *atomic.Int32, row symbol.Listing) *Runner {
	t.Helper()
	r, err := NewRunner(st, []Feed{{
		Venue:    "Bybit",
		Category: symbol.Spot,
		REST:     countREST{listREST: listREST{rows: []symbol.Listing{row}}, n: calls},
	}}, Options{
		Interval: time.Hour,
		Jitter:   time.Hour,
		RandIntn: func(n int64) int64 {
			if n <= 0 {
				return 0
			}
			return n - 1
		},
		Log: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// waitActive polls until the feed has an active row. One minute is the bound
// for an empty database (#405). The runner under test draws a one-hour jitter,
// so a pass means that feed did not wait the draw out.
func waitActive(t *testing.T, st *Store, venue string, category symbol.Category) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		has, err := st.HasActive(context.Background(), venue, category)
		if err != nil {
			t.Fatal(err)
		}
		if has {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s %s has no active row after one minute", venue, category)
		}
		select {
		case <-time.After(5 * time.Millisecond):
		}
	}
}
