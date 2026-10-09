package sym

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest"
)

func TestNextWait(t *testing.T) {
	draw := func(n int64) int64 { return n - 1 }
	if got := nextWait(time.Hour, 10, true, draw); got != 9 {
		t.Fatalf("first wait = %s, want 9ns", got)
	}
	if got := nextWait(time.Hour, 10, false, func(int64) int64 { return 0 }); got != time.Hour {
		t.Fatalf("later wait = %s, want 1h", got)
	}
	called := false
	if got := nextWait(time.Hour, 0, false, func(int64) int64 {
		called = true
		return 1
	}); got != time.Hour || called {
		t.Fatalf("zero jitter wait = %s, rand called = %v", got, called)
	}
}

func TestRunnerDefaults(t *testing.T) {
	r, err := NewRunner(okRefresh{}, nil, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if r.interval != DefaultInterval || r.jitter != DefaultJitter {
		t.Fatalf("interval %s jitter %s", r.interval, r.jitter)
	}
	r, err = NewRunner(okRefresh{}, nil, Options{NoJitter: true, Interval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if r.interval != time.Minute || r.jitter != 0 {
		t.Fatalf("interval %s jitter %s", r.interval, r.jitter)
	}
}

func TestRunnerTriggerIsolatesFeeds(t *testing.T) {
	spotStarted := make(chan struct{})
	release := make(chan struct{})
	perpDone := make(chan struct{})
	var once sync.Once
	fake := &fakeRefresh{fn: func(f Feed) (Report, error) {
		if f.Category == symbol.Spot {
			once.Do(func() { close(spotStarted) })
			<-release
			return Report{}, errors.New("spot down")
		}
		close(perpDone)
		return Report{Venue: f.Venue, Category: f.Category, Seen: 3}, nil
	}}
	r := testRunner(t, fake, []Feed{
		{Venue: "Bybit", Category: symbol.Spot, REST: noopREST{}},
		{Venue: "Bybit", Category: symbol.Perp, REST: noopREST{}},
	})
	done := make(chan []Report, 1)
	go func() {
		reps, err := r.Trigger(context.Background(), "")
		if err != nil {
			t.Errorf("trigger: %v", err)
		}
		done <- reps
	}()
	select {
	case <-spotStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("spot did not start")
	}
	select {
	case <-perpDone:
	case <-time.After(2 * time.Second):
		t.Fatal("perp did not finish while spot was blocked")
	}
	close(release)
	var reps []Report
	select {
	case reps = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("trigger did not return")
	}
	if len(reps) != 2 {
		t.Fatalf("reports = %d", len(reps))
	}
	var sawSpot, sawPerp bool
	for _, rep := range reps {
		switch rep.Category {
		case symbol.Spot:
			sawSpot = true
			if rep.Err == nil || rep.Err.Error() != "spot down" {
				t.Fatalf("spot err = %v", rep.Err)
			}
		case symbol.Perp:
			sawPerp = true
			if rep.Err != nil || rep.Seen != 3 {
				t.Fatalf("perp report = %+v", rep)
			}
		default:
			t.Fatalf("unexpected %s", rep.Category)
		}
	}
	if !sawSpot || !sawPerp {
		t.Fatal("missing a report")
	}
}

func TestRunnerRetriesAfterFailure(t *testing.T) {
	var n atomic.Int32
	fake := &fakeRefresh{fn: func(f Feed) (Report, error) {
		if n.Add(1) == 1 {
			return Report{}, errors.New("down")
		}
		return Report{Venue: f.Venue, Category: f.Category}, nil
	}}
	r := testRunner(t, fake, []Feed{{Venue: "Bybit", Category: symbol.Spot, REST: noopREST{}}})
	r.interval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()
	deadline := time.After(2 * time.Second)
	for n.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("refresh calls = %d, want at least 2", n.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestRunnerTriggerSelectsVenue(t *testing.T) {
	var mu sync.Mutex
	var got []string
	fake := &fakeRefresh{fn: func(f Feed) (Report, error) {
		mu.Lock()
		got = append(got, f.Venue+"/"+string(f.Category))
		mu.Unlock()
		return Report{Venue: f.Venue, Category: f.Category}, nil
	}}
	r := testRunner(t, fake, []Feed{
		{Venue: "Bybit", Category: symbol.Spot, REST: noopREST{}},
		{Venue: "Bybit", Category: symbol.Perp, REST: noopREST{}},
		{Venue: "Okx", Category: symbol.Spot, REST: noopREST{}},
	})
	reps, err := r.Trigger(context.Background(), "Bybit")
	if err != nil {
		t.Fatal(err)
	}
	if len(reps) != 2 {
		t.Fatalf("reports = %d, want 2", len(reps))
	}
	mu.Lock()
	defer mu.Unlock()
	for _, name := range got {
		if name == "Okx/Spot" {
			t.Fatal("Okx was refreshed")
		}
	}
	if len(got) != 2 {
		t.Fatalf("calls = %v", got)
	}
	if _, err := r.Trigger(context.Background(), "Nope"); !errors.Is(err, ErrUnknownVenue) {
		t.Fatalf("err = %v, want ErrUnknownVenue", err)
	}
}

func TestFeedsFromRegistry(t *testing.T) {
	bybit, err := exchangetest.New("Bybit")
	if err != nil {
		t.Fatal(err)
	}
	reg, err := exchange.NewRegistry(exchange.Declaration{
		Exchange: "test",
		Venues: []exchange.Venue{
			{Venue: symbol.Venue{Name: "Bybit", Categories: []symbol.Category{symbol.Perp, symbol.Spot}}, REST: bybit},
			{Venue: symbol.Venue{Name: "Okx", Categories: []symbol.Category{symbol.Spot}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	feeds := FeedsFromRegistry(reg)
	if len(feeds) != 2 || feeds[0].Venue != "Bybit" || feeds[0].Category != symbol.Perp || feeds[1].Category != symbol.Spot {
		t.Fatalf("feeds = %+v", feeds)
	}
	if feeds[0].REST != bybit {
		t.Fatal("REST was not the venue reader")
	}
}

func TestNewRunnerRejectsDuplicate(t *testing.T) {
	feeds := []Feed{
		{Venue: "Bybit", Category: symbol.Spot, REST: noopREST{}},
		{Venue: "Bybit", Category: symbol.Spot, REST: noopREST{}},
	}
	if _, err := NewRunner(okRefresh{}, feeds, Options{NoJitter: true}); err == nil {
		t.Fatal("expected duplicate feed to be rejected")
	}
}

type okRefresh struct{}

func (okRefresh) Refresh(context.Context, Feed, int64, zerolog.Logger) (Report, error) {
	return Report{}, nil
}

type fakeRefresh struct {
	fn func(Feed) (Report, error)
}

func (f *fakeRefresh) Refresh(_ context.Context, feed Feed, _ int64, _ zerolog.Logger) (Report, error) {
	if f.fn == nil {
		return Report{Venue: feed.Venue, Category: feed.Category}, nil
	}
	return f.fn(feed)
}

func testRunner(t *testing.T, ref refresher, feeds []Feed) *Runner {
	t.Helper()
	r, err := NewRunner(ref, feeds, Options{NoJitter: true, Log: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

type noopREST struct{}

func (noopREST) Listing(context.Context, symbol.Category) ([]symbol.Listing, error) {
	return nil, nil
}
func (noopREST) Depth(context.Context, *symbol.Info, int) (*exchange.BookUpdate, error) {
	return nil, exchange.ErrNotImplemented
}
func (noopREST) Klines(context.Context, *symbol.Info, string, int) ([]exchange.Kline, error) {
	return nil, exchange.ErrNotImplemented
}
func (noopREST) KlinePage() int { return 0 }
func (noopREST) OpenInterest(context.Context, *symbol.Info) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}
