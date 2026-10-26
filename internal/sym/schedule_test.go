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
	if r.delay == nil {
		t.Fatal("delay is nil")
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
			{Venue: symbol.Venue{Name: "Bybit", Categories: []symbol.Category{symbol.Perp, symbol.Spot}}, REST: bybit.REST()},
			{Venue: symbol.Venue{Name: "Okx", Categories: []symbol.Category{symbol.Spot}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	feeds := FeedsFromRegistry(reg, bybit)
	if len(feeds) != 2 || feeds[0].Venue != "Bybit" || feeds[0].Category != symbol.Perp || feeds[1].Category != symbol.Spot {
		t.Fatalf("feeds = %+v", feeds)
	}
	// The feed reads the venue's PublicREST over the transport it was given.
	perp := symbol.Listing{Venue: "Bybit", Category: symbol.Perp, Base: "BTC", Quote: "USDT", ExchTicker: "BTCUSDT"}
	if err := bybit.Add(perp); err != nil {
		t.Fatal(err)
	}
	got, err := feeds[0].REST.Listing(context.Background(), symbol.Perp)
	if err != nil || len(got) != 1 || got[0].ExchTicker != "BTCUSDT" {
		t.Fatalf("listing over the transport: %v %v", got, err)
	}
}

func TestStartupRefreshSkipsOnlyEmptyFeeds(t *testing.T) {
	const (
		jitter   = 10 * time.Second
		interval = time.Minute
	)
	draw := func(n int64) int64 { return n - 1 }
	later := interval + jitter - 1
	first := jitter - 1

	t.Run("empty refreshes before the wait", func(t *testing.T) {
		steps, waited := runStartup(t, false, nil, draw, interval, jitter)
		if len(steps) != 2 || steps[0] != "refresh" || steps[1] != "wait" {
			t.Fatalf("steps = %v", steps)
		}
		if waited != later {
			t.Fatalf("wait after the immediate refresh = %s, want %s", waited, later)
		}
	})

	t.Run("populated waits out the jitter", func(t *testing.T) {
		steps, waited := runStartup(t, true, nil, draw, interval, jitter)
		if len(steps) != 1 || steps[0] != "wait" {
			t.Fatalf("steps = %v, want only the jitter wait", steps)
		}
		if waited != first {
			t.Fatalf("first wait = %s, want %s", waited, first)
		}
	})

	t.Run("lookup error keeps the jitter", func(t *testing.T) {
		steps, waited := runStartup(t, false, errors.New("db down"), draw, interval, jitter)
		if len(steps) != 1 || steps[0] != "wait" {
			t.Fatalf("steps = %v, want only the jitter wait", steps)
		}
		if waited != first {
			t.Fatalf("first wait = %s, want %s", waited, first)
		}
	})

	t.Run("failed immediate refresh backs off", func(t *testing.T) {
		var n atomic.Int32
		g := gateRefresh{
			has: func(string, symbol.Category) (bool, error) { return false, nil },
			fn: func(Feed) (Report, error) {
				n.Add(1)
				return Report{}, errors.New("down")
			},
		}
		r := startupRunner(t, g, interval, jitter, draw)
		var waited time.Duration
		r.delay = func(_ context.Context, d time.Duration) error {
			waited = d
			return errStopAfterWait
		}
		r.Run(context.Background())
		if n.Load() != 1 {
			t.Fatalf("refresh calls = %d, want 1", n.Load())
		}
		if waited != later {
			t.Fatalf("backoff = %s, want %s", waited, later)
		}
	})

	t.Run("no lookup keeps the jitter", func(t *testing.T) {
		var refreshed atomic.Bool
		fake := &fakeRefresh{fn: func(Feed) (Report, error) {
			refreshed.Store(true)
			return Report{}, nil
		}}
		r := startupRunner(t, fake, interval, jitter, draw)
		var waited time.Duration
		r.delay = func(_ context.Context, d time.Duration) error {
			waited = d
			return errors.New("stop")
		}
		r.Run(context.Background())
		if refreshed.Load() {
			t.Fatal("refreshed before the jitter wait")
		}
		if waited != first {
			t.Fatalf("first wait = %s, want %s", waited, first)
		}
	})
}

func TestStartupRefreshPerFeed(t *testing.T) {
	const (
		jitter   = 10 * time.Second
		interval = time.Minute
	)
	draw := func(n int64) int64 { return n - 1 }
	var mu sync.Mutex
	refreshed := map[symbol.Category]int{}
	g := gateRefresh{
		has: func(_ string, category symbol.Category) (bool, error) {
			return category == symbol.Perp, nil
		},
		fn: func(f Feed) (Report, error) {
			mu.Lock()
			refreshed[f.Category]++
			mu.Unlock()
			return Report{Venue: f.Venue, Category: f.Category}, nil
		},
	}
	r, err := NewRunner(g, []Feed{
		{Venue: "Bybit", Category: symbol.Spot, REST: noopREST{}},
		{Venue: "Bybit", Category: symbol.Perp, REST: noopREST{}},
	}, Options{
		Interval: interval,
		Jitter:   jitter,
		RandIntn: draw,
		Log:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var delays []time.Duration
	r.delay = func(_ context.Context, d time.Duration) error {
		mu.Lock()
		delays = append(delays, d)
		mu.Unlock()
		return errStopAfterWait
	}
	r.Run(context.Background())
	mu.Lock()
	defer mu.Unlock()
	if refreshed[symbol.Spot] != 1 {
		t.Fatalf("spot refreshes = %d", refreshed[symbol.Spot])
	}
	if refreshed[symbol.Perp] != 0 {
		t.Fatalf("perp refreshes = %d", refreshed[symbol.Perp])
	}
	if len(delays) != 2 {
		t.Fatalf("waits = %v", delays)
	}
	var sawJitter, sawLater bool
	for _, d := range delays {
		switch d {
		case jitter - 1:
			sawJitter = true
		case interval + jitter - 1:
			sawLater = true
		default:
			t.Fatalf("unexpected wait %s", d)
		}
	}
	if !sawJitter || !sawLater {
		t.Fatalf("waits = %v", delays)
	}
}

func runStartup(t *testing.T, has bool, lookup error, draw func(int64) int64, interval, jitter time.Duration) ([]string, time.Duration) {
	t.Helper()
	var mu sync.Mutex
	var steps []string
	var waited time.Duration
	g := gateRefresh{
		has: func(string, symbol.Category) (bool, error) { return has, lookup },
		fn: func(f Feed) (Report, error) {
			mu.Lock()
			steps = append(steps, "refresh")
			mu.Unlock()
			return Report{Venue: f.Venue, Category: f.Category}, nil
		},
	}
	r := startupRunner(t, g, interval, jitter, draw)
	r.delay = func(_ context.Context, d time.Duration) error {
		mu.Lock()
		steps = append(steps, "wait")
		waited = d
		mu.Unlock()
		return errStopAfterWait
	}
	r.Run(context.Background())
	mu.Lock()
	defer mu.Unlock()
	out := append([]string(nil), steps...)
	return out, waited
}

func startupRunner(t *testing.T, ref refresher, interval, jitter time.Duration, draw func(int64) int64) *Runner {
	t.Helper()
	r, err := NewRunner(ref, []Feed{{Venue: "Bybit", Category: symbol.Spot, REST: noopREST{}}}, Options{
		Interval: interval,
		Jitter:   jitter,
		RandIntn: draw,
		Log:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

var errStopAfterWait = errors.New("stop after wait")

type gateRefresh struct {
	has func(venue string, category symbol.Category) (bool, error)
	fn  func(Feed) (Report, error)
}

func (g gateRefresh) HasActive(_ context.Context, venue string, category symbol.Category) (bool, error) {
	if g.has == nil {
		return false, nil
	}
	return g.has(venue, category)
}

func (g gateRefresh) Refresh(_ context.Context, feed Feed, _ int64, _ zerolog.Logger) (Report, error) {
	if g.fn == nil {
		return Report{Venue: feed.Venue, Category: feed.Category}, nil
	}
	return g.fn(feed)
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
