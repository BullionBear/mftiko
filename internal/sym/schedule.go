package sym

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

const (
	// DefaultInterval is how often one (venue, category) feed is refreshed
	// after the first run.
	DefaultInterval = time.Hour
	// DefaultJitter is the width of the extra delay drawn uniformly from
	// [0, DefaultJitter). The first wait is only that draw, so feeds that
	// start together do not call the venues together and a restart does not
	// wait a full hour for the first listing.
	DefaultJitter = 6 * time.Minute
)

// ErrUnknownVenue is returned when Trigger names a venue no feed uses.
var ErrUnknownVenue = errors.New("sym: unknown venue")

// refresher is the store operation Runner schedules. *Store implements it.
type refresher interface {
	Refresh(ctx context.Context, f Feed, atNs int64, log zerolog.Logger) (Report, error)
}

// Options tunes a Runner. The zero value refreshes hourly with DefaultJitter.
type Options struct {
	// Interval is the gap between refreshes of one feed, before jitter.
	// Zero and negative select DefaultInterval.
	Interval time.Duration
	// Jitter is the width of the uniform extra delay. Zero selects
	// DefaultJitter unless NoJitter is set. Negative selects DefaultJitter.
	Jitter time.Duration
	// NoJitter forces a zero extra delay. Tests use it so a run does not sleep.
	NoJitter bool
	// Now is the clock. Nil uses time.Now.
	Now func() time.Time
	// RandIntn returns a value in [0, n). Nil uses math/rand/v2. n <= 0 yields 0.
	RandIntn func(n int64) int64
	Log      zerolog.Logger
}

// Runner refreshes each feed on its own clock. One feed's error is logged and
// does not stop the others. Trigger runs a venue immediately, or every feed
// when the venue is empty.
type Runner struct {
	refresh  refresher
	feeds    []*feedRun
	interval time.Duration
	jitter   time.Duration
	now      func() time.Time
	randIntn func(int64) int64
	log      zerolog.Logger
}

type feedRun struct {
	feed Feed
	mu   sync.Mutex
}

// NewRunner schedules feeds. A feed needs a venue, a category and a REST reader.
// Two feeds for the same (venue, category) are rejected.
func NewRunner(ref refresher, feeds []Feed, opt Options) (*Runner, error) {
	if ref == nil {
		return nil, errors.New("sym: refresher is nil")
	}
	interval := opt.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	jitter := opt.Jitter
	if opt.NoJitter {
		jitter = 0
	} else if jitter <= 0 {
		jitter = DefaultJitter
	}
	now := opt.Now
	if now == nil {
		now = time.Now
	}
	randIntn := opt.RandIntn
	if randIntn == nil {
		randIntn = func(n int64) int64 {
			if n <= 0 {
				return 0
			}
			return rand.Int64N(n)
		}
	}
	seen := map[string]struct{}{}
	runs := make([]*feedRun, 0, len(feeds))
	for _, f := range feeds {
		if f.Venue == "" || f.REST == nil || !knownCategory(f.Category) {
			return nil, fmt.Errorf("sym: feed %s %s is incomplete", f.Venue, f.Category)
		}
		key := feedKey(f.Venue, f.Category)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("sym: duplicate feed %s %s", f.Venue, f.Category)
		}
		seen[key] = struct{}{}
		runs = append(runs, &feedRun{feed: f})
	}
	return &Runner{
		refresh:  ref,
		feeds:    runs,
		interval: interval,
		jitter:   jitter,
		now:      now,
		randIntn: randIntn,
		log:      opt.Log,
	}, nil
}

// FeedsFromRegistry builds one feed per (venue, category) that has a public
// REST reader. A venue with a nil reader is skipped. Categories stay in the
// order the venue declared them.
func FeedsFromRegistry(reg *exchange.Registry) []Feed {
	if reg == nil {
		return nil
	}
	var out []Feed
	for _, name := range reg.Venues() {
		v, ok := reg.Venue(name)
		if !ok || v.REST == nil {
			continue
		}
		for _, c := range v.Categories {
			out = append(out, Feed{Venue: name, Category: c, REST: v.REST})
		}
	}
	return out
}

// Run refreshes until ctx is cancelled. Each feed has its own loop. The first
// wait is only the jitter draw; every later wait is the interval plus a new draw.
func (r *Runner) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, f := range r.feeds {
		wg.Add(1)
		go func(f *feedRun) {
			defer wg.Done()
			r.loop(ctx, f)
		}(f)
	}
	wg.Wait()
}

// Trigger refreshes venue now, or every feed when venue is empty. Feeds run
// concurrently. One feed's error is Report.Err and does not cancel the others.
// A venue that matches nothing returns ErrUnknownVenue.
func (r *Runner) Trigger(ctx context.Context, venue string) ([]Report, error) {
	var chosen []*feedRun
	for _, f := range r.feeds {
		if venue == "" || f.feed.Venue == venue {
			chosen = append(chosen, f)
		}
	}
	if venue != "" && len(chosen) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrUnknownVenue, venue)
	}
	reports := make([]Report, len(chosen))
	var wg sync.WaitGroup
	for i, f := range chosen {
		wg.Add(1)
		go func(i int, f *feedRun) {
			defer wg.Done()
			reports[i] = r.refreshFeed(ctx, f)
		}(i, f)
	}
	wg.Wait()
	return reports, nil
}

func (r *Runner) loop(ctx context.Context, f *feedRun) {
	first := true
	for {
		d := nextWait(r.interval, r.jitter, first, r.randIntn)
		first = false
		if err := wait(ctx, d); err != nil {
			return
		}
		rep := r.refreshFeed(ctx, f)
		if ctx.Err() != nil {
			return
		}
		if rep.Err != nil {
			r.log.Error().
				Err(rep.Err).
				Str("venue", f.feed.Venue).
				Str("category", string(f.feed.Category)).
				Msg("refresh failed")
			continue
		}
		ev := r.log.Info().
			Str("venue", f.feed.Venue).
			Str("category", string(f.feed.Category)).
			Int("seen", rep.Seen).
			Int("rejected", rep.Rejected).
			Int("inserted", rep.Inserted).
			Int("updated", rep.Updated).
			Int("delisted", rep.Delisted).
			Int("expired", rep.Expired).
			Bool("changed", rep.Changed).
			Int64("revision", rep.Revision)
		ev.Msg("refreshed")
	}
}

func (r *Runner) refreshFeed(ctx context.Context, f *feedRun) Report {
	f.mu.Lock()
	defer f.mu.Unlock()
	rep, err := r.refresh.Refresh(ctx, f.feed, r.now().UnixNano(), r.log)
	if err != nil {
		rep.Venue = f.feed.Venue
		rep.Category = f.feed.Category
		rep.Err = err
	}
	return rep
}

// nextWait is the delay before a refresh. The first delay is only jitter, in
// [0, jitter). Later delays are interval plus a fresh draw from that range.
func nextWait(interval, jitter time.Duration, first bool, randIntn func(int64) int64) time.Duration {
	extra := time.Duration(0)
	if jitter > 0 && randIntn != nil {
		extra = time.Duration(randIntn(int64(jitter)))
		if extra < 0 {
			extra = 0
		}
	}
	if first {
		return extra
	}
	if interval < 0 {
		interval = 0
	}
	return interval + extra
}

func wait(ctx context.Context, d time.Duration) error {
	if d < 0 {
		d = 0
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func feedKey(venue string, category symbol.Category) string {
	// '/' is not part of a venue or category spelling, and unlike NUL it is
	// valid in a Postgres text parameter used as the advisory-lock key.
	return venue + "/" + string(category)
}

func knownCategory(c symbol.Category) bool {
	switch c {
	case symbol.Spot, symbol.Perp, symbol.Inverse, symbol.Future, symbol.Option:
		return true
	default:
		return false
	}
}
