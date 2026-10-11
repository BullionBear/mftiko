package conn

import (
	"context"
	"time"

	"github.com/BullionBear/mftiko/exchange"
)

// nextDelay is how long to wait before the next dial. connected means the attempt that just
// finished had a socket, so the following dial is immediate and the failure window resets. A
// failed dial waits base, then twice the previous wait, and stops doubling at max. retryAfter
// below zero disables the wait. Zero retryAfter and retryMax mean 1s and 30s.
func nextDelay(wait time.Duration, connected bool, retryAfter, retryMax time.Duration) time.Duration {
	if connected || retryAfter < 0 {
		return 0
	}
	base := time.Second
	if retryAfter > 0 {
		base = retryAfter
	}
	max := 30 * time.Second
	if retryMax > 0 {
		max = retryMax
	}
	if max < base {
		max = base
	}
	if wait <= 0 {
		return base
	}
	n := wait * 2
	if n <= wait || n > max {
		return max
	}
	return n
}

// limiter is a sliding window. Every rate has to have room; the wait is the longest of them.
// N or Per of zero or less turns that one limit off. A hit is recorded when the message is sent,
// not when the wait starts.
type limiter struct {
	rates []exchange.RateLimit
	hits  [][]time.Time
}

func newLimiter(rates []exchange.RateLimit) *limiter {
	return &limiter{
		rates: append([]exchange.RateLimit(nil), rates...),
		hits:  make([][]time.Time, len(rates)),
	}
}

func (l *limiter) wait(ctx context.Context) error {
	if l == nil || len(l.rates) == 0 {
		return nil
	}
	for {
		now := time.Now()
		d := l.delay(now)
		if d <= 0 {
			l.mark(now)
			return nil
		}
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// delay drops hits that are already outside their window. A hit exactly Per ago is outside, so
// the next send can go at that instant. When a window is full, the wait is until its oldest
// counted hit ages out.
func (l *limiter) delay(now time.Time) time.Duration {
	var wait time.Duration
	for i, r := range l.rates {
		if r.N <= 0 || r.Per <= 0 {
			continue
		}
		h := l.hits[i]
		cut := now.Add(-r.Per)
		k := 0
		for k < len(h) && !h[k].After(cut) {
			k++
		}
		h = h[k:]
		l.hits[i] = h
		if len(h) < r.N {
			continue
		}
		oldest := h[len(h)-r.N]
		if d := oldest.Add(r.Per).Sub(now); d > wait {
			wait = d
		}
	}
	return wait
}

func (l *limiter) mark(now time.Time) {
	for i, r := range l.rates {
		if r.N <= 0 || r.Per <= 0 {
			continue
		}
		l.hits[i] = append(l.hits[i], now)
	}
}
