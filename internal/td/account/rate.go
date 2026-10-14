package account

import (
	"time"

	"github.com/BullionBear/mftiko/exchange"
)

// limiter is the account's order-rate gate (D621, D420). The book goroutine owns it.
// A window is used up at 90% (used*10 >= limit*9). A 429 or 418 pauses every send,
// including queries, until Retry-After, else the reported reset, else the configured hold.
type limiter struct {
	wins       []rateWin
	pauseUntil time.Time
}

type rateWin struct {
	limit      int
	per        time.Duration
	bucket     string
	sent       []time.Time
	reported   int
	resetAt    time.Time
	sentAfter  int
	reportedOK bool
}

func newLimiter(rs []exchange.RateLimit) *limiter {
	l := &limiter{}
	for _, r := range rs {
		if r.N <= 0 || r.Per <= 0 {
			continue
		}
		l.wins = append(l.wins, rateWin{limit: r.N, per: r.Per, bucket: r.Bucket})
	}
	return l
}

// allow is false when any window is at 90% or a pause is in force.
func (l *limiter) allow(now time.Time) bool {
	if l == nil {
		return true
	}
	if l.paused(now) {
		return false
	}
	for i := range l.wins {
		if l.wins[i].over(now) {
			return false
		}
	}
	return true
}

func (l *limiter) paused(now time.Time) bool {
	return l != nil && !l.pauseUntil.IsZero() && now.Before(l.pauseUntil)
}

// consume counts one order request the worker is about to send.
func (l *limiter) consume(now time.Time) {
	if l == nil {
		return
	}
	for i := range l.wins {
		w := &l.wins[i]
		w.prune(now)
		w.sent = append(w.sent, now)
		if w.reportedOK && (w.resetAt.IsZero() || now.Before(w.resetAt)) {
			w.sentAfter++
		}
	}
}

// note records usage the venue reported. The latest reset among the reported buckets is
// what a 429 without Retry-After waits for.
func (l *limiter) note(now time.Time, used []exchange.Used) time.Time {
	var reset time.Time
	if l == nil {
		return reset
	}
	for _, u := range used {
		if !u.ResetAt.IsZero() && u.ResetAt.After(reset) {
			reset = u.ResetAt
		}
		for i := range l.wins {
			w := &l.wins[i]
			if w.bucket == "" || u.Bucket != w.bucket {
				continue
			}
			if !u.ResetAt.IsZero() && !now.Before(u.ResetAt) {
				continue
			}
			w.reported, w.resetAt, w.sentAfter, w.reportedOK = u.N, u.ResetAt, 0, true
		}
	}
	return reset
}

// pause stops sends. retryAfter wins, then resetAt, then hold (D420).
func (l *limiter) pause(now time.Time, retryAfter, hold time.Duration, resetAt time.Time) {
	if l == nil {
		return
	}
	until := now.Add(hold)
	switch {
	case retryAfter > 0:
		until = now.Add(retryAfter)
	case !resetAt.IsZero() && resetAt.After(now):
		until = resetAt
	}
	if until.After(l.pauseUntil) {
		l.pauseUntil = until
	}
}

func (w *rateWin) over(now time.Time) bool {
	return w.used(now)*10 >= w.limit*9
}

func (w *rateWin) used(now time.Time) int {
	w.prune(now)
	n := len(w.sent)
	if w.reportedOK && (w.resetAt.IsZero() || now.Before(w.resetAt)) {
		if r := w.reported + w.sentAfter; r > n {
			n = r
		}
	}
	return n
}

func (w *rateWin) prune(now time.Time) {
	cut := now.Add(-w.per)
	i := 0
	for i < len(w.sent) && !w.sent[i].After(cut) {
		i++
	}
	if i > 0 {
		w.sent = w.sent[i:]
	}
}
