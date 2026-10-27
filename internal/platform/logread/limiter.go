//go:build unix

package logread

import (
	"context"
	"sync"
	"time"
)

// Limiter caps how many reads run at once and how fast their bytes leave the
// disk (E14-D007). A nil Limiter imposes neither limit. maxConcurrent or
// bytesPerSec of zero or less turns that half off. The bucket holds one second
// of credit, so a fresh limiter can emit a second of bytes immediately and
// then settles at bytesPerSec. Callers turn a full cap into HTTP 503; WaitN
// does not queue for a slot.
type Limiter struct {
	mu sync.Mutex

	max int // concurrent reads; 0 means unlimited
	in  int

	rate   int64 // bytes per second; 0 means unlimited
	burst  int64
	tokens int64 // may go negative while a waiter is sleeping off a debt
	asOf   time.Time
	now    func() time.Time
}

// NewLimiter returns a limiter. maxConcurrent is the cap on Open and Export
// together. bytesPerSec is the token-bucket rate shared by those reads.
func NewLimiter(maxConcurrent, bytesPerSec int) *Limiter {
	l := &Limiter{now: time.Now}
	if maxConcurrent > 0 {
		l.max = maxConcurrent
	}
	if bytesPerSec > 0 {
		l.rate = int64(bytesPerSec)
		l.burst = l.rate
		l.tokens = l.burst
		l.asOf = l.now()
	}
	return l
}

func (l *Limiter) acquire() error {
	if l == nil || l.max <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.in >= l.max {
		return ErrBusy
	}
	l.in++
	return nil
}

func (l *Limiter) release() {
	if l == nil || l.max <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.in > 0 {
		l.in--
	}
}

// WaitN blocks until n bytes are allowed, or ctx ends. n larger than the
// one-second burst is taken in burst-sized pieces so a frame still moves at
// the configured rate.
func (l *Limiter) WaitN(ctx context.Context, n int) error {
	if l == nil || l.rate <= 0 || n <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for n > 0 {
		take := n
		if int64(take) > l.burst {
			take = int(l.burst)
		}
		delay := l.reserve(take)
		if delay > 0 {
			if err := ctx.Err(); err != nil {
				l.refund(take)
				return err
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				l.refund(take)
				return ctx.Err()
			case <-timer.C:
			}
		}
		n -= take
	}
	return nil
}

// reserve deducts n bytes and reports how long the caller should wait before
// using them. The deduction is immediate so two waiters cannot spend the same
// tokens.
func (l *Limiter) reserve(n int) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.After(l.asOf) && l.rate > 0 {
		elapsed := now.Sub(l.asOf)
		add := int64(elapsed) * l.rate / int64(time.Second)
		if add > 0 {
			l.tokens += add
			if l.tokens > l.burst {
				l.tokens = l.burst
			}
			l.asOf = l.asOf.Add(time.Duration(add) * time.Second / time.Duration(l.rate))
		}
	}
	l.tokens -= int64(n)
	if l.tokens >= 0 {
		return 0
	}
	return time.Duration(-l.tokens) * time.Second / time.Duration(l.rate)
}

func (l *Limiter) refund(n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tokens += int64(n)
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
}
