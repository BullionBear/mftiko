package agent

import (
	"fmt"
	"testing"
	"time"
)

type pollClock struct {
	t     *testing.T
	kind  string
	what  string
	start time.Time
	every time.Duration
	n     int
}

func beginPoll(t *testing.T, kind, what string, every time.Duration) *pollClock {
	t.Helper()
	return &pollClock{t: t, kind: kind, what: what, start: time.Now(), every: every}
}

func (p *pollClock) slept() {
	if p != nil {
		p.n++
	}
}

func (p *pollClock) done() {
	if p == nil || p.t == nil {
		return
	}
	sleepMs := int64(0)
	if p.every > 0 {
		sleepMs = (time.Duration(p.n) * p.every).Milliseconds()
	}
	measureLog(p.t.Name(), p.kind, p.what, time.Since(p.start).Milliseconds(),
		fmt.Sprintf("\tsleep_ms=%d\tsleeps=%d", sleepMs, p.n))
}
