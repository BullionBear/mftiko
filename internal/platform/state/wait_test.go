package state

import (
	"testing"
	"time"
)

// pollInterval is how often a condition wait samples. It is not the bound.
const pollInterval = 20 * time.Millisecond

// until polls cond until it returns nil. within is the upper bound: a true cond
// returns at once, and a false one keeps the bound. what names the condition.
// cond's error is the last observation; a timeout reports both.
func until(t *testing.T, within time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last error
	for {
		last = cond()
		if last == nil {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("waited %s for %s; last: %v", within, what, last)
		}
		time.Sleep(pollInterval)
	}
}

// during polls cond for the whole of within. The first error fails the test.
// Use it for an invariant that has to hold over a window, such as "no stall":
// the window is the bound and is not shortened. what names the invariant.
// cond's error is the observation that broke it.
func during(t *testing.T, within time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if err := cond(); err != nil {
			t.Fatalf("within %s, %s; last: %v", within, what, err)
		}
		if !time.Now().Before(deadline) {
			return
		}
		pause := pollInterval
		if d := time.Until(deadline); d < pause {
			pause = d
		}
		if pause > 0 {
			time.Sleep(pause)
		}
	}
}
