package natsx_test

import (
	"testing"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/platform/natsx/natsxtest"
)

// await waits for a signal. The timer is only the failure path: a message that arrives
// returns immediately.
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v
	case <-timer.C:
		t.Fatal("no signal within 5s")
	}
	panic("unreachable")
}

func start(t *testing.T) (*natsxtest.Server, *natsx.Conn) {
	t.Helper()
	s := natsxtest.Start(t)
	return s, dial(t, s.URL(), natsx.Options{})
}

func dial(t *testing.T, url string, opt natsx.Options) *natsx.Conn {
	t.Helper()
	c, err := natsx.Connect(url, opt)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}
