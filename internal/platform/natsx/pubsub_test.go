package natsx_test

import (
	"bytes"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/natsx"
)

func TestPublishSubscribe(t *testing.T) {
	_, c := start(t)
	// 0x00 is the RPC error mark. Pub/sub must not treat it as one, and must not add bytes.
	payloads := [][]byte{nil, {}, {0x00, 0x01, 0xff}, []byte("md.f-shaped")}
	got := make(chan []byte, len(payloads))
	if _, err := c.Subscribe(t.Context(), "tape", "", func(m natsx.Msg) {
		got <- bytes.Clone(m.Data)
	}); err != nil {
		t.Fatal(err)
	}
	for _, p := range payloads {
		if err := c.Publish("tape", p); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range payloads {
		gotb := await(t, got)
		if !bytes.Equal(gotb, want) {
			t.Fatalf("message %d: got %x, want %x", i, gotb, want)
		}
	}
}

func TestQueueGroupDeliversOnce(t *testing.T) {
	_, c := start(t)
	const n = 32
	got := make(chan byte, n*2)
	for range 2 {
		if _, err := c.Subscribe(t.Context(), "work", "workers", func(m natsx.Msg) {
			if len(m.Data) != 1 {
				t.Errorf("len %d", len(m.Data))
				return
			}
			got <- m.Data[0]
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		if err := c.Publish("work", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[byte]int{}
	for range n {
		seen[await(t, got)]++
	}
	select {
	case extra := <-got:
		t.Fatalf("extra delivery %d", extra)
	default:
	}
	if len(seen) != n {
		t.Fatalf("delivered %d distinct, want %d (%v)", len(seen), n, seen)
	}
}

func TestFanoutWithoutQueue(t *testing.T) {
	_, c := start(t)
	a := make(chan struct{})
	b := make(chan struct{})
	for _, ch := range []chan struct{}{a, b} {
		if _, err := c.Subscribe(t.Context(), "both", "", func(natsx.Msg) { close(ch) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Publish("both", []byte("x")); err != nil {
		t.Fatal(err)
	}
	await(t, a)
	await(t, b)
}

func TestUnsubscribe(t *testing.T) {
	_, c := start(t)
	dead := make(chan struct{}, 1)
	sub, err := c.Subscribe(t.Context(), "live", "", func(natsx.Msg) { dead <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("second unsubscribe: %v", err)
	}
	live := make(chan struct{})
	if _, err := c.Subscribe(t.Context(), "live", "", func(natsx.Msg) { close(live) }); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish("live", []byte("x")); err != nil {
		t.Fatal(err)
	}
	await(t, live)
	select {
	case <-dead:
		t.Fatal("unsubscribed callback ran")
	default:
	}
}
