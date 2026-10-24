package shim

import (
	"testing"
	"time"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

func liveReq(digest string) *shimv1.ShimRequest {
	return &shimv1.ShimRequest{Op: &shimv1.ShimRequest_Live{Live: &workerv1.Live{
		Contract: "fake.v1", Payload: []byte(digest), Digest: digest,
	}}}
}

func isApplied(digest string) func(*shimv1.Event) bool {
	return func(ev *shimv1.Event) bool { return ev.GetApplied() != nil && ev.GetApplied().GetDigest() == digest }
}

// The live config the worker has is what it says in Applied (DECISIONS E01-D011): the shim passes Live on, keeps the
// last Applied in Attached, and appends each to the events. A Live the worker does not apply changes nothing.
func TestC3Live(t *testing.T) {
	p := startShim(t, fakeworker.Script{}, "", "", nil)
	a := dial(t, p.socket())
	if at := a.attachFrom(1, 0); at.GetAppliedDigest() != "" {
		t.Fatalf("applied %q before Init", at.GetAppliedDigest())
	}
	wantOK(t, a.call(&shimv1.ShimRequest{Op: &shimv1.ShimRequest_Init{Init: &workerv1.Init{
		Contract: "fake.v1", LiveDigest: "d1",
	}}}))
	a.until("Applied(d1)", isApplied("d1"))
	a.until("Done(INIT)", isDone(workerv1.Verb_VERB_INIT))
	if at := a.waitPhase(1, shimv1.Phase_PHASE_INITIALIZED); at.GetAppliedDigest() != "d1" {
		t.Fatalf("applied %q after Init, want d1", at.GetAppliedDigest())
	}
	wantOK(t, a.call(liveReq("d2"))) // before Start: live config goes in any phase
	a.until("Applied(d2)", isApplied("d2"))
	if at := a.attachFrom(1, 0); at.GetAppliedDigest() != "d2" {
		t.Fatalf("applied %q, want d2", at.GetAppliedDigest())
	}
	if downs := p.downs(); !containsLive(downs, "d2") {
		t.Fatalf("the worker got no Live d2: %v", downs)
	}
}

// A worker that does not apply a Live keeps the digest it had.
func TestC3LiveNotApplied(t *testing.T) {
	p := startShim(t, fakeworker.Script{IgnoreLive: true}, "", "", nil)
	a := dial(t, p.socket())
	a.started(1)
	wantOK(t, a.call(liveReq("d2")))
	deadline := time.Now().Add(10 * time.Second)
	for !containsLive(p.downs(), "d2") {
		if time.Now().After(deadline) {
			t.Fatal("the worker got no Live")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if at := a.attachFrom(1, 0); at.GetAppliedDigest() != "" {
		t.Fatalf("applied %q after a Live the worker did not apply", at.GetAppliedDigest())
	}
}

func containsLive(downs []*workerv1.WorkerDown, digest string) bool {
	for _, d := range downs {
		if d.GetLive().GetDigest() == digest {
			return true
		}
	}
	return false
}
