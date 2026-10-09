package agent

import (
	"fmt"
	"slices"
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
)

// h is a spec or live config that is its own hash.
type h string

func (x h) Hash() string { return string(x) }

type plans = map[WorkerKey]Plan[h, h]
type observed = map[WorkerKey]Observed

var t0 = time.Unix(1_700_000_000, 0)

// policy restarts after a fixed wait, up to max times, and keeps on stale demand the keys in keep.
type policy struct {
	after time.Duration
	max   uint32 // 0: always
	keep  map[WorkerKey]bool
	asked []Ended
}

func (p *policy) Restart(_ WorkerKey, e Ended) (bool, time.Duration) {
	p.asked = append(p.asked, e)
	return p.max == 0 || e.Restarts < p.max, p.after
}

func (p *policy) KeepWhileStale(k WorkerKey) bool { return p.keep[k] }

// acts renders actions as "kind key", with the spec and live config of a spawn or push, for comparing.
func acts(as []Action[h, h]) []string {
	var out []string
	for _, a := range as {
		s := fmt.Sprintf("%s %s", a.Kind, a.Key)
		switch a.Kind {
		case ActSpawn:
			s += fmt.Sprintf(" %s/%s", a.Spec, a.Live)
		case ActPush:
			s += " " + string(a.Live)
		case ActStop:
			s += ": " + a.Reason
		}
		out = append(out, s)
	}
	return out
}

func want(t *testing.T, got []Action[h, h], w ...string) {
	t.Helper()
	if g := acts(got); !slices.Equal(g, w) {
		t.Fatalf("actions %q, want %q", g, w)
	}
}

func running(k WorkerKey, inc uint32, spec, live h) Observed {
	return Observed{Key: k, Incarnation: inc, SpecHash: string(spec), LiveHash: string(live), State: StateRunning,
		StartedAt: t0}
}

func exited(o Observed, at time.Time) Observed {
	o.State = StateExited
	o.Exit = &commonv1.ExitInfo{AtNs: at.UnixNano(), Code: 20, Outcome: commonv1.Outcome_OUTCOME_CRASHED}
	return o
}

// The diff for workers in every state: spawn what is missing, push a new live config, start what may start, stop
// what is not planned or has another spec, release what ended and is not planned, wait on what is stopping.
func TestStepDiff(t *testing.T) {
	r := &Reconciler[h, h]{Policy: &policy{}}
	p := plans{
		"md/a": {Spawn: "s1", Live: "l1", Start: true}, // missing
		"md/b": {Spawn: "s1", Live: "l2", Start: true}, // running with l1
		"md/c": {Spawn: "s1", Live: "l1", Start: true}, // initialized
		"md/d": {Spawn: "s1", Live: "l1"},              // initialized, not yet allowed to start
		"md/e": {Spawn: "s2", Live: "l1", Start: true}, // running s1
		"md/f": {Spawn: "s2", Live: "l1", Start: true}, // stopping s1, for s2
		"md/g": {Spawn: "s1", Live: "l1", Start: true}, // starting
	}
	init := func(k WorkerKey) Observed { o := running(k, 1, "s1", "l1"); o.State = StateInitialized; return o }
	stopping := func(k WorkerKey) Observed { o := running(k, 1, "s1", "l1"); o.State = StateStopping; return o }
	starting := func(k WorkerKey) Observed { o := running(k, 1, "s1", "l1"); o.State = StateStarting; return o }
	o := observed{
		"md/b": running("md/b", 1, "s1", "l1"),
		"md/c": init("md/c"),
		"md/d": init("md/d"),
		"md/e": running("md/e", 1, "s1", "l1"),
		"md/f": stopping("md/f"),
		"md/g": starting("md/g"),
		"md/x": running("md/x", 1, "s1", "l1"),             // not planned
		"md/y": stopping("md/y"),                           // not planned, stopping
		"md/z": exited(running("md/z", 1, "s1", "l1"), t0), // not planned, ended
	}
	got, wake := r.Step(t0, false, p, o)
	want(t, got,
		"spawn md/a s1/l1",
		"push md/b l2",
		"start md/c",
		"stop md/e: "+ReasonSpecChanged,
		"stop md/x: "+ReasonNotPlanned,
		"release md/z",
	)
	if !wake.IsZero() {
		t.Fatalf("wake %v, want none", wake)
	}
}

// A replaced worker is spawned again with the new spec as soon as it has ended, with no backoff and without
// asking the Policy; one that ended by itself waits for the Policy's backoff.
func TestStepRestart(t *testing.T) {
	pol := &policy{after: 5 * time.Second}
	r := &Reconciler[h, h]{Policy: pol}
	p := plans{"td/1": {Spawn: "s2", Live: "l", Start: true}, "td/2": {Spawn: "s1", Live: "l", Start: true}}
	o := observed{"td/1": running("td/1", 1, "s1", "l"), "td/2": running("td/2", 1, "s1", "l")}
	got, _ := r.Step(t0, false, p, o)
	want(t, got, "stop td/1: "+ReasonSpecChanged)

	at := t0.Add(10 * time.Second)
	o = observed{"td/1": exited(o["td/1"], at), "td/2": exited(o["td/2"], at)}
	got, wake := r.Step(at, false, p, o)
	want(t, got, "spawn td/1 s2/l")
	if !wake.Equal(at.Add(5 * time.Second)) {
		t.Fatalf("wake %v, want the backoff's end %v", wake, at.Add(5*time.Second))
	}
	if len(pol.asked) != 1 || pol.asked[0].Restarts != 0 || pol.asked[0].Ran != 10*time.Second ||
		pol.asked[0].Exit.GetOutcome() != commonv1.Outcome_OUTCOME_CRASHED {
		t.Fatalf("Policy asked %+v, want once, for td/2, which ran 10s", pol.asked)
	}
	o["td/1"] = running("td/1", 2, "s2", "l")           // the Fleet shows the spawn
	got, _ = r.Step(at.Add(4*time.Second), false, p, o) // the same end: the Policy is not asked again
	want(t, got)
	got, _ = r.Step(at.Add(5*time.Second), false, p, o)
	want(t, got, "spawn td/2 s1/l")
	if len(pol.asked) != 1 || r.Restarts("td/2") != 1 {
		t.Fatalf("asked %d times, %d restarts; want 1 and 1", len(pol.asked), r.Restarts("td/2"))
	}
}

// Restarts count up across incarnations, back to 0 once one has run for StableAfter; a worker the Policy gives up
// on stays ended until its spec changes.
func TestStepGiveUp(t *testing.T) {
	pol := &policy{max: 2}
	r := &Reconciler[h, h]{Policy: pol, StableAfter: time.Hour}
	k := WorkerKey("sts/s1")
	p := plans{k: {Spawn: "s1", Live: "l"}}
	now := t0
	for inc := uint32(1); inc <= 3; inc++ {
		o := running(k, inc, "s1", "l")
		o.StartedAt = now
		now = now.Add(time.Minute)
		got, _ := r.Step(now, false, p, observed{k: exited(o, now)})
		if inc < 3 {
			want(t, got, "spawn sts/s1 s1/l")
		} else {
			want(t, got)
		}
	}
	if !r.GaveUp(k) || pol.asked[2].Restarts != 2 {
		t.Fatalf("gave up %v after %+v", r.GaveUp(k), pol.asked)
	}
	o := exited(running(k, 3, "s1", "l"), now)
	got, _ := r.Step(now.Add(time.Hour), false, p, observed{k: o})
	want(t, got)

	p[k] = Plan[h, h]{Spawn: "s2", Live: "l"}
	got, _ = r.Step(now.Add(time.Hour), false, p, observed{k: o})
	want(t, got, "spawn sts/s1 s2/l")

	r = &Reconciler[h, h]{Policy: pol, StableAfter: time.Minute} // a run of StableAfter counts from 0 again
	pol.asked = nil
	for inc := uint32(1); inc <= 3; inc++ {
		o := running(k, inc, "s1", "l")
		o.StartedAt = now
		now = now.Add(time.Minute)
		r.Step(now, false, plans{k: {Spawn: "s1", Live: "l"}}, observed{k: exited(o, now)})
	}
	for _, e := range pol.asked {
		if e.Restarts != 0 {
			t.Fatalf("asked %+v, want every run counted stable", pol.asked)
		}
	}
}

// While the demand is stale nothing is stopped or pushed; a worker is spawned or started only if the Policy keeps
// it (DECISIONS D39).
func TestStepStale(t *testing.T) {
	pol := &policy{keep: map[WorkerKey]bool{"md/kept": true}}
	r := &Reconciler[h, h]{Policy: pol}
	init := running("md/kept-init", 1, "s1", "l1")
	init.State = StateInitialized
	initHeld := init
	initHeld.Key = "md/held-init"
	pol.keep["md/kept-init"] = true
	p := plans{
		"md/kept":      {Spawn: "s1", Live: "l1", Start: true}, // missing
		"md/held":      {Spawn: "s1", Live: "l1", Start: true}, // missing, not kept
		"md/kept-init": {Spawn: "s1", Live: "l1", Start: true},
		"md/held-init": {Spawn: "s1", Live: "l1", Start: true},
		"md/live":      {Spawn: "s1", Live: "l2", Start: true}, // another live config
		"md/spec":      {Spawn: "s2", Live: "l1", Start: true}, // another spec
	}
	o := observed{
		"md/kept-init": init,
		"md/held-init": initHeld,
		"md/live":      running("md/live", 1, "s1", "l1"),
		"md/spec":      running("md/spec", 1, "s1", "l1"),
		"md/gone":      running("md/gone", 1, "s1", "l1"),             // not planned
		"md/done":      exited(running("md/done", 1, "s1", "l1"), t0), // not planned, ended
	}
	got, _ := r.Step(t0, true, p, o)
	want(t, got, "release md/done", "spawn md/kept s1/l1", "start md/kept-init")
	got, _ = r.Step(t0, false, p, o)
	want(t, got,
		"release md/done",
		"stop md/gone: "+ReasonNotPlanned,
		"spawn md/held s1/l1",
		"start md/held-init",
		"spawn md/kept s1/l1",
		"start md/kept-init",
		"push md/live l2",
		"stop md/spec: "+ReasonSpecChanged,
	)
}

// A failed action holds the spawn back for a retry backoff that grows; once the worker runs, it counts from 0.
func TestStepFailed(t *testing.T) {
	r := &Reconciler[h, h]{Policy: &policy{}, Retry: Backoff{Min: time.Second, Max: 4 * time.Second}}
	p := plans{"td/1": {Spawn: "s", Live: "l"}}
	now := t0
	for _, wait := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second} {
		got, _ := r.Step(now, false, p, nil)
		want(t, got, "spawn td/1 s/l")
		r.Failed(now, "td/1")
		got, wake := r.Step(now.Add(wait-time.Millisecond), false, p, nil)
		want(t, got)
		if !wake.Equal(now.Add(wait)) {
			t.Fatalf("wake %v, want %v", wake.Sub(now), wait)
		}
		now = now.Add(wait)
	}
	r.Step(now, false, p, observed{"td/1": running("td/1", 1, "s", "l")})
	r.Failed(now, "td/1")
	if got, _ := r.Step(now.Add(time.Second), false, p, nil); len(got) != 1 {
		t.Fatalf("after a run the retry starts over at 1s: %q", acts(got))
	}
}

func TestBackoff(t *testing.T) {
	b := Backoff{Min: 100 * time.Millisecond, Max: time.Second}
	for n, w := range []time.Duration{100, 200, 400, 800, 1000, 1000} {
		if got := b.After(uint32(n)); got != w*time.Millisecond {
			t.Errorf("After(%d) = %v, want %v", n, got, w*time.Millisecond)
		}
	}
	if got := b.After(1 << 30); got != time.Second {
		t.Errorf("After(huge) = %v", got)
	}
}

func TestDemandStale(t *testing.T) {
	d := Demand[int]{}
	if !d.Stale(t0, time.Hour) {
		t.Error("no contact yet is stale")
	}
	d.LastContact = t0
	if d.Stale(t0.Add(15*time.Second), 15*time.Second) || !d.Stale(t0.Add(16*time.Second), 15*time.Second) {
		t.Error("stale is older than after")
	}
}
