package shim

import (
	"strconv"
	"strings"
	"testing"
	"time"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

// These run a real shim, with the fake worker under it: E1-12's done criteria end to end.

// attachFrom attaches as the controller and asks for events from resume.
func (a *agent) attachFrom(epoch int64, resume uint64) *shimv1.Attached {
	a.t.Helper()
	r := a.call(attachFromReq(epoch, resume, Major))
	wantOK(a.t, r)
	return r.GetAttached()
}

// until reads events until one satisfies ok, and returns it; the ones before it are dropped.
func (a *agent) until(what string, ok func(*shimv1.Event) bool) *shimv1.Event {
	a.t.Helper()
	for {
		ev := a.event()
		if ok(ev) {
			return ev
		}
		if ev.GetSeq() == 0 && ev.GetBeat() == nil && ev.GetReport() == nil && ev.GetRelay() == nil {
			a.t.Fatalf("waiting for %s: %v", what, ev)
		}
	}
}

func isDone(verb workerv1.Verb) func(*shimv1.Event) bool {
	return func(ev *shimv1.Event) bool { return ev.GetDone().GetVerb() == verb }
}

// kept reads the next event that is kept (seq > 0), skipping Beats, Reports and Relays.
func (a *agent) kept() *shimv1.Event {
	a.t.Helper()
	return a.until("a kept event", func(ev *shimv1.Event) bool { return ev.GetSeq() > 0 })
}

// pollNext attaches with no common major, which streams nothing, until the shim's next seq reaches n.
func (a *agent) pollNext(n uint64) *shimv1.Attached {
	a.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r := a.attach(1, Major+1)
		wantOK(a.t, r)
		if r.GetAttached().GetNextSeq() >= n {
			return r.GetAttached()
		}
		if time.Now().After(deadline) {
			a.t.Fatalf("next seq %d, want %d", r.GetAttached().GetNextSeq(), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Done criterion 1: the worker goes on while no agent is there; the agent that comes back resumes where it left off
// and reads every event of the gap, in order, none missing and none twice.
func TestC3EventsWhileAway(t *testing.T) {
	p := startShim(t, fakeworker.Script{Exit: fakeworker.ExitDone, ExitAfterMS: 300}, "", "", nil)
	a := dial(t, p.socket())
	a.attachFrom(1, 0)
	if ev := a.kept(); ev.GetSeq() != 1 || ev.GetStarted().GetPid() == 0 {
		t.Fatalf("first event %v, want Started", ev)
	}
	if ev := a.kept(); ev.GetSeq() != 2 || ev.GetHello().GetKind() != "fake" {
		t.Fatalf("second event %v, want Hello", ev)
	}
	wantOK(t, a.call(initReq()))
	a.until("Done(INIT)", isDone(workerv1.Verb_VERB_INIT))
	wantOK(t, a.call(startReq()))
	a.until("Done(START)", isDone(workerv1.Verb_VERB_START))
	last := a.until("Stage(running)", func(ev *shimv1.Event) bool { return ev.GetStage().GetName() == "running" }).GetSeq()
	wantOK(t, a.call(ackReq(last)))
	a.c.Close() // gone; the worker ends on its own meanwhile

	p.waitLog("worker exited", map[string]any{"code": float64(fakeworker.CodeDone)})
	b := dial(t, p.socket())
	at := b.attachFrom(2, last+1)
	if at.GetOldestSeq() != last+1 || at.GetNextSeq() != last+3 {
		t.Errorf("attached oldest %d next %d, want %d and %d", at.GetOldestSeq(), at.GetNextSeq(), last+1, last+3)
	}
	exiting, stopped := b.kept(), b.kept()
	if exiting.GetSeq() != last+1 || exiting.GetExiting() == nil || exiting.GetGap() {
		t.Errorf("first after the gap %v, want Exiting at %d", exiting, last+1)
	}
	if stopped.GetSeq() != last+2 || !stopped.GetStopped().GetOnStopCompleted() {
		t.Errorf("then %v, want Stopped at %d", stopped, last+2)
	}
}

// Done criterion 2: more than RingSize events while no agent is there. The agent that asks from the start gets the
// newest RingSize of them, the first marked gap.
func TestC3EventsOverflow(t *testing.T) {
	const stages = RingSize + 50
	p := startShim(t, fakeworker.Script{Stages: stages}, "", "", nil)
	p.workerPid()
	next := uint64(2 + stages + 1) // Started, Hello, the stages
	dial(t, p.socket()).pollNext(next)

	b := dial(t, p.socket())
	at := b.attachFrom(2, 0)
	if at.GetOldestSeq() != next-RingSize || at.GetNextSeq() != next {
		t.Fatalf("attached oldest %d next %d", at.GetOldestSeq(), at.GetNextSeq())
	}
	first := b.kept()
	if first.GetSeq() != next-RingSize || !first.GetGap() {
		t.Fatalf("first replayed %v: want seq %d marked gap", first, next-RingSize)
	}
	ev := first
	for range RingSize - 1 {
		prev := ev.GetSeq()
		ev = b.kept()
		if ev.GetSeq() != prev+1 || ev.GetGap() {
			t.Fatalf("after %d: %v", prev, ev)
		}
	}
	if ev.GetStage().GetName() != "stage-"+strconv.Itoa(stages) {
		t.Errorf("last replayed %v, want the last stage", ev)
	}
	wantOK(t, b.call(ackReq(ev.GetSeq())))
	if at := b.attachFrom(2, next); at.GetOldestSeq() != at.GetNextSeq() {
		t.Errorf("after acking everything: oldest %d next %d", at.GetOldestSeq(), at.GetNextSeq())
	}
}

// Done criterion 3: Reports sent while no agent is there are not kept, but the latest is: it is the first thing the
// next agent gets after Attached.
func TestC3ReportLatest(t *testing.T) {
	const reports = 5
	p := startShim(t, fakeworker.Script{ReportMS: 20, Reports: reports}, "", "", nil)
	a := dial(t, p.socket())
	a.started(1)
	a.c.Close()
	waitFor(t, func() bool {
		return countSent(p, func(m *workerv1.WorkerUp) bool { return m.GetReport() != nil }) == reports
	})

	b := dial(t, p.socket())
	b.attachFrom(2, b.pollNext(0).GetNextSeq())
	first := b.event()
	if first.GetReport() == nil || string(first.GetReport().GetPayload()) != strconv.Itoa(reports) || first.GetSeq() != 0 {
		t.Fatalf("first after Attached: %v, want the latest Report (%d)", first, reports)
	}
	time.Sleep(200 * time.Millisecond)
	wantOK(t, b.call(ackReq(0))) // a round trip, to collect what came meanwhile
	for _, ev := range b.events {
		if ev.GetReport() != nil {
			t.Errorf("another Report: %v", ev)
		}
	}
}

// countSent counts what the fake worker sent that matches ok.
func countSent(p *shimProc, ok func(*workerv1.WorkerUp) bool) int {
	p.t.Helper()
	entries, err := fakeworker.ReadJournal(p.journal)
	if err != nil {
		p.t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if e.Event != "sent" {
			continue
		}
		if m, err := e.Up(); err == nil && ok(m) {
			n++
		}
	}
	return n
}

// undeliverables are the corr ids the worker got Undeliverable for, in order.
func (p *shimProc) undeliverables() []string {
	var out []string
	for _, m := range p.downsAll() {
		if u := m.GetUndeliverable(); u != nil {
			out = append(out, string(u.GetCorrId()))
		}
	}
	return out
}

// The worker's requests to the agent: answered Undeliverable by the shim while no agent is there, passed to the
// controller while one is. The worker is told which, with the epoch.
func TestC3WorkerRelay(t *testing.T) {
	p := startShim(t, fakeworker.Script{AskMS: 30}, "", "", nil)
	p.workerPid()
	waitFor(t, func() bool { return len(p.undeliverables()) >= 2 })

	a := dial(t, p.socket())
	a.attachFrom(1, 0)
	relay := a.until("the worker's Relay", func(ev *shimv1.Event) bool { return ev.GetRelay() != nil })
	if !strings.HasPrefix(string(relay.GetRelay().GetCorrId()), "ask-") || relay.GetSeq() != 0 {
		t.Fatalf("relay %v", relay)
	}
	before := len(p.undeliverables())
	a.until("another Relay", func(ev *shimv1.Event) bool { return ev.GetRelay() != nil })
	a.until("another Relay", func(ev *shimv1.Event) bool { return ev.GetRelay() != nil })
	if after := len(p.undeliverables()); after > before+1 { // one may have crossed the Attach
		t.Errorf("undeliverable went on with a controller: %d then %d", before, after)
	}
	a.c.Close()
	waitFor(t, func() bool { return len(p.undeliverables()) > before+2 })

	var told []string
	for _, m := range p.downsAll() {
		if c := m.GetController(); c != nil {
			told = append(told, strconv.FormatBool(c.GetAttached())+"/"+strconv.FormatInt(c.GetEpoch(), 10))
		}
	}
	if strings.Join(told, " ") != "true/1 false/1" {
		t.Errorf("the worker was told %v, want an agent at epoch 1, then none", told)
	}
}

// Beats go on to the controller at most once a second, however often the worker sends them.
func TestC3BeatThrottled(t *testing.T) {
	p := startShim(t, fakeworker.Script{BeatMS: 20}, "", "", nil)
	p.workerPid()
	a := dial(t, p.socket())
	a.attachFrom(1, 0)
	var at []int64
	for len(at) < 3 {
		if ev := a.event(); ev.GetBeat() != nil {
			at = append(at, ev.GetAtNs())
		}
	}
	for i := 1; i < len(at); i++ {
		if d := time.Duration(at[i] - at[i-1]); d < beatEvery || d > beatEvery+500*time.Millisecond {
			t.Errorf("beats %v apart, want a little over %v: the worker beats every 20 ms", d, beatEvery)
		}
	}
}
