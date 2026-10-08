package shim

import (
	"errors"
	"strconv"
	"testing"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
)

func stageEvent(n int) *shimv1.Event {
	return &shimv1.Event{Kind: &shimv1.Event_Stage{Stage: &workerv1.Stage{Name: "s" + strconv.Itoa(n)}}}
}

// appendStages appends n Stage events, s1 to sn.
func appendStages(l *eventLog, n int) {
	for i := 1; i <= n; i++ {
		l.Append(stageEvent(i))
	}
}

func seqs(evs []*shimv1.Event) []uint64 {
	var out []uint64
	for _, ev := range evs {
		out = append(out, ev.GetSeq())
	}
	return out
}

// Events are numbered from 1, in order; Since reads from any point, and its cursor goes on from there.
func TestEventLogSince(t *testing.T) {
	l := newEventLog()
	if oldest, next := l.Bounds(); oldest != 1 || next != 1 {
		t.Fatalf("bounds %d %d of an empty log", oldest, next)
	}
	if evs, next := l.Since(0); len(evs) != 0 || next != 1 {
		t.Fatalf("since 0 of an empty log: %v %d", evs, next)
	}
	appendStages(l, 5)
	for _, c := range []struct {
		from, first, n, next uint64
	}{{0, 1, 5, 6}, {1, 1, 5, 6}, {3, 3, 3, 6}, {6, 0, 0, 6}, {9, 0, 0, 9}} {
		evs, next := l.Since(c.from)
		if uint64(len(evs)) != c.n || next != c.next || (c.n > 0 && evs[0].GetSeq() != c.first) {
			t.Errorf("since %d: %v, next %d", c.from, seqs(evs), next)
		}
		for _, ev := range evs {
			if ev.GetGap() || ev.GetAtNs() == 0 {
				t.Errorf("since %d: %v", c.from, ev)
			}
		}
	}
	// What Since returns is a copy: marking it does not change the log.
	evs, _ := l.Since(1)
	evs[0].Gap = true
	if again, _ := l.Since(1); again[0].GetGap() {
		t.Error("Since handed out the log's own event")
	}
}

// Done criterion 2: past RingSize unacknowledged events the oldest go, and a reader that asks for them gets the
// oldest kept, marked gap. One that asks from where the ring starts loses nothing and gets no gap.
func TestEventLogOverflow(t *testing.T) {
	l := newEventLog()
	appendStages(l, RingSize+6)
	oldest, next := l.Bounds()
	if oldest != 7 || next != RingSize+7 {
		t.Fatalf("bounds %d %d", oldest, next)
	}
	evs, _ := l.Since(1)
	if len(evs) != RingSize || evs[0].GetSeq() != 7 || !evs[0].GetGap() {
		t.Fatalf("since 1: %d events from %d, gap %v", len(evs), evs[0].GetSeq(), evs[0].GetGap())
	}
	for _, ev := range evs[1:] {
		if ev.GetGap() {
			t.Fatalf("gap on %d", ev.GetSeq())
		}
	}
	if evs, _ := l.Since(6); !evs[0].GetGap() {
		t.Error("since 6: seq 6 was dropped, no gap")
	}
	if evs, _ := l.Since(7); evs[0].GetGap() || evs[0].GetSeq() != 7 {
		t.Errorf("since 7: %v", evs[0])
	}
}

// Ack frees what the agent has: the ring then holds RingSize events past it, and a reader that resumes after it
// sees no gap. An Ack of the past changes nothing; one of the future is refused.
func TestEventLogAck(t *testing.T) {
	l := newEventLog()
	appendStages(l, 10)
	if err := l.Ack(4); err != nil {
		t.Fatal(err)
	}
	if oldest, _ := l.Bounds(); oldest != 5 {
		t.Fatalf("oldest %d after ack 4", oldest)
	}
	if err := l.Ack(2); err != nil {
		t.Fatal(err)
	}
	if oldest, _ := l.Bounds(); oldest != 5 {
		t.Fatalf("an ack of the past moved oldest to %d", oldest)
	}
	if err := l.Ack(11); !errors.Is(err, ErrAckAhead) {
		t.Fatalf("ack 11 of 10: %v", err)
	}
	if evs, _ := l.Since(1); evs[0].GetSeq() != 5 || evs[0].GetGap() {
		t.Fatalf("since 1 after ack 4: %v", evs[0])
	}
	appendStages(l, RingSize-6) // 1024 kept: 5 to 1028
	if evs, _ := l.Since(5); len(evs) != RingSize || evs[0].GetGap() {
		t.Fatalf("since 5: %d, gap %v", len(evs), evs[0].GetGap())
	}
	if err := l.Ack(10); err != nil { // everything acked: nothing kept
		t.Fatal(err)
	}
	l.Ack(1028) //nolint:errcheck
	if oldest, next := l.Bounds(); oldest != next || next != 1029 {
		t.Fatalf("bounds %d %d with all acknowledged", oldest, next)
	}
}

// An overflow of events the agent then acknowledges is not a gap: it has them.
func TestEventLogAckAfterOverflow(t *testing.T) {
	l := newEventLog()
	appendStages(l, RingSize+3) // 1 to 3 dropped
	if evs, _ := l.Since(2); !evs[0].GetGap() {
		t.Fatal("no gap before the ack")
	}
	if err := l.Ack(3); err != nil {
		t.Fatal(err)
	}
	if evs, _ := l.Since(2); evs[0].GetGap() {
		t.Error("gap for events the agent acknowledged")
	}
}

// Only the latest Report is kept, unnumbered.
func TestEventLogReport(t *testing.T) {
	l := newEventLog()
	if l.Report() != nil {
		t.Fatal("a report before any")
	}
	for i := range 3 {
		l.SetReport(&shimv1.Event{Kind: &shimv1.Event_Report{Report: &workerv1.Report{Payload: []byte{byte(i)}}}})
	}
	r := l.Report()
	if r.GetSeq() != 0 || r.GetAtNs() == 0 || r.GetReport().GetPayload()[0] != 2 {
		t.Fatalf("report %v", r)
	}
	if _, next := l.Bounds(); next != 1 {
		t.Fatal("a report took a seq")
	}
}

// Notify fires on Append, and one wake may stand for many.
func TestEventLogNotify(t *testing.T) {
	l := newEventLog()
	appendStages(l, 3)
	select {
	case <-l.Notify():
	default:
		t.Fatal("no notify after append")
	}
	select {
	case <-l.Notify():
		t.Fatal("three appends, two notifies: one is enough")
	default:
	}
}
