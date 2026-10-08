package shim

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
)

// RingSize is how many unacknowledged events the shim keeps for an agent that is away (PROTOCOL §4.4).
const RingSize = 1024

// ErrAckAhead is an Ack of a seq the shim has not produced yet.
var ErrAckAhead = errors.New("shim: ack of an event not yet sent")

// eventLog is what the worker said that an agent reads back after it was away (PROTOCOL §4.4, E1-12).
//
// Started, Hello, Done, Stage, Exiting and Stopped are numbered from 1 and kept in a ring of RingSize until the agent
// acknowledges them. When the ring is full the oldest goes, and an agent that asks for it later gets a gap. The
// latest Report is kept on its own. Nothing else is kept: Beat and the worker's Relay go to whoever is the
// controller at that moment, with seq 0.
//
// Readers learn of new events from Notify, then ask for what came after their cursor with Since. That is the only
// way out of the log, for a replay after Attach and for the live stream alike, so a reader never misses an event or
// sees one twice.
type eventLog struct {
	mu          sync.Mutex
	ring        []*shimv1.Event // oldest first; all unacknowledged
	next        uint64          // the seq the next event gets
	lostThrough uint64          // the largest seq dropped unacknowledged when the ring was full
	acked       uint64          // the largest seq acknowledged
	report      *shimv1.Event   // the latest Report, seq 0
	notify      chan struct{}   // one slot: something was appended
	now         func() time.Time
}

func newEventLog() *eventLog {
	return &eventLog{next: 1, notify: make(chan struct{}, 1), now: time.Now}
}

// Notify has a value after an event is appended. One value may stand for several events.
func (l *eventLog) Notify() <-chan struct{} { return l.notify }

// Append numbers ev, stamps it and keeps it. A full ring drops its oldest event.
func (l *eventLog) Append(ev *shimv1.Event) {
	l.mu.Lock()
	ev.Seq, ev.AtNs, ev.Gap = l.next, l.now().UnixNano(), false
	l.next++
	if len(l.ring) == RingSize {
		l.lostThrough = l.ring[0].GetSeq()
		l.ring[0] = nil
		l.ring = l.ring[1:]
	}
	l.ring = append(l.ring, ev)
	l.mu.Unlock()
	select {
	case l.notify <- struct{}{}:
	default:
	}
}

// SetReport keeps rep as the latest Report and returns it as the event to send now. Reports are not numbered.
func (l *eventLog) SetReport(ev *shimv1.Event) *shimv1.Event {
	ev.Seq, ev.AtNs = 0, l.now().UnixNano()
	l.mu.Lock()
	l.report = ev
	l.mu.Unlock()
	return ev
}

// Transient stamps an event that is not kept: a Beat, or a Relay from the worker.
func (l *eventLog) Transient(ev *shimv1.Event) *shimv1.Event {
	ev.Seq, ev.AtNs = 0, l.now().UnixNano()
	return ev
}

// Report is the latest Report, nil before the first.
func (l *eventLog) Report() *shimv1.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.report
}

// Since returns copies of the kept events from seq from on, and the cursor after them. from 0 is from the start. The
// first is marked gap when events from from on were dropped before the agent acknowledged them: the reader asked for
// them and they are gone.
func (l *eventLog) Since(from uint64) (evs []*shimv1.Event, next uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	from = max(from, 1)
	if from >= l.next {
		return nil, max(from, l.next)
	}
	i := 0
	if len(l.ring) > 0 {
		i = int(min(max(from, l.ring[0].GetSeq()), l.next) - l.ring[0].GetSeq())
	}
	for _, ev := range l.ring[i:] {
		evs = append(evs, proto.CloneOf(ev))
	}
	if len(evs) > 0 && from <= l.lostThrough && l.acked < l.lostThrough {
		evs[0].Gap = true
	}
	return evs, l.next
}

// Ack frees every kept event up to seq: the agent has it. An Ack of an earlier seq changes nothing; one of a seq not
// yet produced is ErrAckAhead.
func (l *eventLog) Ack(seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq >= l.next {
		return fmt.Errorf("%w: seq %d, the last is %d", ErrAckAhead, seq, l.next-1)
	}
	if seq <= l.acked {
		return nil
	}
	l.acked = seq
	i := 0
	for i < len(l.ring) && l.ring[i].GetSeq() <= seq {
		l.ring[i] = nil
		i++
	}
	l.ring = l.ring[i:]
	return nil
}

// Bounds are the oldest seq kept, and the seq the next event gets: Attached.oldest_seq and next_seq. With nothing
// kept the oldest is the next.
func (l *eventLog) Bounds() (oldest, next uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ring) == 0 {
		return l.next, l.next
	}
	return l.ring[0].GetSeq(), l.next
}
