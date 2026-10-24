package agent

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/internal/md/conn"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// sync writes one ack per demand key (PROTOCOL §3.5). A demand that disappeared loses its ack at
// once: linger keeps the subscription, not the ack.
func (p *plane) sync(ctx context.Context, _ agent.Demand[Entry], observed map[agent.WorkerKey]agent.Observed, gaveUp func(agent.WorkerKey) bool) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	want := map[string]string{}
	keep := map[string]bool{}
	now := p.clock()
	for key, o := range p.outcomes {
		ack, ok := demandAck(p.inst, key)
		if !ok {
			continue
		}
		st, reason, write := decide(o, observed, gaveUp)
		reason = p.waiting(o, st, reason, observed)
		if !write {
			keep[ack] = true
			continue
		}
		msg := statusOf(o, st, reason, observed, now)
		if prev, ok := p.acks[ack]; ok && sameAck(prev, msg) {
			want[ack] = prev
			continue
		}
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		if err != nil {
			return err
		}
		if err := p.w.Put(ctx, ack, string(b)); err != nil {
			return err
		}
		if p.acks == nil {
			p.acks = map[string]string{}
		}
		p.acks[ack] = string(b)
		want[ack] = string(b)
	}
	for ack := range p.acks {
		if _, ok := want[ack]; ok || keep[ack] {
			continue
		}
		if err := p.w.Delete(ctx, ack); err != nil {
			return err
		}
		delete(p.acks, ack)
	}
	return nil
}

// waiting replaces "starting" on a conn worker that has never been spawned with why it cannot be: no worker
// artifact is ready yet (E12-D009). The entry stays PENDING; pushing a good latest fixes it.
func (p *plane) waiting(o outcome, st statev1.EntryState, reason string, observed map[agent.WorkerKey]agent.Observed) string {
	if st != statev1.EntryState_ENTRY_STATE_PENDING || p.pending == nil || o.Worker == "" {
		return reason
	}
	if _, seen := observed[o.Worker]; seen {
		return reason
	}
	if why := p.pending(); why != "" {
		return why
	}
	return reason
}

// decide is the ack for one outcome. LOST keeps the previous ack (P6). READY is a feed the
// conn worker has reported SUBSCRIBED while that worker is running. REJECTED in the same
// report is copied through. gaveUp is DEAD while the demand is still here.
func decide(o outcome, observed map[agent.WorkerKey]agent.Observed, gaveUp func(agent.WorkerKey) bool) (statev1.EntryState, string, bool) {
	if o.Reject != "" {
		return statev1.EntryState_ENTRY_STATE_REJECTED, o.Reject, true
	}
	obs, seen := observed[o.Worker]
	if seen && obs.State == agent.StateLost {
		return 0, "", false
	}
	if seen && obs.State.Ended() && gaveUp != nil && gaveUp(o.Worker) {
		return statev1.EntryState_ENTRY_STATE_DEAD, "gave up", true
	}
	if !seen {
		return statev1.EntryState_ENTRY_STATE_PENDING, "starting", true
	}
	if obs.State.Ended() {
		return statev1.EntryState_ENTRY_STATE_PENDING, "restarting", true
	}
	st, reason, ok := feedVerdict(obs.Report, o.Feed)
	if ok && st == mdctlv1.FeedState_FEED_STATE_REJECTED {
		if reason == "" {
			reason = "rejected"
		}
		return statev1.EntryState_ENTRY_STATE_REJECTED, reason, true
	}
	if obs.State == agent.StateRunning && ok && st == mdctlv1.FeedState_FEED_STATE_SUBSCRIBED {
		return statev1.EntryState_ENTRY_STATE_READY, "", true
	}
	return statev1.EntryState_ENTRY_STATE_PENDING, "starting", true
}

func statusOf(o outcome, st statev1.EntryState, reason string, observed map[agent.WorkerKey]agent.Observed, now time.Time) *statev1.EntryStatus {
	msg := &statev1.EntryStatus{
		State: st, Reason: reason, Subject: o.Subject, UpdatedAtNs: now.UnixNano(),
	}
	if o.Worker == "" {
		return msg
	}
	msg.Worker = &commonv1.WorkerRef{Key: string(o.Worker)}
	if obs, ok := observed[o.Worker]; ok {
		msg.Worker.Incarnation = obs.Incarnation
	}
	return msg
}

func sameAck(prev string, next *statev1.EntryStatus) bool {
	old := &statev1.EntryStatus{}
	if err := proto.Unmarshal([]byte(prev), old); err != nil {
		return false
	}
	return old.GetState() == next.GetState() && old.GetReason() == next.GetReason() &&
		old.GetSubject() == next.GetSubject() &&
		old.GetWorker().GetKey() == next.GetWorker().GetKey() &&
		old.GetWorker().GetIncarnation() == next.GetWorker().GetIncarnation()
}

// feedVerdict is this feed's state in the worker's latest Report. ok is false when the report
// does not mention it.
func feedVerdict(rep *workerv1.Report, feed string) (mdctlv1.FeedState, string, bool) {
	if rep == nil || rep.GetContract() != conn.Contract || len(rep.GetPayload()) == 0 || feed == "" {
		return 0, "", false
	}
	p := &mdctlv1.Report{}
	if err := proto.Unmarshal(rep.GetPayload(), p); err != nil {
		return 0, "", false
	}
	for _, f := range p.GetFeeds() {
		if f.GetFeed() == feed {
			return f.GetState(), f.GetReason(), true
		}
	}
	return 0, "", false
}

func demandAck(inst, demandKey string) (string, bool) {
	session, entry, ok := splitDemand(demandKey)
	if !ok {
		return "", false
	}
	return state.MDAckKey(inst, session, entry), true
}
