package agent

import (
	"context"
	"strconv"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// sync writes one ack per demand key (PROTOCOL §3.5), only when it changes. A demand that left
// loses its ack at once: linger keeps the worker, not the ack.
func (p *plane) sync(ctx context.Context, _ agent.Demand[Entry], observed map[agent.WorkerKey]agent.Observed, gaveUp func(agent.WorkerKey) bool) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	want := map[string]bool{}
	now := p.clock()
	for _, o := range p.outcomes {
		ack := state.TDAckKey(p.inst, o.Session, o.APIID)
		st, reason, write := decide(o, observed, gaveUp)
		want[ack] = true
		if !write {
			continue
		}
		msg := &statev1.EntryStatus{
			State: st, Reason: reason, UpdatedAtNs: now.UnixNano(),
			Subject: "td.order." + strconv.FormatInt(o.APIID, 10),
		}
		if o.Worker != "" {
			msg.Worker = &commonv1.WorkerRef{Key: string(o.Worker)}
			if obs, ok := observed[o.Worker]; ok {
				msg.Worker.Incarnation = obs.Incarnation
			}
		}
		if prev, ok := p.acks[ack]; ok && sameAck(prev, msg) {
			continue
		}
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		if err != nil {
			return err
		}
		if err := p.w.Put(ctx, ack, string(b)); err != nil {
			return err
		}
		p.acks[ack] = string(b)
	}
	for ack := range p.acks {
		if want[ack] {
			continue
		}
		if err := p.w.Delete(ctx, ack); err != nil {
			return err
		}
		delete(p.acks, ack)
	}
	return nil
}
