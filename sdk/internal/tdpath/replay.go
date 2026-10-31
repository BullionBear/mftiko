package tdpath

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/internal/evlog"
)

// ErrNoRound means a recorded order_reply has no request of this replay waiting for it: the
// strategy did not make that call this time.
var ErrNoRound = errors.New("tdpath: no replayed request waits for this reply")

// ReplayEvent applies a recorded td_event as the hook that ran it, on the replay's hook goroutine.
func (p *Path) ReplayEvent(apiID int64, raw []byte) error {
	ev := &tdv1.AccountEvent{}
	if err := proto.Unmarshal(raw, ev); err != nil {
		return err
	}
	p.mu.Lock()
	acc := p.byAPI[apiID]
	if acc == nil {
		p.mu.Unlock()
		return ErrUnknownAccount
	}
	acc.seq = ev.GetSeq()
	p.mu.Unlock()
	p.apply(apiID, ev)
	return nil
}

// ReplayView applies a recorded td_view body: the Recon reply of the request whose generation it
// carries.
func (p *Path) ReplayView(apiID int64, body []byte) error {
	gen, raw, err := eventlog.ParseTDView(body)
	if err != nil {
		return err
	}
	rep := &tdv1.ViewReply{}
	if err := proto.Unmarshal(raw, rep); err != nil {
		return err
	}
	if rep.GetRecon() == nil {
		return errNotRecon
	}
	if _, err := p.account(apiID); err != nil {
		return err
	}
	p.beginReply(apiID, rep, gen)
	return nil
}

// ReplayReply finishes the replayed request of call with its recorded order_reply.
func (p *Path) ReplayReply(call uint64, flags uint16, body []byte) error {
	p.mu.Lock()
	r, ok := p.rounds[call]
	delete(p.rounds, call)
	p.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: call %d", ErrNoRound, call)
	}
	err := evlog.Err(flags, body)
	reply := &tdv1.OrderReply{}
	if err == nil {
		err = proto.Unmarshal(body, reply)
	}
	p.finishOrder(r.apiID, r.id, r.ids, r.kind, reply, err)
	return nil
}
