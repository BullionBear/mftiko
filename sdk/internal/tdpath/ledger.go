package tdpath

import (
	"google.golang.org/protobuf/proto"

	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
)

// Position is the account's position for uid. The pointer is a copy of the ledger
// row. The read takes the book lock only; it does not wait on NATS (D856).
func (p *Path) Position(apiID int64, uid uint32) (*tdv1.Position, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc := p.byAPI[apiID]
	if acc == nil {
		return nil, false
	}
	pos := acc.positions[uid]
	if pos == nil {
		return nil, false
	}
	return clonePosition(pos), true
}

// Balance is the account's balance of asset, as TD last sent it. The pointer is a
// copy. The read takes the book lock only; it does not wait on NATS (D853, D856).
func (p *Path) Balance(apiID int64, asset string) (*tdv1.Balance, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc := p.byAPI[apiID]
	if acc == nil {
		return nil, false
	}
	b := acc.balances[asset]
	if b == nil {
		return nil, false
	}
	return cloneBalance(b), true
}

// applyPosition overwrites each listed uid with that absolute position, then calls
// the hook. A size of zero stays until a Recon omits the uid. Keys the update does
// not mention are left as they were. The write happens even when no hook is set.
func (p *Path) applyPosition(apiID int64, ev *tdv1.AccountEvent) {
	u := ev.GetPositionUpdate()
	if u == nil {
		return
	}
	rows := clonePositions(u.GetPositions())
	p.mu.Lock()
	acc := p.byAPI[apiID]
	if acc == nil {
		p.mu.Unlock()
		return
	}
	if acc.positions == nil {
		acc.positions = map[uint32]*tdv1.Position{}
	}
	for _, pos := range rows {
		acc.positions[pos.GetUid()] = clonePosition(pos)
	}
	p.mu.Unlock()
	if p.hooks.Position != nil {
		p.hooks.Position(PositionEvent{TD: eventTD(apiID, ev), Positions: rows})
	}
}

// applyBalance overwrites each listed asset with that absolute balance, then calls
// the hook. free, locked and prelock are stored unchanged (D853). The write happens
// even when no hook is set.
func (p *Path) applyBalance(apiID int64, ev *tdv1.AccountEvent) {
	u := ev.GetBalanceUpdate()
	if u == nil {
		return
	}
	rows := cloneBalances(u.GetBalances())
	p.mu.Lock()
	acc := p.byAPI[apiID]
	if acc == nil {
		p.mu.Unlock()
		return
	}
	if acc.balances == nil {
		acc.balances = map[string]*tdv1.Balance{}
	}
	for _, b := range rows {
		acc.balances[b.GetAsset()] = cloneBalance(b)
	}
	p.mu.Unlock()
	if p.hooks.Balance != nil {
		p.hooks.Balance(BalanceEvent{TD: eventTD(apiID, ev), Balances: rows})
	}
}

// replaceLedger installs the snapshot's positions and balances. An id the snapshot
// does not list is gone. A repeated uid or asset keeps the later row.
func (a *account) replaceLedger(positions []*tdv1.Position, balances []*tdv1.Balance) {
	nextP := make(map[uint32]*tdv1.Position, len(positions))
	for _, pos := range positions {
		if pos == nil {
			continue
		}
		nextP[pos.GetUid()] = clonePosition(pos)
	}
	nextB := make(map[string]*tdv1.Balance, len(balances))
	for _, b := range balances {
		if b == nil {
			continue
		}
		nextB[b.GetAsset()] = cloneBalance(b)
	}
	a.positions = nextP
	a.balances = nextB
}

func clonePosition(pos *tdv1.Position) *tdv1.Position {
	if pos == nil {
		return nil
	}
	return proto.Clone(pos).(*tdv1.Position)
}

func cloneBalance(b *tdv1.Balance) *tdv1.Balance {
	if b == nil {
		return nil
	}
	return proto.Clone(b).(*tdv1.Balance)
}
