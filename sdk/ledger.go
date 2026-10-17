package sdk

import (
	"sync"

	"google.golang.org/protobuf/proto"

	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
)

// ledger is the session's last absolute positions and balances (D805). It is not the
// order state machine deferred to #238: orders stay in tdpath. A position or balance
// event updates the keys it lists. A recon replaces that account's maps.
type ledger struct {
	mu  sync.Mutex
	pos map[int64]map[uint32]*tdv1.Position
	bal map[int64]map[string]*tdv1.Balance
}

func newLedger() *ledger {
	return &ledger{
		pos: map[int64]map[uint32]*tdv1.Position{},
		bal: map[int64]map[string]*tdv1.Balance{},
	}
}

func (l *ledger) applyPositions(api int64, in []*tdv1.Position) {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := l.pos[api]
	if m == nil {
		m = map[uint32]*tdv1.Position{}
		l.pos[api] = m
	}
	for _, p := range in {
		if p == nil {
			continue
		}
		m[p.GetUid()] = proto.Clone(p).(*tdv1.Position)
	}
}

func (l *ledger) applyBalances(api int64, in []*tdv1.Balance) {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := l.bal[api]
	if m == nil {
		m = map[string]*tdv1.Balance{}
		l.bal[api] = m
	}
	for _, b := range in {
		if b == nil {
			continue
		}
		m[b.GetAsset()] = proto.Clone(b).(*tdv1.Balance)
	}
}

func (l *ledger) replace(api int64, recon *tdv1.Recon) {
	if recon == nil {
		return
	}
	pos := map[uint32]*tdv1.Position{}
	for _, p := range recon.GetPositions() {
		if p != nil {
			pos[p.GetUid()] = proto.Clone(p).(*tdv1.Position)
		}
	}
	bal := map[string]*tdv1.Balance{}
	for _, b := range recon.GetBalances() {
		if b != nil {
			bal[b.GetAsset()] = proto.Clone(b).(*tdv1.Balance)
		}
	}
	l.mu.Lock()
	l.pos[api] = pos
	l.bal[api] = bal
	l.mu.Unlock()
}

func (l *ledger) position(api int64, uid uint32) (*tdv1.Position, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.pos[api][uid]
	if p == nil {
		return nil, false
	}
	return proto.Clone(p).(*tdv1.Position), true
}

func (l *ledger) balance(api int64, asset string) (*tdv1.Balance, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.bal[api][asset]
	if b == nil {
		return nil, false
	}
	return proto.Clone(b).(*tdv1.Balance), true
}
