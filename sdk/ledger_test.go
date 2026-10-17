package sdk

import (
	"testing"

	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
)

func TestLedgerUpdatesThenReconReplaces(t *testing.T) {
	l := newLedger()
	l.applyPositions(7, []*tdv1.Position{{Uid: 1, Size: 2}, {Uid: 3, Size: 4}})
	l.applyBalances(7, []*tdv1.Balance{{Asset: "USDT", Free: 9}})
	l.applyPositions(7, []*tdv1.Position{{Uid: 1, Size: 5}})

	got, ok := l.position(7, 1)
	if !ok || got.GetSize() != 5 {
		t.Fatalf("position %+v %v", got, ok)
	}
	if _, ok := l.position(7, 3); !ok {
		t.Fatal("untouched uid dropped")
	}
	got.Size = 99
	got, ok = l.position(7, 1)
	if !ok || got.GetSize() != 5 {
		t.Fatalf("stored position changed %+v", got)
	}

	l.replace(7, &tdv1.Recon{
		Positions: []*tdv1.Position{{Uid: 8, Size: 1}},
		Balances:  []*tdv1.Balance{{Asset: "BTC", Free: 2}},
	})
	if _, ok := l.position(7, 1); ok {
		t.Fatal("old uid survived recon")
	}
	if _, ok := l.balance(7, "USDT"); ok {
		t.Fatal("old asset survived recon")
	}
	if p, ok := l.position(7, 8); !ok || p.GetSize() != 1 {
		t.Fatalf("recon position %+v %v", p, ok)
	}
	if b, ok := l.balance(7, "BTC"); !ok || b.GetFree() != 2 {
		t.Fatalf("recon balance %+v %v", b, ok)
	}
	if _, ok := l.position(8, 8); ok {
		t.Fatal("other account")
	}
}
