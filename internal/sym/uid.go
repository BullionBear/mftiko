package sym

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

// uidMin and uidMax are the inclusive interval Insert draws from.
//
// 0 is illegal. The decided interval is 1..4294967295 (2^32-1). The store keeps
// the value as uint64 so it does not depend on the contract field's width.
// A candidate outside the interval is discarded and another is drawn; it is not
// inserted. The BIGINT columns check the same interval (uid BETWEEN 1 AND 4294967295).
const (
	uidMin uint64 = 1
	uidMax uint64 = 4294967295
)

// maxUIDDraws bounds Insert. A collision is rare at this width; the cap is what
// a stuck source hits instead of looping.
const maxUIDDraws = 16

// Source draws candidate UIDs. Insert discards anything outside uidMin..uidMax
// and retries when the insert reports that the uid is already stored.
// RandomSource is the production source. Tests pass a fixed sequence.
type Source interface {
	NextUID() (uint64, error)
}

// RandomSource draws uniformly from uidMin..uidMax.
func RandomSource() Source { return randomSource{} }

type randomSource struct{}

func (randomSource) NextUID() (uint64, error) {
	if uidMin == 0 || uidMin > uidMax {
		return 0, fmt.Errorf("sym: uid interval %d..%d is empty", uidMin, uidMax)
	}
	// big.Int so the span is the count of values in the interval, not a wrapping subtraction.
	span := new(big.Int).Sub(new(big.Int).SetUint64(uidMax), new(big.Int).SetUint64(uidMin))
	span.Add(span, big.NewInt(1))
	n, err := rand.Int(rand.Reader, span)
	if err != nil {
		return 0, fmt.Errorf("sym: draw uid: %w", err)
	}
	return n.Uint64() + uidMin, nil
}
