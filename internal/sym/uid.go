package sym

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

// uidMin and uidMax are the inclusive interval Insert draws from.
//
// D203 makes 0 illegal and leaves the width open (PROTOCOL §10 P13). These two
// constants are the suggested signed BIGINT interval, and they are the only
// place that interval is written down. A later decision changes them. Values
// outside the interval are discarded and another candidate is drawn; they are
// not inserted. The column itself only rejects 0, so a narrower or wider
// interval that still fits in BIGINT does not need a migration.
const (
	uidMin uint64 = 1
	uidMax uint64 = 1<<63 - 1
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
	// big.Int so a future uidMax of 2^64-1 does not wrap the span.
	span := new(big.Int).Sub(new(big.Int).SetUint64(uidMax), new(big.Int).SetUint64(uidMin))
	span.Add(span, big.NewInt(1))
	n, err := rand.Int(rand.Reader, span)
	if err != nil {
		return 0, fmt.Errorf("sym: draw uid: %w", err)
	}
	return n.Uint64() + uidMin, nil
}
