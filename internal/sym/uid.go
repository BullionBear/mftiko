package sym

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// maxUIDDraws bounds Insert. A collision is rare at this width; the cap is what
// a stuck source hits instead of looping.
const maxUIDDraws = 16

// Source draws candidate UIDs. Insert discards anything symbol.ValidUID rejects
// and retries when the insert reports that the uid is already stored.
// RandomSource is the production source. Tests pass a fixed sequence.
// The value is uint32, the same type as Info.UID. The interval is symbol.MinUID
// to symbol.MaxUID. The BIGINT columns check that same interval.
type Source interface {
	NextUID() (uint32, error)
}

// RandomSource draws uniformly from symbol.MinUID to symbol.MaxUID.
func RandomSource() Source { return randomSource{} }

type randomSource struct{}

func (randomSource) NextUID() (uint32, error) {
	if symbol.MinUID > symbol.MaxUID {
		return 0, fmt.Errorf("sym: uid interval %d..%d is empty", symbol.MinUID, symbol.MaxUID)
	}
	// big.Int so the span is the count of values in the interval, not a wrapping subtraction.
	span := new(big.Int).SetUint64(uint64(symbol.MaxUID) - uint64(symbol.MinUID) + 1)
	n, err := rand.Int(rand.Reader, span)
	if err != nil {
		return 0, fmt.Errorf("sym: draw uid: %w", err)
	}
	return symbol.MinUID + uint32(n.Uint64()), nil
}
