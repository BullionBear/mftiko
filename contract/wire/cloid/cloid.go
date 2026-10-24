package cloid

import (
	"errors"
	"fmt"
	"strconv"
)

// Layout, most significant bits first: ver(4) | algo(10) | ts_ms(40) | seq(10).
// Sent as the decimal string of the uint64. ver stays in 1..7 so the top bit is 0 and an int64 parse works.
const (
	VerBits  = 4
	AlgoBits = 10
	TsBits   = 40
	SeqBits  = 10

	Version = 1

	// EpochMs is 2026-01-01T00:00:00Z in unix milliseconds. 40 bits of ms last until 2060-11.
	EpochMs int64 = 1767225600000

	// AlgoMin and AlgoMax bound a declared algo_id; 0 is reserved (DECISIONS E06-D002).
	AlgoMin = 1
	AlgoMax = 1<<AlgoBits - 1

	// SeqMax is the last seq in one millisecond.
	SeqMax = 1<<SeqBits - 1

	// MaxLen is the longest client_order_id: the top bit is 0, so the value is below 2^63, which
	// has 19 decimal digits. Every venue takes at least that (AccountLimits.ClientOrderIDMaxLen).
	MaxLen = 19

	seqShift  = 0
	tsShift   = SeqBits
	algoShift = SeqBits + TsBits
	verShift  = SeqBits + TsBits + AlgoBits
	verMax    = 7
	tsSpan    = int64(1) << TsBits
)

// ErrNotPlatform: the string is not a client_order_id of this platform's layout. Orders placed by
// hand or by another system carry such ids; they belong to no algo.
var ErrNotPlatform = errors.New("cloid: not a platform client_order_id")

// ID is one decoded client_order_id.
type ID struct {
	Ver  uint8
	Algo uint32
	TsMs int64 // unix milliseconds
	Seq  uint32
}

// String is the client_order_id: the decimal of the uint64, with no sign and no leading zero.
func (id ID) String() string {
	v := uint64(id.Ver)<<verShift | uint64(id.Algo)<<algoShift | uint64(id.TsMs-EpochMs)<<tsShift | uint64(id.Seq)<<seqShift
	return strconv.FormatUint(v, 10)
}

func (id ID) valid() error {
	switch {
	case id.Ver < 1 || id.Ver > verMax:
		return fmt.Errorf("cloid: version %d outside 1..%d", id.Ver, verMax)
	case id.Algo < AlgoMin || id.Algo > AlgoMax:
		return fmt.Errorf("cloid: algo %d outside %d..%d", id.Algo, AlgoMin, AlgoMax)
	case id.TsMs < EpochMs || id.TsMs-EpochMs >= tsSpan:
		return fmt.Errorf("cloid: ts_ms %d outside the 40-bit window from %d", id.TsMs, EpochMs)
	case id.Seq > SeqMax:
		return fmt.Errorf("cloid: seq %d above %d", id.Seq, SeqMax)
	}
	return nil
}

// Encode is the client_order_id of algo at tsMs (unix milliseconds) with seq, in the current
// Version. It fails when a field does not fit the layout.
func Encode(algo uint32, tsMs int64, seq uint32) (string, error) {
	id := ID{Ver: Version, Algo: algo, TsMs: tsMs, Seq: seq}
	if err := id.valid(); err != nil {
		return "", err
	}
	return id.String(), nil
}

// Decode reads a client_order_id. Only the one spelling Encode makes is a platform id: decimal
// digits with no sign and no leading zero, a version in 1..7 and an algo that is not 0. Anything
// else is ErrNotPlatform, which is not a malformed request: it is an order the platform did not
// number.
func Decode(s string) (ID, error) {
	if len(s) == 0 || len(s) > MaxLen || s[0] == '0' {
		return ID{}, ErrNotPlatform
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return ID{}, ErrNotPlatform
		}
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return ID{}, ErrNotPlatform
	}
	id := ID{
		Ver:  uint8(v >> verShift),
		Algo: uint32(v >> algoShift & AlgoMax),
		TsMs: int64(v>>tsShift&uint64(tsSpan-1)) + EpochMs,
		Seq:  uint32(v & SeqMax),
	}
	if id.valid() != nil {
		return ID{}, ErrNotPlatform
	}
	return id, nil
}

// AlgoOf is the algo a client_order_id belongs to, and false for one that is not a platform id:
// an order placed by hand or by another system belongs to no algo (E06-D002).
//
// The check guards against mistakes, not malice. A hand-placed order whose id happens to be a valid
// platform id is taken as that algo's.
func AlgoOf(s string) (uint32, bool) {
	id, err := Decode(s)
	if err != nil {
		return 0, false
	}
	return id.Algo, true
}
