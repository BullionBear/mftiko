package cloidgen

import (
	"errors"
	"fmt"
	"sync/atomic"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
)

// ErrAlgoNotDeclared means algo is not in the account's AccountDecl.algo_ids (E08-D014, E06-D033).
// Next returns it without taking a seq.
var ErrAlgoNotDeclared = errors.New("cloidgen: algo is not declared for the account")

// tsSpan is the 40-bit window of milliseconds from cloid.EpochMs (E06-D002).
const tsSpan = int64(1) << cloid.TsBits

// nextSlot is the one counter for this process (E08-D011). It packs the next id to issue:
// the high bits are ts_ms-cloid.EpochMs and the low SeqBits are seq (E08-D014).
//
// The zero value is epoch, seq 0, so the first id of a fresh process can be seq 0.
// Storing the next id, rather than the last one issued, is what makes that true:
// a zero last-issued word would look like epoch seq 0 had already been handed out.
//
// Next advances it with one compare-and-swap. There is no mutex.
var nextSlot atomic.Uint64

// Next is the next client_order_id for algo on account at nowMs.
// nowMs is the caller's clock, in unix milliseconds; Next does not read the time itself.
// algo must be listed in account's algo_ids. The id is cloid.Encode of version 1 (E08-D014).
func Next(account *sessionv1.AccountDecl, algo uint32, nowMs int64) (string, error) {
	if !declared(account, algo) {
		return "", fmt.Errorf("cloidgen: algo %d: %w", algo, ErrAlgoNotDeclared)
	}
	if algo < cloid.AlgoMin || algo > cloid.AlgoMax {
		// Listed, but it cannot be encoded. Do not take a number.
		return cloid.Encode(algo, cloid.EpochMs, 0)
	}
	tsMs, seq, err := issue(nowMs)
	if err != nil {
		return "", err
	}
	return cloid.Encode(algo, tsMs, seq)
}

func declared(account *sessionv1.AccountDecl, algo uint32) bool {
	for _, id := range account.GetAlgoIds() {
		if id == algo {
			return true
		}
	}
	return false
}

// issue takes the next (ts_ms, seq). nowMs before the counter, including a clock before
// the epoch, keeps the counter's ts_ms. nowMs ahead of the counter jumps to that
// millisecond and seq 0. A millisecond whose seq is exhausted borrows the next one.
// A ts_ms cloid.Encode cannot represent is refused and the counter is left as it was.
func issue(nowMs int64) (int64, uint32, error) {
	// A nowMs before the epoch is behind every ts the counter can hold. Subtracting it
	// from EpochMs would overflow int64 when nowMs is near MinInt64.
	nowOff := int64(-1)
	if nowMs >= cloid.EpochMs {
		nowOff = nowMs - cloid.EpochMs
	}
	for {
		old := nextSlot.Load()
		ts := int64(old >> cloid.SeqBits)
		seq := uint32(old & cloid.SeqMax)

		issueTs := ts
		issueSeq := seq
		if nowOff > ts {
			issueTs = nowOff
			issueSeq = 0
		}
		if issueTs < 0 || issueTs >= tsSpan {
			return 0, 0, windowErr(issueTs)
		}

		nextTs := issueTs
		nextSeq := issueSeq + 1
		if nextSeq > cloid.SeqMax {
			nextTs = issueTs + 1
			nextSeq = 0
		}
		packed := uint64(nextTs)<<cloid.SeqBits | uint64(nextSeq)
		if nextSlot.CompareAndSwap(old, packed) {
			return cloid.EpochMs + issueTs, issueSeq, nil
		}
	}
}

// windowErr is cloid.Encode's error for a ts offset outside the 40-bit window.
func windowErr(tsOff int64) error {
	_, err := cloid.Encode(cloid.AlgoMin, cloid.EpochMs+tsOff, 0)
	if err == nil {
		return errors.New("cloidgen: ts_ms outside the 40-bit window")
	}
	return err
}
