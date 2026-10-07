package exchange

import (
	"errors"
	"strconv"
)

var (
	// ErrNotImplemented: the adapter has the method but no code behind it yet.
	ErrNotImplemented = errors.New("exchange: not implemented")
	// ErrUnsupported: the venue does not offer this (a topic it does not push, a query it lacks).
	ErrUnsupported = errors.New("exchange: unsupported on this venue")
)

// Reject is the venue refusing an order request. Code follows PROTOCOL §7.2: 200–299 for refusals
// the adapter mapped to the platform table (copied from mftik), otherwise the venue's own code.
// The account worker passes it to the strategy as OrderReject or CancelReject.
type Reject struct {
	Code   int32
	Native string // the venue's code as sent, kept even when Code is mapped
	Reason string
}

func (r *Reject) Error() string {
	return "exchange: rejected " + strconv.Itoa(int(r.Code)) + " (" + r.Native + "): " + r.Reason
}

// RateLimited: the venue answered 429 or its equivalent. The caller decides whether to wait.
type RateLimited struct {
	RetryAfterMs int64 // 0 = the venue did not say
}

func (e *RateLimited) Error() string { return "exchange: rate limited" }
