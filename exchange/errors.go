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
	// ErrAuth: the venue refused the credentials: a bad signature, a key without the permission, an
	// address off the key's allow list, an expired key. Not retried; the worker raises it (E03-D002).
	ErrAuth = errors.New("exchange: authentication refused")
	// ErrUnknownOutcome: a request that may have reached the venue got no answer the adapter could
	// read: a timeout, a dropped connection, a 5xx. The transport wraps it; for a request that
	// Mutates, the worker finds out from the private stream, a recon, or by querying the order by
	// client order id once the request's validity window has passed (E06-D005), and never counts it as
	// refused (E03-D002).
	ErrUnknownOutcome = errors.New("exchange: outcome unknown")
	// ErrNotFound: the venue says it has no such order (Account.QueryOrder). Past the request's
	// validity window it means the request never took effect (E06-D005); before it, the request may
	// still arrive.
	ErrNotFound = errors.New("exchange: no such order")
)

// Reject is the venue refusing an order request. Code follows PROTOCOL §7.2: 200–299 for refusals
// the adapter mapped to the platform table (copied from mftik), otherwise the venue's own code.
// The account worker passes it on as a td.v1.OrderUpdate whose cause is REJECTED or CANCEL_REJECTED (E06-D010).
type Reject struct {
	Code   int32
	Native string // the venue's code as sent, kept even when Code is mapped
	Reason string
}

func (r *Reject) Error() string {
	return "exchange: rejected " + strconv.Itoa(int(r.Code)) + " (" + r.Native + "): " + r.Reason
}

// RateLimited: the venue answered 429, 418, or its equivalent. The caller decides whether to wait.
// RetryAfterMs is the Retry-After header in milliseconds. 0 means the venue did not say.
// Status is the HTTP status when the venue sent one (429 or 418); 0 when a venue code was mapped
// without one.
type RateLimited struct {
	RetryAfterMs int64
	Status       int32
}

func (e *RateLimited) Error() string { return "exchange: rate limited" }
