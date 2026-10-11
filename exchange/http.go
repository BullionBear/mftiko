package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// The REST half of every adapter is sans-I/O (DECISIONS D302): a method builds an HTTPRequest and the
// function that parses its HTTPResponse, as a Call; whoever owns the connection sends it. The adapter
// never dials, never reads a clock (now comes in), never sleeps and never retries.

// Header is one HTTP header.
type Header struct {
	Name, Value string
}

// Cost is what one request takes from one of the venue's rate-limit buckets. Bucket names are the
// adapter's own (Binance "weight", "orders_10s"); the limits are in the adapter's Limits.
type Cost struct {
	Bucket string
	N      int
}

// Used is a bucket's usage as the venue reports it in a response (Binance X-MBX-USED-WEIGHT-1M).
// ResetAt is the absolute time that counter returns to zero. The zero time means the venue did
// not say when (D420).
type Used struct {
	Bucket  string
	N       int
	ResetAt time.Time
}

// HTTPRequest is one REST request, ready to send: the query is encoded and the request signed. Body is
// what was signed, when the venue signs the body; nothing may marshal it again (D008).
type HTTPRequest struct {
	Method string
	Base   string // scheme://host: the venue's default, or the configured endpoint
	Path   string
	Query  []byte // encoded, without '?'
	Header []Header
	Body   []byte
	Cost   []Cost
	// Mutates: sending it twice can change the account (Submit, Cancel). A transport failure after it
	// may have gone out is ErrUnknownOutcome, never a plain failure.
	Mutates bool
}

// String is the request as it may be logged or put in an error: header names with every value
// masked (headers carry the API key, OKX's passphrase and the signature), and the path, query and
// body with any listenKey masked (MaskListenKey). Format and MarshalJSON give the same, so neither a
// %v nor zerolog's Interface can leak one.
func (r HTTPRequest) String() string {
	b := make([]byte, 0, 64+len(r.Path)+len(r.Query)+len(r.Body))
	b = append(append(append(append(b, r.Method...), ' '), r.Base...), MaskListenKey([]byte(r.Path))...)
	if len(r.Query) > 0 {
		b = append(append(b, '?'), MaskListenKey(r.Query)...)
	}
	b = appendHeaders(b, r.Header, true)
	if len(r.Body) > 0 {
		b = append(append(b, ' '), MaskListenKey(r.Body)...)
	}
	return string(b)
}

func (r HTTPRequest) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, r.String()) }

func (r HTTPRequest) MarshalJSON() ([]byte, error) { return json.Marshal(r.String()) }

// HTTPResponse is what came back. Body is lent to Parse for the duration of the call: anything Parse
// keeps it copies (D302).
type HTTPResponse struct {
	Status int
	Header []Header
	Body   []byte
}

// String is the response as it may be logged: the status, the headers, and the body with any
// listenKey masked (Binance answers a listenKey's start and keepalive with it). Format and
// MarshalJSON give the same.
func (r HTTPResponse) String() string {
	b := strconv.AppendInt(make([]byte, 0, 32+len(r.Body)), int64(r.Status), 10)
	b = appendHeaders(b, r.Header, false)
	if len(r.Body) > 0 {
		b = append(append(b, ' '), MaskListenKey(r.Body)...)
	}
	return string(b)
}

func (r HTTPResponse) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, r.String()) }

func (r HTTPResponse) MarshalJSON() ([]byte, error) { return json.Marshal(r.String()) }

// Get returns the first value of header name, compared case-insensitively.
func (r *HTTPResponse) Get(name string) string {
	for _, h := range r.Header {
		if equalFold(h.Name, name) {
			return h.Value
		}
	}
	return ""
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}

// Call is one REST request and the parse of its answer. Parse classifies the response: a *Reject, a
// *RateLimited, ErrAuth, or another error for an answer it cannot read.
type Call[T any] struct {
	Req   HTTPRequest
	Parse func(*HTTPResponse) (T, error)
}

// Page is one page of a paginated read. Next builds the request for the following page, with the
// time it is sent at, so a signed page carries a fresh timestamp; nil when this was the last.
type Page[E any] struct {
	Items []E
	Next  func(now time.Time) (Call[Page[E]], error)
}

// Doer sends HTTP requests: the transport (exchange/transport/fasthttp, or a fake in a test). It
// returns an error only when no answer Parse could read came back. For a request that may have gone
// out (a timeout, a dropped connection, a 5xx) it wraps ErrUnknownOutcome; for one that certainly did
// not (the dial failed) it does not.
type Doer interface {
	Do(ctx context.Context, req *HTTPRequest) (*HTTPResponse, error)
}

// Lender is a Doer that can lend its response instead of handing it over: use gets the response
// for the duration of the call, and the Lender reuses it once use returns (D302). Lend returns
// use's error, or what Do would have. Run and Collect lend when they can.
type Lender interface {
	Doer
	Lend(ctx context.Context, req *HTTPRequest, use func(*HTTPResponse) error) error
}

// ErrNoTransport: a Call was run with no Doer to send it.
var ErrNoTransport = errors.New("exchange: no HTTP transport")

// Run sends c over d and parses the answer.
func Run[T any](ctx context.Context, d Doer, c Call[T]) (T, error) {
	var zero T
	if d == nil {
		return zero, ErrNoTransport
	}
	if l, ok := d.(Lender); ok {
		var out T
		err := l.Lend(ctx, &c.Req, func(resp *HTTPResponse) error {
			var err error
			out, err = c.Parse(resp)
			return err
		})
		return out, err
	}
	resp, err := d.Do(ctx, &c.Req)
	if err != nil {
		return zero, err
	}
	return c.Parse(resp)
}

// Collect runs a paginated read to the end: every page, or an error and nothing. now stamps each
// following page.
func Collect[E any](ctx context.Context, d Doer, now func() time.Time, first Call[Page[E]]) ([]E, error) {
	var out []E
	c := first
	for {
		p, err := Run(ctx, d, c)
		if err != nil {
			return nil, err
		}
		out = append(out, p.Items...)
		if p.Next == nil {
			return out, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c, err = p.Next(now()); err != nil {
			return nil, err
		}
	}
}

// Lister is what SYM reads: every instrument of one category, read to the end, or an error (D201).
type Lister interface {
	Listing(ctx context.Context, c symbol.Category) ([]symbol.Listing, error)
}

// ListingOver is a venue's PublicREST read over d: what SYM and the conformance tool call.
func ListingOver(rest PublicREST, d Doer, now func() time.Time) Lister {
	return &restLister{rest: rest, d: d, now: now}
}

type restLister struct {
	rest PublicREST
	d    Doer
	now  func() time.Time
}

func (l *restLister) Listing(ctx context.Context, c symbol.Category) ([]symbol.Listing, error) {
	first, err := l.rest.Listing(c)
	if err != nil {
		return nil, fmt.Errorf("exchange: listing %s: %w", c, err)
	}
	return Collect(ctx, l.d, l.now, first)
}
