// Package fasthttp is the REST transport: an exchange.Doer over github.com/valyala/fasthttp
// (DECISIONS D302). It sends the HTTPRequest an adapter built as it is, and classifies what has no
// answer:
//
//   - The request certainly did not go out (the dial, the proxy or the TLS handshake failed; the
//     context ended or the deadline passed before sending; no free connection): a plain error.
//   - It may have gone out (a timeout, a dropped connection, a 5xx): the error wraps
//     exchange.ErrUnknownOutcome. For a request that Mutates, the worker finds out from the private
//     stream or a recon.
//
// Anything else is an answer, and Parse judges it. fasthttp has no context: a request is sent with
// DoDeadline at the context's deadline and cannot be called back once sent. It never retries
// (MaxIdemponentCallAttempts is 1): the worker decides. HTTP/1.1.
package fasthttp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	fh "github.com/valyala/fasthttp"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport"
)

// DefaultTimeout bounds a request whose context has no deadline.
const DefaultTimeout = 10 * time.Second

type Options struct {
	// Timeout bounds a request whose context has no deadline: fasthttp needs one. 0 is
	// DefaultTimeout.
	Timeout time.Duration
}

// Doer is the fasthttp exchange.Doer. It is also an exchange.Lender: exchange.Run hands Parse the
// response in place and the Doer takes it back once Parse returns, while Do returns a copy. Safe for
// concurrent use.
type Doer struct {
	c       fh.Client
	timeout time.Duration
}

var _ exchange.Lender = (*Doer)(nil)

// New builds a Doer that dials through n (nil: transport.Direct).
func New(n *transport.Net, o Options) *Doer {
	if n == nil {
		n = transport.Direct()
	}
	d := &Doer{timeout: o.Timeout}
	if d.timeout <= 0 {
		d.timeout = DefaultTimeout
	}
	d.c = fh.Client{
		MaxIdemponentCallAttempts:     1,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,
		ConfigureClient: func(hc *fh.HostClient) error {
			hc.Dial = nil
			hc.DialTimeout = dialer(n, hc.IsTLS)
			return nil
		},
	}
	return d
}

// dialer dials one host's connections through n, the TLS handshake included, so fasthttp leaves the
// connection as it is. Every error it returns is a dialError: the request did not go out.
func dialer(n *transport.Net, isTLS bool) fh.DialFuncWithTimeout {
	return func(addr string, timeout time.Duration) (net.Conn, error) {
		ctx := context.Background()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		dial := n.DialContext
		if isTLS {
			dial = n.DialTLSContext
		}
		conn, err := dial(ctx, "tcp", addr)
		if err != nil {
			return nil, &dialError{err: err}
		}
		return conn, nil
	}
}

type dialError struct{ err error }

func (e *dialError) Error() string { return "dial: " + e.err.Error() }
func (e *dialError) Unwrap() error { return e.err }

// Do sends req and returns a copy of the answer, which the caller keeps.
func (d *Doer) Do(ctx context.Context, req *exchange.HTTPRequest) (*exchange.HTTPResponse, error) {
	var out *exchange.HTTPResponse
	err := d.Lend(ctx, req, func(resp *exchange.HTTPResponse) error {
		out = &exchange.HTTPResponse{
			Status: resp.Status,
			Header: append([]exchange.Header(nil), resp.Header...),
			Body:   append([]byte(nil), resp.Body...),
		}
		return nil
	})
	return out, err
}

// Lend sends req and passes the answer to use. The response, its Body included, goes back to
// fasthttp's pool once use returns.
func (d *Doer) Lend(ctx context.Context, req *exchange.HTTPRequest, use func(*exchange.HTTPResponse) error) error {
	if err := ctx.Err(); err != nil {
		return notSent(req, err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(d.timeout)
	}
	if !time.Now().Before(deadline) {
		return notSent(req, context.DeadlineExceeded)
	}
	if !strings.HasPrefix(req.Base, "https://") && !strings.HasPrefix(req.Base, "http://") {
		return notSent(req, fmt.Errorf("base %q is not http:// or https://", req.Base))
	}

	freq := fh.AcquireRequest()
	defer fh.ReleaseRequest(freq)
	freq.Header.DisableNormalizing()
	freq.Header.SetMethod(req.Method)
	freq.SetRequestURI(req.Base + req.Path)
	if len(req.Query) > 0 {
		freq.URI().SetQueryStringBytes(req.Query)
	}
	freq.Header.SetNoDefaultContentType(true)
	for _, h := range req.Header {
		freq.Header.Add(h.Name, h.Value)
	}
	if len(req.Body) > 0 {
		freq.SetBodyRaw(req.Body)
	}

	fresp := fh.AcquireResponse()
	defer fh.ReleaseResponse(fresp)
	if err := d.c.DoDeadline(freq, fresp, deadline); err != nil {
		return classify(req, err)
	}
	if s := fresp.StatusCode(); s >= 500 {
		return fmt.Errorf("fasthttp: %s %s: status %d: %w", req.Method, req.Path, s, exchange.ErrUnknownOutcome)
	}

	r := responses.Get().(*exchange.HTTPResponse)
	defer func() {
		clear(r.Header)
		*r = exchange.HTTPResponse{Header: r.Header[:0]}
		responses.Put(r)
	}()
	r.Status = fresp.StatusCode()
	for k, v := range fresp.Header.All() {
		r.Header = append(r.Header, exchange.Header{Name: string(k), Value: string(v)})
	}
	r.Body = fresp.Body()
	return use(r)
}

var responses = sync.Pool{New: func() any { return &exchange.HTTPResponse{Header: make([]exchange.Header, 0, 16)} }}

// classify sorts a fasthttp error by whether the request may have gone out.
func classify(req *exchange.HTTPRequest, err error) error {
	var de *dialError
	switch {
	case errors.As(err, &de),
		errors.Is(err, fh.ErrNoFreeConns),
		errors.Is(err, fh.ErrorInvalidURI),
		errors.Is(err, fh.ErrHostClientRedirectToDifferentScheme),
		errors.Is(err, fh.ErrConnPoolStrategyNotImpl):
		return notSent(req, err)
	}
	return fmt.Errorf("fasthttp: %s %s: %w: %w", req.Method, req.Path, exchange.ErrUnknownOutcome, err)
}

// notSent is the error for a request that did not go out.
func notSent(req *exchange.HTTPRequest, err error) error {
	return fmt.Errorf("fasthttp: %s %s: not sent: %w", req.Method, req.Path, err)
}
