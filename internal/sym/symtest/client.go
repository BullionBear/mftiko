// Package symtest is SYM's protocol conformance suite (E22-D002): one table of requests and the
// answers PROTOCOL §7.3 gives them, run against every protocol. A protocol joins by serving a
// sym.API and handing back a Client of itself; Run does the rest. Tests import it; nothing else
// does.
package symtest

import (
	"context"
	"errors"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/internal/sym"
)

// Client is one protocol's view of SymService's reads.
//
// A business failure is an *Error. The adapter derives Code from what its protocol carries:
// NATS from the text before ": ", gRPC from the status code. So Run checks the protocol's
// mapping as well as the text.
type Client interface {
	Get(ctx context.Context, req *symv1.GetRequest) (*symv1.SymbolInfo, error)
	GetByUID(ctx context.Context, req *symv1.GetByUidRequest) (*symv1.SymbolInfo, error)
	List(ctx context.Context, req *symv1.ListRequest) (*symv1.SymbolList, error)
	Reverse(ctx context.Context, req *symv1.ReverseRequest) (*symv1.SymbolInfo, error)
	Revisions(ctx context.Context, req *symv1.RevisionsRequest) (*symv1.RevisionList, error)
}

// Error is a business failure as a client received it: sym.CodeInvalid, sym.CodeNotFound or
// sym.CodeUnavailable, and the "<code>: <reason>" text (E22-D004).
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

// Direct is the logic layer itself as a Client: the answer every protocol must match.
func Direct(api sym.API) Client { return direct{api: api} }

type direct struct{ api sym.API }

func (d direct) Get(ctx context.Context, req *symv1.GetRequest) (*symv1.SymbolInfo, error) {
	out, err := d.api.Get(ctx, req)
	return out, business(err)
}

func (d direct) GetByUID(ctx context.Context, req *symv1.GetByUidRequest) (*symv1.SymbolInfo, error) {
	out, err := d.api.GetByUID(ctx, req)
	return out, business(err)
}

func (d direct) List(ctx context.Context, req *symv1.ListRequest) (*symv1.SymbolList, error) {
	out, err := d.api.List(ctx, req)
	return out, business(err)
}

func (d direct) Reverse(ctx context.Context, req *symv1.ReverseRequest) (*symv1.SymbolInfo, error) {
	out, err := d.api.Reverse(ctx, req)
	return out, business(err)
}

func (d direct) Revisions(ctx context.Context, req *symv1.RevisionsRequest) (*symv1.RevisionList, error) {
	out, err := d.api.Revisions(ctx, req)
	return out, business(err)
}

func business(err error) error {
	var e *sym.Error
	if errors.As(err, &e) {
		return &Error{Code: e.Code, Message: e.Error()}
	}
	return err
}
