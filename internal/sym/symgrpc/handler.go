package symgrpc

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1/symv1connect"
	"github.com/BullionBear/mftiko/internal/sym"
)

// handler is SymService over sym.API. WatchChanges answers Unimplemented until E22-07.
type handler struct {
	symv1connect.UnimplementedSymServiceHandler
	api sym.API
}

var _ symv1connect.SymServiceHandler = (*handler)(nil)

func (h *handler) Get(ctx context.Context, req *connect.Request[symv1.GetRequest]) (*connect.Response[symv1.GetResponse], error) {
	info, err := h.api.Get(ctx, req.Msg)
	if err != nil {
		return nil, status(err)
	}
	return connect.NewResponse(&symv1.GetResponse{Info: info}), nil
}

func (h *handler) GetByUid(ctx context.Context, req *connect.Request[symv1.GetByUidRequest]) (*connect.Response[symv1.GetByUidResponse], error) {
	info, err := h.api.GetByUID(ctx, req.Msg)
	if err != nil {
		return nil, status(err)
	}
	return connect.NewResponse(&symv1.GetByUidResponse{Info: info}), nil
}

func (h *handler) List(ctx context.Context, req *connect.Request[symv1.ListRequest]) (*connect.Response[symv1.ListResponse], error) {
	list, err := h.api.List(ctx, req.Msg)
	if err != nil {
		return nil, status(err)
	}
	return connect.NewResponse(&symv1.ListResponse{List: list}), nil
}

func (h *handler) Reverse(ctx context.Context, req *connect.Request[symv1.ReverseRequest]) (*connect.Response[symv1.ReverseResponse], error) {
	info, err := h.api.Reverse(ctx, req.Msg)
	if err != nil {
		return nil, status(err)
	}
	return connect.NewResponse(&symv1.ReverseResponse{Info: info}), nil
}

func (h *handler) Revisions(ctx context.Context, req *connect.Request[symv1.RevisionsRequest]) (*connect.Response[symv1.RevisionsResponse], error) {
	revs, err := h.api.Revisions(ctx, req.Msg)
	if err != nil {
		return nil, status(err)
	}
	return connect.NewResponse(&symv1.RevisionsResponse{Revisions: revs}), nil
}

// status maps a business error to its gRPC code (E22-D004). The message is the same
// "<code>: <reason>" the sym subject sends; the cause stays in the server's log.
func status(err error) error {
	var e *sym.Error
	if !errors.As(err, &e) {
		return connect.NewError(connect.CodeInternal, errors.New("internal error"))
	}
	code := connect.CodeUnavailable
	switch {
	case e.Code == sym.CodeInvalid:
		code = connect.CodeInvalidArgument
	case e.Code == sym.CodeNotFound:
		code = connect.CodeNotFound
	case errors.Is(err, context.Canceled):
		code = connect.CodeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		code = connect.CodeDeadlineExceeded
	}
	return connect.NewError(code, errors.New(e.Error()))
}
