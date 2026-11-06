package symgrpc

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1/symv1connect"
	"github.com/BullionBear/mftiko/internal/sym"
)

// handler is SymService over sym.API.
type handler struct {
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

// WatchChanges sends the snapshot, then every commit after it (E22-D009). A watch that falls
// behind ends with ResourceExhausted; SYM shutting down ends it with Unavailable.
func (h *handler) WatchChanges(ctx context.Context, req *connect.Request[symv1.WatchChangesRequest], stream *connect.ServerStream[symv1.WatchChangesResponse]) error {
	w, err := h.api.Watch(ctx, sym.WatchFilter{Venue: req.Msg.GetVenue(), Category: req.Msg.GetCategory()})
	if err != nil {
		return status(err)
	}
	defer w.Close()
	if err := stream.Send(&symv1.WatchChangesResponse{Event: &symv1.WatchChangesResponse_Snapshot{Snapshot: w.Snapshot}}); err != nil {
		return err
	}
	for {
		ch, err := w.Next(ctx)
		if err != nil {
			return watchEnd(ctx, err)
		}
		if err := stream.Send(&symv1.WatchChangesResponse{Event: &symv1.WatchChangesResponse_Changed{Changed: ch}}); err != nil {
			return err
		}
	}
}

// watchEnd is the status a stream ends with when Next fails.
func watchEnd(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, sym.ErrSlowWatcher):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("watch fell behind; open a new watch for a new snapshot"))
	case ctx.Err() != nil:
		return ctx.Err()
	default:
		return status(err)
	}
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
