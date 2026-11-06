package symgrpc

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1/symv1connect"
	"github.com/BullionBear/mftiko/internal/sym"
	"github.com/BullionBear/mftiko/internal/sym/symtest"
)

// TestConformance runs the protocol conformance table over gRPC, h2c (E22-06).
func TestConformance(t *testing.T) {
	symtest.Run(t, func(t *testing.T, api sym.API) symtest.Client {
		s := serve(t, api, nil)
		var protocols http.Protocols
		protocols.SetUnencryptedHTTP2(true)
		transport := &http.Transport{Protocols: &protocols}
		t.Cleanup(transport.CloseIdleConnections)
		return grpcClient{c: symv1connect.NewSymServiceClient(&http.Client{Transport: transport}, "http://"+s.addr,
			connect.WithGRPC(), connect.WithInterceptors(withBearer(testToken)))}
	})
}

func withBearer(token string) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", "Bearer "+token)
			return next(ctx, req)
		}
	}
}

// grpcClient is a connect client speaking gRPC. A business failure's code comes from the status
// code (E22-D004), not from the text.
type grpcClient struct{ c symv1connect.SymServiceClient }

var businessCode = map[connect.Code]string{
	connect.CodeInvalidArgument:  sym.CodeInvalid,
	connect.CodeNotFound:         sym.CodeNotFound,
	connect.CodeUnavailable:      sym.CodeUnavailable,
	connect.CodeCanceled:         sym.CodeUnavailable,
	connect.CodeDeadlineExceeded: sym.CodeUnavailable,
}

func business(err error) error {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return err
	}
	code, ok := businessCode[ce.Code()]
	if !ok {
		return err
	}
	return &symtest.Error{Code: code, Message: ce.Message()}
}

func (g grpcClient) Get(ctx context.Context, req *symv1.GetRequest) (*symv1.SymbolInfo, error) {
	resp, err := g.c.Get(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, business(err)
	}
	return resp.Msg.GetInfo(), nil
}

func (g grpcClient) GetByUID(ctx context.Context, req *symv1.GetByUidRequest) (*symv1.SymbolInfo, error) {
	resp, err := g.c.GetByUid(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, business(err)
	}
	return resp.Msg.GetInfo(), nil
}

func (g grpcClient) List(ctx context.Context, req *symv1.ListRequest) (*symv1.SymbolList, error) {
	resp, err := g.c.List(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, business(err)
	}
	return resp.Msg.GetList(), nil
}

func (g grpcClient) Reverse(ctx context.Context, req *symv1.ReverseRequest) (*symv1.SymbolInfo, error) {
	resp, err := g.c.Reverse(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, business(err)
	}
	return resp.Msg.GetInfo(), nil
}

func (g grpcClient) Revisions(ctx context.Context, req *symv1.RevisionsRequest) (*symv1.RevisionList, error) {
	resp, err := g.c.Revisions(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, business(err)
	}
	return resp.Msg.GetRevisions(), nil
}
