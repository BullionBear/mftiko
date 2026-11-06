package symgrpc

import (
	"context"
	"errors"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/bearer"
)

// gate checks the bearer token of every call it wraps and writes one access log line per call,
// with the token's name (E22-D007).
type gate struct {
	tokens *bearer.Tokens
	log    zerolog.Logger
}

var _ connect.Interceptor = (*gate)(nil)

func (g *gate) check(h http.Header) (string, error) {
	token, ok := bearer.FromHeader(h)
	if !ok {
		return "", connect.NewError(connect.CodeUnauthenticated, errors.New("bearer token required"))
	}
	name, ok := g.tokens.Match(token)
	if !ok {
		return "", connect.NewError(connect.CodeUnauthenticated, errors.New("invalid bearer token"))
	}
	return name, nil
}

func (g *gate) access(procedure, peer, name string, start time.Time, err error) {
	code := "ok"
	if err != nil {
		code = connect.CodeOf(err).String()
	}
	g.log.Info().
		Str("rpc", procedure).
		Str("name", name).
		Str("peer", peer).
		Str("code", code).
		Dur("took", time.Since(start)).
		Msg("grpc")
}

func (g *gate) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		start := time.Now()
		name, err := g.check(req.Header())
		var resp connect.AnyResponse
		if err == nil {
			resp, err = next(ctx, req)
		}
		g.access(req.Spec().Procedure, req.Peer().Addr, name, start, err)
		return resp, err
	}
}

func (g *gate) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		start := time.Now()
		name, err := g.check(conn.RequestHeader())
		if err == nil {
			err = next(ctx, conn)
		}
		g.access(conn.Spec().Procedure, conn.Peer().Addr, name, start, err)
		return err
	}
}

func (g *gate) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}
