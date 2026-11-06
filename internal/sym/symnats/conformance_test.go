package symnats

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/platform/natsx/natsxtest"
	"github.com/BullionBear/mftiko/internal/sym"
	"github.com/BullionBear/mftiko/internal/sym/symtest"
)

// TestConformance runs the protocol conformance table over the sym subject (E22-06).
func TestConformance(t *testing.T) {
	symtest.Run(t, func(t *testing.T, api sym.API) symtest.Client {
		ns := natsxtest.Start(t)
		if _, err := Serve(t.Context(), dialSym(t, ns.URL()), api, zerolog.Nop()); err != nil {
			t.Fatal(err)
		}
		return natsClient{conn: dialSym(t, ns.URL())}
	})
}

// natsClient is a natsx Request per call. A business failure is natsx.CodeHandler; its code is
// the text before ": ".
type natsClient struct{ conn *natsx.Conn }

func (c natsClient) call(ctx context.Context, req *symv1.Request) (*symv1.Reply, error) {
	var rep symv1.Reply
	err := c.conn.Request(ctx, Subject, req, &rep)
	var re *natsx.ReplyError
	if errors.As(err, &re) && re.Code == natsx.CodeHandler {
		code, _, _ := strings.Cut(re.Message, ": ")
		return nil, &symtest.Error{Code: code, Message: re.Message}
	}
	if err != nil {
		return nil, err
	}
	if rep.GetError() != "" || rep.GetReason() != "" {
		return nil, errors.New("Reply.error is set: " + rep.GetError())
	}
	return &rep, nil
}

func (c natsClient) Get(ctx context.Context, req *symv1.GetRequest) (*symv1.SymbolInfo, error) {
	rep, err := c.call(ctx, &symv1.Request{Req: &symv1.Request_Get{Get: req}})
	return rep.GetInfo(), err
}

func (c natsClient) GetByUID(ctx context.Context, req *symv1.GetByUidRequest) (*symv1.SymbolInfo, error) {
	rep, err := c.call(ctx, &symv1.Request{Req: &symv1.Request_GetByUid{GetByUid: req}})
	return rep.GetInfo(), err
}

func (c natsClient) List(ctx context.Context, req *symv1.ListRequest) (*symv1.SymbolList, error) {
	rep, err := c.call(ctx, &symv1.Request{Req: &symv1.Request_List{List: req}})
	return rep.GetList(), err
}

func (c natsClient) Reverse(ctx context.Context, req *symv1.ReverseRequest) (*symv1.SymbolInfo, error) {
	rep, err := c.call(ctx, &symv1.Request{Req: &symv1.Request_Reverse{Reverse: req}})
	return rep.GetInfo(), err
}

func (c natsClient) Revisions(ctx context.Context, req *symv1.RevisionsRequest) (*symv1.RevisionList, error) {
	rep, err := c.call(ctx, &symv1.Request{Req: &symv1.Request_Revisions{Revisions: req}})
	return rep.GetRevisions(), err
}
