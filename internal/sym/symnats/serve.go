package symnats

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/BullionBear/mftiko/internal/sym"
)

const (
	// Subject is the sym request subject (PROTOCOL §7). Serve joins the queue group of the
	// same name, so any replica answers.
	Subject = "sym"

	// maxReplyBytes is one byte under NATS's default 1 MiB payload, so a published reply is
	// accepted. sym.Service already caps a List page; this guards the Reply envelope around it.
	maxReplyBytes = 1<<20 - 1
)

// Serve answers Subject from api in the queue group of the same name, so any replica
// answers. It returns once the server has the interest. The handler stops when ctx is
// cancelled.
//
// A success is a symv1.Reply and nothing else. A *sym.Error is a natsx error reply:
// natsx.CodeHandler and the error's text. A payload that is not a Request is
// natsx.CodeBadRequest, which natsx answers before api is called.
func Serve(ctx context.Context, c *natsx.Conn, api sym.API, log zerolog.Logger) (*natsx.Subscription, error) {
	if api == nil {
		return nil, errors.New("symnats: nil api")
	}
	if c == nil {
		return nil, errors.New("symnats: nil connection")
	}
	s := &server{api: api, log: log}
	return c.Serve(ctx, Subject, Subject, func() proto.Message {
		return &symv1.Request{}
	}, s.handle)
}

type server struct {
	api sym.API
	log zerolog.Logger
}

func (s *server) handle(ctx context.Context, m proto.Message) (proto.Message, error) {
	req, ok := m.(*symv1.Request)
	if !ok || req == nil {
		return nil, emptyRequest()
	}
	reply, err := s.dispatch(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := fits(reply); err != nil {
		s.note(err)
		return nil, err
	}
	return reply, nil
}

func (s *server) dispatch(ctx context.Context, req *symv1.Request) (*symv1.Reply, error) {
	switch r := req.GetReq().(type) {
	case *symv1.Request_Get:
		info, err := s.api.Get(ctx, r.Get)
		return infoReply(info, err)
	case *symv1.Request_GetByUid:
		info, err := s.api.GetByUID(ctx, r.GetByUid)
		return infoReply(info, err)
	case *symv1.Request_List:
		list, err := s.api.List(ctx, r.List)
		if err != nil {
			return nil, err
		}
		return &symv1.Reply{Result: &symv1.Reply_List{List: list}}, nil
	case *symv1.Request_Reverse:
		info, err := s.api.Reverse(ctx, r.Reverse)
		return infoReply(info, err)
	case *symv1.Request_Refresh:
		res, err := s.api.Refresh(ctx, r.Refresh)
		if err != nil {
			return nil, err
		}
		return &symv1.Reply{Result: &symv1.Reply_Refresh{Refresh: res}}, nil
	case *symv1.Request_Revisions:
		revs, err := s.api.Revisions(ctx, r.Revisions)
		if err != nil {
			return nil, err
		}
		return &symv1.Reply{Result: &symv1.Reply_Revisions{Revisions: revs}}, nil
	default:
		return nil, emptyRequest()
	}
}

func infoReply(info *symv1.SymbolInfo, err error) (*symv1.Reply, error) {
	if err != nil {
		return nil, err
	}
	return &symv1.Reply{Result: &symv1.Reply_Info{Info: info}}, nil
}

func emptyRequest() error {
	return &sym.Error{Code: sym.CodeInvalid, Reason: "request is empty"}
}

// note logs an unavailable failure this adapter raised. api logs its own.
func (s *server) note(err error) {
	var e *sym.Error
	if !errors.As(err, &e) || e.Code != sym.CodeUnavailable {
		return
	}
	s.log.Error().Str("reason", e.Reason).Msg("sym unavailable")
}

func fits(m proto.Message) error {
	if m == nil {
		return unavailable("reply is empty")
	}
	n := proto.Size(m)
	if n <= maxReplyBytes {
		return nil
	}
	if page := replyPage(m); page != nil {
		return unavailable(fmt.Sprintf("page of %d rows is %d bytes, exceeds 1 MiB limit; lower limit", len(page.GetSymbols()), n))
	}
	return unavailable(fmt.Sprintf("reply is %d bytes, exceeds 1 MiB limit", n))
}

func unavailable(reason string) error {
	return &sym.Error{Code: sym.CodeUnavailable, Reason: reason}
}

func replyPage(m proto.Message) *symv1.SymbolList {
	reply, ok := m.(*symv1.Reply)
	if !ok {
		return nil
	}
	return reply.GetList()
}
