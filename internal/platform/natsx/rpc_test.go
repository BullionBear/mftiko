package natsx_test

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func errorIs(err, target error) bool { return errors.Is(err, target) }

func TestRequestSuccess(t *testing.T) {
	_, c := start(t)
	if _, err := c.Serve(t.Context(), "echo", "echoers",
		func() proto.Message { return &wrapperspb.StringValue{} },
		func(_ context.Context, req proto.Message) (proto.Message, error) {
			return proto.Clone(req), nil
		},
	); err != nil {
		t.Fatal(err)
	}
	for _, in := range []string{"", "hello"} {
		var out wrapperspb.StringValue
		if err := c.Request(t.Context(), "echo", &wrapperspb.StringValue{Value: in}, &out); err != nil {
			t.Fatalf("request %q: %v", in, err)
		}
		if out.GetValue() != in {
			t.Fatalf("got %q, want %q", out.GetValue(), in)
		}
	}
}

func TestRequestTimeout(t *testing.T) {
	_, c := start(t)
	got := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	if _, err := c.Serve(t.Context(), "slow", "",
		func() proto.Message { return &wrapperspb.StringValue{} },
		func(context.Context, proto.Message) (proto.Message, error) {
			close(got)
			<-release
			return &wrapperspb.StringValue{}, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		errc <- c.Request(ctx, "slow", &wrapperspb.StringValue{}, &wrapperspb.StringValue{})
	}()
	select {
	case <-got:
	case err := <-errc:
		t.Fatalf("deadline before the handler ran: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	err := await(t, errc)
	if !errorIs(err, natsx.ErrTimeout) {
		t.Fatalf("timeout: %v", err)
	}
	if errorIs(err, natsx.ErrNoResponders) {
		t.Fatalf("timeout reported as no responders: %v", err)
	}
}

func TestNoResponders(t *testing.T) {
	_, c := start(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := c.Request(ctx, "nobody.home", &wrapperspb.StringValue{}, &wrapperspb.StringValue{})
	if !errorIs(err, natsx.ErrNoResponders) {
		t.Fatalf("no responders: %v", err)
	}
}

func TestHandlerErrorReply(t *testing.T) {
	_, c := start(t)
	long := strings.Repeat("界", 2000) // cuts mid-rune at the 1024-byte cap
	if _, err := c.Serve(t.Context(), "nope", "",
		func() proto.Message { return &wrapperspb.StringValue{} },
		func(context.Context, proto.Message) (proto.Message, error) {
			return nil, errors.New(long)
		},
	); err != nil {
		t.Fatal(err)
	}
	err := c.Request(t.Context(), "nope", &wrapperspb.StringValue{}, &wrapperspb.StringValue{})
	var re *natsx.ReplyError
	if !errors.As(err, &re) {
		t.Fatalf("got %v, want ReplyError", err)
	}
	if re.Code != natsx.CodeHandler {
		t.Fatalf("code %d", re.Code)
	}
	if len(re.Message) > 1024 || !utf8.ValidString(re.Message) || !strings.HasPrefix(re.Message, "界") {
		t.Fatalf("message len %d valid %v prefix %q", len(re.Message), utf8.ValidString(re.Message), re.Message[:min(8, len(re.Message))])
	}
}

func TestNilResponseIsAnErrorReply(t *testing.T) {
	_, c := start(t)
	if _, err := c.Serve(t.Context(), "nil", "",
		func() proto.Message { return &wrapperspb.StringValue{} },
		func(context.Context, proto.Message) (proto.Message, error) { return nil, nil },
	); err != nil {
		t.Fatal(err)
	}
	err := c.Request(t.Context(), "nil", &wrapperspb.StringValue{}, &wrapperspb.StringValue{})
	var re *natsx.ReplyError
	if !errors.As(err, &re) || re.Code != natsx.CodeBadResponse {
		t.Fatalf("got %v", err)
	}
}

func TestServeIgnoresPlainPublish(t *testing.T) {
	_, c := start(t)
	called := make(chan struct{}, 1)
	if _, err := c.Serve(t.Context(), "svc", "",
		func() proto.Message { return &wrapperspb.StringValue{} },
		func(context.Context, proto.Message) (proto.Message, error) {
			called <- struct{}{}
			return &wrapperspb.StringValue{}, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	saw := make(chan struct{})
	if _, err := c.Subscribe(t.Context(), "svc", "", func(natsx.Msg) { close(saw) }); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish("svc", []byte("not a request")); err != nil {
		t.Fatal(err)
	}
	await(t, saw)
	select {
	case <-called:
		t.Fatal("plain publish was handled as a request")
	default:
	}
}

func TestCodecErrors(t *testing.T) {
	s, client := start(t)
	raw, err := nats.Connect(s.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)

	// What Serve sends back for a payload that is not the request protobuf.
	if _, err := client.Serve(t.Context(), "dec", "",
		func() proto.Message { return &wrapperspb.StringValue{} },
		func(context.Context, proto.Message) (proto.Message, error) {
			t.Error("handler ran on an undecodable request")
			return &wrapperspb.StringValue{}, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	resp, err := raw.Request("dec", []byte{0xff, 0x00}, time.Second)
	if err != nil {
		t.Fatalf("raw request: %v", err)
	}
	code, msg, ok := parseErrorReply(resp.Data)
	if !ok || code != natsx.CodeBadRequest {
		t.Fatalf("bad request reply: ok %v code %d msg %q bytes %x", ok, code, msg, resp.Data)
	}

	// What Request does with replies that are not a success and not a valid error reply.
	replies := map[string][]byte{
		"truncated error": {0x00, 0x01, 0x00},
		"bad version":     {0x00, 0x09, 0x00, 0x02, 0x00, 0x00},
		"not protobuf":    {0x0a, 0xff},
		"bad utf-8":       {0x00, 0x01, 0x00, 0x09, 0x00, 0x01, 0xff},
	}
	for name, body := range replies {
		t.Run(name, func(t *testing.T) {
			subject := "raw." + strings.ReplaceAll(name, " ", ".")
			if _, err := raw.Subscribe(subject, func(m *nats.Msg) { _ = m.Respond(body) }); err != nil {
				t.Fatal(err)
			}
			if err := raw.Flush(); err != nil {
				t.Fatal(err)
			}
			err := client.Request(t.Context(), subject, &wrapperspb.StringValue{}, &wrapperspb.StringValue{})
			if !errorIs(err, natsx.ErrCodec) {
				t.Fatalf("got %v, want codec error", err)
			}
		})
	}

	// A peer that speaks the layout, parsed by Request rather than by Serve's own encoder.
	if _, err := raw.Subscribe("crafted", func(m *nats.Msg) {
		_ = m.Respond(errorReplyBytes(natsx.CodeHandler, "nope"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := raw.Flush(); err != nil {
		t.Fatal(err)
	}
	err = client.Request(t.Context(), "crafted", &wrapperspb.StringValue{}, &wrapperspb.StringValue{})
	var re *natsx.ReplyError
	if !errors.As(err, &re) || re.Code != natsx.CodeHandler || re.Message != "nope" {
		t.Fatalf("crafted reply: %v", err)
	}
}

// errorReplyBytes is the layout in rpc.go, written out here so a bug shared by the encoder and
// the decoder cannot hide.
func errorReplyBytes(code uint16, msg string) []byte {
	b := make([]byte, 6+len(msg))
	b[0] = 0x00
	b[1] = 1
	binary.BigEndian.PutUint16(b[2:4], code)
	binary.BigEndian.PutUint16(b[4:6], uint16(len(msg)))
	copy(b[6:], msg)
	return b
}

func parseErrorReply(b []byte) (code uint16, msg string, ok bool) {
	if len(b) < 6 || b[0] != 0x00 || b[1] != 1 {
		return 0, "", false
	}
	n := int(binary.BigEndian.Uint16(b[4:6]))
	if 6+n != len(b) || !utf8.Valid(b[6:]) {
		return 0, "", false
	}
	return binary.BigEndian.Uint16(b[2:4]), string(b[6:]), true
}
