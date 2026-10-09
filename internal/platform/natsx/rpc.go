package natsx

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// An RPC success reply is the response protobuf and nothing else, the same rule as the rest of
// the data plane: the subject says what the payload is (PROTOCOL §7), and the data plane does
// not use NATS headers. An error cannot be told from that protobuf by unmarshalling it, because
// a wrong protobuf often decodes as a zero message. Error replies are therefore not protobuf.
//
// A marshaled protobuf never starts with 0x00: field numbers start at 1, so the first tag byte
// is at least 0x08, and an empty message is zero bytes rather than a 0x00. An error reply
// starts with that impossible byte:
//
//	offset 0  uint8   0x00, the mark
//	offset 1  uint8   version, 1
//	offset 2  uint16  code, big-endian
//	offset 4  uint16  message length, big-endian
//	offset 6  bytes   the message, UTF-8, exactly that many bytes
//
// The whole reply is 6 plus the message length. A peer that does not understand the version
// treats the reply as a codec error and does not try the bytes as the response protobuf.
//
// Two failures are not replies. The server itself answers a request that nobody subscribed to
// with an empty status 503; Request returns ErrNoResponders without waiting out the deadline.
// A handler that never replies, or replies too late, is ErrTimeout.
const (
	errReplyMark    byte = 0x00
	errReplyVersion byte = 1
	errReplyHeader       = 6

	// maxReplyMsg is the longest text an error reply carries. Longer handler errors are cut, on a
	// rune boundary.
	maxReplyMsg = 1024
)

// Codes carried in an error reply. Unknown codes still decode: a newer peer may add one.
const (
	// CodeBadRequest: the request payload is not the protobuf Serve was given.
	CodeBadRequest uint16 = 1
	// CodeHandler: the handler returned an error. The message is that error's text.
	CodeHandler uint16 = 2
	// CodeBadResponse: the handler returned no message, or its message did not encode.
	CodeBadResponse uint16 = 3
)

var (
	// ErrTimeout: the deadline passed with no reply. Distinct from ErrNoResponders, which means
	// the server has no subscription on the subject and does not wait.
	ErrTimeout = errors.New("natsx: timeout")
	// ErrNoResponders: no subscription matched the request subject.
	ErrNoResponders = errors.New("natsx: no responders")
)

// ReplyError is an error reply from the peer. It is not a codec failure: the framing decoded.
type ReplyError struct {
	Code    uint16
	Message string
}

func (e *ReplyError) Error() string {
	if e == nil {
		return "natsx: reply error"
	}
	return fmt.Sprintf("natsx: reply %d: %s", e.Code, e.Message)
}

// Request sends req as protobuf and unmarshals one reply into resp. The wait is the context's
// deadline, or DefaultRequestTimeout (or Options.RequestTimeout) when it has none.
//
// A ReplyError means the peer handled the framing and refused the call. ErrCodec means the
// reply was neither that framing nor resp's protobuf. ErrNoResponders and ErrTimeout mean no
// reply arrived.
func (c *Conn) Request(ctx context.Context, subject string, req, resp proto.Message) error {
	if err := c.usable(); err != nil {
		return err
	}
	if req == nil || resp == nil {
		return fmt.Errorf("%w: nil message", ErrCodec)
	}
	if err := ctx.Err(); err != nil {
		return classifyRequest(subject, err)
	}
	payload, err := Marshal(req)
	if err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.reqTimeout)
		defer cancel()
	}
	msg, err := c.nc.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return classifyRequest(subject, err)
	}
	return decodeReply(msg.Data, resp)
}

// Serve answers subject. newReq is called for every request and must return an empty message;
// the handler may keep that message. h runs on the connection's read goroutine, one call at a
// time, and must return promptly. Its context is cancelled when ctx is.
//
// queue is the queue group when more than one process serves the subject. An empty queue
// delivers every request to every server, and each of them replies.
//
// A publish with no reply subject is ignored: Serve is request/reply, not a second subscription.
// Serve returns after the server has registered the interest.
func (c *Conn) Serve(
	ctx context.Context,
	subject, queue string,
	newReq func() proto.Message,
	h func(context.Context, proto.Message) (proto.Message, error),
) (*Subscription, error) {
	if newReq == nil || h == nil {
		return nil, errors.New("natsx: nil handler")
	}
	return c.Subscribe(ctx, subject, queue, func(m Msg) {
		if m.Reply == "" {
			return
		}
		req := newReq()
		if req == nil {
			c.replyError(m.Reply, CodeBadRequest, "nil request")
			return
		}
		if err := Unmarshal(m.Data, req); err != nil {
			c.replyError(m.Reply, CodeBadRequest, err.Error())
			return
		}
		resp, err := h(ctx, req)
		if err != nil {
			c.replyError(m.Reply, CodeHandler, err.Error())
			return
		}
		if resp == nil {
			c.replyError(m.Reply, CodeBadResponse, "nil response")
			return
		}
		b, err := Marshal(resp)
		if err != nil {
			c.replyError(m.Reply, CodeBadResponse, err.Error())
			return
		}
		_ = c.Publish(m.Reply, b)
	})
}

func (c *Conn) replyError(subject string, code uint16, msg string) {
	_ = c.Publish(subject, encodeReplyError(code, msg))
}

func encodeReplyError(code uint16, msg string) []byte {
	msg = clip(msg, maxReplyMsg)
	b := make([]byte, errReplyHeader+len(msg))
	b[0] = errReplyMark
	b[1] = errReplyVersion
	binary.BigEndian.PutUint16(b[2:4], code)
	binary.BigEndian.PutUint16(b[4:6], uint16(len(msg)))
	copy(b[errReplyHeader:], msg)
	return b
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[:n]
	for s != "" && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

func decodeReply(data []byte, resp proto.Message) error {
	if len(data) > 0 && data[0] == errReplyMark {
		re, err := decodeReplyError(data)
		if err != nil {
			return err
		}
		return re
	}
	return Unmarshal(data, resp)
}

func decodeReplyError(b []byte) (*ReplyError, error) {
	if len(b) < errReplyHeader || b[0] != errReplyMark {
		return nil, fmt.Errorf("%w: short error reply", ErrCodec)
	}
	if b[1] != errReplyVersion {
		return nil, fmt.Errorf("%w: error reply version %d", ErrCodec, b[1])
	}
	n := int(binary.BigEndian.Uint16(b[4:6]))
	if errReplyHeader+n != len(b) {
		return nil, fmt.Errorf("%w: error reply length", ErrCodec)
	}
	msg := string(b[errReplyHeader:])
	if !utf8.ValidString(msg) {
		return nil, fmt.Errorf("%w: error reply is not utf-8", ErrCodec)
	}
	return &ReplyError{Code: binary.BigEndian.Uint16(b[2:4]), Message: msg}, nil
}

func classifyRequest(subject string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, nats.ErrNoResponders):
		return fmt.Errorf("natsx: request %s: %w", subject, ErrNoResponders)
	case errors.Is(err, nats.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("natsx: request %s: %w", subject, ErrTimeout)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("natsx: request %s: %w", subject, context.Canceled)
	default:
		return classify("request", subject, err)
	}
}
