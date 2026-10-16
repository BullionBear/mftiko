package symclient

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// requestTimeout is how long a request waits when its context has no deadline.
// It matches the platform NATS client's default. A caller that has a deadline,
// including OnInit's deadline_ms, keeps that deadline instead.
const requestTimeout = 5 * time.Second

// errReplyMark is the first byte of a NATS error reply (natsx option A). A protobuf
// never starts with 0x00, so the two cannot be confused.
const errReplyMark byte = 0x00

// NATS adapts a connected nats.Conn. It does not install a reconnect handler, so it
// does not replace one the session worker already set. The worker calls
// Client.Reconnect from that handler.
func NATS(nc *nats.Conn) Bus {
	if nc == nil {
		return nil
	}
	return natsBus{nc: nc}
}

type natsBus struct {
	nc *nats.Conn
}

func (b natsBus) Request(ctx context.Context, subject string, req, resp proto.Message) error {
	if req == nil || resp == nil {
		return errors.New("symclient: nil message")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("symclient: request %s: %w", subject, err)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, requestTimeout)
		defer cancel()
	}
	msg, err := b.nc.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return fmt.Errorf("symclient: request %s: %w", subject, err)
	}
	if err := decodeReply(msg.Data, resp); err != nil {
		return err
	}
	return nil
}

func (b natsBus) Subscribe(ctx context.Context, subject string, fn func(subject string, data []byte)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("symclient: nil handler")
	}
	sub, err := b.nc.Subscribe(subject, func(m *nats.Msg) {
		// The client keeps the note, not these bytes. Copy anyway: the callback
		// must not assume the library's buffer outlives the call.
		fn(m.Subject, append([]byte(nil), m.Data...))
	})
	if err != nil {
		return fmt.Errorf("symclient: subscribe %s: %w", subject, err)
	}
	if err := b.nc.FlushTimeout(requestTimeout); err != nil {
		_ = sub.Unsubscribe()
		return fmt.Errorf("symclient: subscribe %s: %w", subject, err)
	}
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()
	return nil
}

func decodeReply(data []byte, resp proto.Message) error {
	if len(data) > 0 && data[0] == errReplyMark {
		re, err := decodeReplyError(data)
		if err != nil {
			return err
		}
		if re.code == 2 && (re.msg == "not_found" || strings.HasPrefix(re.msg, "not_found:")) {
			return fmt.Errorf("%w: %s", symbol.ErrNotFound, re.msg)
		}
		return fmt.Errorf("symclient: %s", re.msg)
	}
	if err := proto.Unmarshal(data, resp); err != nil {
		return fmt.Errorf("symclient: reply: %w", err)
	}
	return nil
}

type replyError struct {
	code uint16
	msg  string
}

func decodeReplyError(b []byte) (replyError, error) {
	if len(b) < 6 || b[0] != errReplyMark {
		return replyError{}, errors.New("symclient: short error reply")
	}
	if b[1] != 1 {
		return replyError{}, fmt.Errorf("symclient: error reply version %d", b[1])
	}
	n := int(binary.BigEndian.Uint16(b[4:6]))
	if 6+n != len(b) {
		return replyError{}, errors.New("symclient: error reply length")
	}
	msg := string(b[6:])
	if !utf8.ValidString(msg) {
		return replyError{}, errors.New("symclient: error reply is not utf-8")
	}
	return replyError{code: binary.BigEndian.Uint16(b[2:4]), msg: msg}, nil
}
