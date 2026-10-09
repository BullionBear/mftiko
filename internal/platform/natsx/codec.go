package natsx

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ErrCodec: a payload is not the protobuf, or not the error-reply framing, that the caller asked
// for. A handler's error is a ReplyError, not this.
var ErrCodec = errors.New("natsx: codec")

// Marshal encodes m as protobuf. The bytes are what Publish would send for a protobuf message;
// pub/sub itself does not call this, because some payloads (md.f) are not bare protobuf.
func Marshal(m proto.Message) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil message", ErrCodec)
	}
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCodec, err)
	}
	return b, nil
}

// Unmarshal decodes a protobuf payload into m. An empty buffer is an empty message, which is
// what a protobuf of all default fields encodes to.
func Unmarshal(b []byte, m proto.Message) error {
	if m == nil {
		return fmt.Errorf("%w: nil message", ErrCodec)
	}
	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("%w: %w", ErrCodec, err)
	}
	return nil
}
