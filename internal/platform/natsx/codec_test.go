package natsx_test

import (
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/natsx"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCodecRoundTrip(t *testing.T) {
	in := &wrapperspb.StringValue{Value: "tick"}
	b, err := natsx.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out wrapperspb.StringValue
	if err := natsx.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("got %v", &out)
	}
}

func TestCodecEmptyIsEmptyMessage(t *testing.T) {
	b, err := natsx.Marshal(&wrapperspb.StringValue{})
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 0 {
		t.Fatalf("empty message encoded to %x", b)
	}
	var out wrapperspb.StringValue
	if err := natsx.Unmarshal(nil, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetValue() != "" {
		t.Fatalf("value %q", out.GetValue())
	}
}

func TestCodecRejectsNilAndGarbage(t *testing.T) {
	if _, err := natsx.Marshal(nil); !errorIs(err, natsx.ErrCodec) {
		t.Fatalf("nil marshal: %v", err)
	}
	if err := natsx.Unmarshal(nil, nil); !errorIs(err, natsx.ErrCodec) {
		t.Fatalf("nil unmarshal: %v", err)
	}
	var out wrapperspb.StringValue
	// 0x00 cannot start a protobuf. It must not decode as an empty success.
	if err := natsx.Unmarshal([]byte{0x00}, &out); !errorIs(err, natsx.ErrCodec) {
		t.Fatalf("mark byte: %v", err)
	}
	if err := natsx.Unmarshal([]byte{0x0a, 0xff}, &out); !errorIs(err, natsx.ErrCodec) {
		t.Fatalf("truncated: %v", err)
	}
}
