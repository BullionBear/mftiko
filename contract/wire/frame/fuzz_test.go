package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Any byte stream: the Reader never panics, never returns a body over MaxLen, accounts for every byte, and ends
// with exactly one of io.EOF (only at a boundary), ErrTruncated or ErrTooLarge.
func FuzzNext(f *testing.F) {
	var two bytes.Buffer
	w := NewWriter(&two)
	_ = w.Write(wrapperspb.Bytes([]byte("one")))
	_ = w.Write(wrapperspb.Bytes(nil))
	for _, seed := range [][]byte{
		nil,
		two.Bytes(),
		two.Bytes()[:5],
		{0, 0},
		{0, 0, 0, 0},
		binary.BigEndian.AppendUint32(nil, MaxLen),
		binary.BigEndian.AppendUint32(nil, MaxLen+1),
		{0xff, 0xff, 0xff, 0xff, 1, 2, 3},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		src := bytes.NewReader(data)
		r := NewReader(src)
		consumed := 0
		for {
			body, err := r.Next()
			if err == nil {
				if len(body) > MaxLen {
					t.Fatalf("body of %d bytes", len(body))
				}
				consumed += HeaderLen + len(body)
				continue
			}
			rest := data[consumed:]
			switch {
			case err == io.EOF:
				if len(rest) != 0 {
					t.Fatalf("io.EOF with %d bytes left", len(rest))
				}
			case errors.Is(err, ErrTooLarge):
				if len(rest) < HeaderLen || binary.BigEndian.Uint32(rest) <= MaxLen {
					t.Fatalf("ErrTooLarge for % x", rest[:min(len(rest), HeaderLen)])
				}
			case errors.Is(err, ErrTruncated):
				if len(rest) >= HeaderLen && int(binary.BigEndian.Uint32(rest)) <= len(rest)-HeaderLen {
					t.Fatalf("ErrTruncated with a whole frame left: % x", rest)
				}
				if len(rest) == 0 {
					t.Fatal("ErrTruncated at a boundary")
				}
			default:
				t.Fatalf("unexpected error %v", err)
			}
			return
		}
	})
}

// Any payload survives a Write and a Read unchanged.
func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("hello"))
	f.Add(bytes.Repeat([]byte{0xab}, 70000))
	f.Fuzz(func(t *testing.T, p []byte) {
		var b bytes.Buffer
		m := wrapperspb.Bytes(p)
		if err := NewWriter(&b).Write(m); err != nil {
			if errors.Is(err, ErrTooLarge) && proto.Size(m) > MaxLen {
				return
			}
			t.Fatal(err)
		}
		var got wrapperspb.BytesValue
		if err := NewReader(&b).Read(&got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Value, p) {
			t.Fatalf("payload of %d bytes came back as %d", len(p), len(got.Value))
		}
	})
}
