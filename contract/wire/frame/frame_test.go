package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// payload is n deterministic bytes, distinct per seed.
func payload(seed, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + seed*13)
	}
	return b
}

// bytesOfSize is a message whose encoding is exactly size bytes: 0, or 3 and more (an empty value is omitted).
func bytesOfSize(t testing.TB, size int) *wrapperspb.BytesValue {
	t.Helper()
	if size == 0 {
		return wrapperspb.Bytes(nil)
	}
	// field 1, wire type 2: one tag byte, a varint length, then the data.
	for lenLen := 1; lenLen <= 5; lenLen++ {
		d := size - 1 - lenLen
		if d >= 0 && len(binary.AppendUvarint(nil, uint64(d))) == lenLen {
			m := wrapperspb.Bytes(payload(size, d))
			if got := proto.Size(m); got != size {
				t.Fatalf("message for %d bytes is %d", size, got)
			}
			return m
		}
	}
	t.Fatalf("no BytesValue encodes to %d bytes", size)
	return nil
}

func TestRoundTrip(t *testing.T) {
	sizes := []int{0, 3, 100, keepBuf + 1, MaxLen - 1, MaxLen}
	var stream bytes.Buffer
	w := NewWriter(&stream)
	var sent []proto.Message
	for _, n := range sizes {
		m := bytesOfSize(t, n)
		if err := w.Write(m); err != nil {
			t.Fatalf("Write %d bytes: %v", n, err)
		}
		sent = append(sent, m)
	}
	if err := w.Write(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	sent = append(sent, &emptypb.Empty{})

	r := NewReader(&stream)
	for i, want := range sent {
		got := want.ProtoReflect().New().Interface()
		if err := r.Read(got); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !proto.Equal(got, want) {
			t.Fatalf("frame %d differs", i)
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("after the last frame: %v, want io.EOF", err)
	}
}

func TestWireFormat(t *testing.T) {
	var b bytes.Buffer
	if err := NewWriter(&b).Write(wrapperspb.Bytes([]byte("hi"))); err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 0, 0, 4, 0x0a, 2, 'h', 'i'} // big-endian length 4, then field 1 = "hi"
	if !bytes.Equal(b.Bytes(), want) {
		t.Fatalf("wire % x, want % x", b.Bytes(), want)
	}
	var e bytes.Buffer
	if err := NewWriter(&e).Write(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.Bytes(), []byte{0, 0, 0, 0}) {
		t.Fatalf("empty message on the wire: % x", e.Bytes())
	}
}

func TestWriteTooLarge(t *testing.T) {
	var b bytes.Buffer
	err := NewWriter(&b).Write(bytesOfSize(t, MaxLen+1))
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Write of MaxLen+1: %v", err)
	}
	if b.Len() != 0 {
		t.Fatalf("%d bytes written for a refused frame", b.Len())
	}
}

// countingReader counts what has been read from it.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func TestReadTooLarge(t *testing.T) {
	for _, size := range []uint32{MaxLen + 1, 1<<32 - 1} {
		hdr := binary.BigEndian.AppendUint32(nil, size)
		src := &countingReader{r: io.MultiReader(bytes.NewReader(hdr), strings.NewReader(strings.Repeat("x", 4096)))}
		_, err := NewReader(src).Next()
		if !errors.Is(err, ErrTooLarge) {
			t.Fatalf("header %d: %v", size, err)
		}
		if src.n != HeaderLen {
			t.Fatalf("header %d: read %d bytes, want only the header", size, src.n)
		}
	}
}

// Cut a two-frame stream at every offset: a cut between frames is io.EOF, one inside a frame is ErrTruncated and
// says where.
func TestTruncatedAtEveryOffset(t *testing.T) {
	var b bytes.Buffer
	w := NewWriter(&b)
	for _, m := range []proto.Message{wrapperspb.Bytes([]byte("first")), wrapperspb.Bytes([]byte("second frame"))} {
		if err := w.Write(m); err != nil {
			t.Fatal(err)
		}
	}
	stream := b.Bytes()
	first := HeaderLen + int(binary.BigEndian.Uint32(stream))
	for cut := 0; cut <= len(stream); cut++ {
		r := NewReader(bytes.NewReader(stream[:cut]))
		frames := 0
		var err error
		for {
			if _, err = r.Next(); err != nil {
				break
			}
			frames++
		}
		start := 0 // where the frame being read when the stream ended starts
		if frames == 1 {
			start = first
		}
		switch {
		case cut == 0 || cut == first || cut == len(stream):
			if err != io.EOF {
				t.Fatalf("cut %d (a boundary): %v after %d frames, want io.EOF", cut, err, frames)
			}
		case cut-start < HeaderLen:
			if !errors.Is(err, ErrTruncated) || !errors.Is(err, io.ErrUnexpectedEOF) || !strings.Contains(err.Error(), "header") {
				t.Fatalf("cut %d (in a header): %v", cut, err)
			}
		default:
			if !errors.Is(err, ErrTruncated) || !errors.Is(err, io.ErrUnexpectedEOF) || !strings.Contains(err.Error(), "body") {
				t.Fatalf("cut %d (in a body): %v", cut, err)
			}
		}
	}
}

// A body that is not the expected message is a decode error, but the frame was read whole: the next one reads.
func TestDecodeErrorKeepsStep(t *testing.T) {
	var b bytes.Buffer
	b.Write([]byte{0, 0, 0, 2, 0xff, 0xff}) // not a valid protobuf
	if err := NewWriter(&b).Write(wrapperspb.Bytes([]byte("next"))); err != nil {
		t.Fatal(err)
	}
	r := NewReader(&b)
	var m wrapperspb.BytesValue
	err := r.Read(&m)
	if err == nil || errors.Is(err, ErrTruncated) || errors.Is(err, ErrTooLarge) {
		t.Fatalf("Read of an invalid body: %v", err)
	}
	if err := r.Read(&m); err != nil || string(m.Value) != "next" {
		t.Fatalf("Read after a decode error: %q, %v", m.Value, err)
	}
}

// Read's message does not share memory with the Reader's buffer, which the next frame overwrites.
func TestReadDoesNotAlias(t *testing.T) {
	var b bytes.Buffer
	w := NewWriter(&b)
	for _, s := range []string{"aaaaaaaa", "bbbbbbbb"} {
		if err := w.Write(wrapperspb.Bytes([]byte(s))); err != nil {
			t.Fatal(err)
		}
	}
	r := NewReader(&b)
	var first, second wrapperspb.BytesValue
	if err := r.Read(&first); err != nil {
		t.Fatal(err)
	}
	if err := r.Read(&second); err != nil {
		t.Fatal(err)
	}
	if string(first.Value) != "aaaaaaaa" {
		t.Fatalf("first message changed to %q after the next read", first.Value)
	}
}

var errBroken = errors.New("broken")

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errBroken }

type failingReader struct{ after []byte }

func (f *failingReader) Read(p []byte) (int, error) {
	if len(f.after) == 0 {
		return 0, errBroken
	}
	n := copy(p, f.after)
	f.after = f.after[n:]
	return n, nil
}

func TestUnderlyingErrors(t *testing.T) {
	if err := NewWriter(failingWriter{}).Write(wrapperspb.Bytes([]byte("x"))); !errors.Is(err, errBroken) {
		t.Fatalf("Write to a failing writer: %v", err)
	}
	for _, after := range [][]byte{nil, {0, 0}, {0, 0, 0, 9, 1}} {
		_, err := NewReader(&failingReader{after: after}).Next()
		if !errors.Is(err, errBroken) || errors.Is(err, ErrTruncated) || err == io.EOF {
			t.Fatalf("after % x: %v", after, err)
		}
	}
}

// A writer's buffer is reused for small frames and dropped after a big one.
func TestBuffersKeptSmall(t *testing.T) {
	w := NewWriter(io.Discard)
	if err := w.Write(bytesOfSize(t, keepBuf+100)); err != nil {
		t.Fatal(err)
	}
	if w.buf != nil {
		t.Fatalf("writer kept a %d-byte buffer", cap(w.buf))
	}
	if err := w.Write(bytesOfSize(t, 100)); err != nil {
		t.Fatal(err)
	}
	if cap(w.buf) == 0 || cap(w.buf) > keepBuf {
		t.Fatalf("writer buffer cap %d after a small frame", cap(w.buf))
	}

	var b bytes.Buffer
	ww := NewWriter(&b)
	for _, n := range []int{keepBuf + 100, 100} {
		if err := ww.Write(bytesOfSize(t, n)); err != nil {
			t.Fatal(err)
		}
	}
	r := NewReader(&b)
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if r.buf != nil {
		t.Fatalf("reader kept a %d-byte buffer", cap(r.buf))
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if cap(r.buf) < minBuf || cap(r.buf) > keepBuf {
		t.Fatalf("reader buffer cap %d after a small frame", cap(r.buf))
	}
}

func ExampleWriter() {
	var conn bytes.Buffer // a unix socket or the worker's fd 3 in real use
	if err := NewWriter(&conn).Write(wrapperspb.String("hello")); err != nil {
		panic(err)
	}
	var got wrapperspb.StringValue
	if err := NewReader(&conn).Read(&got); err != nil {
		panic(err)
	}
	fmt.Println(got.GetValue())
	// Output: hello
}
