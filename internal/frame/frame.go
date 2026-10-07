package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"google.golang.org/protobuf/proto"
)

const (
	// HeaderLen is the length prefix: 4 bytes, big-endian.
	HeaderLen = 4
	// MaxLen is the largest message body accepted on C3 and C4: 1 MiB, not counting the header.
	MaxLen = 1 << 20

	// keepBuf is the largest buffer a Reader or Writer keeps between frames; a larger one, needed for an
	// occasional big frame, is dropped after use so an idle connection does not hold a megabyte.
	keepBuf = 64 << 10
	// minBuf is the smallest buffer a Reader allocates, so frames that grow a little at a time do not each
	// allocate.
	minBuf = 4 << 10
)

var (
	// ErrTooLarge: a frame's body is longer than MaxLen. On Write nothing was written. On read the header declared
	// it and nothing of the body was read; the stream is unusable.
	ErrTooLarge = errors.New("frame: body longer than 1 MiB")
	// ErrTruncated: the stream ended inside a frame, in its header or its body. It also matches io.ErrUnexpectedEOF.
	ErrTruncated = errors.New("frame: stream ended inside a frame")
)

// Writer writes one frame per message. Safe for concurrent use: frames from different goroutines never interleave,
// because each is written whole, header and body together, in one Write call under a lock.
type Writer struct {
	mu  sync.Mutex
	w   io.Writer
	buf []byte
}

// NewWriter writes frames to w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// Write marshals m and writes it as one frame. A body longer than MaxLen is ErrTooLarge and nothing is written. An
// error from the underlying writer is returned as is, wrapped; after one, the stream may hold part of a frame and is
// unusable.
func (w *Writer) Write(m proto.Message) error {
	n := proto.Size(m)
	if n > MaxLen {
		return fmt.Errorf("%w: %d bytes", ErrTooLarge, n)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	buf := w.buf[:0]
	if cap(buf) < HeaderLen+n {
		buf = make([]byte, 0, HeaderLen+n)
	}
	buf = append(buf, 0, 0, 0, 0)
	buf, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(buf, m)
	if err != nil {
		return fmt.Errorf("frame: encode %T: %w", m, err)
	}
	body := len(buf) - HeaderLen
	if body != n { // m changed between Size and Marshal: the caller's race, but never send a wrong length
		return fmt.Errorf("frame: %T changed while it was being written (%d bytes, then %d)", m, n, body)
	}
	binary.BigEndian.PutUint32(buf, uint32(body))
	_, err = w.w.Write(buf)
	w.keep(buf)
	if err != nil {
		return fmt.Errorf("frame: write: %w", err)
	}
	return nil
}

func (w *Writer) keep(buf []byte) {
	if cap(buf) <= keepBuf {
		w.buf = buf[:0]
	} else {
		w.buf = nil
	}
}

// Reader reads frames one at a time. Not safe for concurrent use: a connection has one reader. It makes two Read
// calls per frame, one for the header and one for the body; wrap r in a bufio.Reader to make fewer.
type Reader struct {
	r   io.Reader
	hdr [HeaderLen]byte
	buf []byte
}

// NewReader reads frames from r.
func NewReader(r io.Reader) *Reader { return &Reader{r: r} }

// Next reads the next frame and returns its body, which stays valid until the next call. A body can be empty: an
// empty message encodes to zero bytes.
//
//   - io.EOF: the stream ended between frames, which is how a peer that closed its end looks.
//   - ErrTruncated: the stream ended inside a frame; the error says whether in the header or the body.
//   - ErrTooLarge: the header declared more than MaxLen; nothing of the body was read.
//   - Anything else is the underlying reader's error, wrapped.
//
// After any error the stream is unusable.
func (r *Reader) Next() ([]byte, error) {
	n, err := io.ReadFull(r.r, r.hdr[:])
	switch {
	case err == io.EOF:
		return nil, io.EOF
	case err == io.ErrUnexpectedEOF:
		return nil, fmt.Errorf("%w: header, %d of %d bytes: %w", ErrTruncated, n, HeaderLen, io.ErrUnexpectedEOF)
	case err != nil:
		return nil, fmt.Errorf("frame: read header: %w", err)
	}
	size := binary.BigEndian.Uint32(r.hdr[:])
	if size > MaxLen {
		return nil, fmt.Errorf("%w: header says %d bytes", ErrTooLarge, size)
	}
	if cap(r.buf) < int(size) {
		r.buf = make([]byte, size, max(int(size), minBuf))
	}
	body := r.buf[:size]
	if n, err := io.ReadFull(r.r, body); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("%w: body, %d of %d bytes: %w", ErrTruncated, n, size, io.ErrUnexpectedEOF)
		}
		return nil, fmt.Errorf("frame: read body: %w", err)
	}
	if cap(r.buf) > keepBuf {
		r.buf = nil // body still points at the big buffer until the next call; then it can be collected
	}
	return body, nil
}

// Read reads the next frame into m, replacing its contents. The errors are Next's, plus a decode error when the
// body is not a valid m; that frame was read whole, so the stream is still in step and the next Read can go on.
// m does not share memory with the Reader's buffer.
func (r *Reader) Read(m proto.Message) error {
	body, err := r.Next()
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(body, m); err != nil {
		return fmt.Errorf("frame: decode %d bytes as %T: %w", len(body), m, err)
	}
	return nil
}
