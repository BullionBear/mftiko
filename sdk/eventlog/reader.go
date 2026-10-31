package eventlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

// Reader reads one file. Next skips a kind this reader does not know (E20-D008).
type Reader struct {
	r *bufio.Reader
	c io.Closer
	h Header
}

// Open opens one file and reads its header.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	r.c = f
	return r, nil
}

// NewReader reads the header from r. The caller closes r.
func NewReader(r io.Reader) (*Reader, error) {
	br := bufio.NewReaderSize(r, 1<<16)
	h, err := readHeader(br)
	if err != nil {
		return nil, err
	}
	return &Reader{r: br, h: h}, nil
}

// Header is the file's header.
func (r *Reader) Header() Header { return r.h }

// Next returns the next frame of a known kind. The body is the caller's. io.EOF is the end of
// the file at a frame boundary. ErrTruncated is a file that ends inside a frame, which is what a
// process killed in the middle of a write leaves.
func (r *Reader) Next() (Record, error) {
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r.r, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return Record{}, io.EOF
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return Record{}, ErrTruncated
			}
			return Record{}, err
		}
		n := binary.LittleEndian.Uint32(lenBuf[:])
		if n < FrameHead-4 || n > MaxFrame {
			return Record{}, ErrFrame
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r.r, buf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return Record{}, ErrTruncated
			}
			return Record{}, err
		}
		rec := Record{
			Kind:   Kind(binary.LittleEndian.Uint16(buf[0:])),
			Flags:  binary.LittleEndian.Uint16(buf[2:]),
			Seq:    binary.LittleEndian.Uint64(buf[4:]),
			RecvNs: int64(binary.LittleEndian.Uint64(buf[12:])),
			TNs:    int64(binary.LittleEndian.Uint64(buf[20:])),
			Ref:    binary.LittleEndian.Uint64(buf[28:]),
			Body:   buf[FrameHead-4:],
		}
		if !rec.Kind.Known() {
			continue
		}
		return rec, nil
	}
}

// Close closes the file Open opened.
func (r *Reader) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Close()
}

func readHeader(r io.Reader) (Header, error) {
	var fixed [headerFixed]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Header{}, ErrMagic
		}
		return Header{}, err
	}
	if string(fixed[:8]) != Magic {
		return Header{}, ErrMagic
	}
	h := Header{
		FormatVersion: binary.LittleEndian.Uint16(fixed[8:]),
		Incarnation:   binary.LittleEndian.Uint32(fixed[12:]),
		N:             binary.LittleEndian.Uint32(fixed[16:]),
		CreatedNs:     int64(binary.LittleEndian.Uint64(fixed[20:])),
	}
	if h.FormatVersion != FormatVersion {
		return Header{}, ErrVersion
	}
	total := int(binary.LittleEndian.Uint16(fixed[10:]))
	if total < headerFixed+6 {
		return Header{}, ErrMagic
	}
	rest := make([]byte, total-headerFixed)
	if _, err := io.ReadFull(r, rest); err != nil {
		return Header{}, ErrTruncated
	}
	var ok bool
	if h.SessionID, rest, ok = cutStr(rest); !ok {
		return Header{}, ErrMagic
	}
	if h.SDKVersion, rest, ok = cutStr(rest); !ok {
		return Header{}, ErrMagic
	}
	if h.SDKCommit, _, ok = cutStr(rest); !ok {
		return Header{}, ErrMagic
	}
	return h, nil
}
