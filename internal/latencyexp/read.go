package latencyexp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/BullionBear/mftiko/sdk/eventlog"
)

const (
	scanOK = iota
	scanShort
	scanBad
)

// minHeader is the fixed prefix plus three empty string lengths (PROTOCOL §12.2).
const minHeader = 34

var (
	errNeedMore = errors.New("event log header is incomplete")
	errShrunk   = errors.New("event log file shrank")
	errTorn     = errors.New("event log ended inside a frame")
)

// rawFrame is one complete frame's kind and rec_seq, including a kind this
// process's reader skips. The gap counter uses every seq so an unknown kind
// is not reported as a lost frame.
type rawFrame struct {
	kind eventlog.Kind
	seq  uint64
}

// cursor is how far one file has been consumed. header is the raw file header,
// kept so a later chunk can be handed to the reader, which always starts at a
// header.
type cursor struct {
	header []byte
	off    int64
	done   bool
}

// peekHeader reads the header length. errNeedMore means the file does not yet
// hold the fixed prefix or the bytes the length asks for; the caller waits.
func peekHeader(b []byte) (int, error) {
	if len(b) < 12 {
		return 0, errNeedMore
	}
	if string(b[:8]) != eventlog.Magic {
		return 0, eventlog.ErrMagic
	}
	if binary.LittleEndian.Uint16(b[8:10]) != eventlog.FormatVersion {
		return 0, eventlog.ErrVersion
	}
	n := int(binary.LittleEndian.Uint16(b[10:12]))
	if n < minHeader || n > 1<<20 {
		return 0, eventlog.ErrMagic
	}
	if len(b) < n {
		return 0, errNeedMore
	}
	return n, nil
}

// scanFrames walks complete frames in b. consumed is the number of bytes of
// whole frames. A short tail is an unfinished write (E17-D005). A length that
// cannot be a frame is damage; PROTOCOL §12.3 stops the file there.
func scanFrames(b []byte) (frames []rawFrame, consumed int, status int) {
	pos := 0
	for pos+4 <= len(b) {
		n := binary.LittleEndian.Uint32(b[pos:])
		if n < eventlog.FrameHead-4 || n > eventlog.MaxFrame {
			return frames, pos, scanBad
		}
		total := 4 + int(n)
		if pos+total > len(b) {
			return frames, pos, scanShort
		}
		frames = append(frames, rawFrame{
			kind: eventlog.Kind(binary.LittleEndian.Uint16(b[pos+4:])),
			seq:  binary.LittleEndian.Uint64(b[pos+8:]),
		})
		pos += total
	}
	if pos < len(b) {
		return frames, pos, scanShort
	}
	return frames, pos, scanOK
}

// readRecords decodes header and the complete frames after it with the
// sdk/eventlog reader. The reader skips a kind it does not know; frames still
// lists those seqs.
func readRecords(header, frames []byte) (eventlog.Header, []eventlog.Record, error) {
	b := make([]byte, 0, len(header)+len(frames))
	b = append(b, header...)
	b = append(b, frames...)
	rd, err := eventlog.NewReader(bytes.NewReader(b))
	if err != nil {
		return eventlog.Header{}, nil, err
	}
	var recs []eventlog.Record
	for {
		rec, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return rd.Header(), recs, nil
		}
		if err != nil {
			return eventlog.Header{}, nil, err
		}
		recs = append(recs, rec)
	}
}
