//go:build unix

package logread

import "unicode/utf8"

// maxChunk is the most one record carries. Frame.data is at most 262144
// bytes (PROTOCOL §11, E14-D007); cutting here keeps a record inside that bound.
const maxChunk = 256 << 10

// cut splits b into the next record and the bytes that stay buffered.
//
// A newline inside the first max bytes ends the record after that newline, so
// a line is not split while it still fits. With no newline, force false and a
// short buffer wait for more bytes. Otherwise the cut is on a UTF-8 boundary
// (E14-03). Bytes that are not valid UTF-8 count as one unit so a binary log
// still advances.
func cut(b []byte, max int, force bool) (chunk, rest []byte) {
	if len(b) == 0 || max <= 0 {
		return nil, b
	}
	limit := len(b)
	if limit > max {
		limit = max
	}
	if i := lastNewline(b[:limit]); i >= 0 {
		return b[:i+1], b[i+1:]
	}
	if !force && len(b) < max {
		return nil, b
	}
	n := trimRune(b, limit)
	if n <= 0 {
		return nil, b
	}
	return b[:n], b[n:]
}

func lastNewline(b []byte) int {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == '\n' {
			return i
		}
	}
	return -1
}

// trimRune is the largest index <= limit at which b ends on a code-point
// boundary. A sequence that continues past limit stays in the remainder. A
// sequence that runs off the end of b and still does not fit is emitted one
// byte at a time so a truncated file cannot stall the reader.
func trimRune(b []byte, limit int) int {
	if limit > len(b) {
		limit = len(b)
	}
	i := 0
	for i < limit {
		if b[i] < utf8.RuneSelf {
			i++
			continue
		}
		size := seqLen(b[i:])
		if i+size > limit {
			break
		}
		i += size
	}
	if i == 0 && limit > 0 {
		return 1
	}
	return i
}

// seqLen is the byte length of the UTF-8 sequence at the start of b, or 1 when
// those bytes are not a well-formed sequence. An incomplete but plausible
// prefix is reported as the full width it still needs, so trimRune can hold it
// back until the rest is buffered.
func seqLen(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	need := expectedLen(b[0])
	if need <= 1 {
		return 1
	}
	if len(b) < need {
		return need
	}
	r, size := utf8.DecodeRune(b)
	if r == utf8.RuneError && size == 1 {
		return 1
	}
	return size
}

func expectedLen(first byte) int {
	switch {
	case first < 0x80:
		return 1
	case first&0xE0 == 0xC0:
		return 2
	case first&0xF0 == 0xE0:
		return 3
	case first&0xF8 == 0xF0:
		return 4
	default:
		return 1
	}
}
