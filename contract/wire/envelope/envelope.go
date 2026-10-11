package envelope

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Layout of version 1. The eight-byte fields are aligned to 8. Bytes 1 through 7 are reserved: writers
// store zero and readers reject anything else. The length is a property of the version, so v1 has no
// length byte (D406, D411).
const (
	V1   = 1  // value of the version byte
	Size = 40 // v1 envelope length; the payload starts at this offset

	OffVersion   = 0  // uint8
	OffMDTsNs    = 8  // int64 LE: conn worker publish time, MD host clock
	OffConnEpoch = 16 // uint64 LE
	OffSeq       = 24 // uint64 LE: per (feed, conn_epoch); restarts at 1 after a reconnect
	OffDigest    = 32 // uint64 LE: xxHash64 of the payload, seed 0, not covering this envelope
)

var (
	// ErrShort: fewer than Size bytes, so the buffer is not a v1 message.
	ErrShort = errors.New("envelope: message shorter than 40 bytes")
	// ErrVersion: the version byte is not V1. Another version is a different layout and is refused.
	ErrVersion = errors.New("envelope: unknown version")
	// ErrReserved: one of the bytes between the version and md_ts_ns is not zero.
	ErrReserved = errors.New("envelope: reserved byte is not zero")
)

// Append appends a v1 message to dst: the 40-byte envelope, then a copy of payload. The digest is
// xxHash64 of payload with seed 0. payload must not overlap the region this writes into. A dst with
// enough spare capacity is filled in place and nothing is allocated.
func Append(dst []byte, mdTsNs int64, connEpoch, seq uint64, payload []byte) []byte {
	off := len(dst)
	dst = grow(dst, Size+len(payload))
	hdr := dst[off : off+Size]
	// version in the low byte, reserved bytes zero. One store, so a reused buffer cannot keep stale bytes.
	binary.LittleEndian.PutUint64(hdr, uint64(V1))
	binary.LittleEndian.PutUint64(hdr[OffMDTsNs:], uint64(mdTsNs))
	binary.LittleEndian.PutUint64(hdr[OffConnEpoch:], connEpoch)
	binary.LittleEndian.PutUint64(hdr[OffSeq:], seq)
	binary.LittleEndian.PutUint64(hdr[OffDigest:], sum64(payload))
	copy(dst[off+Size:], payload)
	return dst
}

// Parse reads a v1 message. payload aliases msg and is empty when msg is exactly Size bytes long.
//
// A buffer shorter than Size is ErrShort, before the version is considered. A version other than V1 is
// ErrVersion, before the reserved bytes are considered: those bytes are reserved only in v1. A non-zero
// reserved byte is ErrReserved. The digest is returned as stored.
func Parse(msg []byte) (mdTsNs int64, connEpoch, seq, digest uint64, payload []byte, err error) {
	if err = reject(msg); err != nil {
		return 0, 0, 0, 0, nil, err
	}
	return int64(binary.LittleEndian.Uint64(msg[OffMDTsNs:])),
		binary.LittleEndian.Uint64(msg[OffConnEpoch:]),
		binary.LittleEndian.Uint64(msg[OffSeq:]),
		binary.LittleEndian.Uint64(msg[OffDigest:]),
		msg[Size:],
		nil
}

// MDTsNs reads the publish time from a v1 message. It applies the same checks as Parse and does not look
// at the payload. The prehook compares this with the local clock and drops the message without decoding.
func MDTsNs(msg []byte) (int64, error) {
	if err := reject(msg); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(msg[OffMDTsNs:])), nil
}

// reject reports why msg is not a v1 envelope. The order is length, then version, then the reserved
// bytes. A nil error means the first Size bytes are a v1 header.
func reject(msg []byte) error {
	if len(msg) < Size {
		return fmt.Errorf("%w: %d bytes", ErrShort, len(msg))
	}
	head := binary.LittleEndian.Uint64(msg)
	if v := byte(head); v != V1 {
		return fmt.Errorf("%w: %d", ErrVersion, v)
	}
	if head>>8 != 0 {
		for i := OffVersion + 1; i < OffMDTsNs; i++ {
			if msg[i] != 0 {
				return fmt.Errorf("%w: offset %d is %#x", ErrReserved, i, msg[i])
			}
		}
		return ErrReserved
	}
	return nil
}

// grow extends dst's length by n, allocating only when cap is short.
func grow(dst []byte, n int) []byte {
	if cap(dst)-len(dst) < n {
		next := make([]byte, len(dst), len(dst)+n)
		copy(next, dst)
		dst = next
	}
	return dst[:len(dst)+n]
}
