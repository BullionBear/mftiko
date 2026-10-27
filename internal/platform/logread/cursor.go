//go:build unix

package logread

import (
	"encoding/base64"
	"encoding/binary"
	"hash/fnv"
	"math"
)

// prefixN is how many bytes before a cursor's offset go into the hash (E14-D016).
const prefixN = 64

// Cursor is an opaque resume token. Callers store it and pass it back; they do
// not interpret the bytes (E14-D016).
type Cursor string

// cursor is the decoded token: the file's identity, the offset in that file,
// and the hash of the (up to) 64 bytes before the offset.
type cursor struct {
	dev  uint64
	ino  uint64
	off  int64
	hash uint64
}

// encode packs dev, inode, offset, and hash as four little-endian uint64
// values and then base64url without padding. The layout is part of E14-D016;
// the token is still opaque to callers.
func encode(dev, ino uint64, off int64, hash uint64) Cursor {
	var raw [32]byte
	binary.LittleEndian.PutUint64(raw[0:8], dev)
	binary.LittleEndian.PutUint64(raw[8:16], ino)
	binary.LittleEndian.PutUint64(raw[16:24], uint64(off))
	binary.LittleEndian.PutUint64(raw[24:32], hash)
	return Cursor(base64.RawURLEncoding.EncodeToString(raw[:]))
}

// decode reverses encode. A malformed token is ErrCursor, not a gap: a gap
// means a well-formed token that no longer matches a file (E14-D017).
func decode(c Cursor) (cursor, error) {
	if c == "" {
		return cursor{}, ErrCursor
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(string(c))
	if err != nil || len(raw) != 32 {
		return cursor{}, ErrCursor
	}
	uoff := binary.LittleEndian.Uint64(raw[16:24])
	if uoff > uint64(math.MaxInt64) {
		return cursor{}, ErrCursor
	}
	return cursor{
		dev:  binary.LittleEndian.Uint64(raw[0:8]),
		ino:  binary.LittleEndian.Uint64(raw[8:16]),
		off:  int64(uoff),
		hash: binary.LittleEndian.Uint64(raw[24:32]),
	}, nil
}

// fnv64 is FNV-1a 64, the same function as hash/fnv's New64a. It is a checksum
// so a reused inode cannot impersonate a cursor, not a security hash (E14-D016).
func fnv64(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}
