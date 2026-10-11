package envelope

import "encoding/binary"

// xxHash64 (XXH64, Yann Collet) of b, seed 0. Lanes are little-endian. Nothing is allocated and unsafe
// is not used. The contract tree imports only the standard library and protobuf, so the algorithm lives
// here rather than in a third-party package.
//
// The primes and the round, merge, and avalanche steps are the XXH64 definition. seed 0 is what
// "xxHash64" means when no seed is stated (D410).
const (
	prime64_1 uint64 = 0x9E3779B185EBCA87
	prime64_2 uint64 = 0xC2B2AE3D27D4EB4F
	prime64_3 uint64 = 0x165667B19E3779F9
	prime64_4 uint64 = 0x85EBCA77C2B2AE63
	prime64_5 uint64 = 0x27D4EB2F165667C5
)

// sum64 is XXH64(b, seed 0).
func sum64(b []byte) uint64 {
	n := len(b)
	var h uint64
	i := 0
	// seed is a variable so the sums wrap, which is what XXH64 specifies. A constant
	// prime64_1 + prime64_2 does not fit in uint64 and is rejected at compile time.
	seed := uint64(0)
	if n >= 32 {
		v1 := seed + prime64_1 + prime64_2
		v2 := seed + prime64_2
		v3 := seed
		v4 := seed - prime64_1
		for i+32 <= n {
			v1 = round(v1, binary.LittleEndian.Uint64(b[i:]))
			v2 = round(v2, binary.LittleEndian.Uint64(b[i+8:]))
			v3 = round(v3, binary.LittleEndian.Uint64(b[i+16:]))
			v4 = round(v4, binary.LittleEndian.Uint64(b[i+24:]))
			i += 32
		}
		h = rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18)
		h = mergeRound(h, v1)
		h = mergeRound(h, v2)
		h = mergeRound(h, v3)
		h = mergeRound(h, v4)
	} else {
		h = prime64_5
	}
	h += uint64(n)
	for i+8 <= n {
		h ^= round(0, binary.LittleEndian.Uint64(b[i:]))
		h = rotl(h, 27)*prime64_1 + prime64_4
		i += 8
	}
	if i+4 <= n {
		h ^= uint64(binary.LittleEndian.Uint32(b[i:])) * prime64_1
		h = rotl(h, 23)*prime64_2 + prime64_3
		i += 4
	}
	for ; i < n; i++ {
		h ^= uint64(b[i]) * prime64_5
		h = rotl(h, 11) * prime64_1
	}
	h ^= h >> 33
	h *= prime64_2
	h ^= h >> 29
	h *= prime64_3
	h ^= h >> 32
	return h
}

func round(acc, input uint64) uint64 {
	acc += input * prime64_2
	acc = rotl(acc, 31)
	acc *= prime64_1
	return acc
}

func mergeRound(acc, val uint64) uint64 {
	acc ^= round(0, val)
	return acc*prime64_1 + prime64_4
}

func rotl(x uint64, k uint) uint64 {
	return (x << k) | (x >> (64 - k))
}
