package envelope

import (
	"bytes"
	"strconv"
	"testing"
)

// benchSizes are the payload lengths for the xxHash64 cost, and for encode and decode of a full message.
var benchSizes = []int{0, 32, 256, 1024, 4096, 16384}

func BenchmarkAppend(b *testing.B) {
	for _, n := range benchSizes {
		payload := bytes.Repeat([]byte{0xab}, n)
		// One leading byte puts the envelope at an odd address. cap is exact, so Append does not allocate.
		buf := make([]byte, 1, 1+Size+n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(Size + n))
			for b.Loop() {
				sinkBuf = Append(buf[:1], 1_700_000_000_000_000_000, 9, 1, payload)
			}
		})
	}
}

func BenchmarkParse(b *testing.B) {
	for _, n := range benchSizes {
		payload := bytes.Repeat([]byte{0xab}, n)
		msg := Append(nil, 1_700_000_000_000_000_000, 9, 1, payload)
		odd := make([]byte, len(msg)+1)
		copy(odd[1:], msg)
		msg = odd[1:]
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(msg)))
			for b.Loop() {
				var err error
				// Every field is stored. Assigning any of them to _ would let an inlined Parse
				// drop that load, and the decode cost would be understated.
				sinkI64, sinkEpoch, sinkSeq, sinkDig, sinkBuf, err = Parse(msg)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkXXHash64(b *testing.B) {
	for _, n := range benchSizes {
		raw := bytes.Repeat([]byte{0xab}, n+1)
		payload := raw[1:]
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n))
			for b.Loop() {
				sinkU64 = sum64(payload)
			}
		})
	}
}

var (
	sinkBuf            []byte
	sinkU64            uint64
	sinkI64            int64
	sinkEpoch, sinkSeq uint64
	sinkDig            uint64
)
