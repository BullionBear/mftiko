package envelope

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func FuzzRoundTrip(f *testing.F) {
	f.Add(int64(0), uint64(0), uint64(0), []byte(nil))
	f.Add(int64(-1), uint64(1), ^uint64(0), []byte{1, 2, 3, 4})
	f.Add(int64(42), uint64(7), uint64(99), bytes.Repeat([]byte{9}, 40))
	f.Fuzz(func(t *testing.T, ts int64, epoch, seq uint64, payload []byte) {
		msg := Append(nil, ts, epoch, seq, payload)
		gotTs, gotEpoch, gotSeq, dig, gotP, err := Parse(msg)
		if err != nil {
			t.Fatal(err)
		}
		if gotTs != ts || gotEpoch != epoch || gotSeq != seq || dig != sum64(payload) || !bytes.Equal(gotP, payload) {
			t.Fatalf("round trip mismatch")
		}
		md, err := MDTsNs(msg)
		if err != nil || md != ts {
			t.Fatalf("MDTsNs %d %v", md, err)
		}
	})
}

func FuzzParse(f *testing.F) {
	f.Add(Append(nil, 1, 2, 3, []byte("abc")))
	f.Add([]byte(nil))
	f.Add([]byte{2, 0})
	f.Fuzz(func(t *testing.T, msg []byte) {
		ts, epoch, seq, dig, payload, err := Parse(msg)
		md, err2 := MDTsNs(msg)
		switch {
		case err == nil && err2 != nil, err != nil && err2 == nil:
			t.Fatalf("Parse %v, MDTsNs %v", err, err2)
		case err != nil:
			for _, e := range []error{err, err2} {
				if !errors.Is(e, ErrShort) && !errors.Is(e, ErrVersion) && !errors.Is(e, ErrReserved) {
					t.Fatalf("unexpected %v", e)
				}
			}
			if errors.Is(err, ErrShort) != errors.Is(err2, ErrShort) ||
				errors.Is(err, ErrVersion) != errors.Is(err2, ErrVersion) ||
				errors.Is(err, ErrReserved) != errors.Is(err2, ErrReserved) {
				t.Fatalf("Parse %v, MDTsNs %v", err, err2)
			}
			return
		}
		if md != ts || len(msg) < Size || msg[0] != V1 {
			t.Fatalf("accepted %d bytes version %d", len(msg), msg[0])
		}
		for i := 1; i < OffMDTsNs; i++ {
			if msg[i] != 0 {
				t.Fatalf("accepted reserved %#x at %d", msg[i], i)
			}
		}
		if epoch != binary.LittleEndian.Uint64(msg[OffConnEpoch:]) ||
			seq != binary.LittleEndian.Uint64(msg[OffSeq:]) ||
			dig != binary.LittleEndian.Uint64(msg[OffDigest:]) ||
			ts != int64(binary.LittleEndian.Uint64(msg[OffMDTsNs:])) ||
			!bytes.Equal(payload, msg[Size:]) {
			t.Fatal("Parse disagrees with the bytes")
		}
	})
}
