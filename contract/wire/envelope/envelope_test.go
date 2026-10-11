package envelope

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"testing"
)

// Golden messages. Each hex string is the whole v1 message, built outside this package: version 1,
// seven zero reserved bytes, then the little-endian fields, then the payload. The digest is XXH64 of
// the payload only.
var golden = []struct {
	name                string
	hex                 string
	mdTsNs              int64
	connEpoch, seq, dig uint64
	payload             []byte
}{
	{
		name:      "empty payload",
		hex:       "0100000000000000e8030000000000000100000000000000010000000000000099e9d85137db46ef",
		mdTsNs:    1000,
		connEpoch: 1,
		seq:       1,
		dig:       0xef46db3751d8e999,
	},
	{
		name:      "three payload bytes",
		hex:       "0100000000000000ffffffffffffffff07000000000000002a000000000000001191c0bcd63fbcea102030",
		mdTsNs:    -1,
		connEpoch: 7,
		seq:       42,
		dig:       0xeabc3fd6bcc09111,
		payload:   []byte{0x10, 0x20, 0x30},
	},
	{
		name:      "one stripe",
		hex:       "0100000000000000080706050403020118171615141312112827262524232221b432ff16519cf5cb000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
		mdTsNs:    0x0102030405060708,
		connEpoch: 0x1112131415161718,
		seq:       0x2122232425262728,
		dig:       0xcbf59c5116ff32b4,
		payload:   seqBytes(32),
	},
}

func seqBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func TestGolden(t *testing.T) {
	for _, g := range golden {
		t.Run(g.name, func(t *testing.T) {
			want, err := hex.DecodeString(g.hex)
			if err != nil {
				t.Fatal(err)
			}
			if len(want) != Size+len(g.payload) {
				t.Fatalf("golden length %d, want %d", len(want), Size+len(g.payload))
			}
			got := Append(nil, g.mdTsNs, g.connEpoch, g.seq, g.payload)
			if !bytes.Equal(got, want) {
				t.Fatalf("Append:\n got %x\nwant %x", got, want)
			}
			ts, epoch, seq, dig, payload, err := Parse(want)
			if err != nil {
				t.Fatal(err)
			}
			if ts != g.mdTsNs || epoch != g.connEpoch || seq != g.seq || dig != g.dig || !bytes.Equal(payload, g.payload) {
				t.Fatalf("Parse = (%d, %d, %d, %#x, %x)", ts, epoch, seq, dig, payload)
			}
			md, err := MDTsNs(want)
			if err != nil || md != g.mdTsNs {
				t.Fatalf("MDTsNs = %d, %v", md, err)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	payloads := [][]byte{nil, {}, []byte("feed"), seqBytes(31), seqBytes(32), seqBytes(33), bytes.Repeat([]byte{0xab}, 300)}
	times := []int64{0, 1, -1, math.MinInt64, math.MaxInt64}
	epochs := []uint64{0, 1, math.MaxUint64}
	seqs := []uint64{0, 1, 42, math.MaxUint64}
	for _, p := range payloads {
		for _, ts := range times {
			for _, epoch := range epochs {
				for _, seq := range seqs {
					msg := Append(nil, ts, epoch, seq, p)
					gotTs, gotEpoch, gotSeq, dig, gotP, err := Parse(msg)
					if err != nil {
						t.Fatalf("Parse: %v", err)
					}
					if gotTs != ts || gotEpoch != epoch || gotSeq != seq || !bytes.Equal(gotP, p) {
						t.Fatalf("fields changed: ts %d/%d epoch %d/%d seq %d/%d payload %d/%d",
							gotTs, ts, gotEpoch, epoch, gotSeq, seq, len(gotP), len(p))
					}
					if dig != sum64(p) {
						t.Fatalf("digest %#x, xxHash64(payload) %#x", dig, sum64(p))
					}
					md, err := MDTsNs(msg)
					if err != nil || md != ts {
						t.Fatalf("MDTsNs = %d, %v", md, err)
					}
				}
			}
		}
	}
}

func TestDigestIgnoresEnvelope(t *testing.T) {
	p := []byte("same-payload")
	a := Append(nil, 1, 1, 1, p)
	b := Append(nil, 2, 9, 100, p)
	_, _, _, da, _, err := Parse(a)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, db, _, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if da != db || da != sum64(p) {
		t.Fatalf("digests %#x %#x, payload %#x", da, db, sum64(p))
	}
}

func TestDigestNotChecked(t *testing.T) {
	msg := Append(nil, 5, 6, 7, []byte("body"))
	msg[OffDigest] ^= 0xff
	_, _, _, dig, payload, err := Parse(msg)
	if err != nil {
		t.Fatal(err)
	}
	if dig == sum64(payload) {
		t.Fatal("flipped digest still matches")
	}
	want := binary.LittleEndian.Uint64(msg[OffDigest:])
	if dig != want || !bytes.Equal(payload, []byte("body")) {
		t.Fatalf("Parse = digest %#x payload %x", dig, payload)
	}
}

func TestAppendPreservesPrefixAndZerosReserved(t *testing.T) {
	dst := bytes.Repeat([]byte{0xff}, 80)
	prefix := bytes.Clone(dst[:4])
	out := Append(dst[:4], -5, 8, 9, []byte{1, 2})
	if !bytes.Equal(out[:4], prefix) {
		t.Fatalf("prefix changed: %x", out[:4])
	}
	hdr := out[4 : 4+Size]
	if hdr[0] != V1 {
		t.Fatalf("version %d", hdr[0])
	}
	for i := 1; i < OffMDTsNs; i++ {
		if hdr[i] != 0 {
			t.Fatalf("reserved offset %d is %#x", i, hdr[i])
		}
	}
	if !bytes.Equal(out[4+Size:], []byte{1, 2}) {
		t.Fatalf("payload %x", out[4+Size:])
	}
}

func TestReject(t *testing.T) {
	ok := Append(nil, 1, 2, 3, []byte{9})
	cases := []struct {
		name string
		msg  []byte
		want error
	}{
		{"empty", nil, ErrShort},
		{"one byte", []byte{V1}, ErrShort},
		{"39 bytes", ok[:Size-1], ErrShort},
		{"version 0", versioned(ok, 0), ErrVersion},
		{"version 2", versioned(ok, 2), ErrVersion},
		{"version 255", versioned(ok, 255), ErrVersion},
		{"version wins over reserved", func() []byte {
			m := versioned(ok, 2)
			m[3] = 1
			return m
		}(), ErrVersion},
	}
	for i := 1; i < OffMDTsNs; i++ {
		m := bytes.Clone(ok)
		m[i] = 0x7f
		cases = append(cases, struct {
			name string
			msg  []byte
			want error
		}{name: "reserved", msg: m, want: ErrReserved})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, _, _, _, err := Parse(c.msg); !errors.Is(err, c.want) {
				t.Fatalf("Parse err %v, want %v", err, c.want)
			}
			if _, err := MDTsNs(c.msg); !errors.Is(err, c.want) {
				t.Fatalf("MDTsNs err %v, want %v", err, c.want)
			}
		})
	}
	// Exactly Size bytes is an empty payload, not a short message.
	if _, _, _, _, p, err := Parse(ok[:Size]); err != nil || len(p) != 0 {
		t.Fatalf("empty payload: %q %v", p, err)
	}
}

func versioned(msg []byte, v byte) []byte {
	m := bytes.Clone(msg)
	m[0] = v
	return m
}

func TestUnalignedParse(t *testing.T) {
	msg := Append(nil, math.MinInt64, math.MaxUint64, 1, []byte("odd"))
	buf := make([]byte, len(msg)+1)
	copy(buf[1:], msg)
	ts, epoch, seq, dig, p, err := Parse(buf[1:])
	if err != nil {
		t.Fatal(err)
	}
	if ts != math.MinInt64 || epoch != math.MaxUint64 || seq != 1 || dig != sum64([]byte("odd")) || !bytes.Equal(p, []byte("odd")) {
		t.Fatalf("unaligned Parse = %d %d %d %#x %x", ts, epoch, seq, dig, p)
	}
	if got, err := MDTsNs(buf[1:]); err != nil || got != math.MinInt64 {
		t.Fatalf("unaligned MDTsNs = %d %v", got, err)
	}
}

func TestAllocs(t *testing.T) {
	payload := bytes.Repeat([]byte{0xab}, 256)
	dst := make([]byte, 0, Size+len(payload))
	var msg []byte
	allocs := testing.AllocsPerRun(200, func() {
		msg = Append(dst[:0], 1, 2, 3, payload)
		if _, _, _, _, _, err := Parse(msg); err != nil {
			t.Fatal(err)
		}
		if _, err := MDTsNs(msg); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("%v allocs, want 0", allocs)
	}
}
