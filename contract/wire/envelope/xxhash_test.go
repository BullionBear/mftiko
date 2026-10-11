package envelope

import "testing"

// xxh64Vec is an XXH64 seed-0 vector. The empty input is the algorithm's own value, ef46db3751d8e999.
// The others are the same function on short strings and on the byte sequence 0,1,2,... at the lengths
// where the stripe, the 8-byte tail, the 4-byte tail and the byte tail change.
var xxh64Vec = []struct {
	n    int // if text is empty and n >= 0, the input is the bytes 0,1,...,n-1; n < 0 means text
	text string
	want uint64
}{
	{n: 0, want: 0xef46db3751d8e999},
	{n: -1, text: "a", want: 0xd24ec4f1a98c6e5b},
	{n: -1, text: "ab", want: 0x65f708ca92d04a61},
	{n: -1, text: "abc", want: 0x44bc2cf5ad770999},
	{n: -1, text: "abcd", want: 0xde0327b0d25d92cc},
	{n: -1, text: "abcde", want: 0x07e3670c0c8dc7eb},
	{n: -1, text: "hello", want: 0x26c7827d889f6da3},
	{n: 1, want: 0xe934a84adb052768},
	{n: 3, want: 0xe5c7bb4533bc65dd},
	{n: 4, want: 0xffced8604453cc1e},
	{n: 7, want: 0x14cc643f630c72d2},
	{n: 8, want: 0x884a173614b81b8d},
	{n: 15, want: 0xa948f5f0f6abac2d},
	{n: 16, want: 0x44b6ef2fb84169f7},
	{n: 31, want: 0xc346d2b59b4d8ee1},
	{n: 32, want: 0xcbf59c5116ff32b4},
	{n: 33, want: 0x0c535d1acafb8ead},
	{n: 63, want: 0xe26aa9e2a95f8e4f},
	{n: 64, want: 0xf7c67301db6713f0},
}

func TestSum64Vectors(t *testing.T) {
	if got := sum64(nil); got != 0xef46db3751d8e999 {
		t.Fatalf("nil: %#x", got)
	}
	seq := make([]byte, 64)
	for i := range seq {
		seq[i] = byte(i)
	}
	for _, v := range xxh64Vec {
		var in []byte
		switch {
		case v.n < 0:
			in = []byte(v.text)
		case v.n == 0:
			in = []byte{}
		default:
			in = seq[:v.n]
		}
		if got := sum64(in); got != v.want {
			t.Errorf("sum64(%q) = %#x, want %#x", in, got, v.want)
		}
		// The same bytes at an odd offset: NATS msg.Data is not 8-byte aligned.
		odd := make([]byte, len(in)+1)
		copy(odd[1:], in)
		if got := sum64(odd[1:]); got != v.want {
			t.Errorf("unaligned sum64(%q) = %#x, want %#x", in, got, v.want)
		}
	}
}
