//go:build goexperiment.jsonv2 && !go1.27

// sonic as configured here against encoding/json/v2, on the same fixtures into the same structs, numbers read
// by Decimal in both (issue #106). encoding/json/v2 is behind GOEXPERIMENT in Go 1.26, and so is this file:
//
//	GOEXPERIMENT=jsonv2 go test ./exchange/internal/codec -run JSONv2 -bench . -benchmem
//
// Go 1.27 turns the experiment on by default, but the module's go line is 1.26, so stdversion rejects
// encoding/json/v2 here and the file stays off under 1.27 until go.mod moves to 1.27 and the tag goes (issue #498).
//
// encoding/json/v2 matches names case-sensitively, like both configurations, and always copies strings.

package codec

import (
	jsonv2 "encoding/json/v2"
	"fmt"
	"testing"
)

func decodeV2(b []byte, v any) error { return jsonv2.Unmarshal(b, v) }

func TestSonicMatchesJSONv2(t *testing.T) {
	for _, f := range fixtures {
		in := f.load(t)
		a, b := f.new(), f.new()
		if err := f.decode(in, a); err != nil {
			t.Fatalf("%s: sonic: %v", f.name, err)
		}
		if err := decodeV2(in, b); err != nil {
			t.Fatalf("%s: encoding/json/v2: %v", f.name, err)
		}
		// %+v prints NaN as NaN on both sides, where reflect.DeepEqual would call two Absents unequal.
		if sa, sb := fmt.Sprintf("%+v", a), fmt.Sprintf("%+v", b); sa != sb {
			t.Errorf("%s: sonic and encoding/json/v2 disagree:\n%s\n%s", f.name, sa, sb)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, f := range fixtures {
		in := f.load(b)
		for _, lib := range []struct {
			name   string
			decode func([]byte, any) error
		}{{"sonic", f.decode}, {"jsonv2", decodeV2}} {
			b.Run(f.name+"/"+lib.name, func(b *testing.B) {
				b.SetBytes(int64(len(in)))
				b.ReportAllocs()
				v := f.new()
				if err := lib.decode(in, v); err != nil { // and sonic compiles the type here, outside the loop
					b.Fatal(err)
				}
				for b.Loop() {
					if !f.frame {
						v = f.new()
					}
					if err := lib.decode(in, v); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
