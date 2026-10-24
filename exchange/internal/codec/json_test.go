package codec

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/bytedance/sonic"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
)

func scribble(b []byte) {
	for i := range b {
		b[i] = '#'
	}
}

func TestDecodeFixtures(t *testing.T) {
	for _, f := range fixtures {
		v := f.new()
		if err := f.decode(f.load(t), v); err != nil {
			t.Errorf("%s: %v", f.name, err)
		}
	}

	var depth binanceDepth
	if err := DecodeFrame(fixtures[2].load(t), &depth); err != nil {
		t.Fatal(err)
	}
	d := depth.Data
	if d.Event != "depthUpdate" || d.EventMs != 1760046205123 || d.TxMs != 1760046205119 || d.Symbol != "BTCUSDT" ||
		d.First != 8712345678901 || d.Last != 8712345679050 || d.Prev != 8712345678899 {
		t.Errorf("depth header %+v", d)
	}
	if len(d.Bids) != 20 || len(d.Asks) != 20 || d.Bids[0] != (level{121034.5, 30.68}) || d.Asks[0][0] != 121034.6 {
		t.Errorf("depth levels: %d bids, %d asks, best %v / %v", len(d.Bids), len(d.Asks), d.Bids[0], d.Asks[0])
	}

	var okx okxBooks
	if err := DecodeFrame(fixtures[3].load(t), &okx); err != nil {
		t.Fatal(err)
	}
	if len(okx.Data) != 1 || okx.Data[0].Ts != 1760046205123 || okx.Data[0].Checksum != -1200119424 ||
		okx.Data[0].Asks[0] != [4]Decimal{121034.6, 68, 0, 7} {
		t.Errorf("okx books %+v", okx.Data)
	}

	var inst okxInstruments
	if err := DecodeBody(fixtures[7].load(t), &inst); err != nil {
		t.Fatal(err)
	}
	doge := inst.Data[3]
	if doge.InstID != "DOGE-USDT-SWAP" || doge.CtVal != 1000 || doge.TickSz != 0.00001 || doge.Lever != 50 {
		t.Errorf("okx instrument %+v", doge)
	}
	if decimal.Has(float64(doge.Stk)) {
		t.Errorf(`stk "" = %v, want Absent`, doge.Stk)
	}
}

// sonic falls back to encoding/json on a Go or CPU it does not support, and the fallback ignores both
// configurations: it matches names case-insensitively and copies every string. E03-D002: before each Go upgrade,
// sonic must support it.
func TestSonicIsNative(t *testing.T) {
	if sonic.APIKind != sonic.UseSonicJSON {
		t.Fatalf("sonic runs as encoding/json on %s/%s %s: upgrade sonic or hold Go back (E03-D002)",
			runtime.GOOS, runtime.GOARCH, runtime.Version())
	}
}

// A frame decoded into the connection's scratch value allocates nothing (E03-D002). The race detector makes
// sync.Pool drop a quarter of what it is given; AllocsPerRun's average is integral, so that rounds to 0.
func TestDecodeFrameAllocatesNothing(t *testing.T) {
	for _, f := range fixtures {
		if !f.frame {
			continue
		}
		in, v := f.load(t), f.new()
		if err := DecodeFrame(in, v); err != nil {
			t.Fatal(err)
		}
		if n := testing.AllocsPerRun(100, func() { _ = DecodeFrame(in, v) }); n != 0 {
			t.Errorf("%s: %v allocations per frame", f.name, n)
		}
	}
}

// Binance's bookTicker sends b, B, a, A: bid price, bid quantity, ask price, ask quantity. A decoder that
// matched names case-insensitively would put a quantity into a price the struct declares in lower case only.
func TestFieldNamesAreCaseSensitive(t *testing.T) {
	var prices struct {
		Data struct {
			Bid Decimal `json:"b"`
			Ask Decimal `json:"a"`
			E   string  `json:"e"`
		} `json:"data"`
	}
	b := fixtures[1].load(t)
	for name, decode := range map[string]func([]byte, any) error{"frame": DecodeFrame, "body": DecodeBody} {
		if err := decode(b, &prices); err != nil {
			t.Fatal(err)
		}
		if p := prices.Data; p.Bid != 121034.5 || p.Ask != 121034.6 || p.E != "bookTicker" {
			t.Errorf("%s: %+v; B, A or E landed in b, a or e", name, p)
		}
	}
}

// The borrowing configuration leaves strings pointing into the frame: zero copies, valid until the buffer is
// reused (E03-D002). The copying one survives the reuse, map keys and strings inside an any included.
func TestFrameBorrowsBodyCopies(t *testing.T) {
	const src = `{"s":"BTCUSDT","m":{"key":"val"},"l":["x","y"],"i":{"k":["v"]},"p":"1.5"}`
	type doc struct {
		S string            `json:"s"`
		M map[string]string `json:"m"`
		L []string          `json:"l"`
		I any               `json:"i"`
		P Decimal           `json:"p"`
	}
	want := fmt.Sprint(doc{"BTCUSDT", map[string]string{"key": "val"}, []string{"x", "y"}, map[string]any{"k": []any{"v"}}, 1.5})

	buf := []byte(src)
	var body doc
	if err := DecodeBody(buf, &body); err != nil {
		t.Fatal(err)
	}
	scribble(buf)
	if got := fmt.Sprint(body); got != want {
		t.Errorf("DecodeBody after the body was reused: %s, want %s", got, want)
	}

	buf = []byte(src)
	var frame doc
	if err := DecodeFrame(buf, &frame); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(frame); got != want {
		t.Fatalf("DecodeFrame: %s, want %s", got, want)
	}
	scribble(buf)
	if frame.S != "#######" || frame.L[0] != "#" {
		t.Errorf("DecodeFrame copied: %q %q", frame.S, frame.L)
	}
	if frame.P != 1.5 {
		t.Errorf("a number changed with the buffer: %v", frame.P)
	}
}

// sonic's syntax errors quote the input; the error must not change when the caller reuses the buffer.
func TestDecodeErrorOutlivesBuffer(t *testing.T) {
	for name, decode := range map[string]func([]byte, any) error{"frame": DecodeFrame, "body": DecodeBody} {
		buf := []byte(`{"s":"BTCUSDT", oops}`)
		var v struct {
			S string `json:"s"`
		}
		err := decode(buf, &v)
		if err == nil {
			t.Fatalf("%s: accepted invalid JSON", name)
		}
		msg := err.Error()
		scribble(buf)
		if err.Error() != msg {
			t.Errorf("%s: the error changed with the buffer:\n%s\n%s", name, msg, err.Error())
		}
	}
}

func TestDecimal(t *testing.T) {
	absent := math.NaN()
	good := map[string]float64{
		`"0.0010"`: 0.001, `0.001`: 0.001, `"121034.50"`: 121034.5, `"-0.5"`: -0.5, `"+2"`: 2, `"1e-5"`: 0.00001,
		`"100000000.0000000000000000"`: 1e8, `7`: 7, `""`: absent, `null`: absent,
	}
	for in, want := range good {
		d := Decimal(42)
		if err := d.UnmarshalJSON([]byte(in)); err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if got := float64(d); got != want && !(math.IsNaN(got) && math.IsNaN(want)) {
			t.Errorf("%s = %v, want %v", in, got, want)
		}
	}
	for _, in := range []string{`"NaN"`, `"Infinity"`, `"-Inf"`, `"0x10"`, `"1_000"`, `"abc"`, `true`, `"1e999"`, `"\u0031"`, `" 1"`, `"`} {
		d := Decimal(42)
		err := d.UnmarshalJSON([]byte(in))
		if !errors.Is(err, decimal.ErrInvalid) {
			t.Errorf("%s: err %v, want decimal.ErrInvalid", in, err)
		}
		if d != 42 {
			t.Errorf("%s: wrote %v on error", in, d)
		}
	}

	// The same through the decoders, which hand UnmarshalJSON the raw value, quotes and null included.
	var v struct {
		A Decimal `json:"a"`
		B Decimal `json:"b"`
		C Decimal `json:"c"`
		D Decimal `json:"d"`
	}
	v.D = Decimal(decimal.Absent())
	if err := DecodeFrame([]byte(`{"a":"0.10","b":null,"c":""}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.A != 0.1 || decimal.Has(float64(v.B)) || decimal.Has(float64(v.C)) || decimal.Has(float64(v.D)) {
		t.Errorf("%+v", v)
	}
	if err := DecodeFrame([]byte(`{"a":"NaN"}`), &v); err == nil {
		t.Error(`"NaN" decoded`)
	}
}

// The number is read from the input in place: no string in between, so nothing allocated.
func TestDecimalReadsInPlace(t *testing.T) {
	in := []byte(`"121034.50"`)
	var d Decimal
	if n := testing.AllocsPerRun(100, func() { _ = d.UnmarshalJSON(in) }); n != 0 {
		t.Errorf("UnmarshalJSON allocates %v times", n)
	}
}

func TestMarshal(t *testing.T) {
	b, err := Marshal(struct {
		InstID string `json:"instId"`
		Lever  string `json:"lever"`
		Mode   string `json:"mgnMode"`
		Note   string `json:"note"`
	}{"BTC-USDT", "5", "isolated", "<&>"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), `{"instId":"BTC-USDT","lever":"5","mgnMode":"isolated","note":"<&>"}`; got != want {
		t.Errorf("%s, want %s", got, want)
	}
	if _, err := Marshal(struct{ X float64 }{math.NaN()}); err == nil {
		t.Error("NaN marshalled")
	}
}
