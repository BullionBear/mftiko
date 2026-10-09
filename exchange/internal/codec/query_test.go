package codec

import (
	"net/url"
	"strings"
	"testing"
)

func TestQuery(t *testing.T) {
	q := Query(nil).Add("symbol", "BTCUSDT").AddDecimal("price", 121034.56, 0.1).AddDecimal("quantity", 0.01, 0.001).
		AddInt("timestamp", 1760046205119).Add("newClientOrderId", "").AddInt("n", -3)
	if got, want := string(q), "symbol=BTCUSDT&price=121034.6&quantity=0.010&timestamp=1760046205119&newClientOrderId=&n=-3"; got != want {
		t.Errorf("%s\nwant %s", got, want)
	}

	// The same buffer, reused: no separator leads, and encoding allocates nothing while it has room.
	buf := make(Query, 0, 256)
	if got := string(buf.AddInt("a", 1)); got != "a=1" {
		t.Errorf("%s", got)
	}
	sig := []byte("HZ8H/+n7=")
	n := testing.AllocsPerRun(100, func() {
		_ = buf[:0].Add("symbol", "１２３").AddInt("timestamp", 1760046205119).AddBytes("signature", sig)
	})
	if n != 0 {
		t.Errorf("allocates %v times", n)
	}
}

// What Binance shows for a percent-encoded base64 signature (REST docs, RSA example, step 3).
func TestQueryEscapesBase64LikeBinance(t *testing.T) {
	const sig = "HZ8HOjiJ1s/igS9JA+n7+7Ti/ihtkRF5BIWcPIEluJP6tlbFM/Bf44LfZka/iemtahZAZzcO9TnI5uaXh3++lrqtNonCwp6/245UFWkiW1elpgtVAmJPbogcAv6rSlokztAfWk296ZJXzRDYAtzGH0gq7CgSJKfH+XxaCmR0WcvlKjNQnp12/eKXJYO4tDap8UCBLuyxDnR7oJKLHQHJLP0r0EAVOOSIbrFang/1WOq+Jaq4Efc4XpnTgnwlBbWTmhWDR1pvS9iVEzcSYLHT/fNnMRxFc7u+j3qI//5yuGuu14KR0MuQKKCSpViieD+fIti46sxPTsjSemoUKp0oXA=="
	const want = "signature=HZ8HOjiJ1s%2FigS9JA%2Bn7%2B7Ti%2FihtkRF5BIWcPIEluJP6tlbFM%2FBf44LfZka%2FiemtahZAZzcO9TnI5uaXh3%2B%2BlrqtNonCwp6%2F245UFWkiW1elpgtVAmJPbogcAv6rSlokztAfWk296ZJXzRDYAtzGH0gq7CgSJKfH%2BXxaCmR0WcvlKjNQnp12%2FeKXJYO4tDap8UCBLuyxDnR7oJKLHQHJLP0r0EAVOOSIbrFang%2F1WOq%2BJaq4Efc4XpnTgnwlBbWTmhWDR1pvS9iVEzcSYLHT%2FfNnMRxFc7u%2Bj3qI%2F%2F5yuGuu14KR0MuQKKCSpViieD%2BfIti46sxPTsjSemoUKp0oXA%3D%3D"
	if got := string(Query(nil).AddBytes("signature", []byte(sig))); got != want {
		t.Errorf("%s\nwant %s", got, want)
	}
}

// Every byte is escaped as url.QueryEscape escapes it, except a space, which is %20 rather than '+'; and
// url.ParseQuery reads every pair back.
func TestQueryEscaping(t *testing.T) {
	for c := 0; c < 256; c++ {
		s := string([]byte{byte(c)})
		got := string(escape(nil, s))
		if want := strings.ReplaceAll(url.QueryEscape(s), "+", "%20"); got != want {
			t.Errorf("byte %#x: %s, want %s", c, got, want)
		}
	}

	pairs := [][2]string{
		{"symbol", "１２３４５６"}, {"a b", "c d"}, {"x", "a&b=c"}, {"plus", "1+1"}, {"pct", "100%"},
		{"json", `[{"symbol":"BTCUSDT","side":"BUY"}]`}, {"empty", ""}, {"~._-", "~._-"},
	}
	var q Query
	for _, p := range pairs {
		q = q.Add(p[0], p[1])
	}
	vals, err := url.ParseQuery(string(q))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pairs {
		if got := vals.Get(p[0]); got != p[1] || !vals.Has(p[0]) {
			t.Errorf("%q = %q, want %q (in %s)", p[0], got, p[1], q)
		}
	}
}
