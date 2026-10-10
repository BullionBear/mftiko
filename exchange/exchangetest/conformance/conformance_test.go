package conformance_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

// TestFakexConforms is what a venue's test looks like: one call.
func TestFakexConforms(t *testing.T) { conformance.Run(t, fakeSpec(faults{})) }

func FuzzFakexDecode(f *testing.F)  { conformance.FuzzDecode(f, fakeSpec(faults{}), "public") }
func FuzzFakexPrivate(f *testing.F) { conformance.FuzzDecodePrivate(f, fakeSpec(faults{})) }
func FuzzFakexSubmit(f *testing.F)  { conformance.FuzzParse(f, fakeSpec(faults{}), "submit_ok") }

// TestChecksCatchFaults: each fault of the fake venue is reported by the check meant for it, and
// the clean venue reports nothing.
func TestChecksCatchFaults(t *testing.T) {
	if conformance.EnvUpdateSet() {
		t.Skip("updating golden files")
	}
	clean, err := conformance.Check(fakeSpec(faults{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(clean) != 0 {
		t.Fatalf("the clean venue fails: %v", clean)
	}
	for _, tc := range []struct {
		name  string
		f     faults
		check string
		says  string
	}{
		{"a frame kept past Decode", faults{keepFrame: true}, "sessions", "keeps a reference into a frame"},
		{"unknown frames dropped", faults{dropUnknown: true}, "unknown_frames", "returned no error"},
		{"a secret in a request", faults{leakSecret: true}, "credentials", "a secret appears"},
		{"a channel over budget", faults{bigChannel: true}, "planning", "Cost 11 against Budget 10"},
		{"an absent field filled", faults{fillTicker: true}, "capabilities", "declared absent and filled"},
		{"a gap not noticed", faults{blindBook: true}, "books", "went unnoticed"},
		{"fill ids that disagree", faults{otherFillID: true}, "fills", "not in History"},
		{"a 401 not classified as auth", faults{authAs500: true}, "golden", "submit_auth"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ps, err := conformance.Check(fakeSpec(tc.f))
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range ps {
				if p.Check == tc.check && strings.Contains(p.String(), tc.says) {
					return
				}
			}
			t.Fatalf("no %s problem saying %q; got %v", tc.check, tc.says, ps)
		})
	}
}

// TestMissingGolden: a fixture without its golden file fails; it does not pass by default.
func TestMissingGolden(t *testing.T) {
	if conformance.EnvUpdateSet() {
		t.Skip("updating golden files")
	}
	s := fakeSpec(faults{})
	s.Cases = append(s.Cases, conformance.Call("not_recorded", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
		return e.REST.Listing(symbol.Spot)
	}))
	ps, err := conformance.Check(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ps {
		if p.Where == "http/not_recorded.golden.json" && strings.Contains(p.What, "no golden file") {
			return
		}
	}
	t.Fatalf("a case without a golden file passed: %v", ps)
}

// pipe is an in-memory websocket: the test plays the venue on the other end.
type pipe struct {
	mu     sync.Mutex
	in     chan []byte // venue -> recorder
	sent   [][]byte    // recorder -> venue
	closed chan struct{}
	once   sync.Once
}

func newPipe() *pipe { return &pipe{in: make(chan []byte, 64), closed: make(chan struct{})} }

func (p *pipe) Read(ctx context.Context) ([]byte, error) {
	select {
	case b := <-p.in:
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closed:
		return nil, io.EOF
	}
}

func (p *pipe) Write(_ context.Context, b []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, append([]byte(nil), b...))
	return nil
}

func (p *pipe) Close() error { p.once.Do(func() { close(p.closed) }); return nil }

// doer answers every request with one fixed response.
type doer struct{ resp *exchange.HTTPResponse }

func (d doer) Do(context.Context, *exchange.HTTPRequest) (*exchange.HTTPResponse, error) {
	return d.resp, nil
}

// TestRecordMarket: a recorded session replays through the codec it was recorded with.
func TestRecordMarket(t *testing.T) {
	m := &market{}
	p := newPipe()
	frames := []string{
		`{"op":"ack","id":1}`,
		`{"ch":"trade:BTCUSDT","ts":1,"tid":"t1","px":"100","sz":"1","side":"buy"}`,
		`{"ch":"trade:BTCUSDT","ts":2,"tid":"t2","px":"101","sz":"2","side":"sell"}`,
	}
	for _, f := range frames {
		p.in <- []byte(f)
	}
	var out bytes.Buffer
	var dialed string
	err := conformance.RecordMarket(context.Background(), conformance.MarketRecording{
		Venue: venue, Market: m, Route: "public", Channels: []string{"trade:BTCUSDT"},
		Dial:     func(_ context.Context, url string) (conformance.FrameConn, error) { dialed = url; return p, nil },
		Out:      &out,
		Duration: 300 * time.Millisecond,
		Snapshots: []conformance.Snapshot{{Ch: "book:BTCUSDT", Req: exchange.HTTPRequest{Method: "GET",
			Base: "https://api.fakex.example", Path: "/v1/depth", Query: []byte("sym=BTCUSDT&signature=abc")}}},
		SnapshotAfter: 1,
		Doer:          doer{resp: &exchange.HTTPResponse{Status: 200, Body: []byte(`{"seq":1,"bids":[],"asks":[]}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if dialed != "wss://ws.fakex.example/public" {
		t.Fatalf("dialed %q", dialed)
	}
	if len(p.sent) != 1 || !strings.Contains(string(p.sent[0]), `"op":"sub"`) {
		t.Fatalf("sent %q", p.sent)
	}
	recs, err := conformance.ReadRecords(&out)
	if err != nil {
		t.Fatal(err)
	}
	var in, outs, snaps, checks int
	for _, r := range recs {
		switch r.Kind {
		case conformance.KindIn:
			in++
		case conformance.KindOut:
			outs++
		case conformance.KindHTTP:
			snaps++
			if r.Check {
				checks++
			}
			if strings.Contains(r.Exchange.Request.Query, "abc") {
				t.Fatalf("the signature was recorded: %q", r.Exchange.Request.Query)
			}
		}
	}
	if recs[0].Kind != conformance.KindMeta || recs[0].Route != "public" || in != len(frames) || outs != 1 || snaps != 2 || checks != 1 {
		t.Fatalf("records: meta %+v, in %d, out %d, snapshots %d (checks %d)", recs[0], in, outs, snaps, checks)
	}
	// What was recorded decodes with the codec it came from.
	sess, _ := m.NewSession("public")
	for _, r := range recs {
		if r.Kind == conformance.KindIn {
			b, _ := r.Bytes()
			if err := sess.Decode(b, nopSink{}); err != nil {
				t.Fatalf("replay %s: %v", b, err)
			}
		}
	}
}

// TestRecordPrivate: the credentials never reach the fixture, login frames included.
func TestRecordPrivate(t *testing.T) {
	creds := exchange.Credentials{Type: exchange.KeyHMAC, Key: "real-testnet-key", Secret: "real-testnet-secret"}
	p := newPipe()
	p.in <- []byte(`{"op":"login","ok":true}`)
	var out bytes.Buffer
	err := conformance.RecordPrivate(context.Background(), conformance.PrivateRecording{
		Venue: venue, Account: &account{c: creds}, Credentials: creds,
		Dial:     func(context.Context, string) (conformance.FrameConn, error) { return p, nil },
		Out:      &out,
		Duration: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "real-testnet") {
		t.Fatalf("credentials recorded:\n%s", out.String())
	}
	if len(p.sent) != 1 || !strings.Contains(string(p.sent[0]), "real-testnet-key") {
		t.Fatalf("the venue did not get the real login frame: %q", p.sent)
	}
	if !strings.Contains(out.String(), conformance.Redacted) {
		t.Fatalf("nothing redacted:\n%s", out.String())
	}
}

func TestSaveExchangeRedacts(t *testing.T) {
	var b bytes.Buffer
	req := &exchange.HTTPRequest{Method: "POST", Base: "https://x", Path: "/o", Query: []byte("a=1&apiKey=k1&signature=s1"),
		Header: []exchange.Header{{Name: "X-MBX-APIKEY", Value: "k1"}, {Name: "Accept", Value: "json"}}, Body: []byte(`{"secret":"zz"}`)}
	resp := &exchange.HTTPResponse{Status: 200, Body: []byte(`{"listenKey":"zz"}`)}
	if err := conformance.SaveExchange(&b, req, resp, "zz"); err != nil {
		t.Fatal(err)
	}
	s := b.String()
	for _, leak := range []string{"k1", "s1", "zz"} {
		if strings.Contains(s, leak) {
			t.Fatalf("%q leaked:\n%s", leak, s)
		}
	}
	if !strings.Contains(s, `"a=1&apiKey=REDACTED&signature=REDACTED"`) || !strings.Contains(s, `"json"`) {
		t.Fatalf("redacted too much or too little:\n%s", s)
	}
}

func TestNormalize(t *testing.T) {
	type ev struct {
		A float64
		B *exchange.Level
		C error
		d int
	}
	got := string(conformance.Encode(ev{A: decimal.Absent(), B: &exchange.Level{Price: 1.5, Qty: 0}, C: exchange.ErrAuth}))
	if got != `{"A":null,"B":{"Price":1.5,"Qty":0},"C":"auth"}` {
		t.Fatalf("Encode = %s", got)
	}
	if k := conformance.Kind(&exchange.Reject{Code: 201, Native: "-2010"}); k != "reject 201 -2010" {
		t.Fatalf("Kind = %q", k)
	}
	if k := conformance.Kind(errors.Join(errors.New("x"), exchange.ErrUnknownOutcome)); k != "unknown_outcome" {
		t.Fatalf("Kind = %q", k)
	}
}

type nopSink struct{}

func (nopSink) Ack(uint64, error)                           {}
func (nopSink) BestQuote(string, *exchange.BestQuote)       {}
func (nopSink) Trade(string, *exchange.Trade)               {}
func (nopSink) AggTrade(string, *exchange.AggTrade)         {}
func (nopSink) Book(string, *exchange.BookUpdate)           {}
func (nopSink) Kline(string, *exchange.Kline)               {}
func (nopSink) Ticker(string, *exchange.Ticker)             {}
func (nopSink) Liquidation(string, *exchange.Liquidation)   {}
func (nopSink) FundingRate(string, *exchange.FundingRate)   {}
func (nopSink) OpenInterest(string, *exchange.OpenInterest) {}
func (nopSink) Greeks(string, *exchange.Greeks)             {}
func (nopSink) MarkPrice(string, *exchange.MarkPrice)       {}
func (nopSink) FeedEnd(string, *exchange.FeedEnd)           {}
