package conformance

import (
	"testing"

	"github.com/BullionBear/mftiko/exchange"
)

// FuzzDecode fuzzes the market codec of route, seeded with every ws.in frame of the market sessions
// on that route. Decode may return an error; it may not panic. A venue calls it from its own fuzz
// function, which Go requires to live in the venue's test file:
//
//	func FuzzDecodePublic(f *testing.F) { conformance.FuzzDecode(f, spec(), "public") }
func FuzzDecode(f *testing.F, s Spec, route string) {
	r, err := newRunner(s)
	if err != nil {
		f.Fatal(err)
	}
	if s.Market == nil {
		f.Fatal("conformance: Spec.Market is nil")
	}
	r.seed(f, func(m Record) bool { return m.Side == SideMkt && m.Route == route })
	f.Fuzz(func(t *testing.T, frame []byte) {
		c, err := s.Market.NewSession(route)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.Decode(frame, &marketSink{})
	})
}

// FuzzDecodePrivate is FuzzDecode for the private stream.
func FuzzDecodePrivate(f *testing.F, s Spec) {
	r, err := newRunner(s)
	if err != nil {
		f.Fatal(err)
	}
	if r.env.Account == nil {
		f.Fatal("conformance: Spec.Account is nil")
	}
	r.seed(f, func(m Record) bool { return m.Side == SidePriv })
	f.Fuzz(func(t *testing.T, frame []byte) {
		c, err := r.env.Account.NewStream()
		if err != nil {
			t.Fatal(err)
		}
		_ = c.Decode(frame, &accountSink{})
	})
}

// FuzzDecodeTrade is FuzzDecode for the TradeStream (D303).
func FuzzDecodeTrade(f *testing.F, s Spec) {
	r, err := newRunner(s)
	if err != nil {
		f.Fatal(err)
	}
	if ts, err := r.tradeStream(); err != nil || ts == nil {
		f.Fatalf("conformance: no TradeStream: %v", err)
	}
	r.seed(f, func(m Record) bool { return m.Side == SideTrade })
	f.Fuzz(func(t *testing.T, frame []byte) {
		c, err := r.env.Account.NewTradeStream()
		if err != nil {
			t.Fatal(err)
		}
		_ = c.Decode(frame, &tradeSink{})
	})
}

func (r *runner) seed(f *testing.F, keep func(meta Record) bool) {
	names, err := Sessions(r.fsys)
	if err != nil {
		f.Fatal(err)
	}
	for _, name := range names {
		s, err := LoadSession(r.fsys, name)
		if err != nil {
			f.Fatal(err)
		}
		if !keep(s.Meta) {
			continue
		}
		for _, rec := range s.Records {
			if rec.Kind == KindIn {
				if b, err := rec.Bytes(); err == nil {
					f.Add(b)
				}
			}
		}
	}
	for _, u := range UnknownFrames {
		f.Add(u)
	}
}

// FuzzParse fuzzes the Parse of one case with arbitrary statuses and bodies, seeded with its recorded
// response. Parse may return an error; it may not panic.
func FuzzParse(f *testing.F, s Spec, name string) {
	r, err := newRunner(s)
	if err != nil {
		f.Fatal(err)
	}
	c, ok := r.caseByName(name)
	if !ok {
		f.Fatalf("conformance: no case %q", name)
	}
	_, parse, err := c.build(r.env)
	if err != nil {
		f.Fatalf("conformance: case %q: %v", name, err)
	}
	if ex, err := LoadExchange(r.fsys, name); err == nil {
		resp := ex.Response.HTTP()
		f.Add(resp.Status, resp.Body)
	}
	f.Add(200, []byte(`{}`))
	f.Add(429, []byte(``))
	f.Add(500, []byte(`not json`))
	f.Fuzz(func(t *testing.T, status int, body []byte) {
		_, _ = parse(&exchange.HTTPResponse{Status: status, Body: body})
	})
}
