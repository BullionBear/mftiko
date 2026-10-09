package conformance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/BullionBear/mftiko/exchange"
)

type decoded struct {
	sess   *Session
	events []Event
	mkt    *marketSink
	acct   *accountSink
}

// decode runs a session fixture through a fresh codec, once.
func (r *runner) decode(name string) (*decoded, error) {
	if d, ok := r.sessions[name]; ok {
		return d, nil
	}
	s, err := LoadSession(r.fsys, name)
	if err != nil {
		return nil, err
	}
	d, err := r.replay(s, false)
	if err != nil {
		return nil, err
	}
	r.sessions[name] = d
	for _, e := range d.events {
		r.saw(string(e.Value))
	}
	return d, nil
}

// replay feeds every ws.in record of s to a new codec. With scribble set, every frame goes through
// one buffer that is overwritten as soon as Decode returns, as the conn worker's is (D302).
func (r *runner) replay(s *Session, scribble bool) (*decoded, error) {
	d := &decoded{sess: s}
	var decode func([]byte) error
	switch s.Meta.Side {
	case SideMkt:
		if r.spec.Market == nil {
			return nil, fmt.Errorf("%s is a market session and Spec.Market is nil", s.Name)
		}
		c, err := r.spec.Market.NewSession(s.Meta.Route)
		if err != nil {
			return nil, fmt.Errorf("NewSession(%q): %w", s.Meta.Route, err)
		}
		d.mkt = &marketSink{}
		decode = func(b []byte) error { return c.Decode(b, d.mkt) }
	case SideTrade:
		if r.env.Account == nil {
			return nil, fmt.Errorf("%s is a trade session and Spec.Account is nil", s.Name)
		}
		c, err := r.env.Account.NewTradeStream()
		if err != nil {
			return nil, fmt.Errorf("NewTradeStream: %w", err)
		}
		ts := &tradeSink{}
		d.acct = &ts.accountSink
		decode = func(b []byte) error { return c.Decode(b, ts) }
	case SidePriv:
		if r.env.Account == nil {
			return nil, fmt.Errorf("%s is a private session and Spec.Account is nil", s.Name)
		}
		c, err := r.env.Account.NewStream()
		if err != nil {
			return nil, fmt.Errorf("NewStream: %w", err)
		}
		d.acct = &accountSink{}
		decode = func(b []byte) error { return c.Decode(b, d.acct) }
	}
	var buf []byte
	for i, rec := range s.Records {
		if rec.Kind != KindIn {
			continue
		}
		b, err := rec.Bytes()
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		frame := append([]byte(nil), b...)
		if scribble {
			buf = append(buf[:0], b...)
			frame = buf
		}
		if d.mkt != nil {
			d.mkt.frame = i
		} else {
			d.acct.frame = i
		}
		if err := decode(frame); err != nil {
			r.saw(err.Error())
			ev := Event{Frame: i, Call: "error", Error: Kind(err)}
			if d.mkt != nil {
				d.mkt.events = append(d.mkt.events, ev)
			} else {
				d.acct.events = append(d.acct.events, ev)
			}
		}
		if scribble {
			for j := range buf {
				buf[j] = 0xA5
			}
		}
	}
	if d.mkt != nil {
		d.events = d.mkt.events
	} else {
		d.events = d.acct.events
	}
	return d, nil
}

// checkSessions: each session decodes to its golden events, and decodes the same when its frames
// share one buffer that is overwritten after every Decode (D302: what the codec keeps across frames
// must not point into a frame).
func (r *runner) checkSessions() []Problem {
	names, err := Sessions(r.fsys)
	if err != nil {
		return []Problem{{"sessions", "ws/", err.Error()}}
	}
	var out []Problem
	for _, name := range names {
		d, err := r.decode(name)
		if err != nil {
			out = append(out, Problem{"sessions", name, err.Error()})
			continue
		}
		out = append(out, r.golden("ws/"+name+".golden.jsonl", jsonLines(d.events))...)
		s2, err := r.replay(d.sess, true)
		if err != nil {
			out = append(out, Problem{"sessions", name, err.Error()})
			continue
		}
		if i := firstEventDiff(d.events, s2.events); i >= 0 {
			out = append(out, Problem{"sessions", name, fmt.Sprintf(
				"decoding changes when the frame buffer is reused: event %d (frame %d) differs; the codec keeps a reference into a frame after Decode returns (D302)",
				i, frameOf(d.events, s2.events, i))})
		}
	}
	return out
}

func firstEventDiff(a, b []Event) int {
	for i := 0; i < len(a) || i < len(b); i++ {
		if i >= len(a) || i >= len(b) || a[i].Call != b[i].Call || a[i].Ch != b[i].Ch ||
			a[i].Error != b[i].Error || !bytes.Equal(a[i].Value, b[i].Value) {
			return i
		}
	}
	return -1
}

func frameOf(a, b []Event, i int) int {
	if i < len(a) {
		return a[i].Frame
	}
	return b[i].Frame
}

// UnknownFrames are what every codec must refuse with an error, never drop silently.
var UnknownFrames = [][]byte{
	[]byte(`{"conformance":"a frame no venue sends"}`),
	[]byte(`["conformance",1]`),
	[]byte("\x00\x01 not json"),
}

// checkUnknown: Decode returns an error for a frame it does not recognize and reports nothing.
func (r *runner) checkUnknown() []Problem {
	var out []Problem
	try := func(where string, decode func([]byte) (int, error)) {
		for _, f := range UnknownFrames {
			n, err := decode(f)
			if err == nil {
				out = append(out, Problem{"unknown_frames", where, fmt.Sprintf("Decode(%q) returned no error", f)})
			}
			if n > 0 {
				out = append(out, Problem{"unknown_frames", where, fmt.Sprintf("Decode(%q) reported %d events", f, n)})
			}
		}
	}
	if m := r.spec.Market; m != nil {
		for _, route := range m.Routes() {
			c, err := m.NewSession(route)
			if err != nil {
				out = append(out, Problem{"unknown_frames", route, err.Error()})
				continue
			}
			try("route "+route, func(b []byte) (int, error) {
				s := &marketSink{}
				err := c.Decode(b, s)
				return len(s.events), err
			})
		}
	}
	if a := r.env.Account; a != nil {
		c, err := a.NewStream()
		if err != nil {
			out = append(out, Problem{"unknown_frames", "private", err.Error()})
		} else {
			try("private", func(b []byte) (int, error) {
				s := &accountSink{}
				err := c.Decode(b, s)
				return len(s.events), err
			})
		}
		switch c, err := a.NewTradeStream(); {
		case errors.Is(err, exchange.ErrUnsupported):
		case err != nil:
			out = append(out, Problem{"unknown_frames", "trade", err.Error()})
		default:
			try("trade", func(b []byte) (int, error) {
				s := &tradeSink{}
				err := c.Decode(b, s)
				return len(s.events), err
			})
		}
	}
	return out
}

// optional are the fields of each event that md.v1 makes optional: the ones a venue may leave
// absent and must then declare in TopicCaps.Absent (D401). A field that is not optional is never
// absent.
var optional = map[string][]string{
	"Kline":        {"QuoteVolume"},
	"Ticker":       {"Last", "Open24h", "High24h", "Low24h", "Volume24h", "QuoteVolume24h", "ChangePct24h"},
	"OpenInterest": {"OpenInterestValue"},
	"Greeks":       {"Delta", "Gamma", "Vega", "Theta", "Rho", "MarkIV", "BidIV", "AskIV", "MarkPrice", "UnderlyingPrice"},
	"MarkPrice":    {"IndexPrice"},
	"FundingRate":  {"IntervalS"},
}

// topicOf is the topic an event call belongs to; "" for calls with no capability row of their own.
var topicOf = map[string]exchange.Topic{
	"BestQuote": exchange.TopicBestQuote, "Trade": exchange.TopicTrade, "AggTrade": exchange.TopicAggTrade,
	"Ticker": exchange.TopicTicker, "Liquidation": exchange.TopicLiquidation, "FundingRate": exchange.TopicFundingRate,
	"OpenInterest": exchange.TopicOpenInterest, "Greeks": exchange.TopicGreeks, "MarkPrice": exchange.TopicMarkPrice,
}

var eventTypes = map[string]reflect.Type{
	"BestQuote": reflect.TypeFor[exchange.BestQuote](), "Trade": reflect.TypeFor[exchange.Trade](),
	"AggTrade": reflect.TypeFor[exchange.AggTrade](), "Kline": reflect.TypeFor[exchange.Kline](),
	"Ticker": reflect.TypeFor[exchange.Ticker](), "Liquidation": reflect.TypeFor[exchange.Liquidation](),
	"FundingRate": reflect.TypeFor[exchange.FundingRate](), "OpenInterest": reflect.TypeFor[exchange.OpenInterest](),
	"Greeks": reflect.TypeFor[exchange.Greeks](), "MarkPrice": reflect.TypeFor[exchange.MarkPrice](),
}

// checkCaps: the capability table matches what the venue sends (D401). A topic it emits is listed;
// a field it declares absent is never filled; an optional field it does not declare absent is
// filled at least once in the fixtures; a required field is never absent.
func (r *runner) checkCaps() []Problem {
	m := r.spec.Market
	if m == nil {
		return nil
	}
	caps := map[exchange.Topic]map[string]bool{}
	var klineAbsent map[string]bool
	var out []Problem
	for _, c := range m.Topics() {
		abs := map[string]bool{}
		for _, f := range c.Absent {
			abs[f] = true
		}
		caps[c.Topic] = abs
		if strings.HasPrefix(string(c.Topic), exchange.KlinePrefix) {
			klineAbsent = abs
		}
		call := callOf(c.Topic)
		if t, ok := eventTypes[call]; ok {
			for _, f := range c.Absent {
				if !slices.Contains(optional[call], f) {
					if _, ok := t.FieldByName(f); !ok {
						out = append(out, Problem{"capabilities", string(c.Topic), fmt.Sprintf("Absent names %q, which %s does not have", f, call)})
					} else {
						out = append(out, Problem{"capabilities", string(c.Topic), fmt.Sprintf("Absent names %q, which md.v1 requires", f)})
					}
				}
			}
		}
	}
	names, _ := Sessions(r.fsys)
	filled := map[string]map[string]bool{} // call -> optional field -> filled at least once
	count := map[string]int{}
	for _, name := range names {
		d, err := r.decode(name)
		if err != nil || d.mkt == nil {
			continue
		}
		for _, e := range d.events {
			t, known := eventTypes[e.Call]
			if !known {
				continue
			}
			var abs map[string]bool
			if e.Call == "Kline" {
				if klineAbsent == nil {
					out = append(out, Problem{"capabilities", name, "emits Kline but Topics lists no kline topic"})
					continue
				}
				abs = klineAbsent
			} else {
				var ok bool
				if abs, ok = caps[topicOf[e.Call]]; !ok {
					out = append(out, Problem{"capabilities", name, fmt.Sprintf("emits %s but Topics does not list %s", e.Call, topicOf[e.Call])})
					continue
				}
			}
			var v map[string]any
			_ = json.Unmarshal(e.Value, &v)
			count[e.Call]++
			if filled[e.Call] == nil {
				filled[e.Call] = map[string]bool{}
			}
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if f.Type.Kind() != reflect.Float64 && !(e.Call == "FundingRate" && f.Name == "IntervalS") {
					continue
				}
				present := v[f.Name] != nil && !(f.Name == "IntervalS" && v[f.Name] == float64(0))
				isOpt := slices.Contains(optional[e.Call], f.Name)
				switch {
				case !isOpt && !present:
					out = append(out, Problem{"capabilities", name, fmt.Sprintf("frame %d: %s.%s is required and absent", e.Frame, e.Call, f.Name)})
				case isOpt && abs[f.Name] && present:
					out = append(out, Problem{"capabilities", name, fmt.Sprintf("frame %d: %s.%s is declared absent and filled", e.Frame, e.Call, f.Name)})
				case isOpt && present:
					filled[e.Call][f.Name] = true
				}
			}
		}
	}
	for call, n := range count {
		if n == 0 {
			continue
		}
		abs := caps[topicOf[call]]
		if call == "Kline" {
			abs = klineAbsent
		}
		for _, f := range optional[call] {
			if !abs[f] && !filled[call][f] {
				out = append(out, Problem{"capabilities", call, fmt.Sprintf(
					"%s is never filled in %d events: declare it in Absent, or record a fixture that has it", f, n)})
			}
		}
	}
	return out
}

func callOf(t exchange.Topic) string {
	if strings.HasPrefix(string(t), exchange.KlinePrefix) {
		return "Kline"
	}
	for call, topic := range topicOf {
		if topic == t {
			return call
		}
	}
	return ""
}

// dialGolden is ws/private_dial.golden.json: what the private stream does before and at connecting,
// at Spec.Now with the test credentials.
type dialGolden struct {
	Prepare      *Request `json:"prepare,omitempty"`
	PrepareError string   `json:"prepare_error,omitempty"`
	URL          string   `json:"url,omitempty"`
	Frames       []string `json:"frames,omitempty"`
	DialError    string   `json:"dial_error,omitempty"`
	Refresh      *Request `json:"refresh,omitempty"`
	RefreshError string   `json:"refresh_error,omitempty"`
}

// DialToken is the token Dial gets in the check, standing for what Prepare's call returned.
const DialToken = "conformance-listen-token"

// checkDial: the private stream's Prepare, Dial and Refresh match their golden file. Signed login
// frames are thereby byte for byte stable.
func (r *runner) checkDial() []Problem {
	a := r.env.Account
	if a == nil {
		return nil
	}
	s, err := a.NewStream()
	if err != nil {
		return []Problem{{"private_dial", "NewStream", err.Error()}}
	}
	var g dialGolden
	now := r.spec.Now
	token := ""
	if c, err := s.Prepare(now); err != nil {
		g.PrepareError = Kind(err)
		r.saw(err.Error())
	} else if c != nil {
		r.sawRequest(&c.Req)
		g.Prepare, token = RequestOf(&c.Req), DialToken
	}
	if d, err := s.Dial(now, token); err != nil {
		g.DialError = Kind(err)
		r.saw(err.Error())
	} else if d != nil {
		g.URL = d.URL
		r.saw(d.URL)
		for _, f := range d.Frames {
			g.Frames = append(g.Frames, string(f))
			r.saw(string(f))
		}
	}
	if c, err := s.Refresh(now); err != nil {
		g.RefreshError = Kind(err)
		r.saw(err.Error())
	} else if c != nil {
		r.sawRequest(&c.Req)
		g.Refresh = RequestOf(&c.Req)
	}
	return r.golden("ws/private_dial.golden.json", indented(g))
}

// checkCredentials: no secret part of the test credentials appears in anything the adapter
// produced during the other checks: requests, frames, events, results, error messages.
func (r *runner) checkCredentials() []Problem {
	var out []Problem
	for _, secret := range secrets(r.creds) {
		for _, s := range r.seen {
			if strings.Contains(s, secret) {
				out = append(out, Problem{"credentials", "output", fmt.Sprintf("a secret appears in %q", trim(s))})
				break
			}
		}
	}
	return out
}

func trim(s string) string {
	if len(s) > 120 {
		return s[:120] + "…"
	}
	return s
}
