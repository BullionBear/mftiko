package conformance

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/BullionBear/mftiko/exchange"
)

// tradeDialGolden is ws/trade_dial.golden.json: what the TradeStream sends, at Spec.Now with the test
// credentials: the logon (and the subscription, on a connection that carries events), and the frame
// of each TradeSample under ids 1, 2, ...
type tradeDialGolden struct {
	URL       string         `json:"url,omitempty"`
	Frames    []string       `json:"frames,omitempty"`
	DialError string         `json:"dial_error,omitempty"`
	Events    bool           `json:"events"`
	Samples   []sampleGolden `json:"samples,omitempty"`
}

type sampleGolden struct {
	Name  string `json:"name"`
	ID    uint64 `json:"id"`
	Frame string `json:"frame,omitempty"`
	Error string `json:"error,omitempty"`
}

// tradeStream is the account's TradeStream; nil, nil when it has none (ErrUnsupported).
func (r *runner) tradeStream() (exchange.TradeStream, error) {
	if r.env.Account == nil {
		return nil, nil
	}
	ts, err := r.env.Account.NewTradeStream()
	if errors.Is(err, exchange.ErrUnsupported) {
		return nil, nil
	}
	return ts, err
}

// checkTradeDial: the TradeStream's logon and request frames match their golden file (E03-D003), so
// what is signed, and how, is pinned byte for byte.
func (r *runner) checkTradeDial() []Problem {
	ts, err := r.tradeStream()
	if err != nil {
		return []Problem{{"trade_dial", "NewTradeStream", err.Error()}}
	}
	if ts == nil {
		if len(r.spec.TradeSamples) > 0 {
			return []Problem{{"trade_dial", "TradeSamples", "samples given, and the account has no TradeStream"}}
		}
		return nil
	}
	now := r.spec.Now
	g := tradeDialGolden{Events: ts.Limits().Events}
	if d, err := ts.Dial(now); err != nil {
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
	var out []Problem
	if len(r.spec.TradeSamples) == 0 {
		out = append(out, Problem{"trade_dial", "TradeSamples", "no samples: give at least a Submit and a Cancel"})
	}
	for i, s := range r.spec.TradeSamples {
		id := uint64(i + 1)
		sg := sampleGolden{Name: s.Name, ID: id}
		var f []byte
		var err error
		switch {
		case s.Submit != nil && s.Cancel == nil:
			f, err = ts.Submit(now, id, s.Submit)
		case s.Cancel != nil && s.Submit == nil:
			f, err = ts.Cancel(now, id, *s.Cancel)
		default:
			out = append(out, Problem{"trade_dial", s.Name, "a sample is a Submit or a Cancel"})
			continue
		}
		if err != nil {
			sg.Error = Kind(err)
			r.saw(err.Error())
		} else {
			sg.Frame = string(f)
			r.saw(sg.Frame)
		}
		g.Samples = append(g.Samples, sg)
	}
	return append(out, r.golden("ws/trade_dial.golden.json", indented(g))...)
}

// checkTradeAnswers: in every trade session each request id is answered at most once, and every
// answer names an id the worker could have sent (at least 1). A codec that answers twice, or under
// an id of its own, would make the worker match an answer to the wrong order (E03-D003).
func (r *runner) checkTradeAnswers() []Problem {
	names, err := Sessions(r.fsys)
	if err != nil {
		return nil
	}
	var out []Problem
	for _, name := range names {
		d, err := r.decode(name)
		if err != nil || d.sess.Meta.Side != SideTrade {
			continue
		}
		answered := map[uint64]int{}
		for _, e := range d.events {
			if e.Call != "Submitted" && e.Call != "Canceled" {
				continue
			}
			var v struct {
				ID uint64 `json:"id"`
			}
			if err := json.Unmarshal(e.Value, &v); err != nil {
				out = append(out, Problem{"trade_answers", name, err.Error()})
				continue
			}
			if v.ID == 0 {
				out = append(out, Problem{"trade_answers", name, fmt.Sprintf("frame %d: an answer to id 0, which the worker never sends", e.Frame)})
			}
			if prev, ok := answered[v.ID]; ok {
				out = append(out, Problem{"trade_answers", name, fmt.Sprintf("frame %d: id %d answered again (first at frame %d)", e.Frame, v.ID, prev)})
				continue
			}
			answered[v.ID] = e.Frame
		}
	}
	return out
}
