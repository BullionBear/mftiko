package conformance

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"strconv"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

// caseGolden is http/<name>.golden.json: the request as built at Spec.Now with the test credentials,
// and what Parse made of the recorded response.
type caseGolden struct {
	Request    *Request        `json:"request,omitempty"`
	BuildError string          `json:"build_error,omitempty"`
	Response   string          `json:"response"` // recorded | none
	Result     json.RawMessage `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
}

type caseRun struct {
	golden caseGolden
	result any
	probs  []Problem
}

// runCase builds the case's request and parses its recorded response, once.
func (r *runner) runCase(c Case) *caseRun {
	if v, ok := r.results[c.Name]; ok {
		return v.(*caseRun)
	}
	out := &caseRun{}
	r.results[c.Name] = out
	req, parse, err := c.build(r.env)
	if err != nil {
		out.golden.BuildError = Kind(err)
		r.saw(err.Error())
		if errors.Is(err, exchange.ErrNotImplemented) {
			out.probs = append(out.probs, Problem{"cases", c.Name, "the method is not implemented"})
		}
		out.golden.Response = "none"
		return out
	}
	r.sawRequest(&req)
	out.golden.Request = RequestOf(&req)
	ex, err := LoadExchange(r.fsys, c.Name)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		out.golden.Response = "none"
		return out
	case err != nil:
		out.probs = append(out.probs, Problem{"cases", c.Name, err.Error()})
		out.golden.Response = "none"
		return out
	}
	out.golden.Response = "recorded"
	res, perr := parse(ex.Response.HTTP())
	if perr != nil {
		out.golden.Error = Kind(perr)
		r.saw(perr.Error())
		return out
	}
	out.result = res
	out.golden.Result = Encode(res)
	r.saw(string(out.golden.Result))
	return out
}

func (r *runner) caseByName(name string) (Case, bool) {
	for _, c := range r.spec.Cases {
		if c.Name == name {
			return c, true
		}
	}
	return Case{}, false
}

// checkCases: each case's request and parse match its golden file. The request golden is the
// signed request byte for byte (fixed time, test credentials); the parse golden covers the error
// kinds as well, so a recorded refusal shows how it is classified.
func (r *runner) checkCases() []Problem {
	var out []Problem
	seen := map[string]bool{}
	for _, c := range r.spec.Cases {
		if c.Name == "" || seen[c.Name] {
			out = append(out, Problem{"cases", c.Name, "a case needs a unique name"})
			continue
		}
		seen[c.Name] = true
		run := r.runCase(c)
		out = append(out, run.probs...)
		out = append(out, r.golden("http/"+c.Name+".golden.json", indented(run.golden))...)
	}
	return out
}

// listings returns every listing the listing cases parsed, with the case it came from.
func (r *runner) listings() map[string][]symbol.Listing {
	out := map[string][]symbol.Listing{}
	for _, c := range r.spec.Cases {
		switch v := r.runCase(c).result.(type) {
		case exchange.Page[symbol.Listing]:
			out[c.Name] = v.Items
		case []symbol.Listing:
			out[c.Name] = v
		}
	}
	return out
}

// checkListing: every listing the venue returns is one SYM stores (D203): canonical asset codes,
// Validate, a ticker; and its tick and step survive float64 (D008).
func (r *runner) checkListing() []Problem {
	var out []Problem
	for name, ls := range r.listings() {
		for i, l := range ls {
			where := fmt.Sprintf("%s[%d] %s", name, i, l.ExchTicker)
			if l.Venue != "" && l.Venue != r.spec.Venue {
				out = append(out, Problem{"listing", where, fmt.Sprintf("venue %q, want %q", l.Venue, r.spec.Venue)})
			}
			c := canonical(l, r.spec.Venue)
			if err := c.Validate(); err != nil {
				out = append(out, Problem{"listing", where, "Validate: " + err.Error()})
				continue
			}
			if _, err := c.Ticker(); err != nil {
				out = append(out, Problem{"listing", where, "Ticker: " + err.Error()})
			}
			out = append(out, steps(where, c.Filters)...)
		}
	}
	return out
}

// canonical is l the way SYM sees it before Validate: asset codes canonical, venue filled.
func canonical(l symbol.Listing, venue string) symbol.Listing {
	if l.Venue == "" {
		l.Venue = venue
	}
	for _, p := range []*string{&l.Base, &l.Quote, &l.ContractValueCcy, &l.PriceCcy, &l.Settle} {
		if *p != "" {
			if c, err := symbol.CanonicalAsset(*p); err == nil {
				*p = c
			}
		}
	}
	return l
}

// steps checks that prices and quantities on this instrument fit float64 (D008): a value of the
// filter's largest magnitude, written at the step's decimals, has at most 15 significant digits,
// so it comes back from text unchanged.
func steps(where string, f symbol.Filters) []Problem {
	var out []Problem
	for _, pair := range [][2]string{{symbol.PriceTick, symbol.MaxPrice}, {symbol.QtyStep, symbol.MaxQty}} {
		step, ok := f[pair[0]]
		if !ok || !decimal.Has(step) {
			continue
		}
		if !(step > 0) || math.IsInf(step, 0) {
			out = append(out, Problem{"listing", where, fmt.Sprintf("%s is %v", pair[0], step)})
			continue
		}
		top := 1e7 // when the venue publishes no upper bound: ten million
		if m, ok := f[pair[1]]; ok && decimal.Has(m) && m > 0 {
			top = m
		}
		digits := len(strconv.FormatFloat(math.Floor(top), 'f', 0, 64)) + decimal.Places(step)
		if digits > 15 {
			out = append(out, Problem{"listing", where, fmt.Sprintf(
				"%s %v up to %v needs %d significant digits; float64 holds 15 (D008)", pair[0], step, top, digits)})
			continue
		}
		v := decimal.Snap(top, step)
		if back, err := decimal.Parse(decimal.Format(v, step)); err != nil || decimal.Format(back, step) != decimal.Format(v, step) {
			out = append(out, Problem{"listing", where, fmt.Sprintf("%v at %s %v does not survive text", v, pair[0], step)})
		}
	}
	return out
}

// instruments are Spec.Instruments and the valid listings of the listing cases, as SYM would serve
// them (UIDs are made up here).
func (r *runner) instruments() []*symbol.Info {
	if r.insts != nil {
		return r.insts
	}
	out := append([]*symbol.Info(nil), r.spec.Instruments...)
	uid := uint32(1_000_000)
	for _, ls := range r.listings() {
		for _, l := range ls {
			c := canonical(l, r.spec.Venue)
			if c.Validate() != nil {
				continue
			}
			t, err := c.Ticker()
			if err != nil {
				continue
			}
			uid++
			out = append(out, &symbol.Info{
				UID: uid, Ticker: t, ExchTicker: c.ExchTicker, Base: c.Base, Quote: c.Quote,
				ContractSize: c.ContractSize, ContractValueCcy: c.ContractValueCcy, PriceCcy: c.PriceCcy,
				Settle: c.Settle, ExpiryNs: c.ExpiryNs, Strike: c.Strike, OptionType: c.OptionType,
				Status: c.Status, Filters: c.Filters,
			})
		}
	}
	r.insts = out
	return out
}

// checkFills: a fill on the private stream is in History with the same fill_id, order and amounts
// (D601: the database deduplicates on the fill_id, so the two must agree).
func (r *runner) checkFills() []Problem {
	var out []Problem
	for _, p := range r.spec.FillPairs {
		where := p.Session + " ~ " + p.Case
		d, err := r.decode(p.Session)
		if err != nil {
			out = append(out, Problem{"fills", where, err.Error()})
			continue
		}
		c, ok := r.caseByName(p.Case)
		if !ok {
			out = append(out, Problem{"fills", where, "no such case"})
			continue
		}
		var hist []exchange.Fill
		switch v := r.runCase(c).result.(type) {
		case *exchange.FillPage:
			hist = v.Fills
		case exchange.Page[exchange.Fill]:
			hist = v.Items
		default:
			out = append(out, Problem{"fills", where, fmt.Sprintf("the case returns %T, not fills", v)})
			continue
		}
		byID := map[string]exchange.Fill{}
		for _, f := range hist {
			byID[f.FillID] = f
		}
		if len(d.acct.fills) == 0 {
			out = append(out, Problem{"fills", where, "the session has no fills"})
		}
		for _, f := range d.acct.fills {
			h, ok := byID[f.FillID]
			switch {
			case !ok:
				out = append(out, Problem{"fills", where, fmt.Sprintf("fill %q is on the stream but not in History", f.FillID)})
			case h.OrderID != f.OrderID || h.Price != f.Price || h.Qty != f.Qty || h.Side != f.Side:
				out = append(out, Problem{"fills", where, fmt.Sprintf("fill %q differs: stream %s, History %s",
					f.FillID, Encode(f), Encode(h))})
			}
		}
	}
	return out
}
