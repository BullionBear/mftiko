package conformance

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/BullionBear/mftiko/exchange"
)

// Event is one Sink or AccountSink call, copied while the call was in progress: what a well-behaved
// downstream keeps (D302).
type Event struct {
	Frame int             `json:"frame"`        // index of the ws.in record it came from
	Call  string          `json:"call"`         // the Sink method
	Ch    string          `json:"ch,omitempty"` // the channel, for market events
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"` // Decode's error for this frame, as a kind
}

// Normalize turns v into plain JSON values: structs into objects of their exported fields, NaN
// (decimal.Absent) into null, ±Inf into "+Inf"/"-Inf", errors into their Kind. A time.Time is UTC
// RFC3339 with nanoseconds, and a zero time is left out. The result shares no memory with v.
func Normalize(v any) any { return normalize(reflect.ValueOf(v)) }

var timeType = reflect.TypeFor[time.Time]()

var errType = reflect.TypeFor[error]()

func normalize(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}
	if v.Type().Implements(errType) && v.Kind() != reflect.Struct {
		if v.IsNil() {
			return nil
		}
		return Kind(v.Interface().(error))
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return normalize(v.Elem())
	case reflect.Struct:
		if v.Type() == timeType {
			return v.Interface().(time.Time).UTC().Format(time.RFC3339Nano)
		}
		out := map[string]any{}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			fv := v.Field(i)
			if fv.Type() == timeType && fv.Interface().(time.Time).IsZero() {
				continue
			}
			out[f.Name] = normalize(fv)
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		fallthrough
	case reflect.Array:
		out := make([]any, v.Len())
		for i := range out {
			out[i] = normalize(v.Index(i))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		out := map[string]any{}
		for _, k := range v.MapKeys() {
			out[fmt.Sprint(k.Interface())] = normalize(v.MapIndex(k))
		}
		return out
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		switch {
		case math.IsNaN(f):
			return nil
		case math.IsInf(f, 1):
			return "+Inf"
		case math.IsInf(f, -1):
			return "-Inf"
		}
		return f
	case reflect.String:
		return v.String()
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	case reflect.Func:
		if v.IsNil() {
			return nil
		}
		return "func"
	default:
		return fmt.Sprintf("<%s>", v.Kind())
	}
}

// Encode is Normalize, marshalled. Map keys come out sorted, so equal values encode equal.
func Encode(v any) json.RawMessage {
	b, err := marshal(Normalize(v), false)
	if err != nil {
		panic(fmt.Sprintf("conformance: encode %T: %v", v, err))
	}
	return b
}

// Kind classifies an error the way the platform acts on it (D302): reject <code> <native>,
// rate_limited, auth, unknown_outcome, unsupported, not_implemented, or error.
func Kind(err error) string {
	var rej *exchange.Reject
	var rl *exchange.RateLimited
	switch {
	case err == nil:
		return ""
	case errors.As(err, &rej):
		return fmt.Sprintf("reject %d %s", rej.Code, rej.Native)
	case errors.As(err, &rl):
		return "rate_limited"
	case errors.Is(err, exchange.ErrAuth):
		return "auth"
	case errors.Is(err, exchange.ErrUnknownOutcome):
		return "unknown_outcome"
	case errors.Is(err, exchange.ErrUnsupported):
		return "unsupported"
	case errors.Is(err, exchange.ErrNotImplemented):
		return "not_implemented"
	default:
		return "error"
	}
}

// marketSink records a market session. The copies are made inside each call.
type marketSink struct {
	frame  int
	events []Event
	books  []bookEvent // typed copies of every Book call, for the book checks
}

type bookEvent struct {
	frame int
	ch    string
	u     exchange.BookUpdate
}

var _ exchange.Sink = (*marketSink)(nil)

func (s *marketSink) add(call, ch string, v any) {
	s.events = append(s.events, Event{Frame: s.frame, Call: call, Ch: ch, Value: Encode(v)})
}

func (s *marketSink) Ack(id uint64, err error) {
	s.add("Ack", "", map[string]any{"id": id, "error": Kind(err)})
}
func (s *marketSink) BestQuote(ch string, e *exchange.BestQuote) { s.add("BestQuote", ch, e) }
func (s *marketSink) Trade(ch string, e *exchange.Trade)         { s.add("Trade", ch, e) }
func (s *marketSink) AggTrade(ch string, e *exchange.AggTrade)   { s.add("AggTrade", ch, e) }
func (s *marketSink) Book(ch string, e *exchange.BookUpdate) {
	s.add("Book", ch, e)
	u := *e
	u.Bids, u.Asks = slices.Clone(e.Bids), slices.Clone(e.Asks)
	s.books = append(s.books, bookEvent{frame: s.frame, ch: ch, u: u})
}
func (s *marketSink) Kline(ch string, e *exchange.Kline)               { s.add("Kline", ch, e) }
func (s *marketSink) Ticker(ch string, e *exchange.Ticker)             { s.add("Ticker", ch, e) }
func (s *marketSink) Liquidation(ch string, e *exchange.Liquidation)   { s.add("Liquidation", ch, e) }
func (s *marketSink) FundingRate(ch string, e *exchange.FundingRate)   { s.add("FundingRate", ch, e) }
func (s *marketSink) OpenInterest(ch string, e *exchange.OpenInterest) { s.add("OpenInterest", ch, e) }
func (s *marketSink) Greeks(ch string, e *exchange.Greeks)             { s.add("Greeks", ch, e) }
func (s *marketSink) MarkPrice(ch string, e *exchange.MarkPrice)       { s.add("MarkPrice", ch, e) }
func (s *marketSink) FeedEnd(ch string, e *exchange.FeedEnd)           { s.add("FeedEnd", ch, e) }

// accountSink records a private session.
type accountSink struct {
	frame  int
	events []Event
	fills  []exchange.Fill // typed copies, for the fill_id check (D601)
}

var _ exchange.AccountSink = (*accountSink)(nil)

func (s *accountSink) add(call string, v any) {
	s.events = append(s.events, Event{Frame: s.frame, Call: call, Value: Encode(v)})
}

func (s *accountSink) Ready()                          { s.add("Ready", nil) }
func (s *accountSink) Expired(reason string)           { s.add("Expired", map[string]any{"reason": reason}) }
func (s *accountSink) Order(e *exchange.OrderUpdate)   { s.add("Order", e) }
func (s *accountSink) Balances(e []exchange.Balance)   { s.add("Balances", e) }
func (s *accountSink) Positions(e []exchange.Position) { s.add("Positions", e) }
func (s *accountSink) Fill(e *exchange.Fill) {
	s.add("Fill", e)
	f := *e
	f.FillID, f.OrderID, f.ClientOrderID = clone(f.FillID), clone(f.OrderID), clone(f.ClientOrderID)
	f.Exch.ExchTicker = clone(f.Exch.ExchTicker)
	s.fills = append(s.fills, f)
}

func clone(s string) string { return string(append([]byte(nil), s...)) }

// tradeSink records a trade session: the answers, and the private events when the connection
// carries them.
type tradeSink struct {
	accountSink
}

var _ exchange.TradeSink = (*tradeSink)(nil)

func (s *tradeSink) Submitted(id uint64, res *exchange.OrderResult, err error) {
	s.add("Submitted", map[string]any{"id": id, "result": Normalize(res), "error": Kind(err)})
}

func (s *tradeSink) Canceled(id uint64, err error) {
	s.add("Canceled", map[string]any{"id": id, "error": Kind(err)})
}

func (s *tradeSink) Used(u []exchange.Used) { s.add("Used", u) }
