package exchangetest

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
)

// TradeVenue is a fake venue for code that drives an exchange.TradeStream (D303): the account
// worker's request ids, deadlines and reconnects. It has two ends. Stream is the client's codec;
// Handle is the venue receiving what the client sent, and Release hands back the answers in the order
// the test chooses, with any it chose to Drop missing. Nothing goes over a network, unless the test
// serves Handler: then the codec dials it, and each request is answered as it arrives.
//
// The wire format is its own JSON. With events set, the connection also carries private events
// (TradeLimits.Events), as Binance spot's does after userDataStream.subscribe.
type TradeVenue struct {
	mu      sync.Mutex
	events  bool
	key     string
	pending []answer
	drop    map[uint64]bool
	reject  map[uint64]int // id -> the venue's refusal code
	refuse  bool           // refuse the next logon
	orders  int

	url        string // what the codec dials; Handler serves it
	conns      map[*streamConn]struct{}
	connsTotal int
}

type answer struct {
	id    uint64
	frame []byte
}

// NewTradeVenue returns a venue whose logon takes key. With events set, its connection carries
// private events.
func NewTradeVenue(key string, events bool) *TradeVenue {
	return &TradeVenue{key: key, events: events, drop: map[uint64]bool{}, reject: map[uint64]int{},
		url: "wss://trade.exchangetest.invalid/ws", conns: map[*streamConn]struct{}{}}
}

// Stream is a new client codec for one connection to this venue.
func (v *TradeVenue) Stream() exchange.TradeStream {
	return &tradeStream{v: v, key: v.key, events: v.events}
}

// Drop makes the answer to request id never come, queued or not.
func (v *TradeVenue) Drop(id uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.drop[id] = true
	kept := v.pending[:0]
	for _, a := range v.pending {
		if a.id != id || id == 0 {
			kept = append(kept, a)
		}
	}
	v.pending = kept
}

// Reject makes the venue refuse request id with code (-2010 is insufficient balance).
func (v *TradeVenue) Reject(id uint64, code int) { v.mu.Lock(); v.reject[id] = code; v.mu.Unlock() }

// RefuseLogon makes the next logon fail.
func (v *TradeVenue) RefuseLogon() { v.mu.Lock(); v.refuse = true; v.mu.Unlock() }

// clientFrame is what the codec sends.
type clientFrame struct {
	Op    string `json:"op"`
	ID    string `json:"id"`
	Key   string `json:"key,omitempty"`
	Ts    int64  `json:"ts,omitempty"`
	Sym   string `json:"sym,omitempty"`
	Side  string `json:"side,omitempty"`
	Px    string `json:"px,omitempty"`
	Qty   string `json:"qty,omitempty"`
	Cid   string `json:"cid,omitempty"`
	Order string `json:"order,omitempty"`
}

// venueFrame is what the venue sends.
type venueFrame struct {
	ID     string `json:"id,omitempty"`
	Status int    `json:"status,omitempty"`
	Order  string `json:"order,omitempty"`
	State  string `json:"state,omitempty"`
	Ts     int64  `json:"ts,omitempty"`
	Code   int    `json:"code,omitempty"`
	Msg    string `json:"msg,omitempty"`
	Used   int    `json:"used,omitempty"`
	Event  string `json:"event,omitempty"`
	Fid    string `json:"fid,omitempty"`
	Sym    string `json:"sym,omitempty"`
	Side   string `json:"side,omitempty"`
	Px     string `json:"px,omitempty"`
	Qty    string `json:"qty,omitempty"`
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Handle is the venue receiving one frame from the client. Answers queue until Release.
func (v *TradeVenue) Handle(frame []byte) error {
	var f clientFrame
	if err := json.Unmarshal(frame, &f); err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	switch f.Op {
	case "logon":
		st := 200
		if v.refuse || f.Key != v.key {
			st, v.refuse = 401, false
		}
		v.pending = append(v.pending, answer{0, mustJSON(venueFrame{ID: "logon", Status: st})})
	case "subscribe":
		v.pending = append(v.pending, answer{0, mustJSON(venueFrame{ID: "sub", Status: 200})})
	case "place", "cancel":
		_, n, err := wireID(f.ID)
		if err != nil {
			return err
		}
		if v.drop[n] {
			return nil
		}
		a := venueFrame{ID: f.ID, Status: 200, Used: len(v.pending) + 1}
		if code, ok := v.reject[n]; ok {
			a.Status, a.Code, a.Msg = 400, code, "refused"
		} else if f.Op == "place" {
			v.orders++
			a.Order, a.State, a.Ts = "o"+strconv.Itoa(v.orders), "NEW", f.Ts
		}
		v.pending = append(v.pending, answer{n, mustJSON(a)})
	default:
		return fmt.Errorf("exchangetest: op %q", f.Op)
	}
	return nil
}

// Release returns the queued answers. The answers to the ids given come first, in that order; the
// rest follow in the order the requests arrived.
func (v *TradeVenue) Release(first ...uint64) [][]byte {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out [][]byte
	for _, id := range first {
		for i, a := range v.pending {
			if a.id == id && id != 0 {
				out = append(out, a.frame)
				v.pending = append(v.pending[:i], v.pending[i+1:]...)
				break
			}
		}
	}
	for _, a := range v.pending {
		out = append(out, a.frame)
	}
	v.pending = nil
	return out
}

// Fill is a private event frame: a fill of order on this connection (events set).
func (v *TradeVenue) Fill(fillID, order, sym string, side exchange.Side, px, qty string) []byte {
	s := "buy"
	if side == exchange.SideSell {
		s = "sell"
	}
	return mustJSON(venueFrame{Event: "fill", Fid: fillID, Order: order, Sym: sym, Side: s, Px: px, Qty: qty})
}

type tradeStream struct {
	v      *TradeVenue
	key    string
	events bool
}

// wireID is what the codec puts on the wire for request id: the op and the worker's id, so that an
// answer reads without memory of what was sent (D303).
func wireID(s string) (op string, id uint64, err error) {
	if len(s) < 2 || (s[0] != 'p' && s[0] != 'c') {
		return "", 0, fmt.Errorf("exchangetest: request id %q", s)
	}
	id, err = strconv.ParseUint(s[1:], 10, 64)
	if err != nil || id == 0 {
		return "", 0, fmt.Errorf("exchangetest: request id %q", s)
	}
	return map[byte]string{'p': "place", 'c': "cancel"}[s[0]], id, nil
}

func (s *tradeStream) Dial(now time.Time) (*exchange.Dial, error) {
	frames := [][]byte{mustJSON(clientFrame{Op: "logon", ID: "logon", Key: s.key, Ts: now.UnixMilli()})}
	if s.events {
		frames = append(frames, mustJSON(clientFrame{Op: "subscribe", ID: "sub"}))
	}
	s.v.mu.Lock()
	u := s.v.url
	s.v.mu.Unlock()
	return &exchange.Dial{URL: u, Frames: frames}, nil
}

func (s *tradeStream) Limits() exchange.TradeLimits {
	return exchange.TradeLimits{Events: s.events, ReadLimit: 1 << 20}
}
func (s *tradeStream) Ping() []byte { return nil }

func (s *tradeStream) Submit(now time.Time, id uint64, req *exchange.OrderRequest) ([]byte, error) {
	if id == 0 {
		return nil, errors.New("exchangetest: request id 0")
	}
	side := "buy"
	if req.Side == exchange.SideSell {
		side = "sell"
	}
	f := clientFrame{Op: "place", ID: "p" + strconv.FormatUint(id, 10), Ts: now.UnixMilli(), Sym: req.Instrument.ExchTicker, Side: side,
		Px:  decimal.Format(req.Price, req.Instrument.Filters[symbol.PriceTick]),
		Qty: decimal.Format(req.Qty, req.Instrument.Filters[symbol.QtyStep]), Cid: req.ClientOrderID}
	return mustJSON(f), nil
}

func (s *tradeStream) Cancel(now time.Time, id uint64, ref exchange.CancelRef) ([]byte, error) {
	if id == 0 {
		return nil, errors.New("exchangetest: request id 0")
	}
	return mustJSON(clientFrame{Op: "cancel", ID: "c" + strconv.FormatUint(id, 10), Ts: now.UnixMilli(),
		Order: ref.OrderID, Cid: ref.ClientOrderID}), nil
}

func classify(f venueFrame) error {
	switch {
	case f.Status == 200:
		return nil
	case f.Status == 401:
		return exchange.ErrAuth
	case f.Status == 429:
		return &exchange.RateLimited{}
	default:
		return venueReject(f.Code, f.Msg)
	}
}

func (s *tradeStream) Decode(b []byte, sink exchange.TradeSink) error {
	var f venueFrame
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	if f.Used > 0 {
		sink.Used([]exchange.Used{{Bucket: "requests", N: f.Used}})
	}
	switch {
	case f.ID == "logon" || f.ID == "sub":
		switch {
		case f.Status == 401:
			return fmt.Errorf("exchangetest: logon refused: %w", exchange.ErrAuth)
		case f.ID == "sub", !s.events:
			sink.Ready()
		}
	case f.ID != "":
		op, n, err := wireID(f.ID)
		if err != nil {
			return err
		}
		err = classify(f)
		if op == "cancel" {
			sink.Canceled(n, err)
			return nil
		}
		if err != nil {
			sink.Submitted(n, nil, err)
			return nil
		}
		sink.Submitted(n, &exchange.OrderResult{OrderID: f.Order, Status: exchange.OrderStatusNew, ExchTsNs: f.Ts * 1e6}, nil)
	default:
		if f.Event != "fill" || !s.events {
			return errors.New("exchangetest: unknown frame")
		}
		px, err := decimal.Parse(f.Px)
		if err != nil {
			return err
		}
		qty, err := decimal.Parse(f.Qty)
		if err != nil {
			return err
		}
		side := exchange.SideBuy
		if f.Side == "sell" {
			side = exchange.SideSell
		}
		sink.Fill(&exchange.Fill{Exch: symbol.ExchKey{ExchTicker: f.Sym}, FillID: f.Fid, OrderID: f.Order,
			Side: side, Price: px, Qty: qty, Fee: decimal.Absent()})
	}
	return nil
}
