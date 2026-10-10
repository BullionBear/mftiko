package conformance_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

// fakex is a made-up venue with the shape of a real one: a JSON websocket codec with acks, trades,
// tickers and a checksummed book; HMAC-signed REST; a private stream with a login frame. Its
// faults are switches, so the tests can show that each check catches the fault it is for.
type faults struct {
	keepFrame   bool // the codec keeps a string pointing into the last frame (D302)
	dropUnknown bool // Decode ignores frames it does not know
	leakSecret  bool // the order request carries the secret in a header
	bigChannel  bool // Cost exceeds Budget
	fillTicker  bool // the ticker fills ChangePct24h, which Topics declares absent
	blindBook   bool // BookSync applies every update, gaps included
	otherFillID bool // History returns another fill id than the stream
	authAs500   bool // a 401 parses as a plain error, not ErrAuth
	answerTwice bool // the trade codec reports every order answer twice
}

const venue = "Fakex"

func fakeSpec(f faults) conformance.Spec {
	m := &market{f: f}
	return conformance.Spec{
		Venue:  venue,
		Dir:    "testdata/fakex",
		Market: m,
		REST:   rest{f: f},
		Account: func(cfg exchange.AccountConfig) (exchange.Account, error) {
			return &account{c: cfg.Credentials, f: f}, nil
		},
		History: func(cfg exchange.AccountConfig) (exchange.History, error) {
			return history{c: cfg.Credentials, f: f}, nil
		},
		Cases: []conformance.Case{
			conformance.Call("listing_perp", func(e conformance.Env) (exchange.Call[exchange.Page[symbol.Listing]], error) {
				return e.REST.Listing(symbol.Perp)
			}),
			conformance.Call("submit_ok", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, order())
			}),
			conformance.Call("submit_reject", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, order())
			}),
			conformance.Call("submit_auth", func(e conformance.Env) (exchange.Call[*exchange.OrderResult], error) {
				return e.Account.Submit(e.Now, order())
			}),
			conformance.Call("open_interest", func(e conformance.Env) (exchange.Call[*exchange.OpenInterest], error) {
				return e.REST.OpenInterest(nil)
			}),
			conformance.Call("fills", func(e conformance.Env) (exchange.Call[*exchange.FillPage], error) {
				return e.History.Fills(e.Now, exchange.FillQuery{FromID: "f0", Limit: 100})
			}),
		},
		FillPairs: []conformance.FillPair{{Session: "private", Case: "fills"}, {Session: "trade", Case: "fills"}},
		TradeSamples: []conformance.TradeSample{
			{Name: "limit buy", Submit: order()},
			{Name: "cancel by client id", Cancel: &exchange.CancelRef{Instrument: order().Instrument, ClientOrderID: "1234567"}},
		},
	}
}

func order() *exchange.OrderRequest {
	return &exchange.OrderRequest{
		Instrument: &symbol.Info{ExchTicker: "BTCUSDT", Filters: symbol.Filters{symbol.PriceTick: 0.5, symbol.QtyStep: 0.001}},
		Side:       exchange.SideBuy, Type: exchange.OrderTypeLimit, TIF: exchange.TIFGTC,
		Qty: 0.25, QuoteQty: decimal.Absent(), Price: 65000.5, ClientOrderID: "1234567",
	}
}

func sign(secret string, msg string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(msg))
	return hex.EncodeToString(h.Sum(nil))
}

func num(s string) (float64, error) { return decimal.Parse(s) }

// --- market ---

type market struct{ f faults }

func (m *market) Topics() []exchange.TopicCaps {
	return []exchange.TopicCaps{
		{Topic: exchange.TopicTrade},
		{Topic: exchange.TopicOrderBook},
		{Topic: exchange.TopicTicker, Absent: []string{"ChangePct24h", "QuoteVolume24h"}},
	}
}

func (m *market) Atom(t exchange.Topic, inst *symbol.Info) (exchange.AtomRef, error) {
	prefix := map[exchange.Topic]string{exchange.TopicTrade: "trade:", exchange.TopicOrderBook: "book:", exchange.TopicTicker: "ticker:"}[t]
	if prefix == "" || inst.Ticker.Category != symbol.Perp {
		return exchange.AtomRef{}, exchange.ErrUnsupported
	}
	return exchange.AtomRef{Route: "public", Channel: prefix + inst.ExchTicker}, nil
}

func (m *market) Routes() []string  { return []string{"public"} }
func (m *market) Budget(string) int { return 10 }
func (m *market) Cost(ch string) int {
	if m.f.bigChannel {
		return 11
	}
	return 1
}

func (m *market) Endpoint(route, override string) (string, error) {
	host := "wss://ws.fakex.example"
	if override != "" {
		host = override
	}
	return host + "/" + route, nil
}

func (m *market) NewSession(string) (exchange.Session, error) { return &session{f: m.f}, nil }

func (m *market) Book(ch string) (exchange.BookSync, bool) {
	if !strings.HasPrefix(ch, "book:") {
		return nil, false
	}
	return bookSync{f: m.f}, true
}

type session struct {
	f    faults
	next uint64
	last string // with keepFrame: the previous trade id, pointing into its frame
}

func (s *session) Limits() exchange.SessionLimits { return exchange.SessionLimits{PerRequest: 2} }
func (s *session) Ping() []byte                   { return nil }

func (s *session) requests(op string, chs []string) ([]exchange.Request, error) {
	var out []exchange.Request
	for i := 0; i < len(chs); i += 2 {
		part := chs[i:min(i+2, len(chs))]
		s.next++
		b, _ := json.Marshal(map[string]any{"op": op, "id": s.next, "args": part})
		out = append(out, exchange.Request{ID: s.next, Channels: part, Frame: b})
	}
	return out, nil
}

func (s *session) Subscribe(chs []string) ([]exchange.Request, error) { return s.requests("sub", chs) }
func (s *session) Unsubscribe(chs []string) ([]exchange.Request, error) {
	return s.requests("unsub", chs)
}

type frame struct {
	Op   string      `json:"op"`
	ID   uint64      `json:"id"`
	Ch   string      `json:"ch"`
	Type string      `json:"type"`
	Ts   int64       `json:"ts"`
	Tid  string      `json:"tid"`
	Px   string      `json:"px"`
	Sz   string      `json:"sz"`
	Side string      `json:"side"`
	Seq  uint64      `json:"seq"`
	Prev uint64      `json:"prev"`
	Cs   int64       `json:"cs"`
	Bids [][2]string `json:"bids"`
	Asks [][2]string `json:"asks"`
	Last string      `json:"last"`
	Open string      `json:"open"`
	High string      `json:"high"`
	Low  string      `json:"low"`
	Vol  string      `json:"vol"`
	Chg  string      `json:"chg"`
}

func levels(raw [][2]string) ([]exchange.Level, error) {
	out := make([]exchange.Level, 0, len(raw))
	for _, l := range raw {
		p, err := num(l[0])
		if err != nil {
			return nil, err
		}
		q, err := num(l[1])
		if err != nil {
			return nil, err
		}
		out = append(out, exchange.Level{Price: p, Qty: q})
	}
	return out, nil
}

func (s *session) Decode(b []byte, sink exchange.Sink) error {
	var f frame
	if err := json.Unmarshal(b, &f); err != nil {
		if s.f.dropUnknown {
			return nil
		}
		return err
	}
	switch {
	case f.Op == "ack":
		sink.Ack(f.ID, nil)
	case strings.HasPrefix(f.Ch, "trade:"):
		px, err := num(f.Px)
		if err != nil {
			return err
		}
		sz, err := num(f.Sz)
		if err != nil {
			return err
		}
		side := exchange.SideBuy
		if f.Side == "sell" {
			side = exchange.SideSell
		}
		id := f.Tid
		if s.f.keepFrame {
			// The fault: a string that aliases the frame, kept until the next one.
			i := strings.Index(string(b), `"tid":"`) + len(`"tid":"`)
			prev := s.last
			s.last = unsafe.String(&b[i], len(f.Tid))
			if prev != "" {
				id = prev
			}
		}
		sink.Trade(f.Ch, &exchange.Trade{ExchTsNs: f.Ts * 1e6, TradeID: id, Price: px, Qty: sz, Side: side})
	case strings.HasPrefix(f.Ch, "book:"):
		bids, err := levels(f.Bids)
		if err != nil {
			return err
		}
		asks, err := levels(f.Asks)
		if err != nil {
			return err
		}
		sink.Book(f.Ch, &exchange.BookUpdate{ExchTsNs: f.Ts * 1e6, Bids: bids, Asks: asks,
			Snapshot: f.Type == "snapshot", Last: f.Seq, Prev: f.Prev, Checksum: f.Cs})
	case strings.HasPrefix(f.Ch, "ticker:"):
		t := &exchange.Ticker{ExchTsNs: f.Ts * 1e6, ChangePct24h: decimal.Absent(), QuoteVolume24h: decimal.Absent()}
		for _, p := range []struct {
			dst *float64
			s   string
		}{{&t.Last, f.Last}, {&t.Open24h, f.Open}, {&t.High24h, f.High}, {&t.Low24h, f.Low}, {&t.Volume24h, f.Vol}} {
			v, err := num(p.s)
			if err != nil {
				return err
			}
			*p.dst = v
		}
		if s.f.fillTicker {
			t.ChangePct24h = 1.5
		}
		sink.Ticker(f.Ch, t)
	default:
		if s.f.dropUnknown {
			return nil
		}
		return fmt.Errorf("fakex: unknown frame")
	}
	return nil
}

// The checksum is the sum of price*qty over the top three levels of each side: integers in the
// fixtures, so exact.
func checksum(bids, asks []exchange.Level) int64 {
	var s float64
	for i := 0; i < 3 && i < len(bids); i++ {
		s += bids[i].Price * bids[i].Qty
	}
	for i := 0; i < 3 && i < len(asks); i++ {
		s += asks[i].Price * asks[i].Qty
	}
	return int64(s)
}

type bookSync struct{ f faults }

func (b bookSync) Snapshot() exchange.SnapshotSource { return exchange.SnapshotStream }
func (b bookSync) Depth() int                        { return 5 }
func (b bookSync) Check(last exchange.BookSeq, u *exchange.BookUpdate) exchange.BookVerdict {
	if b.f.blindBook {
		return exchange.BookApply
	}
	if !last.Valid || u.Prev != last.Last {
		return exchange.BookResync
	}
	return exchange.BookApply
}
func (b bookSync) Verify(bids, asks []exchange.Level, u *exchange.BookUpdate) bool {
	return b.f.blindBook || checksum(bids, asks) == u.Checksum
}

// --- public REST ---

type rest struct{ f faults }

const base = "https://api.fakex.example"

func (r rest) Listing(c symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	return r.listingPage(c, "")
}

func (r rest) listingPage(c symbol.Category, cursor string) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	q := url.Values{"cat": {string(c)}}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	return exchange.Call[exchange.Page[symbol.Listing]]{
		Req: exchange.HTTPRequest{Method: "GET", Base: base, Path: "/v1/instruments", Query: []byte(q.Encode()),
			Cost: []exchange.Cost{{Bucket: "weight", N: 5}}},
		Parse: func(resp *exchange.HTTPResponse) (exchange.Page[symbol.Listing], error) {
			var body struct {
				Next string `json:"next"`
				List []struct {
					Sym, Base, Quote, Tick, Step, Status, Ct string
				} `json:"list"`
			}
			if err := status(resp, r.f); err != nil {
				return exchange.Page[symbol.Listing]{}, err
			}
			if err := json.Unmarshal(resp.Body, &body); err != nil {
				return exchange.Page[symbol.Listing]{}, err
			}
			var p exchange.Page[symbol.Listing]
			for _, x := range body.List {
				tick, err := num(x.Tick)
				if err != nil {
					return p, err
				}
				step, err := num(x.Step)
				if err != nil {
					return p, err
				}
				ct, err := num(x.Ct)
				if err != nil {
					return p, err
				}
				st := symbol.Unknown
				if x.Status == "live" {
					st = symbol.Tradable
				}
				p.Items = append(p.Items, symbol.Listing{Venue: venue, Category: c, Base: x.Base, Quote: x.Quote,
					ExchTicker: x.Sym, ContractSize: ct, ContractValueCcy: x.Base, PriceCcy: x.Quote, Settle: x.Quote,
					Status: st, Filters: symbol.Filters{symbol.PriceTick: tick, symbol.QtyStep: step, symbol.MaxQty: decimal.Absent()}})
			}
			if body.Next != "" {
				next := body.Next
				p.Next = func(time.Time) (exchange.Call[exchange.Page[symbol.Listing]], error) { return r.listingPage(c, next) }
			}
			return p, nil
		},
	}, nil
}

func (r rest) Depth(inst *symbol.Info, limit int) (exchange.Call[*exchange.BookUpdate], error) {
	q := url.Values{"sym": {inst.ExchTicker}, "limit": {strconv.Itoa(limit)}}
	return exchange.Call[*exchange.BookUpdate]{
		Req: exchange.HTTPRequest{Method: "GET", Base: base, Path: "/v1/depth", Query: []byte(q.Encode())},
		Parse: func(resp *exchange.HTTPResponse) (*exchange.BookUpdate, error) {
			var f frame
			if err := json.Unmarshal(resp.Body, &f); err != nil {
				return nil, err
			}
			bids, err := levels(f.Bids)
			if err != nil {
				return nil, err
			}
			asks, err := levels(f.Asks)
			if err != nil {
				return nil, err
			}
			return &exchange.BookUpdate{Bids: bids, Asks: asks, Snapshot: true, Last: f.Seq}, nil
		},
	}, nil
}

func (rest) Klines(*symbol.Info, string, int64, int) (exchange.Call[[]exchange.Kline], error) {
	return exchange.Call[[]exchange.Kline]{}, exchange.ErrUnsupported
}
func (rest) KlinePage() int { return 0 }
func (rest) OpenInterest(*symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	return exchange.Call[*exchange.OpenInterest]{}, exchange.ErrUnsupported
}
func (rest) Usage(resp *exchange.HTTPResponse) []exchange.Used {
	if n, err := strconv.Atoi(resp.Get("X-Used-Weight")); err == nil {
		return []exchange.Used{{Bucket: "weight", N: n}}
	}
	return nil
}

// status classifies an error answer (D302).
func status(resp *exchange.HTTPResponse, f faults) error {
	switch {
	case resp.Status == 200:
		return nil
	case resp.Status == 401 && !f.authAs500:
		return exchange.ErrAuth
	case resp.Status == 429:
		return &exchange.RateLimited{RetryAfterMs: 1000}
	case resp.Status == 400:
		var e struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		}
		if err := json.Unmarshal(resp.Body, &e); err != nil {
			return err
		}
		code := int32(e.Code)
		if e.Code == -2010 {
			code = 201 // insufficient balance, in the platform's table
		}
		return &exchange.Reject{Code: code, Native: strconv.Itoa(e.Code), Reason: e.Msg}
	default:
		return fmt.Errorf("fakex: status %d", resp.Status)
	}
}

// --- account ---

type account struct {
	c exchange.Credentials
	f faults
}

func (a *account) Limits() exchange.AccountLimits {
	return exchange.AccountLimits{ClientOrderIDMaxLen: 32, CancelBatch: 1}
}

func (a *account) Submit(now time.Time, req *exchange.OrderRequest) (exchange.Call[*exchange.OrderResult], error) {
	px := req.Instrument.Filters[symbol.PriceTick]
	step := req.Instrument.Filters[symbol.QtyStep]
	body := fmt.Sprintf(`{"sym":%q,"side":%q,"px":%q,"qty":%q,"cid":%q,"ts":%d}`, req.Instrument.ExchTicker,
		map[exchange.Side]string{exchange.SideBuy: "buy", exchange.SideSell: "sell"}[req.Side],
		decimal.Format(req.Price, px), decimal.Format(req.Qty, step), req.ClientOrderID, now.UnixMilli())
	hdr := []exchange.Header{{Name: "X-Key", Value: a.c.Key}, {Name: "X-Sign", Value: sign(a.c.Secret, body)}}
	if a.f.leakSecret {
		hdr = append(hdr, exchange.Header{Name: "X-Debug", Value: a.c.Secret})
	}
	return exchange.Call[*exchange.OrderResult]{
		Req: exchange.HTTPRequest{Method: "POST", Base: base, Path: "/v1/order", Header: hdr, Body: []byte(body),
			Cost: []exchange.Cost{{Bucket: "orders", N: 1}}, Mutates: true},
		Parse: func(resp *exchange.HTTPResponse) (*exchange.OrderResult, error) {
			if err := status(resp, a.f); err != nil {
				return nil, err
			}
			var r struct {
				ID     string `json:"id"`
				Status string `json:"status"`
				Ts     int64  `json:"ts"`
			}
			if err := json.Unmarshal(resp.Body, &r); err != nil {
				return nil, err
			}
			st := exchange.OrderStatusUnknown
			if r.Status == "NEW" {
				st = exchange.OrderStatusNew
			}
			return &exchange.OrderResult{OrderID: r.ID, Status: st, ExchTsNs: r.Ts * 1e6}, nil
		},
	}, nil
}

func (a *account) Cancel(time.Time, exchange.CancelRef) (exchange.Call[struct{}], error) {
	return exchange.Call[struct{}]{}, exchange.ErrUnsupported
}
func (a *account) CancelBatch(time.Time, []exchange.CancelRef) (exchange.Call[[]error], error) {
	return exchange.Call[[]error]{}, exchange.ErrUnsupported
}
func (a *account) Snapshot(time.Time) ([]exchange.Call[exchange.Page[exchange.AccountSnapshot]], error) {
	return nil, exchange.ErrUnsupported
}
func (a *account) Leverage(time.Time, *symbol.Info) (exchange.Call[float64], error) {
	return exchange.Call[float64]{}, exchange.ErrUnsupported
}
func (a *account) NewStream() (exchange.PrivateStream, error) { return &stream{c: a.c}, nil }
func (a *account) NewTradeStream() (exchange.TradeStream, error) {
	return &trade{c: a.c, f: a.f}, nil
}
func (a *account) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }

type stream struct{ c exchange.Credentials }

func (s *stream) Prepare(time.Time) (*exchange.Call[string], error) { return nil, nil }
func (s *stream) Dial(now time.Time, _ string) (*exchange.Dial, error) {
	ts := strconv.FormatInt(now.UnixMilli(), 10)
	login := fmt.Sprintf(`{"op":"login","key":%q,"ts":%s,"sig":%q}`, s.c.Key, ts, sign(s.c.Secret, ts))
	return &exchange.Dial{URL: "wss://ws.fakex.example/private", Frames: [][]byte{[]byte(login)}}, nil
}
func (s *stream) Refresh(time.Time) (*exchange.Call[struct{}], error) { return nil, nil }
func (s *stream) Limits() exchange.StreamLimits                       { return exchange.StreamLimits{} }
func (s *stream) Ping() []byte                                        { return nil }

type privFrame struct {
	Op, E, ID, Oid, Px, Sz, Side, Sym string
	Ok                                bool
	Ts                                int64
}

func (s *stream) Decode(b []byte, sink exchange.AccountSink) error {
	var f privFrame
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	switch {
	case f.Op == "login" && f.Ok:
		sink.Ready()
	case f.E == "fill":
		fill, err := toFill(f)
		if err != nil {
			return err
		}
		sink.Fill(fill)
	default:
		return errors.New("fakex: unknown private frame")
	}
	return nil
}

func toFill(f privFrame) (*exchange.Fill, error) {
	px, err := num(f.Px)
	if err != nil {
		return nil, err
	}
	sz, err := num(f.Sz)
	if err != nil {
		return nil, err
	}
	side := exchange.SideBuy
	if f.Side == "sell" {
		side = exchange.SideSell
	}
	return &exchange.Fill{Exch: symbol.ExchKey{Venue: venue, Category: symbol.Perp, ExchTicker: f.Sym},
		FillID: f.ID, OrderID: f.Oid, Side: side, Price: px, Qty: sz, Fee: decimal.Absent(), Liquidity: "taker",
		ExchTsNs: f.Ts * 1e6}, nil
}

// --- history ---

type history struct {
	c exchange.Credentials
	f faults
}

func (h history) Traits() exchange.HistoryTraits {
	return exchange.HistoryTraits{ByID: true, PageLimit: 100}
}

func (h history) Fills(now time.Time, q exchange.FillQuery) (exchange.Call[*exchange.FillPage], error) {
	query := fmt.Sprintf("from=%s&limit=%d&ts=%d", q.FromID, q.Limit, now.UnixMilli())
	query += "&sig=" + sign(h.c.Secret, query)
	return exchange.Call[*exchange.FillPage]{
		Req: exchange.HTTPRequest{Method: "GET", Base: base, Path: "/v1/fills", Query: []byte(query),
			Header: []exchange.Header{{Name: "X-Key", Value: h.c.Key}}},
		Parse: func(resp *exchange.HTTPResponse) (*exchange.FillPage, error) {
			if err := status(resp, h.f); err != nil {
				return nil, err
			}
			var rows []privFrame
			if err := json.Unmarshal(resp.Body, &rows); err != nil {
				return nil, err
			}
			p := &exchange.FillPage{}
			for _, r := range rows {
				f, err := toFill(r)
				if err != nil {
					return nil, err
				}
				if h.f.otherFillID {
					f.FillID = "h-" + f.FillID
				}
				p.Fills = append(p.Fills, *f)
			}
			return p, nil
		},
	}, nil
}

func (h history) Order(time.Time, *symbol.Info, string) (exchange.Call[*exchange.OrderUpdate], error) {
	return exchange.Call[*exchange.OrderUpdate]{}, exchange.ErrUnsupported
}
func (h history) Usage(*exchange.HTTPResponse) []exchange.Used { return nil }

// --- trade stream (D303): a WebSocket API that also carries the account's events ---

type trade struct {
	c exchange.Credentials
	f faults
}

func (t *trade) Dial(now time.Time) (*exchange.Dial, error) {
	ts := strconv.FormatInt(now.UnixMilli(), 10)
	logon := fmt.Sprintf(`{"method":"session.logon","id":"logon","params":{"key":%q,"ts":%s,"sig":%q}}`, t.c.Key, ts, sign(t.c.Secret, ts))
	return &exchange.Dial{URL: "wss://ws-api.fakex.example/v1", Frames: [][]byte{
		[]byte(logon), []byte(`{"method":"events.subscribe","id":"sub"}`)}}, nil
}

func (t *trade) Limits() exchange.TradeLimits { return exchange.TradeLimits{Events: true} }
func (t *trade) Ping() []byte                 { return nil }

// The wire id carries the op, so an answer reads with no memory of what was sent (D303).
func (t *trade) Submit(now time.Time, id uint64, req *exchange.OrderRequest) ([]byte, error) {
	params := fmt.Sprintf(`"sym":%q,"side":%q,"px":%q,"qty":%q,"cid":%q,"ts":%d`, req.Instrument.ExchTicker,
		map[exchange.Side]string{exchange.SideBuy: "buy", exchange.SideSell: "sell"}[req.Side],
		decimal.Format(req.Price, req.Instrument.Filters[symbol.PriceTick]),
		decimal.Format(req.Qty, req.Instrument.Filters[symbol.QtyStep]), req.ClientOrderID, now.UnixMilli())
	return []byte(fmt.Sprintf(`{"method":"order.place","id":"o%d","params":{%s,"sig":%q}}`, id, params, sign(t.c.Secret, params))), nil
}

func (t *trade) Cancel(now time.Time, id uint64, ref exchange.CancelRef) ([]byte, error) {
	params := fmt.Sprintf(`"sym":%q,"cid":%q,"ts":%d`, ref.Instrument.ExchTicker, ref.ClientOrderID, now.UnixMilli())
	return []byte(fmt.Sprintf(`{"method":"order.cancel","id":"c%d","params":{%s,"sig":%q}}`, id, params, sign(t.c.Secret, params))), nil
}

type tradeAnswer struct {
	ID     string `json:"id"`
	Status int    `json:"status"`
	Result struct {
		Order string `json:"order"`
		State string `json:"state"`
		Ts    int64  `json:"ts"`
	} `json:"result"`
	Error struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	} `json:"error"`
	Used  int    `json:"used"`
	Event string `json:"event"`
}

func (t *trade) Decode(b []byte, sink exchange.TradeSink) error {
	var a tradeAnswer
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	var err error
	if a.Status >= 400 {
		body, _ := json.Marshal(a.Error)
		err = status(&exchange.HTTPResponse{Status: a.Status, Body: body}, t.f)
	}
	switch {
	case a.ID == "logon":
		if err != nil {
			return err
		}
	case a.ID == "sub":
		sink.Ready()
	case len(a.ID) > 1 && (a.ID[0] == 'o' || a.ID[0] == 'c'):
		id, perr := strconv.ParseUint(a.ID[1:], 10, 64)
		if perr != nil {
			return perr
		}
		if a.Used > 0 {
			sink.Used([]exchange.Used{{Bucket: "weight", N: a.Used}})
		}
		if a.ID[0] == 'c' {
			sink.Canceled(id, err)
			return nil
		}
		var res *exchange.OrderResult
		if err == nil {
			res = &exchange.OrderResult{OrderID: a.Result.Order, Status: exchange.OrderStatusNew, ExchTsNs: a.Result.Ts * 1e6}
		}
		sink.Submitted(id, res, err)
		if t.f.answerTwice {
			sink.Submitted(id, res, err)
		}
	case a.Event == "fill":
		var f privFrame
		if err := json.Unmarshal(b, &f); err != nil {
			return err
		}
		fill, err := toFill(f)
		if err != nil {
			return err
		}
		sink.Fill(fill)
	default:
		return errors.New("fakex: unknown trade frame")
	}
	return nil
}
