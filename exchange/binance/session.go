package binance

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

type subFrame struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     uint64   `json:"id"`
}

func (s *session) Subscribe(channels []string) ([]exchange.Request, error) {
	return s.requests("SUBSCRIBE", channels)
}

func (s *session) Unsubscribe(channels []string) ([]exchange.Request, error) {
	return s.requests("UNSUBSCRIBE", channels)
}

func (s *session) requests(method string, channels []string) ([]exchange.Request, error) {
	if len(channels) == 0 {
		return nil, nil
	}
	out := make([]exchange.Request, 0, 1)
	for _, page := range pageChannels(channels, s.per) {
		s.n++
		frame, err := codec.Marshal(subFrame{Method: method, Params: page, ID: s.n})
		if err != nil {
			return nil, err
		}
		out = append(out, exchange.Request{ID: s.n, Channels: page, Frame: frame})
	}
	return out, nil
}

func pageChannels(chs []string, n int) [][]string {
	if len(chs) == 0 {
		return nil
	}
	if n <= 0 || len(chs) <= n {
		return [][]string{chs}
	}
	out := make([][]string, 0, (len(chs)+n-1)/n)
	for len(chs) > 0 {
		if len(chs) < n {
			n = len(chs)
		}
		out = append(out, chs[:n:n])
		chs = chs[n:]
	}
	return out
}

// Decode reads one frame. A combined-stream wrapper ({"stream","data"}) and a raw event
// are both accepted. An all-market array is one event per element. st other than 1 is a
// CM row and is dropped; a missing st is kept. m on a trade is "the buyer is the maker",
// so the taker side is the other one.
func (s *session) Decode(frame []byte, sink exchange.Sink) error {
	if len(frame) == 0 || sink == nil {
		return errors.New("binance: empty frame")
	}
	if frame[0] == '[' {
		return emitAll(frame, "", sink)
	}
	var h struct {
		Stream string          `json:"stream"`
		ID     *int64          `json:"id"`
		Err    *subErr         `json:"error"`
		E      string          `json:"e"`
		Data   json.RawMessage `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &h); err != nil {
		return err
	}
	if h.Stream == "" && h.E == "" {
		if h.ID == nil {
			// Spot bookTicker has no event name. An ack always carries id.
			if spotBookTicker(frame) {
				return emitBookTicker(frame, "", sink)
			}
			return errors.New("binance: unrecognized frame")
		}
		var err error
		if h.Err != nil {
			err = fmt.Errorf("binance: %d: %s", h.Err.Code, h.Err.Msg)
		}
		sink.Ack(uint64(*h.ID), err)
		return nil
	}
	payload := json.RawMessage(frame)
	stream := h.Stream
	if stream != "" {
		if len(h.Data) == 0 {
			return errors.New("binance: stream frame has no data")
		}
		payload = h.Data
	}
	if len(payload) > 0 && payload[0] == '[' {
		return emitAll(payload, stream, sink)
	}
	return emitOne(payload, stream, sink)
}

type subErr struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func emitAll(frame []byte, stream string, sink exchange.Sink) error {
	var rows []json.RawMessage
	if err := codec.DecodeFrame(frame, &rows); err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.New("binance: empty event array")
	}
	for _, row := range rows {
		if err := knownEvent(row); err != nil {
			return err
		}
	}
	for _, row := range rows {
		if err := emitOne(row, stream, sink); err != nil {
			return err
		}
	}
	return nil
}

func knownEvent(frame []byte) error {
	var m struct {
		E string `json:"e"`
	}
	if err := codec.DecodeFrame(frame, &m); err != nil {
		return err
	}
	if m.E == "" && spotBookTicker(frame) {
		return nil
	}
	switch m.E {
	case "bookTicker", "depthUpdate", "aggTrade", "kline", "24hrTicker", "24hrMiniTicker", "markPriceUpdate", "forceOrder":
		return nil
	default:
		return fmt.Errorf("binance: unknown event %q", m.E)
	}
}

type eventMeta struct {
	E  string `json:"e"`
	S  string `json:"s"`
	St *int   `json:"st"`
}

// keep reports whether this row belongs to UM. st 1 is UM, st 2 is CM.
func keep(st *int) bool { return st == nil || *st == 1 }

func emitOne(frame []byte, stream string, sink exchange.Sink) error {
	var m eventMeta
	if err := codec.DecodeFrame(frame, &m); err != nil {
		return err
	}
	if err := knownEvent(frame); err != nil {
		return err
	}
	if !keep(m.St) {
		return nil
	}
	switch m.E {
	case "", "bookTicker":
		return emitBookTicker(frame, stream, sink)
	case "depthUpdate":
		return emitDepth(frame, stream, sink)
	case "aggTrade":
		return emitAggTrade(frame, stream, sink)
	case "kline":
		return emitKline(frame, stream, sink)
	case "24hrTicker", "24hrMiniTicker":
		return emitTicker(frame, stream, sink)
	case "markPriceUpdate":
		return emitMark(frame, stream, sink)
	case "forceOrder":
		return emitForce(frame, stream, sink)
	default:
		return fmt.Errorf("binance: unknown event %q", m.E)
	}
}

// channel is the atom channel. A wrapped per-symbol stream name is already that
// channel; an all-market array (!miniTicker@arr) is fanned out per symbol.
func channel(stream, symbol, fallback string) string {
	sym := strings.ToLower(symbol)
	if stream != "" && !strings.HasPrefix(stream, "!") && strings.HasPrefix(strings.ToLower(stream), sym+"@") {
		return stream
	}
	return sym + "@" + fallback
}

// spotBookTicker is a spot best-quote object: symbol and the four quote fields, and no event name.
// UM bookTicker always carries "e", so this stays false for it.
func spotBookTicker(frame []byte) bool {
	var e struct {
		E  string `json:"e"`
		S  string `json:"s"`
		ID *int64 `json:"id"`
	}
	if err := codec.DecodeFrame(frame, &e); err != nil || e.E != "" || e.S == "" || e.ID != nil {
		return false
	}
	s := string(frame)
	return strings.Contains(s, `"b"`) && strings.Contains(s, `"B"`) &&
		strings.Contains(s, `"a"`) && strings.Contains(s, `"A"`)
}

func emitBookTicker(frame []byte, stream string, sink exchange.Sink) error {
	var e struct {
		E      int64         `json:"E"`
		T      int64         `json:"T"`
		S      string        `json:"s"`
		Bid    codec.Decimal `json:"b"`
		BidQty codec.Decimal `json:"B"`
		Ask    codec.Decimal `json:"a"`
		AskQty codec.Decimal `json:"A"`
	}
	if err := codec.DecodeFrame(frame, &e); err != nil {
		return err
	}
	if e.S == "" {
		return errors.New("binance: bookTicker has no symbol")
	}
	sink.BestQuote(channel(stream, e.S, "bookTicker"), &exchange.BestQuote{
		ExchTsNs: eventNs(e.T, e.E),
		Bid:      float64(e.Bid), BidQty: float64(e.BidQty),
		Ask: float64(e.Ask), AskQty: float64(e.AskQty),
	})
	return nil
}

func emitDepth(frame []byte, stream string, sink exchange.Sink) error {
	var e struct {
		E    int64              `json:"E"`
		T    int64              `json:"T"`
		S    string             `json:"s"`
		U    int64              `json:"U"`
		Last int64              `json:"u"`
		Pu   int64              `json:"pu"`
		B    [][2]codec.Decimal `json:"b"`
		A    [][2]codec.Decimal `json:"a"`
	}
	if err := codec.DecodeFrame(frame, &e); err != nil {
		return err
	}
	if e.S == "" {
		return errors.New("binance: depthUpdate has no symbol")
	}
	if e.U < 0 || e.Last < 0 || e.Pu < 0 || e.Last < e.U {
		return fmt.Errorf("binance: depth sequence U=%d u=%d pu=%d", e.U, e.Last, e.Pu)
	}
	sink.Book(channel(stream, e.S, "depth@100ms"), &exchange.BookUpdate{
		ExchTsNs: eventNs(e.T, e.E),
		Bids:     levels(e.B), Asks: levels(e.A),
		First: uint64(e.U), Last: uint64(e.Last), Prev: uint64(e.Pu),
	})
	return nil
}

func emitAggTrade(frame []byte, stream string, sink exchange.Sink) error {
	var e struct {
		E     int64         `json:"E"`
		T     int64         `json:"T"`
		S     string        `json:"s"`
		ID    int64         `json:"a"`
		P     codec.Decimal `json:"p"`
		Q     codec.Decimal `json:"q"`
		First int64         `json:"f"`
		Last  int64         `json:"l"`
		M     bool          `json:"m"`
	}
	if err := codec.DecodeFrame(frame, &e); err != nil {
		return err
	}
	if e.S == "" {
		return errors.New("binance: aggTrade has no symbol")
	}
	if e.First < 0 || e.Last < e.First {
		return fmt.Errorf("binance: aggTrade ids %d..%d", e.First, e.Last)
	}
	sink.AggTrade(channel(stream, e.S, "aggTrade"), &exchange.AggTrade{
		ExchTsNs: eventNs(e.T, e.E),
		TradeID:  fmt.Sprintf("%d", e.ID),
		Price:    float64(e.P), Qty: float64(e.Q),
		Side:         takerSide(e.M),
		FirstTradeID: fmt.Sprintf("%d", e.First),
		LastTradeID:  fmt.Sprintf("%d", e.Last),
		MatchCount:   uint32(e.Last - e.First + 1),
	})
	return nil
}

func emitKline(frame []byte, stream string, sink exchange.Sink) error {
	var e struct {
		E int64  `json:"E"`
		S string `json:"s"`
		K struct {
			OpenTime int64         `json:"t"`
			I        string        `json:"i"`
			O        codec.Decimal `json:"o"`
			C        codec.Decimal `json:"c"`
			H        codec.Decimal `json:"h"`
			L        codec.Decimal `json:"l"`
			V        codec.Decimal `json:"v"`
			Q        codec.Decimal `json:"q"`
			X        bool          `json:"x"`
		} `json:"k"`
	}
	if err := codec.DecodeFrame(frame, &e); err != nil {
		return err
	}
	if e.S == "" || e.K.I == "" {
		return errors.New("binance: kline is missing a symbol or interval")
	}
	sink.Kline(channel(stream, e.S, "kline_"+e.K.I), &exchange.Kline{
		ExchTsNs: ms(e.E), Interval: canonInterval(e.K.I), OpenTimeNs: ms(e.K.OpenTime),
		Open: float64(e.K.O), High: float64(e.K.H), Low: float64(e.K.L), Close: float64(e.K.C),
		Volume: float64(e.K.V), QuoteVolume: float64(e.K.Q), Closed: e.K.X,
	})
	return nil
}

func emitTicker(frame []byte, stream string, sink exchange.Sink) error {
	var e struct {
		E     int64          `json:"E"`
		S     string         `json:"s"`
		Event string         `json:"e"`
		Pct   *codec.Decimal `json:"P"`
		Last  codec.Decimal  `json:"c"`
		Open  codec.Decimal  `json:"o"`
		High  codec.Decimal  `json:"h"`
		Low   codec.Decimal  `json:"l"`
		Vol   codec.Decimal  `json:"v"`
		Quote codec.Decimal  `json:"q"`
	}
	if err := codec.DecodeFrame(frame, &e); err != nil {
		return err
	}
	if e.S == "" {
		return errors.New("binance: ticker has no symbol")
	}
	pct := decimal.Absent()
	if e.Pct != nil {
		pct = float64(*e.Pct)
	}
	name := "ticker"
	if e.Event == "24hrMiniTicker" {
		name = "miniTicker"
	}
	sink.Ticker(channel(stream, e.S, name), &exchange.Ticker{
		ExchTsNs: ms(e.E), Last: float64(e.Last),
		Open24h: float64(e.Open), High24h: float64(e.High), Low24h: float64(e.Low),
		Volume24h: float64(e.Vol), QuoteVolume24h: float64(e.Quote), ChangePct24h: pct,
	})
	return nil
}

func emitMark(frame []byte, stream string, sink exchange.Sink) error {
	var e struct {
		E    int64         `json:"E"`
		S    string        `json:"s"`
		P    codec.Decimal `json:"p"`
		I    codec.Decimal `json:"i"`
		R    codec.Decimal `json:"r"`
		Next int64         `json:"T"`
	}
	if err := codec.DecodeFrame(frame, &e); err != nil {
		return err
	}
	if e.S == "" {
		return errors.New("binance: markPrice has no symbol")
	}
	ch := channel(stream, e.S, "markPrice@1s")
	ts := ms(e.E)
	sink.MarkPrice(ch, &exchange.MarkPrice{ExchTsNs: ts, MarkPrice: float64(e.P), IndexPrice: float64(e.I)})
	sink.FundingRate(ch, &exchange.FundingRate{ExchTsNs: ts, Rate: float64(e.R), NextFundingNs: ms(e.Next)})
	return nil
}

func emitForce(frame []byte, stream string, sink exchange.Sink) error {
	var e struct {
		E int64 `json:"E"`
		O struct {
			S    string        `json:"s"`
			Side string        `json:"S"`
			Q    codec.Decimal `json:"q"`
			P    codec.Decimal `json:"p"`
			AP   codec.Decimal `json:"ap"`
			T    int64         `json:"T"`
		} `json:"o"`
	}
	if err := codec.DecodeFrame(frame, &e); err != nil {
		return err
	}
	if e.O.S == "" {
		return errors.New("binance: forceOrder has no symbol")
	}
	side, err := liquidatedSide(e.O.Side)
	if err != nil {
		return err
	}
	px := e.O.AP
	if !(px > 0) {
		px = e.O.P
	}
	sink.Liquidation(channel(stream, e.O.S, "forceOrder"), &exchange.Liquidation{
		ExchTsNs: eventNs(e.O.T, e.E), Side: side, Price: float64(px), Qty: float64(e.O.Q),
	})
	return nil
}

// takerSide: m is true when the buyer is the maker, so the taker sold.
func takerSide(buyerIsMaker bool) exchange.Side {
	if buyerIsMaker {
		return exchange.SideSell
	}
	return exchange.SideBuy
}

// liquidatedSide: S is the forced order. A sell closes a long.
func liquidatedSide(orderSide string) (exchange.Side, error) {
	switch orderSide {
	case "SELL":
		return exchange.SideBuy, nil
	case "BUY":
		return exchange.SideSell, nil
	default:
		return 0, fmt.Errorf("binance: force order side %q", orderSide)
	}
}

func eventNs(tradeMs, eventMs int64) int64 {
	if tradeMs != 0 {
		return ms(tradeMs)
	}
	return ms(eventMs)
}

func levels(rows [][2]codec.Decimal) []exchange.Level {
	if len(rows) == 0 {
		return nil
	}
	out := make([]exchange.Level, len(rows))
	for i, r := range rows {
		out[i] = exchange.Level{Price: float64(r[0]), Qty: float64(r[1])}
	}
	return out
}
