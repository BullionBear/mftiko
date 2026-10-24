package bybit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

type subFrame struct {
	ReqID string   `json:"req_id"`
	Op    string   `json:"op"`
	Args  []string `json:"args"`
}

func (s *pubSession) Subscribe(channels []string) ([]exchange.Request, error) {
	return s.requests("subscribe", channels)
}

func (s *pubSession) Unsubscribe(channels []string) ([]exchange.Request, error) {
	return s.requests("unsubscribe", channels)
}

func (s *pubSession) requests(op string, channels []string) ([]exchange.Request, error) {
	if len(channels) == 0 {
		return nil, nil
	}
	pages, err := chunkArgs(channels, s.per, s.bytes)
	if err != nil {
		return nil, err
	}
	out := make([]exchange.Request, 0, len(pages))
	for _, page := range pages {
		s.n++
		frame, err := marshalBody(subFrame{ReqID: strconv.FormatUint(s.n, 10), Op: op, Args: page})
		if err != nil {
			return nil, err
		}
		out = append(out, exchange.Request{ID: s.n, Channels: page, Frame: frame})
	}
	return out, nil
}

// chunkArgs splits channels so each page has at most maxN args and at most maxBytes of
// channel text. maxN or maxBytes of 0 means that bound is unset. One channel longer than
// maxBytes does not fit a connection.
func chunkArgs(chs []string, maxN, maxBytes int) ([][]string, error) {
	var out [][]string
	var cur []string
	n := 0
	flush := func() {
		if len(cur) == 0 {
			return
		}
		out = append(out, cur)
		cur = nil
		n = 0
	}
	for _, c := range chs {
		if c == "" {
			return nil, errors.New("bybit: empty channel")
		}
		if maxBytes > 0 && len(c) > maxBytes {
			return nil, fmt.Errorf("bybit: channel %q is %d characters, budget %d", c, len(c), maxBytes)
		}
		if len(cur) > 0 && ((maxN > 0 && len(cur) >= maxN) || (maxBytes > 0 && n+len(c) > maxBytes)) {
			flush()
		}
		cur = append(cur, c)
		n += len(c)
	}
	flush()
	return out, nil
}

// Decode reads one public frame. A subscribe ack carries req_id. A topic frame is one or more
// events. A topic this codec does not read is ignored. frame is lent for the call.
func (s *pubSession) Decode(frame []byte, sink exchange.Sink) error {
	if len(frame) == 0 || frame[0] != '{' || sink == nil {
		return errors.New("bybit: unrecognized frame")
	}
	var head struct {
		Op      string `json:"op"`
		Topic   string `json:"topic"`
		Type    string `json:"type"`
		Ts      num64  `json:"ts"`
		Success *bool  `json:"success"`
		RetMsg  string `json:"ret_msg"`
		ReqID   text   `json:"req_id"`
	}
	if err := codec.DecodeFrame(frame, &head); err != nil {
		return fmt.Errorf("bybit: public frame: %w", err)
	}
	if head.Op != "" {
		return pubOp(head.Op, head.ReqID, head.Success, head.RetMsg, sink)
	}
	if head.Topic == "" {
		return errors.New("bybit: public frame has no topic")
	}
	return pubTopic(head.Topic, head.Type, int64(head.Ts), frame, sink)
}

func pubOp(op string, reqID text, ok *bool, msg string, sink exchange.Sink) error {
	switch op {
	case "ping", "pong":
		return nil
	case "subscribe", "unsubscribe":
		if reqID == "" {
			return fmt.Errorf("bybit: %s has no req_id", op)
		}
		id, err := strconv.ParseUint(string(reqID), 10, 64)
		if err != nil {
			return fmt.Errorf("bybit: %s req_id %q", op, reqID)
		}
		var ack error
		if ok == nil || !*ok {
			if msg == "" {
				msg = "refused"
			}
			ack = errors.New(msg)
		}
		sink.Ack(id, ack)
		return nil
	default:
		return fmt.Errorf("bybit: public op %q", op)
	}
}

func pubTopic(topic, typ string, ts int64, frame []byte, sink exchange.Sink) error {
	// The sink keeps ch. DecodeFrame borrows it from the frame, which the conn worker reuses
	// once Decode returns (E03-D002).
	topic = strings.Clone(topic)
	switch {
	case strings.HasPrefix(topic, "orderbook.1."):
		return pubQuote(topic, ts, frame, sink)
	case strings.HasPrefix(topic, "orderbook."):
		return pubBook(topic, typ, ts, frame, sink)
	case strings.HasPrefix(topic, "publicTrade."):
		return pubTrades(topic, frame, sink)
	case strings.HasPrefix(topic, "tickers."):
		return pubTicker(topic, ts, frame, sink)
	case strings.HasPrefix(topic, "kline."):
		return pubKline(topic, ts, frame, sink)
	case strings.HasPrefix(topic, "allLiquidation."):
		return pubLiquidation(topic, frame, sink)
	default:
		return nil
	}
}

type obData struct {
	Bids [][]string `json:"b"`
	Asks [][]string `json:"a"`
	U    uint64     `json:"u"`
}

func pubQuote(topic string, ts int64, frame []byte, sink exchange.Sink) error {
	var msg struct {
		Data obData `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	bids, err := levels(msg.Data.Bids)
	if err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	asks, err := levels(msg.Data.Asks)
	if err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	if len(bids) == 0 || len(asks) == 0 {
		return nil
	}
	sink.BestQuote(topic, &exchange.BestQuote{
		ExchTsNs: ms(ts),
		Bid:      bids[0].Price, BidQty: bids[0].Qty,
		Ask: asks[0].Price, AskQty: asks[0].Qty,
	})
	return nil
}

func pubBook(topic, typ string, ts int64, frame []byte, sink exchange.Sink) error {
	var msg struct {
		Data obData `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	bids, err := levels(msg.Data.Bids)
	if err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	asks, err := levels(msg.Data.Asks)
	if err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	sink.Book(topic, &exchange.BookUpdate{
		ExchTsNs: ms(ts), Bids: bids, Asks: asks,
		// u of 1 is a snapshot after a matching-engine restart, whatever type says.
		Snapshot: typ == "snapshot" || msg.Data.U == 1, Last: msg.Data.U,
	})
	return nil
}

func levels(rows [][]string) ([]exchange.Level, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]exchange.Level, 0, len(rows))
	for i, row := range rows {
		if len(row) < 2 {
			return nil, fmt.Errorf("level %d", i)
		}
		px, err := strconv.ParseFloat(row[0], 64)
		if err != nil {
			return nil, fmt.Errorf("level %d price %q", i, row[0])
		}
		qty, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, fmt.Errorf("level %d qty %q", i, row[1])
		}
		out = append(out, exchange.Level{Price: px, Qty: qty})
	}
	return out, nil
}

func pubTrades(topic string, frame []byte, sink exchange.Sink) error {
	var msg struct {
		Data []struct {
			T    int64  `json:"T"`
			Side string `json:"S"`
			Qty  string `json:"v"`
			Px   string `json:"p"`
			ID   string `json:"i"`
		} `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	if len(msg.Data) == 0 {
		return fmt.Errorf("bybit: %s has no trades", topic)
	}
	for i, row := range msg.Data {
		px, qty, err := pxQty(row.Px, row.Qty)
		if err != nil {
			return fmt.Errorf("bybit: %s trade %d: %w", topic, i, err)
		}
		if row.ID == "" {
			return fmt.Errorf("bybit: %s trade %d has no id", topic, i)
		}
		sink.Trade(topic, &exchange.Trade{
			ExchTsNs: ms(row.T), TradeID: strings.Clone(row.ID), Price: px, Qty: qty, Side: sideOf(row.Side),
		})
	}
	return nil
}

type tickerData struct {
	Last     string `json:"lastPrice"`
	Open     string `json:"prevPrice24h"`
	High     string `json:"highPrice24h"`
	Low      string `json:"lowPrice24h"`
	Vol      string `json:"volume24h"`
	Turnover string `json:"turnover24h"`
	Pct      string `json:"price24hPcnt"`
	Mark     string `json:"markPrice"`
	Index    string `json:"indexPrice"`
	OI       string `json:"openInterest"`
	OIValue  string `json:"openInterestValue"`
	Funding  string `json:"fundingRate"`
	NextFund string `json:"nextFundingTime"`
	Every    string `json:"fundingIntervalHour"`
}

func pubTicker(topic string, ts int64, frame []byte, sink exchange.Sink) error {
	var msg struct {
		Data tickerData `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	d := msg.Data
	t := exchange.Ticker{
		ExchTsNs: ms(ts), Last: parseNum(d.Last), Open24h: parseNum(d.Open),
		High24h: parseNum(d.High), Low24h: parseNum(d.Low),
		Volume24h: parseNum(d.Vol), QuoteVolume24h: parseNum(d.Turnover),
		ChangePct24h: parseNum(d.Pct),
	}
	if tickerFilled(t) {
		sink.Ticker(topic, &t)
	}
	if mark := parseNum(d.Mark); decimal.Has(mark) {
		sink.MarkPrice(topic, &exchange.MarkPrice{ExchTsNs: ms(ts), MarkPrice: mark, IndexPrice: parseNum(d.Index)})
	}
	if oi := parseNum(d.OI); decimal.Has(oi) {
		sink.OpenInterest(topic, &exchange.OpenInterest{
			ExchTsNs: ms(ts), OpenInterest: oi, OpenInterestValue: parseNum(d.OIValue),
		})
	}
	if rate := parseNum(d.Funding); decimal.Has(rate) {
		sink.FundingRate(topic, &exchange.FundingRate{
			ExchTsNs: ms(ts), Rate: rate, NextFundingNs: msString(d.NextFund),
			IntervalS: fundingEvery(d.Every),
		})
	}
	return nil
}

func tickerFilled(t exchange.Ticker) bool {
	for _, v := range []float64{t.Last, t.Open24h, t.High24h, t.Low24h, t.Volume24h, t.QuoteVolume24h, t.ChangePct24h} {
		if decimal.Has(v) {
			return true
		}
	}
	return false
}

func pubKline(topic string, ts int64, frame []byte, sink exchange.Sink) error {
	var msg struct {
		Data []struct {
			Start    int64  `json:"start"`
			Interval string `json:"interval"`
			Open     string `json:"open"`
			Close    string `json:"close"`
			High     string `json:"high"`
			Low      string `json:"low"`
			Volume   string `json:"volume"`
			Turnover string `json:"turnover"`
			Confirm  bool   `json:"confirm"`
			Ts       int64  `json:"timestamp"`
		} `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	if len(msg.Data) == 0 {
		return fmt.Errorf("bybit: %s has no candle", topic)
	}
	for i, row := range msg.Data {
		canon, ok := canonInterval(row.Interval)
		if !ok {
			return fmt.Errorf("bybit: %s interval %q", topic, row.Interval)
		}
		when := row.Ts
		if when == 0 {
			when = ts
		}
		k := exchange.Kline{
			ExchTsNs: ms(when), Interval: canon, OpenTimeNs: ms(row.Start),
			Open: parseNum(row.Open), High: parseNum(row.High), Low: parseNum(row.Low), Close: parseNum(row.Close),
			Volume: parseNum(row.Volume), QuoteVolume: parseNum(row.Turnover), Closed: row.Confirm,
		}
		if !decimal.Has(k.Open) || !decimal.Has(k.High) || !decimal.Has(k.Low) || !decimal.Has(k.Close) || !decimal.Has(k.Volume) {
			return fmt.Errorf("bybit: %s candle %d", topic, i)
		}
		sink.Kline(topic, &k)
	}
	return nil
}

func pubLiquidation(topic string, frame []byte, sink exchange.Sink) error {
	var msg struct {
		Data []struct {
			T     int64  `json:"T"`
			Side  string `json:"S"`
			Size  string `json:"v"`
			Price string `json:"p"`
		} `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("bybit: %s: %w", topic, err)
	}
	if len(msg.Data) == 0 {
		return fmt.Errorf("bybit: %s has no liquidation", topic)
	}
	for i, row := range msg.Data {
		px, qty, err := pxQty(row.Price, row.Size)
		if err != nil {
			return fmt.Errorf("bybit: %s liquidation %d: %w", topic, i, err)
		}
		side := sideOf(row.Side)
		if side == exchange.SideUnspecified {
			return fmt.Errorf("bybit: %s side %q", topic, row.Side)
		}
		sink.Liquidation(topic, &exchange.Liquidation{ExchTsNs: ms(row.T), Side: side, Price: px, Qty: qty})
	}
	return nil
}

// fundingEvery is fundingIntervalHour, in seconds. The field is whole hours. 0 means this
// message did not carry it (a delta, or a dated future).
func fundingEvery(hour string) uint32 {
	if hour == "" {
		return 0
	}
	h, err := strconv.ParseUint(hour, 10, 16)
	if err != nil || h == 0 || h > 24*7 {
		return 0
	}
	return uint32(h) * 3600
}

// num64 reads a timestamp that the venue sends as a number on some topics and a string on others.
type num64 int64

func (n *num64) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if s == "" {
			return nil
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		*n = num64(v)
		return nil
	}
	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	*n = num64(v)
	return nil
}

// text reads req_id, which we send as a string and a peer may echo as a number.
type text string

func (t *text) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*t = text(s)
		return nil
	}
	*t = text(bytes.TrimSpace(b))
	return nil
}

func pxQty(px, qty string) (float64, float64, error) {
	p, err := strconv.ParseFloat(px, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("price %q", px)
	}
	q, err := strconv.ParseFloat(qty, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("qty %q", qty)
	}
	return p, q, nil
}

func parseNum(s string) float64 {
	if s == "" {
		return decimal.Absent()
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return decimal.Absent()
	}
	return v
}
