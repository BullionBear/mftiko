package okx

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

type subFrame struct {
	ID   string   `json:"id"`
	Op   string   `json:"op"`
	Args []subArg `json:"args"`
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
	pages, err := chunkArgs(channels, channelBytes)
	if err != nil {
		return nil, err
	}
	out := make([]exchange.Request, 0, len(pages))
	for _, page := range pages {
		args := make([]subArg, len(page))
		for i, ch := range page {
			name, id, err := splitChannel(ch)
			if err != nil {
				return nil, err
			}
			args[i] = subArg{Channel: name, InstID: id}
		}
		s.n++
		frame, err := marshalBody(subFrame{ID: strconv.FormatUint(s.n, 10), Op: op, Args: args})
		if err != nil {
			return nil, err
		}
		out = append(out, exchange.Request{ID: s.n, Channels: page, Frame: frame})
	}
	return out, nil
}

// chunkArgs splits channels so each page has at most maxBytes of channel text.
// One channel longer than maxBytes does not fit a connection.
func chunkArgs(chs []string, maxBytes int) ([][]string, error) {
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
			return nil, errors.New("okx: empty channel")
		}
		if maxBytes > 0 && len(c) > maxBytes {
			return nil, fmt.Errorf("okx: channel %q is %d characters, budget %d", c, len(c), maxBytes)
		}
		if len(cur) > 0 && maxBytes > 0 && n+len(c) > maxBytes {
			flush()
		}
		cur = append(cur, c)
		n += len(c)
	}
	flush()
	return out, nil
}

func splitChannel(ch string) (string, string, error) {
	name, id, ok := strings.Cut(ch, ":")
	if !ok || name == "" || id == "" || strings.Contains(id, ":") {
		return "", "", fmt.Errorf("okx: channel %q", ch)
	}
	return name, id, nil
}

// Decode reads one public frame. A subscribe ack carries id, once per arg, and several acks
// share the id of the request. A channel frame is one or more events. A channel this codec
// does not read is ignored. "ping" and "pong" are ignored. frame is lent for the call.
func (s *pubSession) Decode(frame []byte, sink exchange.Sink) error {
	if sink == nil || len(frame) == 0 {
		return errors.New("okx: unrecognized frame")
	}
	if bytes.Equal(frame, []byte("pong")) || bytes.Equal(frame, []byte("ping")) {
		return nil
	}
	if frame[0] != '{' {
		return errors.New("okx: unrecognized frame")
	}
	var head struct {
		Event  string `json:"event"`
		ID     text   `json:"id"`
		Code   string `json:"code"`
		Msg    string `json:"msg"`
		Action string `json:"action"`
		Arg    subArg `json:"arg"`
	}
	if err := codec.DecodeFrame(frame, &head); err != nil {
		return fmt.Errorf("okx: public frame: %w", err)
	}
	if head.Event != "" {
		return pubEvent(head.Event, head.ID, head.Code, head.Msg, sink)
	}
	if head.Arg.Channel == "" {
		return errors.New("okx: public frame has no channel")
	}
	ch := head.Arg.Channel
	if head.Arg.InstID != "" {
		ch += ":" + head.Arg.InstID
	}
	return pubChannel(strings.Clone(ch), head.Arg.Channel, head.Arg.InstID, head.Action, frame, sink)
}

func pubEvent(event string, id text, code, msg string, sink exchange.Sink) error {
	switch event {
	case "subscribe", "unsubscribe":
		n, err := parseID(id)
		if err != nil {
			return fmt.Errorf("okx: %s: %w", event, err)
		}
		var ack error
		if code != "" && code != "0" {
			if msg == "" {
				msg = code
			}
			ack = errors.New(msg)
		}
		sink.Ack(n, ack)
		return nil
	case "error":
		n, err := parseID(id)
		if err != nil {
			if msg == "" {
				msg = code
			}
			return fmt.Errorf("okx: %s", msg)
		}
		if msg == "" {
			msg = code
		}
		if msg == "" {
			msg = "refused"
		}
		sink.Ack(n, errors.New(msg))
		return nil
	case "notice":
		return nil
	default:
		return fmt.Errorf("okx: public event %q", event)
	}
}

func parseID(id text) (uint64, error) {
	if id == "" {
		return 0, errors.New("no id")
	}
	n, err := strconv.ParseUint(string(id), 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("id %q", id)
	}
	return n, nil
}

func pubChannel(ch, name, inst, action string, frame []byte, sink exchange.Sink) error {
	switch name {
	case "books":
		return pubBook(ch, action, frame, sink)
	case "bbo-tbt":
		return pubQuote(ch, frame, sink)
	case "trades":
		return pubTrades(ch, frame, sink)
	case "tickers":
		return pubTicker(ch, inst, frame, sink)
	case "funding-rate":
		return pubFunding(ch, frame, sink)
	case "open-interest":
		return pubOpenInterest(ch, frame, sink)
	case "mark-price":
		return pubMark(ch, frame, sink)
	default:
		if strings.HasPrefix(name, "candle") {
			return pubKline(ch, name, inst, frame, sink)
		}
		return nil
	}
}

type bookRow struct {
	Bids [][]string `json:"bids"`
	Asks [][]string `json:"asks"`
	TS   text       `json:"ts"`
	Seq  int64      `json:"seqId"`
	Prev int64      `json:"prevSeqId"`
}

func pubBook(ch, action string, frame []byte, sink exchange.Sink) error {
	rows, err := decodeRows[bookRow](frame)
	if err != nil {
		return fmt.Errorf("okx: %s: %w", ch, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("okx: %s has no book", ch)
	}
	for i, row := range rows {
		if row.Seq <= 0 {
			return fmt.Errorf("okx: %s seqId %d", ch, row.Seq)
		}
		bids, err := levels(row.Bids)
		if err != nil {
			return fmt.Errorf("okx: %s: %w", ch, err)
		}
		asks, err := levels(row.Asks)
		if err != nil {
			return fmt.Errorf("okx: %s: %w", ch, err)
		}
		ts, err := textMS(row.TS)
		if err != nil {
			return fmt.Errorf("okx: %s row %d: %w", ch, i, err)
		}
		u := &exchange.BookUpdate{
			ExchTsNs: ts, Bids: bids, Asks: asks, Last: uint64(row.Seq),
			Snapshot: action == "snapshot" || row.Prev < 0,
		}
		if row.Prev > 0 {
			u.Prev = uint64(row.Prev)
		}
		sink.Book(ch, u)
	}
	return nil
}

func pubQuote(ch string, frame []byte, sink exchange.Sink) error {
	rows, err := decodeRows[bookRow](frame)
	if err != nil {
		return fmt.Errorf("okx: %s: %w", ch, err)
	}
	for _, row := range rows {
		bids, err := levels(row.Bids)
		if err != nil {
			return fmt.Errorf("okx: %s: %w", ch, err)
		}
		asks, err := levels(row.Asks)
		if err != nil {
			return fmt.Errorf("okx: %s: %w", ch, err)
		}
		if len(bids) == 0 || len(asks) == 0 {
			continue
		}
		ts, err := textMS(row.TS)
		if err != nil {
			return fmt.Errorf("okx: %s: %w", ch, err)
		}
		sink.BestQuote(ch, &exchange.BestQuote{
			ExchTsNs: ts,
			Bid:      bids[0].Price, BidQty: bids[0].Qty,
			Ask: asks[0].Price, AskQty: asks[0].Qty,
		})
	}
	return nil
}

func pubTrades(ch string, frame []byte, sink exchange.Sink) error {
	rows, err := decodeRows[struct {
		TradeID string `json:"tradeId"`
		Px      string `json:"px"`
		Sz      string `json:"sz"`
		Side    string `json:"side"`
		TS      text   `json:"ts"`
	}](frame)
	if err != nil {
		return fmt.Errorf("okx: %s: %w", ch, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("okx: %s has no trades", ch)
	}
	for i, row := range rows {
		px, qty, err := pxQty(row.Px, row.Sz)
		if err != nil {
			return fmt.Errorf("okx: %s trade %d: %w", ch, i, err)
		}
		if row.TradeID == "" {
			return fmt.Errorf("okx: %s trade %d has no id", ch, i)
		}
		side, err := taker(row.Side)
		if err != nil {
			return fmt.Errorf("okx: %s trade %d: %w", ch, i, err)
		}
		ts, err := textMS(row.TS)
		if err != nil {
			return fmt.Errorf("okx: %s trade %d: %w", ch, i, err)
		}
		sink.Trade(ch, &exchange.Trade{
			ExchTsNs: ts, TradeID: strings.Clone(row.TradeID), Price: px, Qty: qty, Side: side,
		})
	}
	return nil
}

func pubTicker(ch, inst string, frame []byte, sink exchange.Sink) error {
	rows, err := decodeRows[struct {
		InstType string `json:"instType"`
		Last     string `json:"last"`
		Open     string `json:"open24h"`
		High     string `json:"high24h"`
		Low      string `json:"low24h"`
		Vol      string `json:"vol24h"`
		VolCcy   string `json:"volCcy24h"`
		TS       text   `json:"ts"`
	}](frame)
	if err != nil {
		return fmt.Errorf("okx: %s: %w", ch, err)
	}
	for i, row := range rows {
		ts, err := textMS(row.TS)
		if err != nil {
			return fmt.Errorf("okx: %s row %d: %w", ch, i, err)
		}
		ev := &exchange.Ticker{
			ExchTsNs: ts, Last: numOrAbsent(row.Last), Open24h: numOrAbsent(row.Open),
			High24h: numOrAbsent(row.High), Low24h: numOrAbsent(row.Low),
			ChangePct24h: decimal.Absent(),
		}
		if contractStat(row.InstType, inst) {
			// vol24h is contracts. volCcy24h is the coin amount, which is platform qty on a
			// linear contract (E02-D003). Quote volume is not on the row.
			ev.Volume24h = numOrAbsent(row.VolCcy)
			ev.QuoteVolume24h = decimal.Absent()
		} else {
			ev.Volume24h = numOrAbsent(row.Vol)
			ev.QuoteVolume24h = numOrAbsent(row.VolCcy)
		}
		sink.Ticker(ch, ev)
	}
	return nil
}

func pubFunding(ch string, frame []byte, sink exchange.Sink) error {
	rows, err := decodeRows[struct {
		Rate     string `json:"fundingRate"`
		Time     string `json:"fundingTime"`
		NextTime string `json:"nextFundingTime"`
		TS       text   `json:"ts"`
	}](frame)
	if err != nil {
		return fmt.Errorf("okx: %s: %w", ch, err)
	}
	for i, row := range rows {
		rate, err := mustNum(row.Rate)
		if err != nil {
			return fmt.Errorf("okx: %s row %d: %w", ch, i, err)
		}
		ts, err := textMS(row.TS)
		if err != nil {
			return fmt.Errorf("okx: %s row %d: %w", ch, i, err)
		}
		next, err := parseMS(row.Time)
		if err != nil {
			return fmt.Errorf("okx: %s funding time: %w", ch, err)
		}
		sink.FundingRate(ch, &exchange.FundingRate{
			ExchTsNs: ts, Rate: rate, NextFundingNs: next, IntervalS: fundingInterval(row.Time, row.NextTime),
		})
	}
	return nil
}

// fundingInterval is nextFundingTime minus fundingTime, in seconds. OKX does not send the
// interval. An empty next time leaves it 0.
func fundingInterval(cur, next string) uint32 {
	a, err1 := strconv.ParseInt(cur, 10, 64)
	b, err2 := strconv.ParseInt(next, 10, 64)
	if err1 != nil || err2 != nil || b <= a {
		return 0
	}
	sec := (b - a) / 1000
	if sec <= 0 || sec > 1<<32-1 {
		return 0
	}
	return uint32(sec)
}

func pubOpenInterest(ch string, frame []byte, sink exchange.Sink) error {
	rows, err := decodeRows[struct {
		Oi    string `json:"oi"`
		OiCcy string `json:"oiCcy"`
		OiUsd string `json:"oiUsd"`
		TS    text   `json:"ts"`
	}](frame)
	if err != nil {
		return fmt.Errorf("okx: %s: %w", ch, err)
	}
	for i, row := range rows {
		ts, err := textMS(row.TS)
		if err != nil {
			return fmt.Errorf("okx: %s row %d: %w", ch, i, err)
		}
		// oi is contracts. oiCcy is the coin amount, platform qty on a linear contract.
		oi := row.OiCcy
		if oi == "" {
			oi = row.Oi
		}
		qty, err := mustNum(oi)
		if err != nil {
			return fmt.Errorf("okx: %s row %d: %w", ch, i, err)
		}
		sink.OpenInterest(ch, &exchange.OpenInterest{
			ExchTsNs: ts, OpenInterest: qty, OpenInterestValue: numOrAbsent(row.OiUsd),
		})
	}
	return nil
}

func pubMark(ch string, frame []byte, sink exchange.Sink) error {
	rows, err := decodeRows[struct {
		Mark string `json:"markPx"`
		TS   text   `json:"ts"`
	}](frame)
	if err != nil {
		return fmt.Errorf("okx: %s: %w", ch, err)
	}
	for i, row := range rows {
		px, err := mustNum(row.Mark)
		if err != nil {
			return fmt.Errorf("okx: %s row %d: %w", ch, i, err)
		}
		ts, err := textMS(row.TS)
		if err != nil {
			return fmt.Errorf("okx: %s row %d: %w", ch, i, err)
		}
		sink.MarkPrice(ch, &exchange.MarkPrice{ExchTsNs: ts, MarkPrice: px, IndexPrice: decimal.Absent()})
	}
	return nil
}

func pubKline(ch, name, inst string, frame []byte, sink exchange.Sink) error {
	wire, ok := strings.CutPrefix(name, "candle")
	if !ok {
		return nil
	}
	interval, ok := canonInterval(wire)
	if !ok {
		return nil
	}
	var msg struct {
		Data [][]string `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return fmt.Errorf("okx: %s: %w", ch, err)
	}
	for i, row := range msg.Data {
		k, err := klineFrom(row, interval, inst)
		if err != nil {
			return fmt.Errorf("okx: %s row %d: %w", ch, i, err)
		}
		sink.Kline(ch, &k)
	}
	return nil
}

// klineFrom reads one candle. The row is ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm.
// confirm "0" is the candle still forming. Spot vol is base and volCcy is quote. A derivative's
// vol is contracts and volCcy is the coin amount.
func klineFrom(row []string, interval, inst string) (exchange.Kline, error) {
	if len(row) < 6 {
		return exchange.Kline{}, errors.New("short candle")
	}
	openNs, err := parseMS(row[0])
	if err != nil {
		return exchange.Kline{}, err
	}
	o, err := mustNum(row[1])
	if err != nil {
		return exchange.Kline{}, err
	}
	h, err := mustNum(row[2])
	if err != nil {
		return exchange.Kline{}, err
	}
	l, err := mustNum(row[3])
	if err != nil {
		return exchange.Kline{}, err
	}
	c, err := mustNum(row[4])
	if err != nil {
		return exchange.Kline{}, err
	}
	vol := row[5]
	quote := ""
	if len(row) > 7 && row[7] != "" {
		quote = row[7]
	} else if len(row) > 6 {
		quote = row[6]
	}
	if derivativeID(inst) && len(row) > 6 && row[6] != "" {
		vol = row[6]
	}
	closed := len(row) > 8 && row[8] == "1"
	return exchange.Kline{
		ExchTsNs: openNs, Interval: interval, OpenTimeNs: openNs,
		Open: o, High: h, Low: l, Close: c,
		Volume: mustOrAbsent(vol), QuoteVolume: numOrAbsent(quote), Closed: closed,
	}, nil
}

func decodeRows[T any](frame []byte) ([]T, error) {
	var msg struct {
		Data []T `json:"data"`
	}
	if err := codec.DecodeFrame(frame, &msg); err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func levels(rows [][]string) ([]exchange.Level, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]exchange.Level, 0, len(rows))
	for i, row := range rows {
		lv, err := oneLevel(row)
		if err != nil {
			return nil, fmt.Errorf("level %d: %w", i, err)
		}
		out = append(out, lv)
	}
	return out, nil
}

func oneLevel(row []string) (exchange.Level, error) {
	if len(row) < 2 {
		return exchange.Level{}, errors.New("short level")
	}
	px, err := mustNum(row[0])
	if err != nil {
		return exchange.Level{}, err
	}
	qty, err := mustNum(row[1])
	if err != nil {
		return exchange.Level{}, err
	}
	if qty < 0 {
		return exchange.Level{}, fmt.Errorf("qty %s", row[1])
	}
	return exchange.Level{Price: px, Qty: qty}, nil
}

func pxQty(px, qty string) (float64, float64, error) {
	p, err := mustNum(px)
	if err != nil {
		return 0, 0, fmt.Errorf("price %q", px)
	}
	q, err := mustNum(qty)
	if err != nil {
		return 0, 0, fmt.Errorf("qty %q", qty)
	}
	return p, q, nil
}

func taker(s string) (exchange.Side, error) {
	switch s {
	case "buy":
		return exchange.SideBuy, nil
	case "sell":
		return exchange.SideSell, nil
	default:
		return 0, fmt.Errorf("side %q", s)
	}
}

func contractStat(instType, instID string) bool {
	switch instType {
	case "SPOT", "MARGIN":
		return false
	case "":
		return derivativeID(instID)
	default:
		return true
	}
}

func derivativeID(id string) bool {
	if strings.HasSuffix(id, "-SWAP") {
		return true
	}
	_, ok := expiryCode(id)
	return ok
}

func mustNum(s string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("number %q", s)
	}
	return v, nil
}

func numOrAbsent(s string) float64 {
	if s == "" {
		return decimal.Absent()
	}
	v, err := mustNum(s)
	if err != nil {
		return decimal.Absent()
	}
	return v
}

func mustOrAbsent(s string) float64 {
	v, err := mustNum(s)
	if err != nil {
		return decimal.Absent()
	}
	return v
}

func textMS(s text) (int64, error) { return parseMS(string(s)) }

func parseMS(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("no timestamp")
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("timestamp %q", s)
	}
	return v * int64(time.Millisecond), nil
}

// text reads a JSON string or number. OKX sends id and ts as either.
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
