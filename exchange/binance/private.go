package binance

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/internal/codec"
)

// privateEvents is what this codec reads. Production delivers nothing when events is omitted.
const privateEvents = "ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/listenKeyExpired"

// privateStream is the listenKey connection. It has no logon frame: Prepare creates the key,
// Dial puts it in the URL, and the first event is what makes the stream Ready. A quiet account
// sends nothing until something happens; the worker that must trade before then treats the
// completed dial as live, which this codec cannot see.
type privateStream struct {
	rest  string
	ws    string
	key   string
	ready bool
}

var _ exchange.PrivateStream = (*privateStream)(nil)

func (p *privateStream) Prepare(time.Time) (*exchange.Call[string], error) {
	req := apiKeyCall(p.rest, p.key, http.MethodPost, "/fapi/v1/listenKey", nil)
	return &exchange.Call[string]{Req: req, Parse: parseListenKey}, nil
}

func (p *privateStream) Dial(_ time.Time, token string) (*exchange.Dial, error) {
	if token == "" {
		return nil, fmt.Errorf("binance: listenKey is empty")
	}
	u := p.ws + "?listenKey=" + url.QueryEscape(token) + "&events=" + privateEvents
	return &exchange.Dial{URL: u}, nil
}

func (p *privateStream) Refresh(time.Time) (*exchange.Call[struct{}], error) {
	req := apiKeyCall(p.rest, p.key, http.MethodPut, "/fapi/v1/listenKey", nil)
	return &exchange.Call[struct{}]{Req: req, Parse: parseAck}, nil
}

// release deletes the account's listenKey. The interface has no close; the testnet smoke uses it.
func (p *privateStream) release() exchange.Call[struct{}] {
	return exchange.Call[struct{}]{
		Req:   apiKeyCall(p.rest, p.key, http.MethodDelete, "/fapi/v1/listenKey", nil),
		Parse: parseAck,
	}
}

func (p *privateStream) Limits() exchange.StreamLimits {
	return exchange.StreamLimits{
		Keepalive:    exchange.Keepalive{ServerPing: true, Timeout: 5 * time.Minute},
		RefreshEvery: 30 * time.Minute,
		MaxLifetime:  24 * time.Hour,
		ReadLimit:    1 << 20,
	}
}

func (p *privateStream) Ping() []byte { return nil }

func (p *privateStream) Decode(frame []byte, sink exchange.AccountSink) error {
	var ev struct {
		E string `json:"e"`
	}
	if err := codec.DecodeFrame(frame, &ev); err != nil || ev.E == "" {
		if err == nil {
			err = errors.New("no event name")
		}
		return fmt.Errorf("binance: private frame: %w", err)
	}
	switch ev.E {
	case "listenKeyExpired":
		sink.Expired("listenKeyExpired")
		return nil
	case "ORDER_TRADE_UPDATE":
		return p.order(frame, sink)
	case "ACCOUNT_UPDATE":
		return p.account(frame, sink)
	default:
		// A name this codec does not read. Dropping the connection over it would be worse.
		return nil
	}
}

func (p *privateStream) seen(sink exchange.AccountSink) {
	if !p.ready {
		p.ready = true
		sink.Ready()
	}
}

func (p *privateStream) order(frame []byte, sink exchange.AccountSink) error {
	var ev struct {
		E int64  `json:"E"`
		O umExec `json:"o"`
	}
	if err := codec.DecodeFrame(frame, &ev); err != nil {
		return fmt.Errorf("binance: ORDER_TRADE_UPDATE: %w", err)
	}
	o := ev.O
	if o.Time == 0 {
		o.Time = ev.E
	}
	p.seen(sink)
	up := o.update()
	sink.Order(&up)
	if o.Exec != "TRADE" || o.Trade == 0 || float64(o.LastQty) == 0 {
		return nil
	}
	liq := "taker"
	if o.Maker {
		liq = "maker"
	}
	sink.Fill(&exchange.Fill{
		Exch: exchOf(o.Symbol), FillID: strconv.FormatInt(o.Trade, 10),
		OrderID: strconv.FormatInt(o.OrderID, 10), ClientOrderID: o.ClientID,
		Side: sideOf(o.Side), Price: float64(o.LastPrice), Qty: float64(o.LastQty),
		Fee: float64(o.Fee), FeeAsset: o.FeeAsset, Liquidity: liq, ExchTsNs: ms(o.Time),
	})
	return nil
}

func (p *privateStream) account(frame []byte, sink exchange.AccountSink) error {
	var ev struct {
		A struct {
			B []umBal `json:"B"`
			P []umPos `json:"P"`
		} `json:"a"`
	}
	if err := codec.DecodeFrame(frame, &ev); err != nil {
		return fmt.Errorf("binance: ACCOUNT_UPDATE: %w", err)
	}
	p.seen(sink)
	if len(ev.A.B) > 0 {
		bals := make([]exchange.Balance, len(ev.A.B))
		for i, b := range ev.A.B {
			bals[i] = b.balance()
		}
		sink.Balances(bals)
	}
	if len(ev.A.P) > 0 {
		pos := make([]exchange.Position, len(ev.A.P))
		for i, row := range ev.A.P {
			pos[i] = row.position()
		}
		sink.Positions(pos)
	}
	return nil
}

func parseListenKey(resp *exchange.HTTPResponse) (string, error) {
	if resp.Status >= 400 {
		return "", refuse(resp)
	}
	var body struct {
		ListenKey string `json:"listenKey"`
	}
	if err := codec.DecodeBody(resp.Body, &body); err != nil {
		return "", err
	}
	if body.ListenKey == "" {
		return "", fmt.Errorf("binance: empty listenKey")
	}
	return body.ListenKey, nil
}

func parseAck(resp *exchange.HTTPResponse) (struct{}, error) {
	if resp.Status >= 400 {
		return struct{}{}, refuse(resp)
	}
	return struct{}{}, nil
}

type umExec struct {
	Symbol    string        `json:"s"`
	ClientID  string        `json:"c"`
	Side      string        `json:"S"`
	Type      string        `json:"o"`
	Status    string        `json:"X"`
	Exec      string        `json:"x"`
	Qty       codec.Decimal `json:"q"`
	Price     codec.Decimal `json:"p"`
	Avg       codec.Decimal `json:"ap"`
	Filled    codec.Decimal `json:"z"`
	OrderID   int64         `json:"i"`
	LastQty   codec.Decimal `json:"l"`
	LastPrice codec.Decimal `json:"L"`
	Fee       codec.Decimal `json:"n"`
	FeeAsset  string        `json:"N"`
	Time      int64         `json:"T"`
	Trade     int64         `json:"t"`
	Maker     bool          `json:"m"`
}

func (o umExec) update() exchange.OrderUpdate {
	return exchange.OrderUpdate{
		Exch: exchOf(o.Symbol), OrderID: strconv.FormatInt(o.OrderID, 10), ClientOrderID: o.ClientID,
		Side: sideOf(o.Side), Type: typeOf(o.Type), Status: orderStatus(o.Status, float64(o.Filled)),
		Qty: float64(o.Qty), Price: float64(o.Price), FilledQty: float64(o.Filled),
		AvgPrice: float64(o.Avg), ExchTsNs: ms(o.Time),
	}
}

type umBal struct {
	Asset  string        `json:"a"`
	Wallet codec.Decimal `json:"wb"`
	Cross  codec.Decimal `json:"cw"`
}

func (b umBal) balance() exchange.Balance {
	wallet, cross := float64(b.Wallet), float64(b.Cross)
	locked := 0.0
	if wallet > cross {
		locked = wallet - cross
	}
	return exchange.Balance{Asset: b.Asset, Free: cross, Locked: locked}
}

type umPos struct {
	Symbol string        `json:"s"`
	Amt    codec.Decimal `json:"pa"`
	Entry  codec.Decimal `json:"ep"`
	UPnl   codec.Decimal `json:"up"`
	Side   string        `json:"ps"`
}

func (p umPos) position() exchange.Position {
	return exchange.Position{
		Exch: exchOf(p.Symbol), Size: positionSize(float64(p.Amt), p.Side),
		EntryPrice: float64(p.Entry), UnrealizedPnl: float64(p.UPnl),
	}
}
