package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/exchange/transport/ws"
)

// TestBybitTradeSocket places one resting limit on the testnet trade socket and cancels it.
// It does not run in CI.
//
//	MFTIKO_BYBIT_TRADE=1 go test ./exchange/bybit/ -run TestBybitTradeSocket -v -count=1
func TestBybitTradeSocket(t *testing.T) {
	if os.Getenv("MFTIKO_BYBIT_TRADE") != "1" {
		t.Skip("set MFTIKO_BYBIT_TRADE=1 to place one order on the Bybit trade websocket")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	creds := loadBybitCreds(t)
	acct, err := Declare().AccountKinds[0].Account(exchange.AccountConfig{
		APIID: 1, Credentials: creds, RESTEndpoint: testREST,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := acct.(*account)
	ts, err := raw.NewTradeStream()
	if err != nil {
		t.Fatal(err)
	}
	d, err := ts.Dial(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if d.URL != testTrade {
		t.Fatalf("url %s", d.URL)
	}
	lim := ts.Limits()
	conn, err := ws.Dial(ctx, d.URL, ws.Options{
		ReadLimit: lim.ReadLimit, Keepalive: lim.Keepalive, Ping: ts.Ping(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	feed := newTradeFeed()
	go func() {
		err := conn.Run(ctx, func(frame []byte) error {
			if err := ts.Decode(frame, feed); err != nil {
				return fmt.Errorf("%w (frame %.240q)", err, frame)
			}
			return nil
		})
		feed.die(err)
	}()
	for _, f := range d.Frames {
		if err := conn.Write(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	if err := feed.waitReady(15 * time.Second); err != nil {
		t.Fatal(err)
	}

	inst, tick, _, qty := btcLinear(t)
	for _, id := range linearBTCOpen(t, raw) {
		if err := restCancel(raw, exchange.CancelRef{Instrument: inst, OrderID: id}); err != nil {
			var rej *exchange.Reject
			if !errors.As(err, &rej) {
				t.Fatalf("cancel leftover %s: %v", id, err)
			}
		}
	}
	bid := bookPrice(t, "bid1Price")
	px := decimal.SnapDown(bid*0.5, tick)
	if !(px > 0 && px < bid) {
		t.Fatalf("resting price %v bid %v", px, bid)
	}
	cloid := strconv.FormatInt(time.Now().UnixNano(), 10)
	req := &exchange.OrderRequest{
		Instrument: inst, Side: exchange.SideBuy, Type: exchange.OrderTypeLimit, TIF: exchange.TIFGTC,
		Qty: qty, QuoteQty: decimal.Absent(), Price: px, ClientOrderID: cloid,
	}
	frame, err := ts.Submit(time.Now(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, frame); err != nil {
		t.Fatal(err)
	}
	res, err := feed.waitSubmit(15 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := restCancel(raw, exchange.CancelRef{Instrument: inst, OrderID: res.OrderID, ClientOrderID: cloid})
		var rej *exchange.Reject
		if err != nil && !errors.As(err, &rej) {
			t.Errorf("cleanup cancel: %v", err)
		}
	})
	if res.OrderID == "" || res.Status != exchange.OrderStatusNew {
		t.Fatalf("ack %+v", res)
	}
	t.Logf("created %s at %s qty %v", res.OrderID, decimal.Format(px, tick), qty)

	cancelFrame, err := ts.Cancel(time.Now(), 2, exchange.CancelRef{Instrument: inst, OrderID: res.OrderID})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, cancelFrame); err != nil {
		t.Fatal(err)
	}
	if err := feed.waitCancel(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if !hasID(linearBTCOpen(t, raw), res.OrderID) {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("order %s still open", res.OrderID)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Logf("canceled %s", res.OrderID)
}

func linearBTCOpen(t *testing.T, a *account) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	calls, err := a.Snapshot(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var found exchange.Call[exchange.Page[exchange.AccountSnapshot]]
	ok := false
	for _, c := range calls {
		if c.Req.Path == "/v5/order/realtime" && strings.Contains(string(c.Req.Query), "settleCoin=USDT") {
			found, ok = c, true
			break
		}
	}
	if !ok {
		t.Fatal("snapshot has no linear open orders")
	}
	doer := fasthttp.New(nil, fasthttp.Options{Timeout: 15 * time.Second})
	items, err := exchange.Collect(ctx, doer, time.Now, found)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, item := range items {
		for _, o := range item.OpenOrders {
			if o.Exch.ExchTicker == "BTCUSDT" && o.Exch.Category == symbol.Perp && o.OrderID != "" {
				ids = append(ids, o.OrderID)
			}
		}
	}
	return ids
}

func hasID(ids []string, id string) bool {
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}

func restCancel(a *account, ref exchange.CancelRef) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := a.Cancel(time.Now(), ref)
	if err != nil {
		return err
	}
	doer := fasthttp.New(nil, fasthttp.Options{Timeout: 15 * time.Second})
	resp, err := doer.Do(ctx, &c.Req)
	if err != nil {
		return err
	}
	_, err = c.Parse(resp)
	return err
}

func bookPrice(t *testing.T, field string) float64 {
	t.Helper()
	body := publicGet(t, testREST+"/v5/market/tickers?category=linear&symbol=BTCUSDT")
	var resp struct {
		Code   int `json:"retCode"`
		Result struct {
			List []map[string]string `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Code != 0 || len(resp.Result.List) == 0 {
		t.Fatalf("tickers: %v code %d", err, resp.Code)
	}
	v, err := strconv.ParseFloat(resp.Result.List[0][field], 64)
	if err != nil || !(v > 0) {
		t.Fatalf("ticker %s %q", field, resp.Result.List[0][field])
	}
	return v
}

type tradeFeed struct {
	mu     sync.Mutex
	cond   *sync.Cond
	ready  bool
	sub    *exchange.OrderResult
	subErr error
	gotSub bool
	gotCan bool
	canErr error
	dead   error
	closed bool
}

func newTradeFeed() *tradeFeed {
	f := &tradeFeed{}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *tradeFeed) Ready() {
	f.mu.Lock()
	f.ready = true
	f.cond.Broadcast()
	f.mu.Unlock()
}
func (f *tradeFeed) Expired(string)                {}
func (f *tradeFeed) Order(*exchange.OrderUpdate)   {}
func (f *tradeFeed) Fill(*exchange.Fill)           {}
func (f *tradeFeed) Balances([]exchange.Balance)   {}
func (f *tradeFeed) Positions([]exchange.Position) {}
func (f *tradeFeed) Used([]exchange.Used)          {}
func (f *tradeFeed) Submitted(id uint64, res *exchange.OrderResult, err error) {
	if id != 1 {
		return
	}
	f.mu.Lock()
	f.gotSub, f.subErr = true, err
	if res != nil {
		cp := *res
		cp.OrderID = string([]byte(cp.OrderID))
		f.sub = &cp
	}
	f.cond.Broadcast()
	f.mu.Unlock()
}
func (f *tradeFeed) Canceled(id uint64, err error) {
	if id != 2 {
		return
	}
	f.mu.Lock()
	f.gotCan, f.canErr = true, err
	f.cond.Broadcast()
	f.mu.Unlock()
}
func (f *tradeFeed) die(err error) {
	f.mu.Lock()
	f.dead, f.closed = err, true
	f.cond.Broadcast()
	f.mu.Unlock()
}

func (f *tradeFeed) waitReady(d time.Duration) error {
	return f.wait(d, func() bool { return f.ready })
}

func (f *tradeFeed) waitSubmit(d time.Duration) (*exchange.OrderResult, error) {
	err := f.wait(d, func() bool { return f.gotSub })
	if err != nil {
		return nil, err
	}
	if f.subErr != nil {
		return nil, f.subErr
	}
	return f.sub, nil
}

func (f *tradeFeed) waitCancel(d time.Duration) error {
	if err := f.wait(d, func() bool { return f.gotCan }); err != nil {
		return err
	}
	return f.canErr
}

func (f *tradeFeed) wait(d time.Duration, done func() bool) error {
	deadline := time.Now().Add(d)
	timer := time.AfterFunc(d, func() {
		f.mu.Lock()
		f.cond.Broadcast()
		f.mu.Unlock()
	})
	defer timer.Stop()
	f.mu.Lock()
	defer f.mu.Unlock()
	for !done() {
		if f.closed {
			if f.dead == nil || errors.Is(f.dead, context.Canceled) {
				return fmt.Errorf("trade socket closed")
			}
			return f.dead
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("timed out after %s", d)
		}
		f.cond.Wait()
	}
	return nil
}
