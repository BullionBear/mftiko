package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/sdk"
	"github.com/BullionBear/mftiko/sdk/book"
)

func TestHelloEmptyParams(t *testing.T) {
	f := &fakeCtx{}
	h := &hello{out: &bytes.Buffer{}}
	if err := h.OnInit(f); err != nil {
		t.Fatal(err)
	}
	if len(f.resolved) != 0 || len(h.uids) != 0 {
		t.Fatalf("resolved %v uids %v", f.resolved, h.uids)
	}
}

func TestHelloRejectsBadParams(t *testing.T) {
	h := &hello{}
	f := &fakeCtx{params: []byte("{")}
	if err := h.OnInit(f); err == nil {
		t.Fatal("bad params were accepted")
	}
}

func TestHelloPlacesAndCancels(t *testing.T) {
	raw := []byte(`{"tickers":["Binance_Spot_BTCUSDT"],"api_id":7,"algo_id":1,"offset":50,"qty":0.01}`)
	f := &fakeCtx{
		params: raw,
		book:   staticBook{bid: 100},
		bookOK: true,
	}
	var buf bytes.Buffer
	h := &hello{out: &buf}
	if err := h.OnInit(f); err != nil {
		t.Fatal(err)
	}
	if len(f.resolved) != 1 || f.resolved[0] != "Binance_Spot_BTCUSDT" || len(h.uids) != 1 || h.uids[0] != 1001 {
		t.Fatalf("resolve %v uids %v", f.resolved, h.uids)
	}
	h.OnBook(f, sdk.BookUpdate{MD: sdk.MD{UID: 1001}})
	h.OnBook(f, sdk.BookUpdate{MD: sdk.MD{UID: 1001}})
	if len(f.submitted) != 1 {
		t.Fatalf("submitted %d", len(f.submitted))
	}
	got := f.submitted[0]
	if got.APIID != 7 || got.AlgoID != 1 || got.UID != 1001 || got.Side != commonv1.Side_SIDE_BUY ||
		got.Type != commonv1.OrderType_ORDER_TYPE_LIMIT || got.Price != 50 || got.Qty != 0.01 || decimal.Has(got.QuoteQty) {
		t.Fatalf("order %+v", got)
	}
	h.OnRecon(f, sdk.ReconEvent{
		TD: sdk.TD{APIID: 7},
		Recon: &tdv1.Recon{OpenOrders: []*tdv1.Order{{
			ClientOrderId: "c1",
			Status:        tdv1.OrderStatus_ORDER_STATUS_ACCEPTED,
		}}},
	})
	h.OnOrder(f, sdk.OrderEvent{Order: &tdv1.Order{
		ClientOrderId: "c1",
		Status:        tdv1.OrderStatus_ORDER_STATUS_ACCEPTED,
	}})
	h.OnOrder(f, sdk.OrderEvent{Order: &tdv1.Order{
		ClientOrderId: "c1",
		Status:        tdv1.OrderStatus_ORDER_STATUS_ACCEPTED,
	}})
	if len(f.cancelled) != 1 || f.cancelled[0] != "c1" {
		t.Fatalf("cancels %v", f.cancelled)
	}
	h.OnOrder(f, sdk.OrderEvent{Order: &tdv1.Order{
		ClientOrderId: "c1",
		Status:        tdv1.OrderStatus_ORDER_STATUS_CANCELLED,
	}})
	var sawCancel, sawRecon bool
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var row orderPrint
		if json.Unmarshal(line, &row) == nil && row.Status == tdv1.OrderStatus_ORDER_STATUS_CANCELLED.String() && row.Cloid == "c1" {
			sawCancel = true
		}
		var rec reconPrint
		if json.Unmarshal(line, &rec) == nil && rec.Hook == "OnRecon" && rec.APIID == 7 && len(rec.Open) == 1 && rec.Open[0].Cloid == "c1" {
			sawRecon = true
		}
	}
	if !sawCancel || !sawRecon {
		t.Fatalf("stdout %s", buf.String())
	}
}

func TestHelloSkipsUnsyncedOrInsideTheMarket(t *testing.T) {
	raw := []byte(`{"tickers":["Binance_Spot_BTCUSDT"],"api_id":7,"algo_id":1,"offset":50,"qty":0.01}`)
	unsynced := &fakeCtx{params: raw}
	h := &hello{}
	if err := h.OnInit(unsynced); err != nil {
		t.Fatal(err)
	}
	h.OnBook(unsynced, sdk.BookUpdate{MD: sdk.MD{UID: 1001}})
	if len(unsynced.submitted) != 0 {
		t.Fatal("unsynced book placed an order")
	}
	inside := &fakeCtx{params: raw, book: staticBook{bid: 40}, bookOK: true}
	h2 := &hello{}
	if err := h2.OnInit(inside); err != nil {
		t.Fatal(err)
	}
	h2.OnBook(inside, sdk.BookUpdate{MD: sdk.MD{UID: 1001}})
	if len(inside.submitted) != 0 {
		t.Fatal("non-positive price placed an order")
	}
}

type staticBook struct{ bid float64 }

func (b staticBook) TickSize() float64          { return 0.01 }
func (b staticBook) Window() int                { return 1 }
func (b staticBook) Price(int64) float64        { return b.bid }
func (b staticBook) BestBidTick() (int64, bool) { return 1, b.bid > 0 }
func (b staticBook) BestAskTick() (int64, bool) { return 0, false }
func (b staticBook) Qty(bool, float64) (float64, bool) {
	return 1, true
}
func (b staticBook) Bids(int) book.Iter { return book.Iter{} }
func (b staticBook) Asks(int) book.Iter { return book.Iter{} }

// fakeCtx is the slice of Context hello uses. The other methods are unused.
type fakeCtx struct {
	params    []byte
	resolved  []string
	book      sdk.BookView
	bookOK    bool
	submitted []sdk.Order
	cancelled []string
	open      []*tdv1.Order
}

func (f *fakeCtx) SessionID() string { return "c0ffee" }
func (f *fakeCtx) Params() []byte    { return f.params }
func (f *fakeCtx) Resolve(ticker string) (symbol.Info, error) {
	f.resolved = append(f.resolved, ticker)
	t, err := symbol.Parse(ticker)
	if err != nil {
		return symbol.Info{}, err
	}
	return symbol.Info{UID: 1001, Ticker: t, Status: symbol.Tradable}, nil
}
func (f *fakeCtx) Info(uint32) (symbol.Info, error) { return symbol.Info{}, nil }
func (f *fakeCtx) Submit(o sdk.Order) (string, error) {
	f.submitted = append(f.submitted, o)
	return "c1", nil
}
func (f *fakeCtx) Cancel(_ int64, id string) error {
	f.cancelled = append(f.cancelled, id)
	return nil
}
func (f *fakeCtx) BatchCancel(int64, uint32, ...uint32) error { return nil }
func (f *fakeCtx) RequestRecon(int64) error                   { return nil }
func (f *fakeCtx) Order(int64, string) (*tdv1.Order, bool)    { return nil, false }
func (f *fakeCtx) OpenOrders(int64, uint32) []*tdv1.Order     { return f.open }
func (f *fakeCtx) Position(int64, uint32) (*tdv1.Position, bool) {
	return nil, false
}
func (f *fakeCtx) Balance(int64, string) (*tdv1.Balance, bool) { return nil, false }
func (f *fakeCtx) Book(uint32) (sdk.BookView, bool) {
	if !f.bookOK {
		return nil, false
	}
	return f.book, true
}
func (f *fakeCtx) Tape(uint32, sdk.Topic, sdk.TapeQuery, func(sdk.Context, *mdv1.TapePage, error)) error {
	return nil
}
func (f *fakeCtx) BookSnapshot(uint32, func(sdk.Context, *mdv1.BookSnapshot, error)) error {
	return nil
}
func (f *fakeCtx) FetchKlines(uint32, string, uint32, func(sdk.Context, *mdv1.Klines, error)) error {
	return nil
}
func (f *fakeCtx) FetchOpenInterest(uint32, func(sdk.Context, *mdv1.OpenInterest, error)) error {
	return nil
}
func (f *fakeCtx) After(time.Duration, func(sdk.Context)) {}
func (f *fakeCtx) Post(func(sdk.Context))                 {}
func (f *fakeCtx) Exit()                                  {}
func (f *fakeCtx) Fail(string)                            {}
func (f *fakeCtx) VolumeDir() string                      { return "" }
func (f *fakeCtx) InputDir(string) string                 { return "" }
