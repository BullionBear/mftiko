package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/sdk"
	"github.com/BullionBear/mftiko/sdk/book"
)

const params = `{"ticker":"BinanceUM_Perp_BTCUSDT","api_id":1,"algo_id":2,"offset":0.03,"qty":0.002,"cycle_s":10}`

// started is a drill that has run OnInit and OnStart at t0 on a book with the best bid at 100000.0.
func started(t *testing.T) (*drill, *fakeCtx, *time.Time, *bytes.Buffer) {
	t.Helper()
	now := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	d := &drill{now: func() time.Time { return now }}
	f := &fakeCtx{params: []byte(params), book: tickBook{bid: 1_000_000}, bookOK: true, logs: &buf}
	if err := d.OnInit(f); err != nil {
		t.Fatal(err)
	}
	if err := d.OnStart(f); err != nil {
		t.Fatal(err)
	}
	return d, f, &now, &buf
}

func book1001(d *drill, f *fakeCtx) { d.OnBook(f, sdk.BookUpdate{MD: sdk.MD{UID: 1001}}) }

func order(id string, s tdv1.OrderStatus) sdk.OrderEvent {
	return sdk.OrderEvent{Order: &tdv1.Order{ClientOrderId: id, Status: s}}
}

// One cycle: place 3% below the bid on a whole tick, wait for ACCEPTED and cycle_s, cancel, and after
// CANCELLED place the next.
func TestDrillCycles(t *testing.T) {
	d, f, now, _ := started(t)
	book1001(d, f)
	book1001(d, f)
	if len(f.submitted) != 1 {
		t.Fatalf("submitted %d, want 1 while the first is working", len(f.submitted))
	}
	got := f.submitted[0]
	if got.APIID != 1 || got.AlgoID != 2 || got.UID != 1001 || got.Qty != 0.002 || fmt.Sprintf("%.1f", got.Price) != "97000.0" {
		t.Fatalf("order %+v", got)
	}
	d.OnOrder(f, order("c1", tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
	*now = now.Add(9 * time.Second)
	book1001(d, f)
	if len(f.cancelled) != 0 {
		t.Fatal("cancelled before cycle_s")
	}
	*now = now.Add(time.Second)
	book1001(d, f)
	book1001(d, f)
	if len(f.cancelled) != 1 || f.cancelled[0] != "c1" {
		t.Fatalf("cancels %v, want c1 once", f.cancelled)
	}
	d.OnOrder(f, order("c1", tdv1.OrderStatus_ORDER_STATUS_CANCELLED))
	book1001(d, f)
	if len(f.submitted) != 2 {
		t.Fatalf("submitted %d after the cancel, want 2", len(f.submitted))
	}
}

// A cancel that gets no terminal status within cancelRetry is sent again.
func TestDrillRetriesCancel(t *testing.T) {
	d, f, now, _ := started(t)
	book1001(d, f)
	d.OnOrder(f, order("c1", tdv1.OrderStatus_ORDER_STATUS_ACCEPTED))
	*now = now.Add(10 * time.Second)
	book1001(d, f)
	*now = now.Add(cancelRetry)
	book1001(d, f) // the retry window passed: canceling is cleared
	book1001(d, f) // and the cancel goes again
	if len(f.cancelled) != 2 {
		t.Fatalf("cancels %v, want 2", f.cancelled)
	}
}

// A rejected order, or one a recon no longer lists, frees the slot for the next.
func TestDrillAfterLoss(t *testing.T) {
	d, f, now, _ := started(t)
	book1001(d, f)
	d.OnOrder(f, order("c1", tdv1.OrderStatus_ORDER_STATUS_REJECTED))
	book1001(d, f)
	if len(f.submitted) != 2 {
		t.Fatalf("submitted %d after a reject, want 2", len(f.submitted))
	}
	*now = now.Add(6 * time.Second)
	d.OnRecon(f, sdk.ReconEvent{TD: sdk.TD{APIID: 1}, Recon: &tdv1.Recon{}})
	book1001(d, f)
	if len(f.submitted) != 3 {
		t.Fatalf("submitted %d after a recon without c2, want 3", len(f.submitted))
	}
}

// Every hook is one JSON line, and a heartbeat comes every 30 seconds of book updates.
func TestDrillLog(t *testing.T) {
	d, f, now, buf := started(t)
	book1001(d, f)
	*now = now.Add(heartbeat)
	book1001(d, f)
	var hooks []string
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var v struct {
			Hook string `json:"hook"`
			Time string `json:"time"`
		}
		if err := json.Unmarshal([]byte(l), &v); err != nil || v.Time == "" {
			t.Fatalf("line %q: %v", l, err)
		}
		hooks = append(hooks, v.Hook)
	}
	if strings.Join(hooks, ",") != "OnInit,OnStart,submit,heartbeat" {
		t.Fatalf("hooks %v", hooks)
	}
}

// The first recon cancels the algo's open orders, so an earlier incarnation's order does not stay on the
// exchange. sweep_only then places nothing.
func TestDrillCancelsLeftovers(t *testing.T) {
	d, f, _, _ := started(t)
	if len(f.batches) != 0 {
		t.Fatalf("batch cancel %v before the first recon", f.batches)
	}
	recon := sdk.ReconEvent{TD: sdk.TD{APIID: 1}, Recon: &tdv1.Recon{}}
	d.OnRecon(f, recon)
	d.OnRecon(f, recon)
	if strings.Join(f.batches, ",") != "1/2" {
		t.Fatalf("batch cancels %v, want one at the first recon", f.batches)
	}

	f2 := &fakeCtx{params: []byte(strings.Replace(params, `"cycle_s":10`, `"cycle_s":10,"sweep_only":true`, 1)), book: tickBook{bid: 1_000_000}, bookOK: true}
	d2 := &drill{}
	if err := d2.OnInit(f2); err != nil {
		t.Fatal(err)
	}
	book1001(d2, f2)
	if len(f2.submitted) != 0 {
		t.Fatalf("sweep_only submitted %d", len(f2.submitted))
	}
}

func TestDrillRejectsBadParams(t *testing.T) {
	for _, p := range []string{`{`, `{"ticker":"BinanceUM_Perp_BTCUSDT","api_id":1,"algo_id":1,"offset":1,"qty":1,"cycle_s":1}`, `{"api_id":1}`} {
		if err := (&drill{}).OnInit(&fakeCtx{params: []byte(p)}); err == nil {
			t.Errorf("params %s were accepted", p)
		}
	}
}

// tickBook is a book whose best bid is at tick bid; a tick is 0.1.
type tickBook struct{ bid int64 }

func (b tickBook) TickSize() float64          { return 0.1 }
func (b tickBook) Window() int                { return 1 }
func (b tickBook) Price(t int64) float64      { return float64(t) * 0.1 }
func (b tickBook) BestBidTick() (int64, bool) { return b.bid, b.bid > 0 }
func (b tickBook) BestAskTick() (int64, bool) { return 0, false }
func (b tickBook) Qty(bool, float64) (float64, bool) {
	return 1, true
}
func (b tickBook) Bids(int) book.Iter { return book.Iter{} }
func (b tickBook) Asks(int) book.Iter { return book.Iter{} }

// fakeCtx is the slice of Context drill uses. The other methods are unused.
type fakeCtx struct {
	params    []byte
	resolved  []string
	book      sdk.BookView
	bookOK    bool
	submitted []sdk.Order
	cancelled []string
	open      []*tdv1.Order
	batches   []string
	logs      *bytes.Buffer
}

func (f *fakeCtx) Log() zerolog.Logger {
	if f.logs == nil {
		return zerolog.Nop()
	}
	return zerolog.New(f.logs).With().Timestamp().Logger()
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
	return fmt.Sprintf("c%d", len(f.submitted)), nil
}
func (f *fakeCtx) Cancel(_ int64, id string) error {
	f.cancelled = append(f.cancelled, id)
	return nil
}
func (f *fakeCtx) BatchCancel(apiID int64, algoID uint32, _ ...uint32) error {
	f.batches = append(f.batches, fmt.Sprintf("%d/%d", apiID, algoID))
	return nil
}
func (f *fakeCtx) RequestRecon(int64) error                { return nil }
func (f *fakeCtx) Order(int64, string) (*tdv1.Order, bool) { return nil, false }
func (f *fakeCtx) OpenOrders(int64, uint32) []*tdv1.Order  { return f.open }
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
