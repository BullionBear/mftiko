// Command drill keeps one order working for the E12-12 testnet drill (#385): it places a buy limit far
// enough below the best bid that it rests, cancels it once the exchange has it on the book and it is
// cycle_s old, and places the next one. Every hook it sees is one JSON line on stderr, through
// ctx.Log (E15-D022), so each drill step can be read off the log: whether orders kept cycling,
// and what the strategy saw meanwhile.
//
// Params are the strategy's own JSON. The platform does not interpret them.
//
//	{"ticker":"BinanceUM_Perp_BTCUSDT","api_id":1,"algo_id":1,"offset":0.03,"qty":0.002,"cycle_s":10}
//
// offset is a fraction of the best bid: 0.03 is 3% below it, inside BinanceUM's price band, so the
// order is accepted and rests. A heartbeat line every 30 seconds shows the book still moving.
//
// The first OnRecon cancels every open order of algo_id on the account (BatchCancel, which needs the
// account's cancel_batch from a recon): an earlier incarnation's working order stays on the exchange
// when its worker is killed, since the platform does not cancel for the strategy (DECISIONS E06-D002).
// OnStop cannot cancel (the SDK refuses orders once the session is stopping), so the working order
// outlives the session. "sweep_only": true cancels and places nothing: run it before ending the drill
// to leave no order behind.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/sdk"
)

// drillParams is drill's own configuration. Field names are not a platform contract.
type drillParams struct {
	Ticker string  `json:"ticker"`
	APIID  int64   `json:"api_id"`
	AlgoID uint32  `json:"algo_id"`
	Offset float64 `json:"offset"`
	Qty    float64 `json:"qty"`
	CycleS float64 `json:"cycle_s"`
	// SweepOnly cancels the algo's open orders at the first recon and places none.
	SweepOnly bool `json:"sweep_only"`
}

const (
	heartbeat   = 30 * time.Second
	cancelRetry = 10 * time.Second // a cancel with no terminal status by then is sent again
)

type drill struct {
	p   drillParams
	uid uint32
	now func() time.Time

	cloid     string    // the working order; "" when none
	sentAt    time.Time // when it was submitted
	acceptAt  time.Time // when the exchange put it on the book; zero until then
	canceling bool
	cancelAt  time.Time // when the cancel was sent
	books     int       // book updates since the last heartbeat
	beatAt    time.Time
	swept     bool // the first recon's leftovers were cancelled
}

func (d *drill) clock() time.Time {
	if d.now != nil {
		return d.now()
	}
	return time.Now()
}

func (d *drill) OnInit(ctx sdk.Context) error {
	if err := json.Unmarshal(ctx.Params(), &d.p); err != nil {
		return err
	}
	switch {
	case d.p.Ticker == "", d.p.APIID == 0, d.p.AlgoID == 0:
		return fmt.Errorf("drill: ticker, api_id and algo_id are required")
	case !(d.p.Offset > 0 && d.p.Offset < 1), !(d.p.Qty > 0), !(d.p.CycleS > 0):
		return fmt.Errorf("drill: offset must be in (0, 1), qty and cycle_s above 0")
	}
	info, err := ctx.Resolve(d.p.Ticker)
	if err != nil {
		return err
	}
	d.uid = info.UID
	d.log(ctx, line{Hook: "OnInit", Ticker: d.p.Ticker})
	return nil
}

func (d *drill) OnStart(ctx sdk.Context) error {
	d.beatAt = d.clock()
	d.log(ctx, line{Hook: "OnStart"})
	return nil
}

func (d *drill) OnStop(ctx sdk.Context) { d.log(ctx, line{Hook: "OnStop", Cloid: d.cloid}) }

// cancelAll cancels the algo's open orders, this incarnation's and any an earlier one left.
func (d *drill) cancelAll(ctx sdk.Context) {
	if err := ctx.BatchCancel(d.p.APIID, d.p.AlgoID); err != nil {
		d.log(ctx, line{Hook: "batch_cancel_error", Reason: err.Error()})
	}
}

func (d *drill) OnBook(ctx sdk.Context, ev sdk.BookUpdate) {
	if ev.UID != d.uid {
		return
	}
	now := d.clock()
	d.books++
	if now.Sub(d.beatAt) >= heartbeat {
		d.log(ctx, line{Hook: "heartbeat", Books: d.books, Cloid: d.cloid})
		d.books, d.beatAt = 0, now
	}
	switch {
	case d.p.SweepOnly:
	case d.cloid == "":
		d.place(ctx, now)
	case d.canceling && now.Sub(d.cancelAt) >= cancelRetry:
		d.canceling = false // the cancel was refused or lost: the order is still live, cancel it again
	case !d.canceling && !d.acceptAt.IsZero() && now.Sub(d.acceptAt).Seconds() >= d.p.CycleS:
		d.canceling, d.cancelAt = true, now
		if err := ctx.Cancel(d.p.APIID, d.cloid); err != nil {
			d.log(ctx, line{Hook: "cancel_error", Cloid: d.cloid, Reason: err.Error()})
			d.canceling = false
		}
	}
}

// place submits a buy limit offset below the best bid, on a whole tick.
func (d *drill) place(ctx sdk.Context, now time.Time) {
	view, ok := ctx.Book(d.uid)
	if !ok || view == nil {
		return
	}
	tick, ok := view.BestBidTick()
	if !ok || !(view.TickSize() > 0) {
		return
	}
	bid := view.Price(tick)
	price := view.Price(tick - int64(math.Ceil(bid*d.p.Offset/view.TickSize())))
	if !(price > 0) {
		return
	}
	id, err := ctx.Submit(sdk.Order{
		APIID:    d.p.APIID,
		AlgoID:   d.p.AlgoID,
		UID:      d.uid,
		Side:     commonv1.Side_SIDE_BUY,
		Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
		TIF:      commonv1.TimeInForce_TIME_IN_FORCE_GTC,
		Qty:      d.p.Qty,
		QuoteQty: decimal.Absent(),
		Price:    price,
	})
	if err != nil {
		d.log(ctx, line{Hook: "submit_error", Reason: err.Error()})
		return
	}
	d.cloid, d.sentAt, d.acceptAt, d.canceling = id, now, time.Time{}, false
	d.log(ctx, line{Hook: "submit", Cloid: id, Price: price})
}

func (d *drill) OnOrder(ctx sdk.Context, ev sdk.OrderEvent) {
	if ev.Order == nil {
		return
	}
	o := ev.Order
	d.log(ctx, line{Hook: "OnOrder", Cloid: o.GetClientOrderId(), Status: o.GetStatus().String(),
		Cause: ev.Cause.String(), Code: ev.Code, Reason: ev.Reason})
	if o.GetClientOrderId() != d.cloid {
		return
	}
	switch o.GetStatus() {
	case tdv1.OrderStatus_ORDER_STATUS_ACCEPTED:
		if d.acceptAt.IsZero() {
			d.acceptAt = d.clock()
		}
	default:
		if terminal(o.GetStatus()) {
			d.cloid = ""
		}
	}
}

func (d *drill) OnFill(ctx sdk.Context, ev sdk.FillEvent) {
	d.log(ctx, line{Hook: "OnFill", Cloid: ev.Fill.GetClientOrderId()})
}

func (d *drill) OnRecon(ctx sdk.Context, ev sdk.ReconEvent) {
	if !d.swept {
		d.swept = true
		d.cancelAll(ctx)
	}
	open := 0
	if ev.Recon != nil {
		open = len(ev.Recon.GetOpenOrders())
		// The working order may have ended while the strategy could not hear about it.
		found := false
		for _, o := range ev.Recon.GetOpenOrders() {
			if o.GetClientOrderId() == d.cloid {
				found = true
			}
		}
		if d.cloid != "" && !found && !d.sentAt.IsZero() && d.clock().Sub(d.sentAt) > 5*time.Second {
			d.cloid = ""
		}
	}
	d.log(ctx, line{Hook: "OnRecon", Open: &open})
}

func (d *drill) OnReject(ctx sdk.Context, ev sdk.RejectEvent) {
	d.log(ctx, line{Hook: "OnReject", Code: ev.Code, Reason: ev.Reason})
	for _, id := range ev.ClientOrderIDs {
		if id == d.cloid {
			if ev.Request == sdk.RejectedCancel {
				d.canceling = false
			} else {
				d.cloid = ""
			}
		}
	}
}

func terminal(s tdv1.OrderStatus) bool {
	switch s {
	case tdv1.OrderStatus_ORDER_STATUS_CANCELLED, tdv1.OrderStatus_ORDER_STATUS_FILLED,
		tdv1.OrderStatus_ORDER_STATUS_REJECTED, tdv1.OrderStatus_ORDER_STATUS_EXPIRED:
		return true
	default:
		return false
	}
}

// line is one log line. Empty fields are left out. The time is the logger's time field.
type line struct {
	Hook   string
	Ticker string
	Cloid  string
	Status string
	Cause  string
	Code   int32
	Reason string
	Price  float64
	Books  int
	Open   *int
}

func (d *drill) log(ctx sdk.Context, l line) {
	log := ctx.Log()
	ev := log.Info().Str("hook", l.Hook)
	if l.Ticker != "" {
		ev = ev.Str("ticker", l.Ticker)
	}
	if l.Cloid != "" {
		ev = ev.Str("cloid", l.Cloid)
	}
	if l.Status != "" {
		ev = ev.Str("status", l.Status)
	}
	if l.Cause != "" {
		ev = ev.Str("cause", l.Cause)
	}
	if l.Code != 0 {
		ev = ev.Int32("code", l.Code)
	}
	if l.Reason != "" {
		ev = ev.Str("reason", l.Reason)
	}
	if l.Price != 0 {
		ev = ev.Float64("price", l.Price)
	}
	if l.Books != 0 {
		ev = ev.Int("books", l.Books)
	}
	if l.Open != nil {
		ev = ev.Int("open", *l.Open)
	}
	ev.Msg("drill")
}

var (
	_ sdk.Strategy      = (*drill)(nil)
	_ sdk.BookHandler   = (*drill)(nil)
	_ sdk.OrderHandler  = (*drill)(nil)
	_ sdk.FillHandler   = (*drill)(nil)
	_ sdk.ReconHandler  = (*drill)(nil)
	_ sdk.RejectHandler = (*drill)(nil)
)

func main() { sdk.Run(&drill{}) }
