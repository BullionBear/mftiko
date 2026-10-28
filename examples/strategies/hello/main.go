// Command hello is the reference strategy (E08-D062).
//
// Params are the strategy's own JSON. The platform does not interpret them.
//
//	{"tickers":["Binance_Spot_BTCUSDT"],"api_id":7,"algo_id":1,"offset":1000,"qty":0.001}
//
// OnInit resolves each ticker. OnBook reads the local book and, once, places a
// buy limit offset below the best bid, far enough that it rests. OnRecon logs
// the live orders. When OnOrder says that order is on the book
// (ORDER_STATUS_ACCEPTED), hello cancels it, and a later OnOrder observes the cancel.
// Those lines go to stderr through ctx.Log (E15-D022).
package main

import (
	"encoding/json"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/sdk"
)

// helloParams is hello's own configuration. Field names are not a platform contract.
type helloParams struct {
	Tickers []string `json:"tickers"`
	APIID   int64    `json:"api_id"`
	AlgoID  uint32   `json:"algo_id"`
	Offset  float64  `json:"offset"`
	Qty     float64  `json:"qty"`
}

type hello struct {
	p         helloParams
	uids      []uint32
	placed    bool
	cloid     string
	cancelled bool
}

func (h *hello) OnInit(ctx sdk.Context) error {
	raw := ctx.Params()
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, &h.p); err != nil {
		return err
	}
	for _, ticker := range h.p.Tickers {
		info, err := ctx.Resolve(ticker)
		if err != nil {
			return err
		}
		h.uids = append(h.uids, info.UID)
	}
	return nil
}

func (h *hello) OnStart(sdk.Context) error { return nil }

func (h *hello) OnStop(sdk.Context) {}

func (h *hello) OnBook(ctx sdk.Context, ev sdk.BookUpdate) {
	if h.placed || h.p.Offset <= 0 || h.p.Qty <= 0 || h.p.APIID == 0 || h.p.AlgoID == 0 {
		return
	}
	if !listed(h.uids, ev.UID) {
		return
	}
	view, ok := ctx.Book(ev.UID)
	if !ok || view == nil {
		return
	}
	tick, ok := view.BestBidTick()
	if !ok {
		return
	}
	price := view.Price(tick) - h.p.Offset
	if !(price > 0) {
		return
	}
	id, err := ctx.Submit(sdk.Order{
		APIID:    h.p.APIID,
		AlgoID:   h.p.AlgoID,
		UID:      ev.UID,
		Side:     commonv1.Side_SIDE_BUY,
		Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
		TIF:      commonv1.TimeInForce_TIME_IN_FORCE_GTC,
		Qty:      h.p.Qty,
		QuoteQty: decimal.Absent(),
		Price:    price,
	})
	if err != nil {
		return
	}
	h.placed = true
	h.cloid = id
}

func (h *hello) OnRecon(ctx sdk.Context, ev sdk.ReconEvent) {
	line := reconLine(ev)
	log := ctx.Log()
	log.Info().Str("hook", line.Hook).Int64("api_id", line.APIID).Interface("open", line.Open).Msg("recon")
}

func (h *hello) OnOrder(ctx sdk.Context, ev sdk.OrderEvent) {
	if ev.Order == nil || ev.Order.GetClientOrderId() != h.cloid || h.cloid == "" {
		return
	}
	switch ev.Order.GetStatus() {
	case tdv1.OrderStatus_ORDER_STATUS_ACCEPTED:
		if h.cancelled {
			return
		}
		h.cancelled = true
		_ = ctx.Cancel(h.p.APIID, h.cloid)
	case tdv1.OrderStatus_ORDER_STATUS_CANCELLED:
		line := cancelLine(ev.Order)
		log := ctx.Log()
		log.Info().Str("hook", "OnOrder").Str("cloid", line.Cloid).Str("status", line.Status).Msg("order")
	}
}

func listed(uids []uint32, uid uint32) bool {
	for _, id := range uids {
		if id == uid {
			return true
		}
	}
	return false
}

type reconPrint struct {
	Hook  string       `json:"hook"`
	APIID int64        `json:"api_id"`
	Open  []orderPrint `json:"open"`
}

type orderPrint struct {
	Cloid  string `json:"cloid"`
	Status string `json:"status"`
}

func reconLine(ev sdk.ReconEvent) reconPrint {
	out := reconPrint{Hook: "OnRecon", APIID: ev.APIID, Open: []orderPrint{}}
	if ev.Recon == nil {
		return out
	}
	for _, o := range ev.Recon.GetOpenOrders() {
		if o == nil {
			continue
		}
		out.Open = append(out.Open, orderPrint{
			Cloid:  o.GetClientOrderId(),
			Status: o.GetStatus().String(),
		})
	}
	return out
}

func cancelLine(o *tdv1.Order) orderPrint {
	return orderPrint{Cloid: o.GetClientOrderId(), Status: o.GetStatus().String()}
}

var (
	_ sdk.Strategy     = (*hello)(nil)
	_ sdk.BookHandler  = (*hello)(nil)
	_ sdk.ReconHandler = (*hello)(nil)
	_ sdk.OrderHandler = (*hello)(nil)
)

func main() { sdk.Run(&hello{}) }
