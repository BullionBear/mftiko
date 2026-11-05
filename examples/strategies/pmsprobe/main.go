// Command pmsprobe places market orders for the PMS acceptance (E21-09, #521). It is a test
// strategy, not an example of how to trade.
//
// Params are the strategy's own JSON. The platform does not interpret them.
//
//	{"ticker":"BinanceUM_Perp_BTCUSDT","api_id":7,"algo_id":1,"qty":0.002,"mode":"roundtrip","exit_after_ms":5}
//
// On the first OnRecon of api_id it submits a market buy of qty.
//
//   - mode "roundtrip": when the buy is FILLED it submits a reduce-only market sell of the same qty,
//     and when that is FILLED it logs "roundtrip done". Then it idles until the session is ended.
//   - mode "exit": it logs "submitted" with the cloid, waits exit_after_ms (default 5) and exits 0.
//     Submit returns before the request is sent, and an accepted reply runs no hook, so the wait
//     is what lets the SDK put the request on the connection. TD needs only the request, not the
//     requester. The venue sees the order tens of milliseconds later and every td.evt of it comes
//     after that, when no process is subscribed: all of them are dropped, and the fill lands after
//     the session's last_exit. Use restart mode never.
//
// Lines go to stderr through ctx.Log: {"probe":"submitted"|"filled"|"roundtrip done","cloid":...}.
package main

import (
	"encoding/json"
	"os"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/sdk"
)

// params is pmsprobe's own configuration. Field names are not a platform contract.
type params struct {
	Ticker string  `json:"ticker"`
	APIID  int64   `json:"api_id"`
	AlgoID uint32  `json:"algo_id"`
	Qty    float64 `json:"qty"`
	Mode   string  `json:"mode"`
	// ExitAfterMs is how long mode exit waits after Submit; 0 is 5.
	ExitAfterMs int `json:"exit_after_ms"`
}

type probe struct {
	p       params
	uid     uint32
	started bool
	buy     string
	sell    string
	// exit is os.Exit; a test replaces it.
	exit func(int)
}

func (s *probe) OnInit(ctx sdk.Context) error {
	if err := json.Unmarshal(ctx.Params(), &s.p); err != nil {
		return err
	}
	info, err := ctx.Resolve(s.p.Ticker)
	if err != nil {
		return err
	}
	s.uid = info.UID
	return nil
}

func (s *probe) OnStart(sdk.Context) error { return nil }

func (s *probe) OnStop(sdk.Context) {}

func (s *probe) OnRecon(ctx sdk.Context, ev sdk.ReconEvent) {
	if s.started || ev.APIID != s.p.APIID {
		return
	}
	s.started = true
	id, err := s.market(ctx, commonv1.Side_SIDE_BUY, false)
	log := ctx.Log()
	if err != nil {
		log.Error().Err(err).Msg("probe submit")
		return
	}
	s.buy = id
	log.Info().Str("probe", "submitted").Str("cloid", id).Msg("probe")
	if s.p.Mode == "exit" {
		wait := time.Duration(s.p.ExitAfterMs) * time.Millisecond
		if wait <= 0 {
			wait = 5 * time.Millisecond
		}
		time.Sleep(wait)
		s.exit(0)
	}
}

func (s *probe) OnOrder(ctx sdk.Context, ev sdk.OrderEvent) {
	o := ev.Order
	if o == nil || o.GetStatus() != tdv1.OrderStatus_ORDER_STATUS_FILLED {
		return
	}
	log := ctx.Log()
	switch o.GetClientOrderId() {
	case s.buy:
		log.Info().Str("probe", "filled").Str("cloid", s.buy).Msg("probe")
		if s.p.Mode != "roundtrip" || s.sell != "" {
			return
		}
		id, err := s.market(ctx, commonv1.Side_SIDE_SELL, true)
		if err != nil {
			log.Error().Err(err).Msg("probe submit")
			return
		}
		s.sell = id
		log.Info().Str("probe", "submitted").Str("cloid", id).Msg("probe")
	case s.sell:
		log.Info().Str("probe", "roundtrip done").Str("cloid", s.sell).Msg("probe")
	}
}

func (s *probe) market(ctx sdk.Context, side commonv1.Side, reduce bool) (string, error) {
	return ctx.Submit(sdk.Order{
		APIID: s.p.APIID, AlgoID: s.p.AlgoID, UID: s.uid,
		Side: side, Type: commonv1.OrderType_ORDER_TYPE_MARKET,
		Qty: s.p.Qty, QuoteQty: decimal.Absent(), Price: decimal.Absent(),
		ReduceOnly: reduce,
	})
}

var (
	_ sdk.Strategy     = (*probe)(nil)
	_ sdk.ReconHandler = (*probe)(nil)
	_ sdk.OrderHandler = (*probe)(nil)
)

func main() { sdk.Run(&probe{exit: os.Exit}) }
