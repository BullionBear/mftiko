// Command conform is the Go standard conformance strategy (PROTOCOL §9.8, E08-D029, E08-D035).
//
// InitPayload.params selects the behavior. Buffer limits, the prehook, the
// recon watermark, and deadline_ms aborts belong to the SDK. This process
// sleeps, submits, and reads Context. For the checks that have to see events
// only after subscriptions are up, OnStart calls Resolve, and priority calls
// RequestRecon first. The harness holds that Resolve. sdk.Run prints gate once
// subscriptions are up. CI runs this process through mftiko-conformance on
// every pull request (E08-D064).
package main

import (
	"encoding/json"
	"os"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/sdk"
)

// params is the JSON object in InitPayload.params. The tags are the
// cross-language contract in PROTOCOL §9.8.
type params struct {
	HookSleepMS   int    `json:"hook_sleep_ms,omitempty"`
	OrderCount    int    `json:"order_count,omitempty"`
	InitSleepMS   int    `json:"init_sleep_ms,omitempty"`
	StartSleepMS  int    `json:"start_sleep_ms,omitempty"`
	OnStopSleepMS int    `json:"on_stop_sleep_ms,omitempty"`
	SelfExit      bool   `json:"self_exit,omitempty"`
	ProbeReads    bool   `json:"probe_reads,omitempty"`
	Scenario      string `json:"scenario,omitempty"`
	UntilSeq      uint64 `json:"until_seq,omitempty"`
	APIID         int64  `json:"api_id,omitempty"`
	AlgoID        uint32 `json:"algo_id,omitempty"`
	UID           uint32 `json:"uid,omitempty"`
}

// conformTicker is the fixture instrument (PROTOCOL §9.8). It is not a params
// field. Resolve uses it so the harness can hold OnStart.
const conformTicker = "Binance_Spot_BTCUSDT"

type observation struct {
	Hook        string `json:"hook,omitempty"`
	Ctx         *bool  `json:"ctx,omitempty"`
	Seq         uint64 `json:"seq,omitempty"`
	Incarnation uint32 `json:"incarnation,omitempty"`
	View        *bool  `json:"view,omitempty"`
	Fetch       *bool  `json:"fetch,omitempty"`
}

type conform struct {
	p params
}

func (c *conform) OnInit(ctx sdk.Context) error {
	if len(ctx.Params()) > 0 {
		if err := json.Unmarshal(ctx.Params(), &c.p); err != nil {
			return err
		}
	}
	sleepMS(c.p.InitSleepMS)
	writeObs(observation{Hook: "OnInit"})
	return nil
}

func (c *conform) OnStart(ctx sdk.Context) error {
	if c.p.Scenario == "beat" {
		writeObs(observation{Hook: "OnStart"})
		sleepMS(c.p.HookSleepMS)
	} else {
		sleepMS(c.p.StartSleepMS)
	}
	if c.p.Scenario == "priority" {
		if err := ctx.RequestRecon(c.p.APIID); err != nil {
			return err
		}
	}
	if needsHold(c.p.Scenario) {
		if _, err := ctx.Resolve(conformTicker); err != nil {
			return err
		}
	}
	for range c.p.OrderCount {
		if _, err := ctx.Submit(sdk.Order{
			APIID:  c.p.APIID,
			AlgoID: c.p.AlgoID,
			UID:    c.p.UID,
			Side:   commonv1.Side_SIDE_BUY,
			Type:   commonv1.OrderType_ORDER_TYPE_LIMIT,
			TIF:    commonv1.TimeInForce_TIME_IN_FORCE_GTC,
			Qty:    1,
			Price:  1,
		}); err != nil {
			return err
		}
	}
	if c.p.ProbeReads {
		_ = ctx.Tape(c.p.UID, sdk.TopicTrade, sdk.TapeQuery{Limit: 1}, func(sdk.Context, *mdv1.TapePage, error) {
			ok := true
			writeObs(observation{View: &ok})
		})
		_ = ctx.FetchKlines(c.p.UID, "1m", 1, func(sdk.Context, *mdv1.Klines, error) {
			ok := true
			writeObs(observation{Fetch: &ok})
		})
	}
	if c.p.Scenario != "beat" {
		writeObs(observation{Hook: "OnStart"})
		sleepMS(c.p.HookSleepMS)
	}
	if c.p.SelfExit {
		ctx.Exit()
	}
	return nil
}

func (c *conform) OnStop(sdk.Context) {
	sleepMS(c.p.OnStopSleepMS)
	writeObs(observation{Hook: "OnStop"})
}

// needsHold is the scenarios whose events must sit in the SDK buffers until
// OnStart returns. The harness blocks Resolve for those, and only those.
func needsHold(scenario string) bool {
	switch scenario {
	case "priority", "prehook", "ring", "overflow", "ctx":
		return true
	default:
		return false
	}
}

func (c *conform) OnTrade(_ sdk.Context, ev sdk.Trade) {
	writeObs(observation{Hook: "OnTrade", Seq: ev.Seq})
}

func (c *conform) OnOrder(ctx sdk.Context, ev sdk.OrderEvent) {
	id := ""
	if ev.Order != nil {
		id = ev.Order.GetClientOrderId()
	}
	_, ok := ctx.Order(ev.APIID, id)
	writeObs(observation{Hook: "OnOrder", Ctx: &ok, Seq: ev.Seq, Incarnation: ev.Incarnation})
	if c.p.Scenario == "ctx" && !ok {
		ctx.Fail("ctx")
	}
}

func (c *conform) OnPosition(ctx sdk.Context, ev sdk.PositionEvent) {
	var uid uint32
	if len(ev.Positions) > 0 && ev.Positions[0] != nil {
		uid = ev.Positions[0].GetUid()
	}
	_, ok := ctx.Position(ev.APIID, uid)
	writeObs(observation{Hook: "OnPosition", Ctx: &ok, Seq: ev.Seq, Incarnation: ev.Incarnation})
	if c.p.Scenario == "ctx" && !ok {
		ctx.Fail("ctx")
	}
}

func (c *conform) OnRecon(sdk.Context, sdk.ReconEvent) {
	writeObs(observation{Hook: "OnRecon"})
}

func writeObs(o observation) {
	raw, err := json.Marshal(o)
	if err != nil {
		return
	}
	_, _ = os.Stdout.Write(append(raw, '\n'))
}

func sleepMS(ms int) {
	if ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

var (
	_ sdk.Strategy        = (*conform)(nil)
	_ sdk.TradeHandler    = (*conform)(nil)
	_ sdk.OrderHandler    = (*conform)(nil)
	_ sdk.PositionHandler = (*conform)(nil)
	_ sdk.ReconHandler    = (*conform)(nil)
)

func main() { sdk.Run(&conform{}) }
