// Command logevents writes the stderr lines that the log alert rules R1–R5 match, one rule per timer
// tick (DECISIONS E16-D005, E16-D006, E16-D014–E16-D019), so the alerting can be tried on make dev-up.
// It places no orders and binds nothing: the session needs no feeds and no accounts.
//
// Params are the strategy's own JSON. The platform does not interpret them.
//
//	{"rules":["R1","R2","R3","R4","R5"],"interval_ms":1000,"pnl":[5,-2,-12],"floor":-10,"ccy":"USDT"}
//
// rules picks which rules to write, all five when omitted. From OnStart, a timer hook writes the next
// picked rule every interval_ms, in the order R1–R5 and each once. interval_ms is 1000 when omitted.
// pnl, floor and ccy are required when R4 is picked: the floor is in params so that the session's
// spec records it (E16-D007).
//
//   - R1: a warn and an error without event.
//   - R2: the line "panic: logevents simulated (not a real panic)", written straight to stderr. It is
//     simulated: nothing panics, and the session keeps running (E16-D016).
//   - R3: order_rejected and order_unknown. They are simulated: dev has no account, so no order is
//     behind them, and their values are fixed (E16-D019).
//   - R4: pnl for each value of pnl, then pnl_floor_breached when that value is below floor. The
//     values stand in for the PnL a strategy computes.
//   - R5: the hook sleeps 150ms, so the SDK writes "hook exceeded 100ms" (E16-D017). That is far
//     below hook_timeout_s: the session does not end HUNG.
//
// The JSON lines carry session_id: logevents writes through ctx.Log, and the SDK's own line has it
// too. After the last rule logevents writes nothing more and keeps running until the session is
// ended, so mftiko session logs still finds the lines.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/sdk"
)

// logeventsParams is logevents' own configuration. Field names are not a platform contract.
type logeventsParams struct {
	Rules      []string  `json:"rules"`
	IntervalMs int64     `json:"interval_ms"`
	PnL        []float64 `json:"pnl"`
	Floor      *float64  `json:"floor"`
	Ccy        string    `json:"ccy"`
}

var allRules = []string{"R1", "R2", "R3", "R4", "R5"}

const (
	defaultInterval = time.Second
	// slowHook is past the SDK's 100ms soft limit (E08-D017) and far below hook_timeout_s.
	slowHook  = 150 * time.Millisecond
	panicLine = "panic: logevents simulated (not a real panic)\n"
)

// The fixed values of the simulated R3 lines. No order of this platform has these client_order_ids:
// they decode to version 0 (PROTOCOL §2).
const (
	simCode     = int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER)
	simReason   = "simulated by logevents"
	simRejected = "1"
	simUnknown  = "2"
)

type logevents struct {
	p     logeventsParams
	steps []string
	every time.Duration
	errw  io.Writer
	sleep func(time.Duration)
}

func (s *logevents) OnInit(ctx sdk.Context) error {
	if raw := ctx.Params(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &s.p); err != nil {
			return err
		}
	}
	for _, r := range s.p.Rules {
		if !slices.Contains(allRules, r) {
			return fmt.Errorf("rules: %q is not one of R1–R5", r)
		}
	}
	s.steps = nil
	for _, r := range allRules {
		if s.p.Rules == nil || slices.Contains(s.p.Rules, r) {
			s.steps = append(s.steps, r)
		}
	}
	if len(s.steps) == 0 {
		return errors.New("rules is empty")
	}
	if s.p.IntervalMs < 0 {
		return errors.New("interval_ms must not be negative")
	}
	s.every = time.Duration(s.p.IntervalMs) * time.Millisecond
	if s.every == 0 {
		s.every = defaultInterval
	}
	if slices.Contains(s.steps, "R4") && (len(s.p.PnL) == 0 || s.p.Floor == nil || s.p.Ccy == "") {
		return errors.New("R4 needs pnl, floor and ccy")
	}
	if s.errw == nil {
		s.errw = os.Stderr
	}
	if s.sleep == nil {
		s.sleep = time.Sleep
	}
	return nil
}

func (s *logevents) OnStart(ctx sdk.Context) error {
	s.tick(ctx, 0)
	return nil
}

func (s *logevents) OnStop(sdk.Context) {}

// tick arms the timer that writes steps[i] and then arms the next. After the last step, it arms nothing.
func (s *logevents) tick(ctx sdk.Context, i int) {
	if i >= len(s.steps) {
		return
	}
	ctx.After(s.every, func(ctx sdk.Context) {
		s.write(ctx, s.steps[i])
		s.tick(ctx, i+1)
	})
}

func (s *logevents) write(ctx sdk.Context, rule string) {
	log := ctx.Log()
	switch rule {
	case "R1":
		log.Warn().Msg("R1: warn without event")
		log.Error().Msg("R1: error without event")
	case "R2":
		_, _ = io.WriteString(s.errw, panicLine)
	case "R3":
		log.Warn().Str("event", "order_rejected").Int32("code", simCode).Str("reason", simReason).
			Str("client_order_id", simRejected).Msg("R3: order rejected (simulated)")
		log.Error().Str("event", "order_unknown").Str("client_order_id", simUnknown).
			Msg("R3: order outcome unknown (simulated)")
	case "R4":
		floor := *s.p.Floor
		for _, pnl := range s.p.PnL {
			log.Info().Str("event", "pnl").Float64("pnl", pnl).Str("ccy", s.p.Ccy).Msg("R4: pnl")
			if pnl < floor {
				log.Error().Str("event", "pnl_floor_breached").Float64("pnl", pnl).Float64("floor", floor).
					Str("ccy", s.p.Ccy).Msg("R4: pnl below floor")
			}
		}
	case "R5":
		s.sleep(slowHook)
	}
}

var _ sdk.Strategy = (*logevents)(nil)

func main() { sdk.Run(&logevents{}) }
