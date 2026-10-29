package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/wire/cloid"
	"github.com/BullionBear/mftiko/sdk"
)

// fakeCtx is the slice of Context logevents uses. The other methods are unused. Log carries
// session_id as the SDK's does, and writes to the buffer the strategy's stderr also goes to.
type fakeCtx struct {
	sdk.Context
	params []byte
	out    bytes.Buffer
	timers []timer
	slept  []time.Duration
	ended  bool
}

type timer struct {
	d  time.Duration
	fn func(sdk.Context)
}

func (f *fakeCtx) Params() []byte { return f.params }
func (f *fakeCtx) Log() zerolog.Logger {
	return zerolog.New(&f.out).With().Timestamp().Str("session_id", "c0ffee").Logger()
}
func (f *fakeCtx) After(d time.Duration, fn func(sdk.Context)) {
	f.timers = append(f.timers, timer{d: d, fn: fn})
}
func (f *fakeCtx) Exit()       { f.ended = true }
func (f *fakeCtx) Fail(string) { f.ended = true }

// start runs OnInit and OnStart. The strategy records each sleep in slept instead of sleeping.
func start(t *testing.T, params string) (*logevents, *fakeCtx) {
	t.Helper()
	f := &fakeCtx{params: []byte(params)}
	s := &logevents{errw: &f.out, sleep: func(d time.Duration) { f.slept = append(f.slept, d) }}
	if err := s.OnInit(f); err != nil {
		t.Fatal(err)
	}
	if err := s.OnStart(f); err != nil {
		t.Fatal(err)
	}
	if f.out.Len() != 0 {
		t.Fatalf("OnStart wrote %q", f.out.String())
	}
	return s, f
}

// fire runs the one armed timer, as the dispatcher would after d, and returns what its hook wrote.
func (f *fakeCtx) fire(t *testing.T, d time.Duration) []map[string]any {
	t.Helper()
	if len(f.timers) != 1 || f.timers[0].d != d {
		t.Fatalf("armed %v, want one timer of %s", f.timers, d)
	}
	tm := f.timers[0]
	f.timers = nil
	f.out.Reset()
	tm.fn(f)
	return rows(t, f.out.String())
}

// d006 is E16-D006: each event's level, and its fields with their JSON kind.
var d006 = map[string]struct {
	level  string
	fields map[string]string
}{
	"order_rejected":     {"warn", map[string]string{"code": "number", "reason": "string", "client_order_id": "string"}},
	"order_unknown":      {"error", map[string]string{"client_order_id": "string"}},
	"pnl":                {"info", map[string]string{"pnl": "number", "ccy": "string"}},
	"pnl_floor_breached": {"error", map[string]string{"pnl": "number", "floor": "number", "ccy": "string"}},
}

func kind(v any) string {
	switch v.(type) {
	case float64:
		return "number"
	case string:
		return "string"
	}
	return fmt.Sprintf("%T", v)
}

// rows parses what one hook wrote. The R2 line is the only line that is not JSON; its row is nil.
// Each JSON line carries session_id, time, level and message, and a line with event has the level
// and fields of E16-D006.
func rows(t *testing.T, out string) []map[string]any {
	t.Helper()
	var got []map[string]any
	for _, line := range strings.SplitAfter(out, "\n") {
		if line == "" {
			continue
		}
		if line == panicLine {
			got = append(got, nil)
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("%q is not JSON: %v", line, err)
		}
		if row["session_id"] != "c0ffee" || row["time"] == nil || kind(row["level"]) != "string" || kind(row["message"]) != "string" {
			t.Fatalf("%q lacks session_id, time, level or message", line)
		}
		if ev, ok := row["event"]; ok {
			want, known := d006[fmt.Sprint(ev)]
			if !known {
				t.Fatalf("%q: event is not in E16-D006", line)
			}
			if row["level"] != want.level {
				t.Fatalf("%q: level is not %s", line, want.level)
			}
			for field, k := range want.fields {
				if kind(row[field]) != k {
					t.Fatalf("%q: %s is not a %s", line, field, k)
				}
			}
		}
		got = append(got, row)
	}
	return got
}

// shape is each row's level and event, or "not JSON".
func shape(rows []map[string]any) []string {
	var s []string
	for _, row := range rows {
		switch {
		case row == nil:
			s = append(s, "not JSON")
		case row["event"] != nil:
			s = append(s, fmt.Sprint(row["level"], " ", row["event"]))
		default:
			s = append(s, fmt.Sprint(row["level"]))
		}
	}
	return s
}

func wantShape(t *testing.T, rule string, got []map[string]any, want ...string) {
	t.Helper()
	if s := shape(got); !slices.Equal(s, want) {
		t.Fatalf("%s wrote %v, want %v", rule, s, want)
	}
}

func TestLogeventsWritesR1ToR5InOrder(t *testing.T) {
	s, f := start(t, `{"interval_ms":500,"pnl":[5,-2,-12],"floor":-10,"ccy":"USDT"}`)
	every := 500 * time.Millisecond

	wantShape(t, "R1", f.fire(t, every), "warn", "error")

	wantShape(t, "R2", f.fire(t, every), "not JSON")

	r3 := f.fire(t, every)
	wantShape(t, "R3", r3, "warn order_rejected", "error order_unknown")
	if r3[0]["code"] != float64(106) || r3[0]["reason"] == "" || r3[0]["client_order_id"] != "1" || r3[1]["client_order_id"] != "2" {
		t.Fatalf("R3 wrote %v", r3)
	}
	for _, row := range r3 {
		if _, ok := cloid.AlgoOf(row["client_order_id"].(string)); ok {
			t.Fatalf("simulated client_order_id %v is a platform one", row["client_order_id"])
		}
	}

	r4 := f.fire(t, every)
	wantShape(t, "R4", r4, "info pnl", "info pnl", "info pnl", "error pnl_floor_breached")
	for i, pnl := range []float64{5, -2, -12, -12} {
		if r4[i]["pnl"] != pnl || r4[i]["ccy"] != "USDT" {
			t.Fatalf("R4 line %d is %v", i, r4[i])
		}
	}
	if r4[3]["floor"] != float64(-10) {
		t.Fatalf("R4 breach is %v", r4[3])
	}
	if len(f.slept) != 0 {
		t.Fatalf("slept %v before R5", f.slept)
	}

	// R5's line is the SDK's: the hook only has to outlast the 100ms soft limit, far from the hung
	// timeout.
	wantShape(t, "R5", f.fire(t, every))
	if len(f.slept) != 1 || f.slept[0] <= 100*time.Millisecond || f.slept[0] >= time.Second {
		t.Fatalf("R5 slept %v", f.slept)
	}

	if len(f.timers) != 0 {
		t.Fatalf("armed %d timers after R5", len(f.timers))
	}
	f.out.Reset()
	s.OnStop(f)
	if f.out.Len() != 0 || f.ended {
		t.Fatalf("OnStop wrote %q, ended %v", f.out.String(), f.ended)
	}
}

// rules picks rules. They are still written in the order R1–R5, each once, and without R4 the
// PnL params are not needed.
func TestLogeventsPicksRules(t *testing.T) {
	_, f := start(t, `{"rules":["R3","R1","R3"]}`)
	wantShape(t, "R1", f.fire(t, time.Second), "warn", "error")
	wantShape(t, "R3", f.fire(t, time.Second), "warn order_rejected", "error order_unknown")
	if len(f.timers) != 0 || len(f.slept) != 0 {
		t.Fatalf("armed %d timers, slept %v", len(f.timers), f.slept)
	}
}

// A pnl at the floor is not below it.
func TestLogeventsFloorIsStrict(t *testing.T) {
	_, f := start(t, `{"rules":["R4"],"interval_ms":10,"pnl":[-10,-11],"floor":-10,"ccy":"USDT"}`)
	r4 := f.fire(t, 10*time.Millisecond)
	wantShape(t, "R4", r4, "info pnl", "info pnl", "error pnl_floor_breached")
	if r4[2]["pnl"] != float64(-11) || r4[2]["floor"] != float64(-10) {
		t.Fatalf("R4 breach is %v", r4[2])
	}
}

func TestLogeventsRejectsBadParams(t *testing.T) {
	for _, raw := range []string{
		`{`,
		``,
		`{}`,
		`{"rules":[]}`,
		`{"rules":["R6"]}`,
		`{"rules":["R1"],"interval_ms":-1}`,
		`{"rules":["R4"],"floor":-10,"ccy":"USDT"}`,
		`{"rules":["R4"],"pnl":[1],"ccy":"USDT"}`,
		`{"rules":["R4"],"pnl":[1],"floor":-10}`,
	} {
		f := &fakeCtx{params: []byte(raw)}
		if err := (&logevents{errw: &f.out}).OnInit(f); err == nil {
			t.Errorf("%q was accepted", raw)
		}
	}
}
