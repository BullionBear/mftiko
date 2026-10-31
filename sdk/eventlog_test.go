package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/c4"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/eventlog/dump"
	"github.com/BullionBear/mftiko/sdk/internal/symclient"
)

// The event log end to end (DECISIONS E20, PROTOCOL §12): a session over NATS with a fake SYM,
// md.fetch, md feed and TD, the files it leaves in VolumeDir()/eventlog, what dump prints from
// them, and Replay running the same strategy again over them.

const (
	btcUID    = uint32(1001)
	btcTicker = "Binance_Spot_BTCUSDT"
	tradeSubj = "md.v1.f.md-tw-1.trade." + btcTicker
	bookSubj  = "md.v1.f.md-tw-1.orderbook_delta." + btcTicker
	bookView  = "md.v1.view.md-tw-1.orderbook_delta." + btcTicker
)

// TestEventLogSession runs a session with the log on through init, a resolve that fails, start, a
// timer, a Post, a klines read, a td.view snapshot, two orders, a td.evt update, a trade, book
// deltas with their md.view snapshot, a BatchCancel, two bindings snapshots and stop. Every input
// is in the file in the order the strategy saw it, with its timestamps, and dump prints each of
// them.
func TestEventLogSession(t *testing.T) {
	r := runLogged(t, &tape{}, true)

	recs, h, err := readLog(r.dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if h.SessionID != "s1" || h.Incarnation != 3 || h.N != 1 {
		t.Fatalf("header %+v", h)
	}
	checkFrames(t, recs)
	checkDelivery(t, recs, r.strat.list())
	for _, k := range []eventlog.Kind{
		eventlog.KindInit, eventlog.KindResolve, eventlog.KindStart, eventlog.KindTimer, eventlog.KindPost,
		eventlog.KindReadCall, eventlog.KindReadReply, eventlog.KindTDView, eventlog.KindOrderCall,
		eventlog.KindOrderPublish, eventlog.KindOrderReply, eventlog.KindTDEvent, eventlog.KindMDFeed,
		eventlog.KindMDBook, eventlog.KindBindings, eventlog.KindStop, eventlog.KindOnStop,
		eventlog.KindHookEnd, eventlog.KindEnd,
	} {
		if !slices.ContainsFunc(recs, func(x eventlog.Record) bool { return x.Kind == k }) {
			t.Errorf("no %s frame", k)
		}
	}
	if n := count(recs, eventlog.KindResolve); n != 2 {
		t.Errorf("%d resolve frames, want 2", n)
	}
	if n := count(recs, eventlog.KindReadCall); n != 2 {
		t.Errorf("%d read_call frames, want 2 (klines and the book snapshot)", n)
	}
	if !slices.ContainsFunc(recs, func(x eventlog.Record) bool {
		return x.Kind == eventlog.KindOrderCall && x.Flags == eventlog.OpBatchCancel|eventlog.FromBatch
	}) {
		t.Error("no order_call for the BatchCancel round")
	}
	if n := count(recs, eventlog.KindBindings); n != 2 {
		t.Errorf("%d bindings frames, want 2 (Live and Command)", n)
	}
	if !slices.ContainsFunc(recs, func(x eventlog.Record) bool {
		return x.Kind == eventlog.KindBindings && x.Flags == eventlog.FromCommand
	}) {
		t.Error("the Command snapshot is not marked from_command")
	}
	if stop := find(recs, eventlog.KindStop); stop.Flags != eventlog.StopDeliver {
		t.Errorf("stop flags %d, want deliver", stop.Flags)
	}
	if !bytes.Equal(find(recs, eventlog.KindMDFeed).Body, r.trade) {
		t.Error("the md_feed body is not the message as published")
	}

	raw, err := os.ReadFile(filepath.Join(r.dir, "3-1.mel"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"user-jwt-e20", "user-seed-e20", "_INBOX_s1"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Errorf("the file holds the NATS credential %q", secret)
		}
	}

	if st := r.lastStat(); st == nil || st.GetRecords() == 0 || st.GetDropped() != 0 {
		t.Errorf("Report.event_log %v", st)
	}

	var out bytes.Buffer
	if err := dump.Dir(&out, r.dir, 0); err != nil {
		t.Fatal(err)
	}
	checkDump(t, out.Bytes(), len(recs))
}

// TestEventLogReplay replays the session's log: the strategy sees the same inputs at the same
// Context.Now, draws the same Rand, and sends the same orders with the same client order ids.
func TestEventLogReplay(t *testing.T) {
	r := runLogged(t, &tape{}, true)
	again := &tape{}
	rep, err := Replay(again, r.dir, ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Mismatches) > 0 {
		t.Fatalf("mismatches:\n%s", strings.Join(rep.Mismatches, "\n"))
	}
	recs, _, err := readLog(r.dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	inputs := 0
	for _, x := range recs {
		if x.Kind.Input() {
			inputs++
		}
	}
	if rep.SessionID != "s1" || rep.Incarnation != 3 || rep.Inputs != inputs || rep.Skipped != 0 ||
		rep.Missing != 0 || rep.Orders != 3 || rep.Reads != 2 {
		t.Fatalf("report %+v, want %d inputs, 3 orders, 2 reads", rep, inputs)
	}
	live, replayed := r.strat.list(), again.list()
	if !slices.Equal(live, replayed) {
		t.Fatalf("the replay saw\n%s\nthe session saw\n%s", lines(replayed), lines(live))
	}
	ids := r.platform.cloids()
	if len(ids) != 2 || !slices.ContainsFunc(replayed, func(l line) bool { return strings.Contains(l.text, ids[1]) }) {
		t.Fatalf("client order ids %v; replay %s", ids, lines(replayed))
	}

	// A strategy that decides differently is caught: the second order's price moves.
	rep, err = Replay(&tape{nudge: 1}, r.dir, ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Mismatches) != 1 || !strings.HasPrefix(rep.Mismatches[0], "request of order call 2:") {
		t.Fatalf("mismatches of a changed strategy:\n%s", strings.Join(rep.Mismatches, "\n"))
	}
}

// TestEventLogKeepsThePanic: the trade that made OnTrade panic and the hook's end are in the file,
// the session ends CRASHED, and nothing after them is lost (E20-D005). A replay panics the same.
func TestEventLogKeepsThePanic(t *testing.T) {
	r := runLogged(t, &tape{panicOn: "OnTrade"}, true)
	recs, _, err := readLog(r.dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	checkFrames(t, recs)
	checkDelivery(t, recs, r.strat.list())
	feed := find(recs, eventlog.KindMDFeed)
	if !bytes.Equal(feed.Body, r.trade) {
		t.Fatal("the trade that panicked is not in the file")
	}
	i := slices.IndexFunc(recs, func(x eventlog.Record) bool { return x.Kind == eventlog.KindHookEnd && x.Ref == feed.Seq })
	if i < 0 || recs[i].Flags != eventlog.EndPanic || string(recs[i].Body) != "boom" || recs[i].TNs < feed.TNs {
		t.Fatalf("the trade's hook_end %+v", recs[max(i, 0)])
	}
	if stop := find(recs, eventlog.KindStop); stop.Flags != 0 {
		t.Errorf("a CRASHED stop delivers nothing; flags %d", stop.Flags)
	}
	rep, err := Replay(&tape{panicOn: "OnTrade"}, r.dir, ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Mismatches) > 0 || rep.Skipped != 0 {
		t.Fatalf("replay %+v", rep)
	}
}

// TestEventLogKeepsTheHungHook: a hook the watchdog gives up on has only its start in the file,
// the stop frame says HUNG, and the end frame is still written when the worker exits (E20-D005).
// A replay does not run that hook again.
func TestEventLogKeepsTheHungHook(t *testing.T) {
	strat := &tape{blockOn: "OnTrade", release: make(chan struct{})}
	t.Cleanup(func() { close(strat.release) })
	r := runLogged(t, strat, true)
	recs, _, err := readLog(r.dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	feed := find(recs, eventlog.KindMDFeed)
	if !bytes.Equal(feed.Body, r.trade) {
		t.Fatal("the trade of the hung hook is not in the file")
	}
	if slices.ContainsFunc(recs, func(x eventlog.Record) bool { return x.Kind == eventlog.KindHookEnd && x.Ref == feed.Seq }) {
		t.Fatal("the hung hook has an end")
	}
	stop := find(recs, eventlog.KindStop)
	if stop.Flags != eventlog.StopHung || stop.Seq < feed.Seq || stop.RecvNs-feed.TNs < int64(time.Second) {
		t.Fatalf("stop %+v after the hook started at %d", stop, feed.TNs)
	}
	last := recs[len(recs)-1]
	if dropped, err := eventlog.ParseEnd(last.Body); last.Kind != eventlog.KindEnd || err != nil || dropped != 0 ||
		count(recs, eventlog.KindOnStop) != 0 {
		t.Fatalf("last frame %+v", last)
	}
	rep, err := Replay(&tape{}, r.dir, ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Skipped != 1 || len(rep.Mismatches) > 0 {
		t.Fatalf("replay %+v", rep)
	}
}

// TestEventLogOff: with the spec's switch off, nothing is written and Report has no event_log
// (E20-D007).
func TestEventLogOff(t *testing.T) {
	r := runLogged(t, &tape{}, false)
	entries, err := os.ReadDir(r.vol)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("the volume holds %v", entries)
	}
	reports := reportsOf(r.s.frames())
	if len(reports) == 0 {
		t.Fatal("no report")
	}
	for _, rep := range reports {
		if rep.GetEventLog() != nil {
			t.Fatalf("Report.event_log %v with the log off", rep.GetEventLog())
		}
	}
	if _, err := Replay(&tape{}, filepath.Join(r.vol, eventlog.Dir), ReplayOptions{}); err == nil {
		t.Fatal("a replay without a log")
	}
}

// TestOutsidePanicIsWritten: c4 does not recover a panic in OnInit or OnStart, and the process
// ends with it. The input, the hook_end with the panic and the end frame are in the file before
// the panic goes on (E20-D005).
func TestOutsidePanicIsWritten(t *testing.T) {
	vol := t.TempDir()
	p := newPlane(&tape{}, host{log: func(string) {}})
	p.volumeDir = vol
	p.rec = p.openRec(&stsctlv1.InitPayload{SessionId: "s1", Incarnation: 2, EventLog: &sessionv1.EventLog{Enabled: true}})
	var got any
	func() {
		defer func() { got = recover() }()
		_ = p.outside("OnStart", eventlog.KindStart, 1000, 0, nil, []byte("start"), func() error { panic("boom") })
	}()
	if got != "boom" {
		t.Fatalf("recovered %v", got)
	}
	recs, _, err := readLog(filepath.Join(vol, eventlog.Dir), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 || recs[0].Kind != eventlog.KindStart || recs[0].RecvNs != 1000 || string(recs[0].Body) != "start" ||
		recs[1].Kind != eventlog.KindHookEnd || recs[1].Ref != recs[0].Seq || recs[1].Flags != eventlog.EndPanic ||
		string(recs[1].Body) != "boom" || recs[2].Kind != eventlog.KindEnd {
		t.Fatalf("frames %+v", recs)
	}
}

// logged is one session run by runLogged.
type logged struct {
	s        *script
	strat    *tape
	platform *platform
	vol      string
	dir      string
	trade    []byte // the md message published, envelope and payload
}

func (r *logged) lastStat() *stsctlv1.EventLogStat {
	reports := reportsOf(r.s.frames())
	if len(reports) == 0 {
		return nil
	}
	return reports[len(reports)-1].GetEventLog()
}

// runLogged runs strat as one session and returns once the worker has exited. The script waits
// for each step before the next, so the order of the inputs is the order in TestEventLogSession's
// comment; the td.view snapshot and the order replies land where they land.
func runLogged(t *testing.T, strat *tape, on bool) *logged {
	t.Helper()
	prevGate := emitGate
	emitGate = func() {}
	t.Cleanup(func() { emitGate = prevGate })

	url := startNATS(t)
	pf := newPlatform(t, url)
	vol := t.TempDir()
	clock := &manual{}
	s := newScript(t, nil, host{
		natsURL:     url,
		tryConn:     (&dials{}).try,
		getenv:      envDirs(vol, ""),
		reportAfter: clock.After,
		c4log:       discardLog,
		log:         func(string) {},
	})
	s.strat = strat
	r := &logged{s: s, strat: strat, platform: pf, vol: vol, dir: filepath.Join(vol, eventlog.Dir)}
	s.start()

	init := baseInit()
	init.Incarnation = 3
	init.RecvWindowMs = 1000
	init.Nats = &stsctlv1.NatsCredentials{UserJwt: "user-jwt-e20", Seed: "user-seed-e20", InboxPrefix: "_INBOX_s1"}
	init.Accounts = []*sessionv1.AccountDecl{{ApiId: 7, AlgoIds: []uint32{1}}}
	if strat.blockOn != "" {
		init.HookTimeoutS = 1
	}
	if on {
		init.EventLog = &sessionv1.EventLog{Enabled: true, KeepMb: 4}
	}
	s.send(initDown(must(t, init), "L1", 0))
	waitFor(t, func() bool { return doneOK(s.frames(), workerv1.Verb_VERB_INIT) })
	s.send(startDown(must(t, &stsctlv1.StartPayload{Bindings: tradeFeed(1, 1)})))
	waitFor(t, func() bool { return doneOK(s.frames(), workerv1.Verb_VERB_START) })

	// OnStart's timer Posts, the Post reads klines, the klines Submit a buy. td.view answers the
	// first Recon meanwhile.
	waitFor(t, func() bool { return strat.has("OnRecon") && len(pf.cloids()) == 1 })
	pf.event(t, 1, pf.cloids()[0], tdv1.OrderStatus_ORDER_STATUS_NEW)
	waitFor(t, func() bool { return strat.has("OnOrder") })
	r.trade = pf.trade(t, 1)
	switch "OnTrade" {
	case strat.panicOn:
		waitFor(t, func() bool {
			code, got := s.exit()
			return got && code == c4.ExitCrash && exitingAs(s.frames(), commonv1.Outcome_OUTCOME_CRASHED)
		})
		if !strat.has("OnStop") {
			t.Fatal("OnStop did not run after the panic")
		}
		return r
	case strat.blockOn:
		waitFor(t, func() bool {
			code, got := s.exit()
			return got && code == c4.ExitHung && exitingAs(s.frames(), commonv1.Outcome_OUTCOME_HUNG)
		})
		return r
	}
	// The sell is refused, so OnOrder shows its reply has run.
	waitFor(t, func() bool { return strat.count("OnOrder") == 2 })

	// The first delta asks md.view for the book. OnBook runs for a delta applied after the
	// snapshot; the deltas that came while it was asked for are folded into it without a hook.
	for seq := uint64(1); !strat.has("OnBook"); seq++ {
		if seq > 500 {
			t.Fatal("OnBook did not run")
		}
		pf.delta(t, seq)
		time.Sleep(10 * time.Millisecond)
	}
	// OnBook's BatchCancel is refused, so OnReject shows its reply has run.
	waitFor(t, func() bool { return strat.has("OnReject") })

	s.send(liveDown(must(t, tradeFeed(2, 5)), "L2"))
	waitFor(t, func() bool { return applied(s.frames(), "L2") })
	cmd := &stsctlv1.Command{Cmd: &stsctlv1.Command_Bindings{Bindings: tradeFeed(2, 6)}}
	s.send(relayDown(must(t, cmd)))
	waitFor(t, func() bool { return reportVersion(s.frames(), 2, 6) })
	if on {
		waitFor(t, func() bool {
			clock.fire()
			st := r.lastStat()
			return st != nil && st.GetRecords() > 0
		})
	}

	s.send(stopDown())
	waitFor(t, func() bool {
		code, got := s.exit()
		return got && code == 0 && exiting(s.frames(), commonv1.Outcome_OUTCOME_DONE, "stop")
	})
	if !strat.has("OnStop") {
		t.Fatal("OnStop did not run")
	}
	return r
}

func tradeFeed(epoch int64, seq uint64) *stsctlv1.Bindings {
	b := bind(epoch, seq)
	b.Feeds = []*stsctlv1.FeedBinding{{
		Feed:       "trade." + btcTicker,
		MdInstance: "md-tw-1",
		Subject:    tradeSubj,
		State:      stsctlv1.BindingState_BINDING_STATE_READY,
	}, {
		Feed:       "orderbook_delta." + btcTicker,
		MdInstance: "md-tw-1",
		Subject:    bookSubj,
		State:      stsctlv1.BindingState_BINDING_STATE_READY,
	}}
	return b
}

func exitingAs(ups []*workerv1.WorkerUp, o commonv1.Outcome) bool {
	return slices.ContainsFunc(ups, func(m *workerv1.WorkerUp) bool { return m.GetExiting().GetOutcome() == o })
}

// checkFrames: rec_seq counts from 1 without a gap, each input's hook_end comes before the next
// input, timestamps do not run backwards, and the last frame is the end frame with nothing dropped.
func checkFrames(t *testing.T, recs []eventlog.Record) {
	t.Helper()
	if len(recs) == 0 || recs[0].Kind != eventlog.KindInit {
		t.Fatal("the log does not start with init")
	}
	open := uint64(0) // the input whose hook is running
	var lastStart int64
	for i, x := range recs {
		if x.Seq != uint64(i+1) {
			t.Fatalf("frame %d has rec_seq %d", i, x.Seq)
		}
		switch {
		case x.Kind.Input():
			if open != 0 {
				t.Fatalf("rec_seq %d %s starts while the hook of %d runs", x.Seq, x.Kind, open)
			}
			if x.TNs < lastStart || (x.RecvNs != 0 && x.RecvNs > x.TNs) {
				t.Fatalf("rec_seq %d %s: recv %d, start %d, the previous start %d", x.Seq, x.Kind, x.RecvNs, x.TNs, lastStart)
			}
			open, lastStart = x.Seq, x.TNs
		case x.Kind == eventlog.KindHookEnd:
			if x.Ref != open || x.TNs < lastStart {
				t.Fatalf("rec_seq %d hook_end of %d at %d; running %d since %d", x.Seq, x.Ref, x.TNs, open, lastStart)
			}
			open = 0
		}
	}
	last := recs[len(recs)-1]
	dropped, err := eventlog.ParseEnd(last.Body)
	if last.Kind != eventlog.KindEnd || err != nil || dropped != 0 {
		t.Fatalf("last frame %s dropped %d %v", last.Kind, dropped, err)
	}
}

// checkDelivery: each hook the strategy ran is one input in the file, in the same order, and
// Context.Now in the hook is that input's hook start.
func checkDelivery(t *testing.T, recs []eventlog.Record, seen []line) {
	t.Helper()
	kinds := map[string][]eventlog.Kind{
		"OnInit": {eventlog.KindInit}, "OnStart": {eventlog.KindStart}, "timer": {eventlog.KindTimer},
		"post": {eventlog.KindPost}, "klines": {eventlog.KindReadReply}, "OnRecon": {eventlog.KindTDView},
		"OnOrder": {eventlog.KindTDEvent, eventlog.KindOrderReply}, "OnTrade": {eventlog.KindMDFeed},
		"OnBook": {eventlog.KindMDBook}, "OnReject": {eventlog.KindOrderReply}, "OnStop": {eventlog.KindOnStop},
	}
	var last uint64
	for _, l := range seen {
		if l.hook == "submit" || l.hook == "batch" {
			continue // a call inside the hook before it
		}
		i := slices.IndexFunc(recs, func(x eventlog.Record) bool { return x.Kind.Input() && x.TNs == l.now })
		if i < 0 {
			t.Fatalf("%s at %d is not an input in the file", l.hook, l.now)
		}
		x := recs[i]
		if !slices.Contains(kinds[l.hook], x.Kind) || x.Seq <= last {
			t.Fatalf("%s at %d is rec_seq %d %s, after rec_seq %d", l.hook, l.now, x.Seq, x.Kind, last)
		}
		last = x.Seq
	}
}

// checkDump: one header line, then one line per frame, each with the timestamps of its kind.
func checkDump(t *testing.T, out []byte, frames int) {
	t.Helper()
	want := map[string][]string{
		"init":          {"recv_ns", "hook_start_ns", "seed", "cloid_slot", "payload"},
		"resolve":       {"call_ns", "reply_ns", "ticker", "outcome"},
		"start":         {"recv_ns", "hook_start_ns", "payload"},
		"bindings":      {"recv_ns", "applied_ns", "bindings"},
		"md_feed":       {"recv_ns", "hook_start_ns", "md_ts_ns", "seq", "event"},
		"md_book":       {"recv_ns", "hook_start_ns", "md_ts_ns", "seq", "event"},
		"td_event":      {"recv_ns", "hook_start_ns", "api_id", "event"},
		"td_view":       {"recv_ns", "hook_start_ns", "api_id", "gen", "reply"},
		"order_reply":   {"reply_ns", "hook_start_ns", "call", "outcome", "reply"},
		"read_reply":    {"reply_ns", "hook_start_ns", "read", "read_kind", "reply"},
		"timer":         {"recv_ns", "hook_start_ns", "timer"},
		"post":          {"recv_ns", "hook_start_ns", "post"},
		"stop":          {"stop_ns", "onstop_asked_ns", "deliver"},
		"onstop":        {"recv_ns", "hook_start_ns"},
		"hook_end":      {"hook_end_ns", "input", "ending"},
		"order_call":    {"call_ns", "call", "op", "api_id", "cloids"},
		"order_publish": {"publish_ns", "call", "request"},
		"read_call":     {"call_ns", "read", "read_kind", "subject", "request"},
		"end":           {"end_ns", "dropped"},
	}
	rows := bytes.Split(bytes.TrimSuffix(out, []byte("\n")), []byte("\n"))
	if len(rows) != frames+1 {
		t.Fatalf("%d lines for %d frames", len(rows), frames)
	}
	var head map[string]any
	if err := json.Unmarshal(rows[0], &head); err != nil || head["file"] != "3-1.mel" || head["session_id"] != "s1" {
		t.Fatalf("header line %s %v", rows[0], err)
	}
	for _, row := range rows[1:] {
		var f map[string]any
		if err := json.Unmarshal(row, &f); err != nil {
			t.Fatalf("%s: %v", row, err)
		}
		kind, _ := f["kind"].(string)
		keys, ok := want[kind]
		if !ok {
			t.Fatalf("a %q line: %s", kind, row)
		}
		for _, k := range keys {
			if _, ok := f[k]; !ok {
				t.Errorf("%s line without %s: %s", kind, k, row)
			}
		}
		if _, bad := f["decode_error"]; bad {
			t.Errorf("a line that did not decode: %s", row)
		}
	}
	if !bytes.Contains(out, []byte(`"md_ts_ns":`)) || !bytes.Contains(out, []byte(`"outcome":"not_found"`)) {
		t.Errorf("dump:\n%s", out)
	}
}

func count(recs []eventlog.Record, k eventlog.Kind) int {
	n := 0
	for _, x := range recs {
		if x.Kind == k {
			n++
		}
	}
	return n
}

func find(recs []eventlog.Record, k eventlog.Kind) eventlog.Record {
	for _, x := range recs {
		if x.Kind == k {
			return x
		}
	}
	return eventlog.Record{}
}

// line is one hook of tape: which hook, Context.Now in it, and what it saw and did.
type line struct {
	hook string
	now  int64
	text string
}

func lines(ls []line) string {
	var b strings.Builder
	for _, l := range ls {
		fmt.Fprintf(&b, "  %s %d %s\n", l.hook, l.now, l.text)
	}
	return b.String()
}

// tape is the strategy of these tests. It writes down every hook, so a replay can be compared
// with the session line by line. Its orders take their price from Rand and a parameter from Now,
// so a replay that gave either back differently would send a different request.
type tape struct {
	panicOn string        // the hook that panics with "boom"
	blockOn string        // the hook that waits for release
	release chan struct{} // closed by the test
	nudge   float64       // added to the sell price; a strategy that changed since the recording

	mu    sync.Mutex
	lines []line
	btc   uint32
}

func (s *tape) add(ctx Context, hook, format string, args ...any) {
	s.mu.Lock()
	s.lines = append(s.lines, line{hook: hook, now: ctx.Now().UnixNano(), text: fmt.Sprintf(format, args...)})
	s.mu.Unlock()
	switch hook {
	case s.panicOn:
		panic("boom")
	case s.blockOn:
		<-s.release
	}
}

func (s *tape) list() []line {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.lines)
}

func (s *tape) count(hook string) int {
	n := 0
	for _, l := range s.list() {
		if l.hook == hook {
			n++
		}
	}
	return n
}

func (s *tape) has(hook string) bool { return s.count(hook) > 0 }

func (s *tape) OnInit(ctx Context) error {
	info, err := ctx.Resolve(btcTicker)
	if err != nil {
		return err
	}
	s.btc = info.UID
	_, err = ctx.Resolve("Binance_Spot_DOGEUSDT")
	s.add(ctx, "OnInit", "btc %d, doge %v (%t), rand %d", info.UID, err, errors.Is(err, symbol.ErrNotFound), ctx.Rand().IntN(1000))
	return nil
}

func (s *tape) OnStart(ctx Context) error {
	ctx.After(time.Millisecond, func(ctx Context) {
		ctx.Post(func(ctx Context) {
			err := ctx.FetchKlines(s.btc, "1m", 2, func(ctx Context, k *mdv1.Klines, err error) {
				s.add(ctx, "klines", "%d klines, %v", len(k.GetKlines()), err)
				if n := len(k.GetKlines()); n > 0 {
					s.submit(ctx, commonv1.Side_SIDE_BUY, k.GetKlines()[n-1].GetClose())
				}
			})
			s.add(ctx, "post", "fetch %v", err)
		})
		s.add(ctx, "timer", "")
	})
	s.add(ctx, "OnStart", "")
	return nil
}

func (s *tape) submit(ctx Context, side commonv1.Side, price float64) {
	if side == commonv1.Side_SIDE_SELL {
		price += s.nudge
	}
	id, err := ctx.Submit(Order{
		APIID:    7,
		AlgoID:   1,
		UID:      s.btc,
		Side:     side,
		Type:     commonv1.OrderType_ORDER_TYPE_LIMIT,
		TIF:      commonv1.TimeInForce_TIME_IN_FORCE_GTC,
		Qty:      1,
		QuoteQty: decimal.Absent(),
		Price:    price + float64(ctx.Rand().IntN(3)),
		Params:   map[string]string{"at_ms": strconv.FormatInt(ctx.Now().UnixMilli(), 10)},
	})
	s.add(ctx, "submit", "%s %s %v", side, id, err)
}

func (s *tape) OnRecon(ctx Context, ev ReconEvent) {
	s.add(ctx, "OnRecon", "incarnation %d, %d open", ev.Incarnation, len(ev.Recon.GetOpenOrders()))
}

func (s *tape) OnOrder(ctx Context, ev OrderEvent) {
	s.add(ctx, "OnOrder", "%s %s origin %d code %d", ev.Order.GetClientOrderId(), ev.Order.GetStatus(), ev.Origin, ev.Code)
}

func (s *tape) OnTrade(ctx Context, ev Trade) {
	s.add(ctx, "OnTrade", "seq %d at %v", ev.Seq, ev.Data.GetPrice())
	s.submit(ctx, commonv1.Side_SIDE_SELL, ev.Data.GetPrice())
}

// OnBook reads the local book. The first one cancels the algo's orders.
func (s *tape) OnBook(ctx Context, ev BookUpdate) {
	bid := "none"
	if ob, ok := ctx.Book(ev.UID); ok {
		if tick, ok := ob.BestBidTick(); ok {
			qty, _ := ob.Qty(true, ob.Price(tick))
			bid = fmt.Sprintf("%v x %v", ob.Price(tick), qty)
		}
	}
	s.add(ctx, "OnBook", "seq %d, bid %s", ev.Seq, bid)
	if s.count("OnBook") == 1 {
		s.add(ctx, "batch", "%v", ctx.BatchCancel(7, 1))
	}
}

func (s *tape) OnReject(ctx Context, ev RejectEvent) {
	s.add(ctx, "OnReject", "request %d %v code %d %s", ev.Request, ev.ClientOrderIDs, ev.Code, ev.Reason)
}

func (s *tape) OnStop(ctx Context) {
	_, err := ctx.Submit(Order{APIID: 7, AlgoID: 1, UID: s.btc, Side: commonv1.Side_SIDE_SELL, Qty: 1, QuoteQty: decimal.Absent()})
	s.add(ctx, "OnStop", "submit %v", err)
}

// platform is the fake SYM, md.fetch, md.view and TD of these tests, on a connection of its own.
// td.order accepts a buy, refuses a sell as a bad price and a BatchCancel as rate limited.
type platform struct {
	nc *nats.Conn

	mu  sync.Mutex
	ids []string // client order ids of the Submits received, in order
}

func newPlatform(t *testing.T, url string) *platform {
	t.Helper()
	p := &platform{nc: dialNATS(t, url, "platform")}
	for subject, fn := range map[string]nats.MsgHandler{
		"sym":                 p.sym,
		"md.v1.fetch.Binance": p.fetch,
		bookView:              p.book,
		"td.view.7":           p.view,
		"td.order.7":          p.order,
	} {
		if _, err := p.nc.Subscribe(subject, fn); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *platform) respond(m *nats.Msg, rep proto.Message) {
	b, err := proto.Marshal(rep)
	if err == nil {
		_ = m.Respond(b)
	}
}

func (p *platform) sym(m *nats.Msg) {
	var req symv1.Request
	if err := proto.Unmarshal(m.Data, &req); err != nil {
		return
	}
	info := &symv1.Reply{Result: &symv1.Reply_Info{Info: symclient.InfoToProto(symbol.Info{
		UID:     btcUID,
		Ticker:  symbol.UniversalTicker{Venue: "Binance", Category: symbol.Spot, Symbol: "BTCUSDT"},
		Base:    "BTC",
		Quote:   "USDT",
		Status:  symbol.Tradable,
		Filters: symbol.Filters{symbol.PriceTick: 0.1},
	})}}
	switch r := req.GetReq().(type) {
	case *symv1.Request_Get:
		if r.Get.GetUniversalTicker() == btcTicker {
			p.respond(m, info)
			return
		}
	case *symv1.Request_GetByUid:
		if r.GetByUid.GetUid() == btcUID {
			p.respond(m, info)
			return
		}
	case *symv1.Request_Revisions:
		p.respond(m, &symv1.Reply{Result: &symv1.Reply_Revisions{Revisions: &symv1.RevisionList{
			Tables: []*symv1.TableRevision{{Venue: "Binance", Category: "Spot", Revision: 1}},
		}}})
		return
	}
	// natsx error reply: mark, version 1, code 2, then the message.
	msg := "not_found: no such instrument"
	_ = m.Respond(append([]byte{0x00, 1, 0, 2, 0, byte(len(msg))}, msg...))
}

func (p *platform) fetch(m *nats.Msg) {
	kline := func(minute int64, closed float64) *mdv1.Kline {
		return &mdv1.Kline{Interval: "1m", OpenTimeNs: minute * 60e9, Open: 100, High: 102, Low: 99, Close: closed, Volume: 3, Closed: true}
	}
	p.respond(m, &mdv1.FetchReply{Uid: btcUID, Result: &mdv1.FetchReply_Klines{Klines: &mdv1.Klines{
		Klines: []*mdv1.Kline{kline(1, 101), kline(2, 100)},
	}}})
}

// book is the md.view snapshot of BTC's book at (1, 1).
func (p *platform) book(m *nats.Msg) {
	p.respond(m, &mdv1.ViewReply{Result: &mdv1.ViewReply_Book{Book: &mdv1.BookSnapshot{
		Uid: btcUID, ConnEpoch: 1, Seq: 1,
		Bids: []*mdv1.Level{{Price: 100, Qty: 1}},
		Asks: []*mdv1.Level{{Price: 101, Qty: 1}},
	}}})
}

func (p *platform) view(m *nats.Msg) {
	p.respond(m, &tdv1.ViewReply{Incarnation: 1, CancelBatch: 2, Reply: &tdv1.ViewReply_Recon{Recon: &tdv1.Recon{}}})
}

func (p *platform) order(m *nats.Msg) {
	var req tdv1.OrderRequest
	if err := proto.Unmarshal(m.Data, &req); err != nil {
		return
	}
	switch {
	case req.GetBatchCancel() != nil:
		p.respond(m, &tdv1.OrderReply{Code: int32(tdv1.RejectCode_REJECT_CODE_RATE_LIMITED), Reason: "rate"})
	case req.GetSubmit() != nil:
		p.mu.Lock()
		p.ids = append(p.ids, req.GetSubmit().GetClientOrderId())
		p.mu.Unlock()
		if req.GetSubmit().GetSide() == commonv1.Side_SIDE_SELL {
			p.respond(m, &tdv1.OrderReply{Code: int32(tdv1.RejectCode_REJECT_CODE_INVALID_ORDER), Reason: "price"})
			return
		}
		p.respond(m, &tdv1.OrderReply{Accepted: true})
	default:
		p.respond(m, &tdv1.OrderReply{Accepted: true})
	}
}

func (p *platform) cloids() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.ids)
}

// event publishes one td.evt OrderUpdate of account 7, incarnation 1.
func (p *platform) event(t *testing.T, seq uint64, id string, st tdv1.OrderStatus) {
	t.Helper()
	b := must(t, &tdv1.AccountEvent{
		Seq:    seq,
		Source: &commonv1.WorkerRef{Incarnation: 1},
		Event: &tdv1.AccountEvent_OrderUpdate{OrderUpdate: &tdv1.OrderUpdate{Order: &tdv1.Order{
			ClientOrderId: id, Status: st, Uid: btcUID, Side: commonv1.Side_SIDE_BUY,
		}}},
	})
	p.publish(t, "td.evt.7", b)
}

// trade publishes one trade on the feed, stamped now, and returns the message.
func (p *platform) trade(t *testing.T, seq uint64) []byte {
	t.Helper()
	body := must(t, &mdv1.FeedEvent{Uid: btcUID, Event: &mdv1.FeedEvent_Trade{Trade: &mdv1.Trade{
		Meta: &mdv1.Meta{ExchTsNs: 7}, TradeId: "t1", Price: 100, Qty: 2, Side: commonv1.Side_SIDE_BUY,
	}}})
	msg := envelope.Append(nil, time.Now().UnixNano(), 1, seq, body)
	p.publish(t, tradeSubj, msg)
	return msg
}

// delta publishes one orderbook_delta, stamped now, that sets the bid at 100 to seq.
func (p *platform) delta(t *testing.T, seq uint64) {
	t.Helper()
	body := must(t, &mdv1.FeedEvent{Uid: btcUID, Event: &mdv1.FeedEvent_OrderBookDelta{OrderBookDelta: &mdv1.OrderBookDelta{
		Meta: &mdv1.Meta{ExchTsNs: 7}, Bids: []*mdv1.Level{{Price: 100, Qty: float64(seq)}},
	}}})
	p.publish(t, bookSubj, envelope.Append(nil, time.Now().UnixNano(), 1, seq, body))
}

func (p *platform) publish(t *testing.T, subject string, b []byte) {
	t.Helper()
	if err := p.nc.Publish(subject, b); err != nil {
		t.Fatal(err)
	}
	if err := p.nc.Flush(); err != nil {
		t.Fatal(err)
	}
}
