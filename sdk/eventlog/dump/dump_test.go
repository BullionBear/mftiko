package dump

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/eventlog"
)

func pb(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// session is a short session of every kind, at small readable times: Init at 1000 ns, Start at
// 2000, the first Recon at 3000, then a timer, a Post, a klines read, a Submit and its reply, an
// order update whose hook sends a BatchCancel, that BatchCancel's reply timing out, a bindings
// Command, a trade whose hook panics, and the CRASHED ending.
func session(t *testing.T) []eventlog.Record {
	t.Helper()
	slot := eventlog.InitHead(7)
	view := eventlog.TDViewHead(1)
	init := &stsctlv1.InitPayload{SessionId: "s1", Incarnation: 3, RecvWindowMs: 200, HookTimeoutS: 2}
	btc := &symv1.SymbolInfo{UniversalTicker: "Binance_Spot_BTCUSDT", Base: "BTC", Quote: "USDT", Status: symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE, Uid: 1001}
	klines := &mdv1.FetchRequest{Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{Interval: "1m", Limit: 2, Uid: 1001}}}
	klinesReply := &mdv1.FetchReply{Uid: 1001, Result: &mdv1.FetchReply_Klines{Klines: &mdv1.Klines{Klines: []*mdv1.Kline{{Interval: "1m", Close: 100, Closed: true}}}}}
	qty, price := 1.0, 100.0
	submit := &tdv1.OrderRequest{Req: &tdv1.OrderRequest_Submit{Submit: &tdv1.Submit{SessionId: "s1", Uid: 1001, Qty: &qty, Price: &price, ClientOrderId: "c1"}}}
	trade := &mdv1.FeedEvent{Uid: 1001, Event: &mdv1.FeedEvent_Trade{Trade: &mdv1.Trade{TradeId: "t1", Price: 100, Qty: 1}}}
	update := &tdv1.AccountEvent{Seq: 1, Event: &tdv1.AccountEvent_OrderUpdate{OrderUpdate: &tdv1.OrderUpdate{Order: &tdv1.Order{ClientOrderId: "c1", Status: tdv1.OrderStatus_ORDER_STATUS_NEW}}}}
	batch := &tdv1.OrderRequest{Req: &tdv1.OrderRequest_BatchCancel{BatchCancel: &tdv1.BatchCancel{Orders: []*tdv1.BatchCancelOrder{{ClientOrderId: "c1"}}}}}
	return []eventlog.Record{
		{Seq: 1, Kind: eventlog.KindInit, RecvNs: 1000, TNs: 1100, Ref: 42, Body: cat(slot[:], pb(t, init))},
		{Seq: 2, Kind: eventlog.KindResolve, RecvNs: 1300, TNs: 1200, Body: eventlog.AppendResolve(nil, "Binance_Spot_BTCUSDT", pb(t, btc))},
		{Seq: 3, Kind: eventlog.KindResolve, Flags: eventlog.NotFound, RecvNs: 1350, TNs: 1310, Body: eventlog.AppendResolve(nil, "Binance_Spot_DOGEUSDT", []byte("symbol: no such instrument"))},
		{Seq: 4, Kind: eventlog.KindHookEnd, TNs: 1400, Ref: 1},
		{Seq: 5, Kind: eventlog.KindStart, RecvNs: 2000, TNs: 2100, Body: pb(t, &stsctlv1.StartPayload{Bindings: &stsctlv1.Bindings{Version: &commonv1.Version{Epoch: 1, Seq: 1}}})},
		{Seq: 6, Kind: eventlog.KindHookEnd, TNs: 2200, Ref: 5},
		{Seq: 7, Kind: eventlog.KindTDView, RecvNs: 3000, TNs: 3100, Ref: 7, Body: cat(view[:], pb(t, &tdv1.ViewReply{Reply: &tdv1.ViewReply_Recon{Recon: &tdv1.Recon{}}, Incarnation: 1, CancelBatch: 2}))},
		{Seq: 8, Kind: eventlog.KindHookEnd, TNs: 3200, Ref: 7},
		{Seq: 9, Kind: eventlog.KindTimer, RecvNs: 4000, TNs: 4100, Ref: 1},
		{Seq: 10, Kind: eventlog.KindHookEnd, TNs: 4200, Ref: 9},
		{Seq: 11, Kind: eventlog.KindPost, RecvNs: 4300, TNs: 4400, Ref: 1},
		{Seq: 12, Kind: eventlog.KindReadCall, Flags: eventlog.ReadKlines, TNs: 4450, Ref: 1, Body: eventlog.AppendReadCall(nil, "md.v1.fetch.Binance", pb(t, klines))},
		{Seq: 13, Kind: eventlog.KindHookEnd, TNs: 4500, Ref: 11},
		{Seq: 14, Kind: eventlog.KindReadReply, RecvNs: 5000, TNs: 5100, Ref: 1, Body: pb(t, klinesReply)},
		{Seq: 15, Kind: eventlog.KindOrderCall, Flags: eventlog.OpSubmit, TNs: 5150, Ref: 1, Body: eventlog.AppendOrderCall(nil, 7, 1, []string{"c1"})},
		{Seq: 16, Kind: eventlog.KindHookEnd, TNs: 5200, Ref: 14},
		{Seq: 17, Kind: eventlog.KindOrderPublish, TNs: 5250, Ref: 1, Body: pb(t, submit)},
		{Seq: 18, Kind: eventlog.KindOrderReply, RecvNs: 5300, TNs: 5400, Ref: 1, Body: pb(t, &tdv1.OrderReply{Accepted: true, ClientOrderId: "c1"})},
		{Seq: 19, Kind: eventlog.KindHookEnd, TNs: 5500, Ref: 18},
		{Seq: 20, Kind: eventlog.KindTDEvent, RecvNs: 6000, TNs: 6100, Ref: 7, Body: pb(t, update)},
		{Seq: 21, Kind: eventlog.KindOrderCall, Flags: eventlog.OpBatchCancel | eventlog.FromBatch, TNs: 6150, Ref: 2, Body: eventlog.AppendOrderCall(nil, 7, 0, []string{"c1"})},
		{Seq: 22, Kind: eventlog.KindHookEnd, TNs: 6200, Ref: 20},
		{Seq: 23, Kind: eventlog.KindOrderPublish, TNs: 6250, Ref: 2, Body: pb(t, batch)},
		{Seq: 24, Kind: eventlog.KindOrderReply, Flags: eventlog.Timeout, RecvNs: 6450, TNs: 6500, Ref: 2, Body: []byte("context deadline exceeded")},
		{Seq: 25, Kind: eventlog.KindHookEnd, TNs: 6550, Ref: 24},
		{Seq: 26, Kind: eventlog.KindBindings, Flags: eventlog.FromCommand, RecvNs: 7000, TNs: 7100, Body: pb(t, &stsctlv1.Bindings{Version: &commonv1.Version{Epoch: 2, Seq: 5}})},
		{Seq: 27, Kind: eventlog.KindMDFeed, RecvNs: 7500, TNs: 7600, Body: envelope.Append(nil, 7400, 1, 4, pb(t, trade))},
		{Seq: 28, Kind: eventlog.KindHookEnd, Flags: eventlog.EndPanic, TNs: 7700, Ref: 27, Body: []byte("boom")},
		{Seq: 29, Kind: eventlog.KindStop, RecvNs: 8000, TNs: 8100},
		{Seq: 30, Kind: eventlog.KindOnStop, RecvNs: 8100, TNs: 8200},
		{Seq: 31, Kind: eventlog.KindHookEnd, TNs: 8300, Ref: 30},
		{Seq: 32, Kind: eventlog.KindEnd, TNs: 9000, Body: eventlog.AppendEnd(nil, 0)},
	}
}

var sessionLines = []string{
	`{"file":"3-1.mel","format_version":1,"incarnation":3,"n":1,"created_ns":900,"session_id":"s1","sdk_version":"v0.0.0-20261031090000-0a1b2c3d4e5f","sdk_commit":"0a1b2c3d4e5f"}`,
	`{"rec_seq":1,"kind":"init","recv_ns":1000,"hook_start_ns":1100,"seed":42,"cloid_slot":7,"payload":{"session_id":"s1","incarnation":3,"recv_window_ms":200,"hook_timeout_s":2}}`,
	`{"rec_seq":2,"kind":"resolve","call_ns":1200,"reply_ns":1300,"outcome":"ok","ticker":"Binance_Spot_BTCUSDT","info":{"universal_ticker":"Binance_Spot_BTCUSDT","base":"BTC","quote":"USDT","status":"SYMBOL_STATUS_TRADABLE","uid":1001}}`,
	`{"rec_seq":3,"kind":"resolve","call_ns":1310,"reply_ns":1350,"outcome":"not_found","error":"symbol: no such instrument","ticker":"Binance_Spot_DOGEUSDT"}`,
	`{"rec_seq":4,"kind":"hook_end","hook_end_ns":1400,"input":1,"ending":"returned"}`,
	`{"rec_seq":5,"kind":"start","recv_ns":2000,"hook_start_ns":2100,"payload":{"bindings":{"version":{"epoch":"1","seq":"1"}}}}`,
	`{"rec_seq":6,"kind":"hook_end","hook_end_ns":2200,"input":5,"ending":"returned"}`,
	`{"rec_seq":7,"kind":"td_view","recv_ns":3000,"hook_start_ns":3100,"api_id":7,"gen":1,"reply":{"recon":{},"incarnation":1,"cancel_batch":2}}`,
	`{"rec_seq":8,"kind":"hook_end","hook_end_ns":3200,"input":7,"ending":"returned"}`,
	`{"rec_seq":9,"kind":"timer","recv_ns":4000,"hook_start_ns":4100,"timer":1}`,
	`{"rec_seq":10,"kind":"hook_end","hook_end_ns":4200,"input":9,"ending":"returned"}`,
	`{"rec_seq":11,"kind":"post","recv_ns":4300,"hook_start_ns":4400,"post":1}`,
	`{"rec_seq":12,"kind":"read_call","call_ns":4450,"read":1,"read_kind":"klines","subject":"md.v1.fetch.Binance","request":{"klines":{"interval":"1m","limit":2,"uid":1001}}}`,
	`{"rec_seq":13,"kind":"hook_end","hook_end_ns":4500,"input":11,"ending":"returned"}`,
	`{"rec_seq":14,"kind":"read_reply","reply_ns":5000,"hook_start_ns":5100,"outcome":"ok","read":1,"read_kind":"klines","reply":{"uid":1001,"klines":{"klines":[{"interval":"1m","close":100,"closed":true}]}}}`,
	`{"rec_seq":15,"kind":"order_call","call_ns":5150,"call":1,"op":"submit","api_id":7,"submit_gen":1,"cloids":["c1"]}`,
	`{"rec_seq":16,"kind":"hook_end","hook_end_ns":5200,"input":14,"ending":"returned"}`,
	`{"rec_seq":17,"kind":"order_publish","publish_ns":5250,"call":1,"request":{"submit":{"session_id":"s1","uid":1001,"qty":1,"price":100,"client_order_id":"c1"}}}`,
	`{"rec_seq":18,"kind":"order_reply","reply_ns":5300,"hook_start_ns":5400,"outcome":"ok","call":1,"reply":{"accepted":true,"client_order_id":"c1"}}`,
	`{"rec_seq":19,"kind":"hook_end","hook_end_ns":5500,"input":18,"ending":"returned"}`,
	`{"rec_seq":20,"kind":"td_event","recv_ns":6000,"hook_start_ns":6100,"api_id":7,"event":{"seq":"1","order_update":{"order":{"client_order_id":"c1","status":"ORDER_STATUS_NEW"}}}}`,
	`{"rec_seq":21,"kind":"order_call","call_ns":6150,"call":2,"op":"batch_cancel","batch":true,"api_id":7,"cloids":["c1"]}`,
	`{"rec_seq":22,"kind":"hook_end","hook_end_ns":6200,"input":20,"ending":"returned"}`,
	`{"rec_seq":23,"kind":"order_publish","publish_ns":6250,"call":2,"request":{"batch_cancel":{"orders":[{"client_order_id":"c1"}]}}}`,
	`{"rec_seq":24,"kind":"order_reply","reply_ns":6450,"hook_start_ns":6500,"outcome":"timeout","error":"context deadline exceeded","call":2}`,
	`{"rec_seq":25,"kind":"hook_end","hook_end_ns":6550,"input":24,"ending":"returned"}`,
	`{"rec_seq":26,"kind":"bindings","recv_ns":7000,"applied_ns":7100,"from_command":true,"bindings":{"version":{"epoch":"2","seq":"5"}}}`,
	`{"rec_seq":27,"kind":"md_feed","recv_ns":7500,"hook_start_ns":7600,"md_ts_ns":7400,"conn_epoch":1,"seq":4,"event":{"uid":1001,"trade":{"trade_id":"t1","price":100,"qty":1}}}`,
	`{"rec_seq":28,"kind":"hook_end","hook_end_ns":7700,"input":27,"ending":"panic","panic":"boom"}`,
	`{"rec_seq":29,"kind":"stop","stop_ns":8000,"onstop_asked_ns":8100}`,
	`{"rec_seq":30,"kind":"onstop","recv_ns":8100,"hook_start_ns":8200}`,
	`{"rec_seq":31,"kind":"hook_end","hook_end_ns":8300,"input":30,"ending":"returned"}`,
	`{"rec_seq":32,"kind":"end","end_ns":9000,"dropped":0}`,
}

// writeLog writes recs as files of incarnation inc in dir, a new file at each index in splits.
func writeLog(t *testing.T, dir string, inc uint32, recs []eventlog.Record, splits ...int) {
	t.Helper()
	n := uint32(0)
	var buf []byte
	flush := func() {
		if buf != nil {
			if err := os.WriteFile(filepath.Join(dir, eventlog.Name(inc, n)), buf, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		n++
		buf = eventlog.AppendHeader(nil, eventlog.Header{
			Incarnation: inc, N: n, CreatedNs: 900, SessionID: "s1",
			SDKVersion: "v0.0.0-20261031090000-0a1b2c3d4e5f", SDKCommit: "0a1b2c3d4e5f",
		})
	}
	flush()
	for i, r := range recs {
		for _, s := range splits {
			if i == s {
				flush()
			}
		}
		buf = eventlog.AppendFrame(buf, r)
	}
	flush()
}

func TestSessionLines(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, 3, session(t))
	var out bytes.Buffer
	if err := Dir(&out, dir, 0); err != nil {
		t.Fatal(err)
	}
	same(t, out.String(), sessionLines)
}

// TestAcrossFiles splits the session where a read_reply's read_call is in the previous file, adds a
// newer incarnation, and cuts the last file inside a frame.
func TestAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	recs := session(t)
	writeLog(t, dir, 3, recs[:16], 13)
	writeLog(t, dir, 4, recs[:1])
	last := filepath.Join(dir, eventlog.Name(4, 1))
	b, err := os.ReadFile(last)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(last, b[:len(b)-3], 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := Dir(&out, dir, 3); err != nil {
		t.Fatal(err)
	}
	second := strings.Replace(sessionLines[0], `"3-1.mel"`, `"3-2.mel"`, 1)
	second = strings.Replace(second, `"n":1`, `"n":2`, 1)
	want := append(append(append([]string{}, sessionLines[:14]...), second), sessionLines[14:17]...)
	same(t, out.String(), want)

	out.Reset()
	if err := Dir(&out, dir, 4); err != nil {
		t.Fatal(err)
	}
	head := strings.Replace(strings.Replace(sessionLines[0], `"3-1.mel"`, `"4-1.mel"`, 1), `"incarnation":3`, `"incarnation":4`, 1)
	same(t, out.String(), []string{
		head,
		`{"file":"4-1.mel","error":"eventlog: the file ends inside a frame"}`,
	})
}

// TestReadReplyWithoutItsCall prints a read_reply whose read_call is not in the files as its bytes.
func TestReadReplyWithoutItsCall(t *testing.T) {
	f := New(&bytes.Buffer{}).Frame(eventlog.Record{Seq: 5, Kind: eventlog.KindReadReply, RecvNs: 1, TNs: 2, Ref: 9, Body: []byte{0x08, 0x01}})
	if f.Reply != nil || f.DecodeError == "" || !bytes.Equal(f.Body, []byte{0x08, 0x01}) || f.Read != 9 {
		t.Fatalf("frame %+v", f)
	}
}

// TestEndings prints the endings the session above does not have: a HUNG stop, a stop that
// delivers the TD buffer, and an OnStart that returned an error.
func TestEndings(t *testing.T) {
	d := New(&bytes.Buffer{})
	f := d.Frame(eventlog.Record{Seq: 9, Kind: eventlog.KindStop, Flags: eventlog.StopHung, RecvNs: 7000, TNs: 7000})
	if !f.Hung || f.StopNs != 7000 || f.OnStopAskedNs != 0 || f.Deliver {
		t.Fatalf("hung stop %+v", f)
	}
	f = d.Frame(eventlog.Record{Seq: 9, Kind: eventlog.KindStop, Flags: eventlog.StopDeliver, RecvNs: 8000, TNs: 8100})
	if f.Hung || !f.Deliver || f.StopNs != 8000 || f.OnStopAskedNs != 8100 {
		t.Fatalf("stop %+v", f)
	}
	f = d.Frame(eventlog.Record{Seq: 6, Kind: eventlog.KindHookEnd, Flags: eventlog.EndError, TNs: 2200, Ref: 5, Body: []byte("no account 7")})
	if f.Ending != "error" || f.Error != "no account 7" || f.Input != 5 || f.HookEndNs != 2200 {
		t.Fatalf("hook_end %+v", f)
	}
}

func TestPathFile(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, 3, session(t)[:2])
	var out bytes.Buffer
	if err := Path(&out, filepath.Join(dir, eventlog.Name(3, 1)), 0); err != nil {
		t.Fatal(err)
	}
	same(t, out.String(), sessionLines[:3])
	if err := Path(&out, filepath.Join(dir, "missing"), 0); err == nil {
		t.Fatal("a missing path printed")
	}
}

func same(t *testing.T, got string, want []string) {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(got, "\n"), "\n")
	for i := range max(len(lines), len(want)) {
		var g, w string
		if i < len(lines) {
			g = lines[i]
		}
		if i < len(want) {
			w = want[i]
		}
		if g != w {
			t.Errorf("line %d\n got: %s\nwant: %s", i+1, g, w)
		}
	}
}
