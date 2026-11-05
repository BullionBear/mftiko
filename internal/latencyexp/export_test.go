package latencyexp

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/eventlog"
)

func TestFixture(t *testing.T) {
	data := t.TempDir()
	putFile(t, data, "aabbcc", "aabbcc", 1, 1, fixture(t))
	putFile(t, data, "ddeeff", "ddeeff", 1, 1, []eventlog.Record{
		mdRec(t, 1, eventlog.KindMDFeed, 100, 600, 1, 1, trade(9, 0)),
	})
	e := openExp(t, data)
	text := pollText(t, e)
	requireLine(t, text,
		`mftiko_md_hook_delay_seconds_bucket{session_id="aabbcc",kind="md_book",le="0.000001"} 1`,
		`mftiko_md_hook_delay_seconds_sum{session_id="aabbcc",kind="md_book"} 0.000001`,
		`mftiko_md_hook_delay_seconds_count{session_id="aabbcc",kind="md_book"} 1`,
		`mftiko_md_hook_delay_seconds_bucket{session_id="aabbcc",kind="md_feed",le="0"} 0`,
		`mftiko_md_hook_delay_seconds_bucket{session_id="aabbcc",kind="md_feed",le="0.000001"} 2`,
		`mftiko_md_hook_delay_seconds_bucket{session_id="aabbcc",kind="md_feed",le="0.000002"} 3`,
		`mftiko_md_hook_delay_seconds_bucket{session_id="aabbcc",kind="md_feed",le="+Inf"} 3`,
		`mftiko_md_hook_delay_seconds_sum{session_id="aabbcc",kind="md_feed"} 0.0000035`,
		`mftiko_md_hook_delay_seconds_count{session_id="aabbcc",kind="md_feed"} 3`,
		`mftiko_md_hook_delay_seconds_sum{session_id="ddeeff",kind="md_feed"} 0.0000005`,
		`mftiko_md_hook_delay_seconds_count{session_id="ddeeff",kind="md_feed"} 1`,

		`mftiko_hook_duration_seconds_sum{session_id="aabbcc",kind="md_book"} 0.000003`,
		`mftiko_hook_duration_seconds_count{session_id="aabbcc",kind="md_book"} 1`,
		`mftiko_hook_duration_seconds_sum{session_id="aabbcc",kind="md_feed"} 0.0000035`,
		`mftiko_hook_duration_seconds_count{session_id="aabbcc",kind="md_feed"} 3`,
		`mftiko_hook_duration_seconds_sum{session_id="aabbcc",kind="timer"} 0.000003`,
		`mftiko_hook_duration_seconds_count{session_id="aabbcc",kind="timer"} 1`,

		`mftiko_exch_hook_delay_seconds_sum{session_id="aabbcc",kind="md_book"} 0.000002`,
		`mftiko_exch_hook_delay_seconds_count{session_id="aabbcc",kind="md_book"} 1`,
		`mftiko_exch_hook_delay_seconds_sum{session_id="aabbcc",kind="md_feed"} 0.0000045`,
		`mftiko_exch_hook_delay_seconds_count{session_id="aabbcc",kind="md_feed"} 2`,

		`mftiko_order_ack_seconds_bucket{session_id="aabbcc",outcome="accepted",le="0.000002"} 1`,
		`mftiko_order_ack_seconds_bucket{session_id="aabbcc",outcome="accepted",le="0.000005"} 2`,
		`mftiko_order_ack_seconds_sum{session_id="aabbcc",outcome="accepted"} 0.000006`,
		`mftiko_order_ack_seconds_count{session_id="aabbcc",outcome="accepted"} 2`,
		`mftiko_order_ack_seconds_sum{session_id="aabbcc",outcome="local_reject"} 0.000001`,
		`mftiko_order_ack_seconds_count{session_id="aabbcc",outcome="local_reject"} 1`,

		`mftiko_order_exchange_seconds_sum{session_id="aabbcc",outcome="accepted"} 0.0002`,
		`mftiko_order_exchange_seconds_count{session_id="aabbcc",outcome="accepted"} 1`,
		`mftiko_order_exchange_seconds_sum{session_id="aabbcc",outcome="new"} 0.00002`,
		`mftiko_order_exchange_seconds_count{session_id="aabbcc",outcome="new"} 1`,
		`mftiko_order_exchange_seconds_sum{session_id="aabbcc",outcome="rejected"} 0.000004`,
		`mftiko_order_exchange_seconds_count{session_id="aabbcc",outcome="rejected"} 1`,

		`mftiko_order_reply_total{session_id="aabbcc",outcome="no_responders"} 1`,
		`mftiko_order_reply_total{session_id="aabbcc",outcome="timeout"} 1`,
		`mftiko_eventlog_rec_gap_total{session_id="aabbcc"} 1`,
		`mftiko_md_seq_gap_total{session_id="aabbcc",kind="md_feed"} 2`,
	)
	absent(t, text, `mftiko_md_seq_gap_total{session_id="aabbcc",kind="md_book"}`)
	absent(t, text, "c1")
	absent(t, text, "symbol")
	absent(t, text, "uid=")
	if strings.Count(text, "# HELP ") != len(families) {
		t.Fatalf("HELP lines = %d\n%s", strings.Count(text, "# HELP "), text)
	}
	again := pollText(t, e)
	if again != text {
		t.Fatal("second poll changed the text")
	}
	restart := pollText(t, openExp(t, data))
	if restart != text {
		t.Fatal("a new process did not recount the same log")
	}
}

// fixture is a recorded session. Times are nanoseconds and stay small.
// Each frame is there to produce one number in TestFixture.
func fixture(t *testing.T) []eventlog.Record {
	t.Helper()
	call := func(seq, id uint64, tNs int64, cloids ...string) eventlog.Record {
		return eventlog.Record{
			Seq: seq, Kind: eventlog.KindOrderCall, Flags: eventlog.OpSubmit, TNs: tNs, Ref: id,
			Body: eventlog.AppendOrderCall(nil, 7, 1, cloids),
		}
	}
	return []eventlog.Record{
		// md_feed delay 1500, exch 2500. envelope seq 1.
		mdRec(t, 1, eventlog.KindMDFeed, 1_000_000, 1_001_500, 1, 1, trade(7, 999_000)),
		{Seq: 2, Kind: eventlog.KindHookEnd, TNs: 1_003_000, Ref: 1}, // hook 1500, md_feed
		// md_book delay 1000, exch 2000. its own stream, envelope seq 1.
		mdRec(t, 3, eventlog.KindMDBook, 2_000_000, 2_001_000, 1, 1, book(7, 1_999_000)),
		{Seq: 4, Kind: eventlog.KindHookEnd, TNs: 2_004_000, Ref: 3}, // hook 3000, md_book
		// same feed, envelope seq 4: two numbers missing. exch_ts is 0, so no exch sample.
		mdRec(t, 5, eventlog.KindMDFeed, 3_000_000, 3_001_000, 1, 4, trade(7, 0)),
		{Seq: 6, Kind: eventlog.KindHookEnd, TNs: 3_002_000, Ref: 5}, // hook 1000, md_feed
		// conn_epoch 2 restarts the envelope sequence. not a gap. exch 2000.
		mdRec(t, 7, eventlog.KindMDFeed, 4_000_000, 4_001_000, 2, 1, trade(7, 3_999_000)),
		{Seq: 8, Kind: eventlog.KindHookEnd, TNs: 4_002_000, Ref: 7}, // hook 1000, md_feed

		call(9, 1, 5_000_000, "c1"),
		replyRec(t, 10, 0, 5_005_000, 1, true, 0), // ack 5000, accepted
		{Seq: 11, Kind: eventlog.KindTDEvent, RecvNs: 5_010_000, TNs: 5_010_100, Ref: 7, // pending: keep waiting
			Body: upd(t, "c1", tdv1.OrderStatus_ORDER_STATUS_PENDING_NEW)},
		{Seq: 12, Kind: eventlog.KindTDEvent, RecvNs: 5_020_000, TNs: 5_020_100, Ref: 7, // exchange 20000, new
			Body: upd(t, "c1", tdv1.OrderStatus_ORDER_STATUS_NEW)},

		call(13, 2, 6_000_000, "c2"),
		replyRec(t, 14, 0, 6_001_000, 2, false, 106), // ack 1000, local_reject; nothing was sent
		{Seq: 15, Kind: eventlog.KindTDEvent, RecvNs: 6_002_000, TNs: 6_002_100, Ref: 7,
			Body: upd(t, "c2", tdv1.OrderStatus_ORDER_STATUS_NEW)}, // must not count

		call(16, 3, 7_000_000, "c3"),
		{Seq: 17, Kind: eventlog.KindOrderReply, Flags: eventlog.Timeout, RecvNs: 7_100_000, TNs: 7_100_100, Ref: 3,
			Body: []byte("timeout")}, // counter, and the order may still be at the exchange
		{Seq: 18, Kind: eventlog.KindTDEvent, RecvNs: 7_200_000, TNs: 7_200_100, Ref: 7, // exchange 200000, accepted
			Body: upd(t, "c3", tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)},

		call(19, 4, 8_000_000, "c4"),
		{Seq: 20, Kind: eventlog.KindOrderReply, Flags: eventlog.NoResponders, RecvNs: 8_001_000, TNs: 8_001_100, Ref: 4,
			Body: []byte("no responders")},
		{Seq: 21, Kind: eventlog.KindTDEvent, RecvNs: 8_002_000, TNs: 8_002_100, Ref: 7,
			Body: upd(t, "c4", tdv1.OrderStatus_ORDER_STATUS_REJECTED)}, // must not count

		call(22, 5, 9_000_000, "c5"),
		replyRec(t, 23, 0, 9_001_000, 5, true, 0), // ack 1000, accepted
		{Seq: 24, Kind: eventlog.KindTDEvent, RecvNs: 9_002_000, TNs: 9_002_100, Ref: 7,
			Body: recon(t, "c5", tdv1.OrderStatus_ORDER_STATUS_NEW)}, // a snapshot is not an exchange answer
		{Seq: 25, Kind: eventlog.KindTDEvent, RecvNs: 9_004_000, TNs: 9_004_100, Ref: 7, // exchange 4000, rejected
			Body: upd(t, "c5", tdv1.OrderStatus_ORDER_STATUS_REJECTED)},

		// rec_seq 26 is missing: one dropped frame.
		{Seq: 27, Kind: eventlog.KindTimer, RecvNs: 1_000, TNs: 2_000, Ref: 1},
		{Seq: 28, Kind: eventlog.KindHookEnd, TNs: 5_000, Ref: 27}, // hook 3000, timer
	}
}

func TestOrders(t *testing.T) {
	t.Run("filled-consumes", func(t *testing.T) {
		data := t.TempDir()
		putFile(t, data, "s", "s", 1, 1, []eventlog.Record{
			callRec(1, 1_000, "cF"),
			replyRec(t, 2, 0, 1_500, 1, true, 0),
			{Seq: 3, Kind: eventlog.KindTDEvent, RecvNs: 2_000, TNs: 2_100, Body: upd(t, "cF", tdv1.OrderStatus_ORDER_STATUS_FILLED)},
			{Seq: 4, Kind: eventlog.KindTDEvent, RecvNs: 3_000, TNs: 3_100, Body: upd(t, "cF", tdv1.OrderStatus_ORDER_STATUS_ACCEPTED)},
		})
		text := pollText(t, openExp(t, data))
		requireLine(t, text, `mftiko_order_ack_seconds_count{session_id="s",outcome="accepted"} 1`)
		absent(t, text, "mftiko_order_exchange_seconds_count")
	})

	t.Run("batch", func(t *testing.T) {
		data := t.TempDir()
		putFile(t, data, "s", "s", 1, 1, []eventlog.Record{
			callRec(1, 10_000, "ca", "cb"),
			replyRec(t, 2, 0, 11_000, 1, true, 0),
			{Seq: 3, Kind: eventlog.KindTDEvent, RecvNs: 12_000, TNs: 12_100, Body: upd(t, "ca", tdv1.OrderStatus_ORDER_STATUS_NEW)},
			{Seq: 4, Kind: eventlog.KindTDEvent, RecvNs: 14_000, TNs: 14_100, Body: upd(t, "cb", tdv1.OrderStatus_ORDER_STATUS_REJECTED)},
		})
		text := pollText(t, openExp(t, data))
		requireLine(t, text,
			`mftiko_order_ack_seconds_sum{session_id="s",outcome="accepted"} 0.000001`,
			`mftiko_order_exchange_seconds_sum{session_id="s",outcome="new"} 0.000002`,
			`mftiko_order_exchange_seconds_sum{session_id="s",outcome="rejected"} 0.000004`,
		)
	})

	t.Run("codes", func(t *testing.T) {
		data := t.TempDir()
		putFile(t, data, "s", "s", 1, 1, []eventlog.Record{
			callRec(1, 1_000, "c100"),
			replyRec(t, 2, 0, 2_000, 1, false, 100), // 1000ns
			callRec(3, 3_000, "c199"),
			replyRec(t, 4, 0, 5_000, 3, false, 199), // 2000ns
			callRec(5, 6_000, "c99"),
			replyRec(t, 6, 0, 7_000, 5, false, 99),
			callRec(7, 8_000, "c200"),
			replyRec(t, 8, 0, 9_000, 7, false, 200),
			{Seq: 9, Kind: eventlog.KindOrderReply, RecvNs: 9_100, TNs: 9_100, Ref: 9, Body: []byte{0xff}},
			{Seq: 10, Kind: eventlog.KindOrderReply, Flags: eventlog.Timeout, RecvNs: 9_200, TNs: 9_200, Ref: 99,
				Body: []byte("timeout")},
		})
		text := pollText(t, openExp(t, data))
		requireLine(t, text,
			`mftiko_order_ack_seconds_count{session_id="s",outcome="local_reject"} 2`,
			`mftiko_order_ack_seconds_sum{session_id="s",outcome="local_reject"} 0.000003`,
			`mftiko_order_reply_total{session_id="s",outcome="timeout"} 1`,
		)
		absent(t, text, `outcome="accepted"`)
	})
}

func TestTail(t *testing.T) {
	t.Run("half-frame", func(t *testing.T) {
		data := t.TempDir()
		e := openExp(t, data)
		f1 := mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0))
		f2 := mdRec(t, 2, eventlog.KindMDFeed, 3_000, 5_000, 1, 2, trade(1, 0))
		raw := append(append(headerBytes("s", 1, 1), frameBytes(f1)...), frameBytes(f2)...)
		path := writeRaw(t, data, "s", 1, 1, raw[:len(raw)-1])
		text := pollText(t, e)
		requireLine(t, text, `mftiko_md_hook_delay_seconds_count{session_id="s",kind="md_feed"} 1`)
		if again := pollText(t, e); again != text {
			t.Fatal("poll of a short tail changed the text")
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		text = pollText(t, e)
		requireLine(t, text,
			`mftiko_md_hook_delay_seconds_count{session_id="s",kind="md_feed"} 2`,
			`mftiko_md_hook_delay_seconds_sum{session_id="s",kind="md_feed"} 0.000003`,
		)
		if again := pollText(t, e); again != text {
			t.Fatal("third poll changed the text")
		}
	})

	t.Run("append", func(t *testing.T) {
		data := t.TempDir()
		e := openExp(t, data)
		path := writeRaw(t, data, "s", 1, 1, headerBytes("s", 1, 1))
		if text := pollText(t, e); strings.Contains(text, "session_id=") {
			t.Fatalf("header only:\n%s", text)
		}
		raw := append(headerBytes("s", 1, 1), frameBytes(mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0)))...)
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		text := pollText(t, e)
		requireLine(t, text, `mftiko_md_hook_delay_seconds_count{session_id="s",kind="md_feed"} 1`)
		raw = append(raw, frameBytes(mdRec(t, 2, eventlog.KindMDFeed, 3_000, 5_000, 1, 2, trade(1, 0)))...)
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		text = pollText(t, e)
		requireLine(t, text,
			`mftiko_md_hook_delay_seconds_count{session_id="s",kind="md_feed"} 2`,
			`mftiko_md_hook_delay_seconds_sum{session_id="s",kind="md_feed"} 0.000003`,
		)
	})

	t.Run("rotation", func(t *testing.T) {
		data := t.TempDir()
		e := openExp(t, data)
		putFile(t, data, "s", "s", 1, 1, []eventlog.Record{
			mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0)),
		})
		pollText(t, e)
		putFile(t, data, "s", "s", 1, 2, []eventlog.Record{
			mdRec(t, 2, eventlog.KindMDFeed, 3_000, 5_000, 1, 2, trade(1, 0)),
		})
		text := pollText(t, e)
		requireLine(t, text,
			`mftiko_md_hook_delay_seconds_count{session_id="s",kind="md_feed"} 2`,
			`mftiko_md_hook_delay_seconds_sum{session_id="s",kind="md_feed"} 0.000003`,
		)
		absent(t, text, "mftiko_eventlog_rec_gap_total{")
	})

	t.Run("torn-then-rotate", func(t *testing.T) {
		data := t.TempDir()
		f1 := mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0))
		f2 := mdRec(t, 2, eventlog.KindMDFeed, 3_000, 5_000, 1, 2, trade(1, 0))
		raw := append(append(headerBytes("s", 1, 1), frameBytes(f1)...), frameBytes(f2)...)
		writeRaw(t, data, "s", 1, 1, raw[:len(raw)-1])
		putFile(t, data, "s", "s", 1, 2, []eventlog.Record{
			mdRec(t, 3, eventlog.KindMDFeed, 6_000, 8_000, 1, 3, trade(1, 0)),
		})
		text := pollText(t, openExp(t, data))
		requireLine(t, text,
			`mftiko_md_hook_delay_seconds_count{session_id="s",kind="md_feed"} 2`,
			`mftiko_md_hook_delay_seconds_sum{session_id="s",kind="md_feed"} 0.000003`,
			`mftiko_eventlog_rec_gap_total{session_id="s"} 1`,
		)
		if again := pollText(t, openExp(t, data)); again != text {
			t.Fatal("reading the rotated pair twice disagreed")
		}
	})

	t.Run("deleted-while-behind", func(t *testing.T) {
		data := t.TempDir()
		e := openExp(t, data)
		path := putFile(t, data, "s", "s", 1, 1, []eventlog.Record{timer(1), timer(2)})
		pollText(t, e)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		putFile(t, data, "s", "s", 1, 2, []eventlog.Record{timer(6)})
		text := pollText(t, e)
		requireLine(t, text, `mftiko_eventlog_rec_gap_total{session_id="s"} 3`)
	})

	t.Run("deleted-before-read", func(t *testing.T) {
		data := t.TempDir()
		putFile(t, data, "s", "s", 1, 2, []eventlog.Record{timer(5)})
		text := pollText(t, openExp(t, data))
		requireLine(t, text, `mftiko_eventlog_rec_gap_total{session_id="s"} 4`)
	})

	t.Run("incarnation", func(t *testing.T) {
		data := t.TempDir()
		putFile(t, data, "s", "s", 1, 1, []eventlog.Record{timer(1), timer(2)})
		putFile(t, data, "s", "s", 2, 1, []eventlog.Record{timer(1), timer(4)})
		text := pollText(t, openExp(t, data))
		requireLine(t, text, `mftiko_eventlog_rec_gap_total{session_id="s"} 2`)
	})

	t.Run("bad-length", func(t *testing.T) {
		data := t.TempDir()
		raw := append(append(headerBytes("s", 1, 1), frameBytes(mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0)))...),
			0xff, 0xff, 0xff, 0xff)
		writeRaw(t, data, "s", 1, 1, raw)
		putFile(t, data, "s", "s", 1, 2, []eventlog.Record{timer(3)})
		e := openExp(t, data)
		text := pollText(t, e)
		requireLine(t, text,
			`mftiko_md_hook_delay_seconds_count{session_id="s",kind="md_feed"} 1`,
			`mftiko_eventlog_rec_gap_total{session_id="s"} 1`,
		)
		if again := pollText(t, e); again != text {
			t.Fatal("second poll of a damaged file changed the text")
		}
	})

	t.Run("no-eventlog", func(t *testing.T) {
		data := t.TempDir()
		if err := os.MkdirAll(filepath.Join(data, "sessions", "bbbbbb", "volume"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(data, "sessions", "empty", "volume", eventlog.Dir), 0o755); err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(data, "sessions", "aabbcc", "volume", eventlog.Dir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not a log\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, eventlog.Name(1, 1)), []byte("NOTALOG and not a header\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		putFile(t, data, "aabbcc", "aabbcc", 1, 2, []eventlog.Record{
			mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0)),
		})
		text := pollText(t, openExp(t, data))
		requireLine(t, text, `mftiko_md_hook_delay_seconds_count{session_id="aabbcc",kind="md_feed"} 1`)
		absent(t, text, "bbbbbb")
		absent(t, text, `session_id="empty"`)
	})

	t.Run("appears", func(t *testing.T) {
		data := t.TempDir()
		e := openExp(t, data)
		if text := pollText(t, e); strings.Contains(text, "session_id=") {
			t.Fatalf("empty data dir:\n%s", text)
		}
		putFile(t, data, "s", "s", 1, 1, []eventlog.Record{timer(1)})
		text := pollText(t, e)
		requireLine(t, text, `# TYPE mftiko_eventlog_rec_gap_total counter`)
		absent(t, text, "mftiko_eventlog_rec_gap_total{")
	})
}

func TestUnknownKind(t *testing.T) {
	data := t.TempDir()
	putFile(t, data, "s", "s", 1, 1, []eventlog.Record{
		{Seq: 1, Kind: eventlog.KindTimer, RecvNs: 1_000, TNs: 2_000, Ref: 1},
		{Seq: 2, Kind: 99, Body: []byte("later")},
		{Seq: 3, Kind: eventlog.KindHookEnd, TNs: 5_000, Ref: 1},
	})
	text := pollText(t, openExp(t, data))
	requireLine(t, text,
		`mftiko_hook_duration_seconds_sum{session_id="s",kind="timer"} 0.000003`,
		`mftiko_hook_duration_seconds_count{session_id="s",kind="timer"} 1`,
	)
	absent(t, text, "mftiko_eventlog_rec_gap_total{")
	absent(t, text, "kind_99")
}

func TestClockSkew(t *testing.T) {
	data := t.TempDir()
	putFile(t, data, "s", "s", 1, 1, []eventlog.Record{
		mdRec(t, 1, eventlog.KindMDFeed, 1_000, 1_000, 1, 1, trade(1, 1_500)),
		{Seq: 2, Kind: eventlog.KindHookEnd, TNs: 900, Ref: 1},
		mdRec(t, 3, eventlog.KindMDFeed, 0, 50_000, 1, 2, trade(1, 0)),
		{Seq: 4, Kind: eventlog.KindMDFeed, RecvNs: 2_000, TNs: 3_000, Body: []byte("not-an-envelope")},
		{Seq: 5, Kind: eventlog.KindTDEvent, RecvNs: 4_000, TNs: 4_000, Body: []byte{0xff}},
	})
	text := pollText(t, openExp(t, data))
	requireLine(t, text,
		`mftiko_md_hook_delay_seconds_sum{session_id="s",kind="md_feed"} 0.000001`,
		`mftiko_md_hook_delay_seconds_count{session_id="s",kind="md_feed"} 2`,
		`mftiko_exch_hook_delay_seconds_bucket{session_id="s",kind="md_feed",le="0"} 1`,
		`mftiko_exch_hook_delay_seconds_sum{session_id="s",kind="md_feed"} -0.0000005`,
		`mftiko_exch_hook_delay_seconds_count{session_id="s",kind="md_feed"} 1`,
	)
	absent(t, text, "mftiko_hook_duration_seconds_count")
}

func TestSessionLabel(t *testing.T) {
	data := t.TempDir()
	putFile(t, data, "dirone", "fromhdr", 1, 1, []eventlog.Record{
		mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0)),
	})
	putFile(t, data, "dirtwo", "", 1, 1, []eventlog.Record{
		mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0)),
	})
	putFile(t, data, "dirthree", `a"b`, 1, 1, []eventlog.Record{
		mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0)),
	})
	text := pollText(t, openExp(t, data))
	requireLine(t, text,
		`mftiko_md_hook_delay_seconds_count{session_id="fromhdr",kind="md_feed"} 1`,
		`mftiko_md_hook_delay_seconds_count{session_id="dirtwo",kind="md_feed"} 1`,
		`mftiko_md_hook_delay_seconds_count{session_id="a\"b",kind="md_feed"} 1`,
	)
	absent(t, text, `session_id="dirone"`)
	absent(t, text, `session_id="dirthree"`)
}

func TestNew(t *testing.T) {
	if _, err := New(filepath.Join(t.TempDir(), "missing"), zerolog.Nop()); err == nil {
		t.Fatal("missing directory")
	}
	dir := t.TempDir()
	f := filepath.Join(dir, "file")
	if err := os.WriteFile(f, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(f, zerolog.Nop()); err == nil {
		t.Fatal("file accepted as a data directory")
	}
	e := openExp(t, dir)
	text := pollText(t, e)
	if !strings.Contains(text, "# HELP mftiko_md_hook_delay_seconds ") || strings.Contains(text, "session_id=") {
		t.Fatalf("empty exposition:\n%s", text)
	}
}

func TestCheckAddr(t *testing.T) {
	ok := []string{
		"127.0.0.1:9090", "10.0.0.1:1", "192.168.1.1:80", "172.16.0.1:443", "172.31.255.255:1",
		"169.254.1.1:9", "[::1]:9090", "[fe80::1]:9090", "[fe80::1%eth0]:1", "localhost:9090", "LocalHost:0",
	}
	bad := []string{
		"8.8.8.8:9090", "0.0.0.0:9090", "[::]:9090", ":9090", "11.0.0.1:9090", "172.32.0.1:9090",
		"192.169.1.1:1", "example.com:9090", "127.0.0.1", "127.0.0.1:99999", "",
	}
	for _, addr := range ok {
		if err := CheckAddr(addr); err != nil {
			t.Errorf("CheckAddr(%q) = %v", addr, err)
		}
	}
	for _, addr := range bad {
		if err := CheckAddr(addr); err == nil {
			t.Errorf("CheckAddr(%q) accepted", addr)
		}
	}
}

func TestServe(t *testing.T) {
	data := t.TempDir()
	putFile(t, data, "s", "s", 1, 1, []eventlog.Record{
		mdRec(t, 1, eventlog.KindMDFeed, 1_000, 2_000, 1, 1, trade(1, 0)),
	})
	e := openExp(t, data)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- e.Serve(ctx, ln) }()

	url := "http://" + ln.Addr().String() + "/metrics"
	// Serve accepts connections before the first Poll, so a GET can land on an
	// empty registry. Wait until the sample is there.
	want := []byte("mftiko_md_hook_delay_seconds_count{session_id=\"s\",kind=\"md_feed\"} 1\n")
	var resp *http.Response
	var body []byte
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err = http.Get(url)
		if err == nil {
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode == http.StatusOK && bytes.Contains(body, want) {
				break
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatal(err)
			}
			t.Fatalf("status %d GET body:\n%s", resp.StatusCode, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := resp.Header.Get("Content-Type"); got != metricsType {
		t.Fatalf("content type %q", got)
	}
	head, err := http.Head(url)
	if err != nil {
		t.Fatal(err)
	}
	headBody, err := io.ReadAll(head.Body)
	head.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(headBody) != 0 {
		t.Fatalf("HEAD body %q", headBody)
	}
	if got := head.Header.Get("Content-Type"); got != metricsType {
		t.Fatalf("HEAD content type %q", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hung")
	}
}

func openExp(t *testing.T, data string) *Exporter {
	t.Helper()
	e, err := New(data, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func pollText(t *testing.T, e *Exporter) string {
	t.Helper()
	e.Poll()
	return string(e.Text())
}

func requireLine(t *testing.T, text string, lines ...string) {
	t.Helper()
	for _, line := range lines {
		if !strings.Contains(text, line+"\n") {
			t.Errorf("missing %s", line)
		}
	}
	if t.Failed() {
		t.Logf("exposition:\n%s", text)
	}
}

func absent(t *testing.T, text, sub string) {
	t.Helper()
	if strings.Contains(text, sub) {
		t.Errorf("exposition contains %q\n%s", sub, text)
	}
}

func putFile(t *testing.T, data, dirName, sessionID string, inc, n uint32, recs []eventlog.Record) string {
	t.Helper()
	b := headerBytes(sessionID, inc, n)
	for _, r := range recs {
		b = append(b, frameBytes(r)...)
	}
	return writeRaw(t, data, dirName, inc, n, b)
}

func writeRaw(t *testing.T, data, dirName string, inc, n uint32, raw []byte) string {
	t.Helper()
	dir := filepath.Join(data, "sessions", dirName, "volume", eventlog.Dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, eventlog.Name(inc, n))
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func headerBytes(session string, inc, n uint32) []byte {
	return eventlog.AppendHeader(nil, eventlog.Header{
		Incarnation: inc, N: n, CreatedNs: 1_000, SessionID: session,
		SDKVersion: "v1.2.3", SDKCommit: "abc123",
	})
}

func frameBytes(r eventlog.Record) []byte { return eventlog.AppendFrame(nil, r) }

func mustPB(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func trade(uid uint32, exch int64) *mdv1.FeedEvent {
	tr := &mdv1.Trade{TradeId: "t1", Price: 100, Qty: 1}
	if exch != 0 {
		tr.Meta = &mdv1.Meta{ExchTsNs: exch}
	}
	return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_Trade{Trade: tr}}
}

func book(uid uint32, exch int64) *mdv1.FeedEvent {
	d := &mdv1.OrderBookDelta{}
	if exch != 0 {
		d.Meta = &mdv1.Meta{ExchTsNs: exch}
	}
	return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_OrderBookDelta{OrderBookDelta: d}}
}

func mdBody(t *testing.T, epoch, seq uint64, ev *mdv1.FeedEvent) []byte {
	t.Helper()
	return envelope.Append(nil, 1_000, epoch, seq, mustPB(t, ev))
}

func mdRec(t *testing.T, seq uint64, kind eventlog.Kind, recv, tNs int64, epoch, envSeq uint64, ev *mdv1.FeedEvent) eventlog.Record {
	t.Helper()
	return eventlog.Record{Seq: seq, Kind: kind, RecvNs: recv, TNs: tNs, Body: mdBody(t, epoch, envSeq, ev)}
}

func callRec(ref uint64, tNs int64, cloids ...string) eventlog.Record {
	return eventlog.Record{
		Seq: ref, Kind: eventlog.KindOrderCall, Flags: eventlog.OpSubmit, TNs: tNs, Ref: ref,
		Body: eventlog.AppendOrderCall(nil, 7, 1, cloids),
	}
}

func replyRec(t *testing.T, seq uint64, flags uint16, recv int64, ref uint64, accepted bool, code int32) eventlog.Record {
	t.Helper()
	return eventlog.Record{
		Seq: seq, Kind: eventlog.KindOrderReply, Flags: flags, RecvNs: recv, TNs: recv + 100, Ref: ref,
		Body: mustPB(t, &tdv1.OrderReply{Accepted: accepted, Code: code, ClientOrderId: "c"}),
	}
}

func upd(t *testing.T, cloid string, st tdv1.OrderStatus) []byte {
	t.Helper()
	return mustPB(t, &tdv1.AccountEvent{
		Seq: 1,
		Event: &tdv1.AccountEvent_OrderUpdate{OrderUpdate: &tdv1.OrderUpdate{
			Order: &tdv1.Order{ClientOrderId: cloid, Status: st},
		}},
	})
}

func recon(t *testing.T, cloid string, st tdv1.OrderStatus) []byte {
	t.Helper()
	return mustPB(t, &tdv1.AccountEvent{
		Event: &tdv1.AccountEvent_Recon{Recon: &tdv1.Recon{
			OpenOrders: []*tdv1.Order{{ClientOrderId: cloid, Status: st}},
		}},
	})
}

func timer(seq uint64) eventlog.Record {
	return eventlog.Record{Seq: seq, Kind: eventlog.KindTimer, RecvNs: 1_000, TNs: 2_000, Ref: 1}
}
