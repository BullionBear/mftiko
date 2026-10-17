package mdpath

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
)

func TestNewerAndViewSubject(t *testing.T) {
	if newer(0, 0, nil) {
		t.Fatal("zero is not newer than zero")
	}
	if !newer(0, 0, &commonv1.Version{Seq: 1}) {
		t.Fatal("seq 1")
	}
	if newer(2, 9, &commonv1.Version{Epoch: 2, Seq: 9}) {
		t.Fatal("equal")
	}
	if newer(2, 1, &commonv1.Version{Epoch: 1, Seq: 99}) {
		t.Fatal("older epoch")
	}
	if !newer(1, 99, &commonv1.Version{Epoch: 2}) {
		t.Fatal("newer epoch")
	}
	got, ok := viewSubject("md.v1.f.custom.trade.NOT_THE_TICKER")
	if !ok || got != "md.v1.view.custom.trade.NOT_THE_TICKER" {
		t.Fatalf("view %q %v", got, ok)
	}
	if _, ok := viewSubject("other.subject"); ok {
		t.Fatal("prefix")
	}
	if _, err := fetchSubject("Binance"); err != nil {
		t.Fatal(err)
	}
	if _, err := fetchSubject("Binance.Spot"); err == nil {
		t.Fatal("dot")
	}
}

func TestClassifyDiff(t *testing.T) {
	oldA := &feed{key: "trade.T", topic: "trade", subject: "a", view: "va", sub: &nats.Subscription{}}
	oldB := &feed{key: "bestquote.T", topic: "bestquote", subject: "b", view: "vb", sub: &nats.Subscription{}}
	cur := map[string]*feed{"a": oldA, "b": oldB}
	next := map[string]*spec{
		"b": {key: "bestquote.T", topic: "bestquote", subject: "b", view: "vb"},
		"c": {key: "ticker.T", topic: "ticker", subject: "c", view: "vc"},
	}
	add, drop, merged := classify(cur, next)
	if len(add) != 1 || add[0].subject != "c" {
		t.Fatalf("add %v", subjectsOf(add))
	}
	if len(drop) != 1 || drop[0] != oldA {
		t.Fatalf("drop %v", subjectsOf(drop))
	}
	if merged["b"] != oldB {
		t.Fatal("kept subscription was replaced")
	}
	if merged["c"].topic != "ticker" {
		t.Fatal("added topic")
	}
}

func TestWantsSkipsBookAndEmptyKline(t *testing.T) {
	h := Hooks{Trade: func(Trade) {}, Kline: func(Kline) {}}
	if !wants(h, "trade") || !wants(h, "kline_1m") {
		t.Fatal("wanted")
	}
	if wants(h, "kline_") || wants(h, "ticker") || wants(h, topicBook) {
		t.Fatal("skipped")
	}
	if wants(Hooks{}, "trade") {
		t.Fatal("nil hook")
	}
}

func TestOpenRejects(t *testing.T) {
	if _, err := Open(Config{}); err == nil {
		t.Fatal("nil conn")
	}
	url := startNATS(t)
	nc := connect(t, url, "mdpath")
	if _, err := Open(Config{NC: nc}); err == nil {
		t.Fatal("zero window")
	}
	if _, err := Open(Config{NC: nc, RecvWindow: time.Millisecond}); err == nil {
		t.Fatal("nil dispatcher")
	}
	d := dispatch.New(dispatch.Config{RecvWindow: time.Millisecond})
	if _, err := Open(Config{NC: nc, RecvWindow: time.Millisecond, Disp: d}); err == nil {
		t.Fatal("nil tap")
	}
}

func TestVersionAndFeedDiff(t *testing.T) {
	r := newRig(t, time.Second, nil)
	a := subj("trade")
	b := subj("bestquote")
	c := subj("ticker")
	if err := r.p.Apply(snap(1, 1, feedBind("trade", a), feedBind("bestquote", b))); err != nil {
		t.Fatal(err)
	}
	wantSubjects(t, r, a, b)
	epoch, seq, ok := r.p.Applied()
	if !ok || epoch != 1 || seq != 1 {
		t.Fatalf("applied %d %d %v", epoch, seq, ok)
	}
	// Equal and older snapshots do not change the set.
	if err := r.p.Apply(snap(1, 1, feedBind("trade", a), feedBind("bestquote", b), feedBind("ticker", c))); err != nil {
		t.Fatal(err)
	}
	if err := r.p.Apply(snap(1, 0, feedBind("ticker", c))); err != nil {
		t.Fatal(err)
	}
	wantSubjects(t, r, a, b)
	if err := r.p.Apply(snap(1, 2, feedBind("bestquote", b), feedBind("ticker", c))); err != nil {
		t.Fatal(err)
	}
	wantSubjects(t, r, b, c)
	now := time.Now().UnixNano()
	r.pub(a, now, 1, 50, tradeEv(42))
	r.pub(b, now, 1, 1, bestEv(42))
	r.pub(c, now, 1, 1, tickerEv(42))
	waitFor(t, func() bool { return r.best.n() == 1 && r.tick.n() == 1 })
	if r.trades.n() != 0 {
		t.Fatalf("removed feed delivered %d", r.trades.n())
	}
	if err := r.p.Apply(snap(2, 0, feedBind("trade", a))); err != nil {
		t.Fatal(err)
	}
	wantSubjects(t, r, a)
	r.pub(a, now, 1, 2, tradeEv(42))
	waitFor(t, func() bool { return r.trades.n() == 1 })
	if r.trades.get()[0].UID != 42 || r.trades.get()[0].Seq != 2 {
		t.Fatalf("trade %+v", r.trades.get()[0].MD)
	}
}

func TestSelectorAndBadFeedLeaveTheSet(t *testing.T) {
	r := newRig(t, time.Second, nil)
	a := subj("trade")
	if err := r.p.Apply(snap(1, 1, feedBind("trade", a))); err != nil {
		t.Fatal(err)
	}
	withSel := snap(1, 2, feedBind("ticker", subj("ticker")))
	withSel.Selectors = []*stsctlv1.SelectorBinding{{Name: "front", SubjectPrefix: "md.v1.f.md-tw-1"}}
	if err := r.p.Apply(withSel); !errors.Is(err, ErrSelector) {
		t.Fatalf("selector %v", err)
	}
	older := snap(1, 0, feedBind("ticker", subj("ticker")))
	older.Selectors = []*stsctlv1.SelectorBinding{{Name: "front"}}
	if err := r.p.Apply(older); !errors.Is(err, ErrSelector) {
		t.Fatalf("older selector %v", err)
	}
	bad := snap(3, 1, &stsctlv1.FeedBinding{Feed: "noseparator", Subject: "md.v1.f.x"})
	if err := r.p.Apply(bad); err == nil {
		t.Fatal("bad feed key applied")
	}
	empty := snap(3, 1, &stsctlv1.FeedBinding{Feed: "trade.Binance_Spot_BTCUSDT"})
	if err := r.p.Apply(empty); err == nil {
		t.Fatal("empty subject applied")
	}
	two := snap(3, 1,
		feedBind("trade", a),
		&stsctlv1.FeedBinding{Feed: "trade.Binance_Spot_BTCUSDT", Subject: subj("trade") + ".other"},
	)
	if err := r.p.Apply(two); err == nil {
		t.Fatal("two subjects applied")
	}
	wantSubjects(t, r, a)
	epoch, seq, _ := r.p.Applied()
	if epoch != 1 || seq != 1 {
		t.Fatalf("version %d %d", epoch, seq)
	}
	// Accounts are not part of the subject set.
	ok := snap(1, 2, feedBind("bestquote", subj("bestquote")))
	ok.Accounts = []*stsctlv1.AccountBinding{{Name: "a", ApiId: 7}}
	if err := r.p.Apply(ok); err != nil {
		t.Fatal(err)
	}
	wantSubjects(t, r, subj("bestquote"))
}

func TestFirstZeroVersionApplies(t *testing.T) {
	r := newRig(t, time.Second, nil)
	a := subj("trade")
	if err := r.p.Apply(&stsctlv1.Bindings{Feeds: []*stsctlv1.FeedBinding{feedBind("trade", a)}}); err != nil {
		t.Fatal(err)
	}
	epoch, seq, ok := r.p.Applied()
	if !ok || epoch != 0 || seq != 0 {
		t.Fatalf("%d %d %v", epoch, seq, ok)
	}
	if err := r.p.Apply(&stsctlv1.Bindings{Feeds: []*stsctlv1.FeedBinding{feedBind("ticker", subj("ticker"))}}); err != nil {
		t.Fatal(err)
	}
	wantSubjects(t, r, a)
}

func TestSubjectIsNotComposed(t *testing.T) {
	r := newRig(t, time.Second, nil)
	r.fake.listen()
	// The feed key's ticker is not in the subject. The subscription is the binding's string.
	const given = "md.v1.f.custom-inst.trade.NOT_THE_TICKER"
	if err := r.p.Apply(snap(1, 1, feedBind("trade", given))); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	r.pub(subj("trade"), now, 1, 1, tradeEv(42))
	r.pub(given, now, 3, 8, tradeEv(42))
	waitFor(t, func() bool { return r.trades.n() == 1 })
	got := r.trades.get()[0]
	if got.UID != 42 || got.ConnEpoch != 3 || got.Seq != 8 {
		t.Fatalf("delivered %+v", got.MD)
	}
	if r.trades.n() != 1 {
		t.Fatalf("composed subject was subscribed, n=%d", r.trades.n())
	}
	var seen atomic.Value
	r.fake.onView = func(subject string, req *mdv1.ViewRequest) *mdv1.ViewReply {
		seen.Store(subject + "#" + strconv.FormatUint(uint64(req.GetLimit()), 10))
		return tapeReply(3, 8)
	}
	done := make(chan struct{})
	if err := r.p.Tape(42, "trade", TapeQuery{Limit: 4}, func(page *mdv1.TapePage, err error) {
		if err != nil || page.GetHead().GetSeq() != 8 {
			t.Errorf("tape %v %+v", err, page.GetHead())
		}
		close(done)
	}); err != nil {
		t.Fatal(err)
	}
	wait(t, done)
	if got, _ := seen.Load().(string); got != "md.v1.view.custom-inst.trade.NOT_THE_TICKER#4" {
		t.Fatalf("view subject %q", got)
	}
}

func TestTopicDispatch(t *testing.T) {
	r := newRig(t, time.Second, nil)
	topics := []string{
		"bestquote", "trade", "aggtrade", "kline_1m", "ticker",
		"liquidation", "funding_rate", "open_interest", "greeks", "mark_price",
	}
	var feeds []*stsctlv1.FeedBinding
	for _, topic := range topics {
		feeds = append(feeds, feedBind(topic, subj(topic)))
	}
	if err := r.p.Apply(snap(1, 1, feeds...)); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	for i, topic := range topics {
		r.pub(subj(topic), now, 1, uint64(i+1), eventFor(topic, 100+uint32(i)))
	}
	waitFor(t, func() bool {
		return r.best.n()+r.trades.n()+r.agg.n()+r.kline.n()+r.tick.n()+
			r.liq.n()+r.fund.n()+r.oi.n()+r.greeks.n()+r.mark.n() == len(topics)
	})
	if r.best.get()[0].UID != 100 || r.best.get()[0].Data.GetBid() != 1 {
		t.Fatalf("best %+v", r.best.get()[0])
	}
	if r.trades.get()[0].UID != 101 || r.trades.get()[0].Seq != 2 {
		t.Fatalf("trade %+v", r.trades.get()[0].MD)
	}
	if r.kline.get()[0].UID != 103 || r.kline.get()[0].Data.GetInterval() != "1m" {
		t.Fatalf("kline %+v", r.kline.get()[0])
	}
	if r.mark.get()[0].UID != 109 {
		t.Fatalf("mark %d", r.mark.get()[0].UID)
	}
	for _, n := range []int{r.best.n(), r.trades.n(), r.agg.n(), r.kline.n(), r.tick.n(), r.liq.n(), r.fund.n(), r.oi.n(), r.greeks.n(), r.mark.n()} {
		if n != 1 {
			t.Fatalf("cross talk %d", n)
		}
	}
}

func TestSkipBeforeDecode(t *testing.T) {
	r := newRig(t, time.Second, nil)
	r.p.hooks = Hooks{Trade: r.p.hooks.Trade}
	if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")), feedBind("ticker", subj("ticker")))); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	r.pubRaw(subj("trade"), envelope.Append(nil, now, 1, 1, []byte{0xff}))
	waitFor(t, func() bool { return r.logs.count("md payload rejected") == 1 })
	if r.trades.n() != 0 {
		t.Fatal("garbage trade was delivered")
	}
	r.pubRaw(subj("ticker"), envelope.Append(nil, now, 1, 1, []byte{0xff}))
	r.pub(subj("trade"), now, 1, 2, tradeEv(42))
	waitFor(t, func() bool { return r.trades.n() == 1 })
	if n := r.logs.count("md payload rejected"); n != 1 {
		t.Fatalf("ticker was decoded, payload logs %d", n)
	}
	if r.trades.get()[0].Seq != 2 {
		t.Fatalf("seq %d", r.trades.get()[0].Seq)
	}
}

func TestEnvelopeRejected(t *testing.T) {
	r := newRig(t, time.Second, nil)
	if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")))); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	r.pubRaw(subj("trade"), []byte{0x7f})
	badVer := envelope.Append(nil, now, 1, 1, []byte("v"))
	badVer[0] = 9
	r.pubRaw(subj("trade"), badVer)
	reserved := envelope.Append(nil, now, 1, 1, []byte("r"))
	reserved[3] = 0xab
	r.pubRaw(subj("trade"), reserved)
	waitFor(t, func() bool {
		_, md, _, _ := r.d.Stats()
		return md.Dropped >= 3 && r.logs.count("md envelope rejected") >= 3
	})
	select {
	case err := <-r.errc:
		t.Fatalf("session ended: %v", err)
	default:
	}
	r.pub(subj("trade"), now, 1, 4, tradeEv(42))
	waitFor(t, func() bool { return r.trades.n() == 1 })
	_, md, _, _ := r.d.Stats()
	if md.Dropped != 3 {
		t.Fatalf("dropped %d", md.Dropped)
	}
}

func TestLiveIsDeliveredWithoutTape(t *testing.T) {
	r := newRig(t, time.Second, nil)
	r.fake.listen()
	if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")))); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	for seq := uint64(1); seq <= 3; seq++ {
		r.pub(subj("trade"), now, 7, seq, tradeEv(42))
	}
	waitFor(t, func() bool { return r.trades.n() == 3 })
	if n := r.fake.views.Load(); n != 0 {
		t.Fatalf("tape requests %d", n)
	}
}

func TestStrategyDedupUsesHead(t *testing.T) {
	r := newRig(t, time.Second, nil)
	r.fake.listen()
	if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")))); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	for seq := uint64(1); seq <= 3; seq++ {
		r.pub(subj("trade"), now, 7, seq, tradeEv(42))
	}
	waitFor(t, func() bool { return r.trades.n() == 3 })
	if n := r.fake.views.Load(); n != 0 {
		t.Fatalf("sdk asked for tape on its own: %d", n)
	}
	r.fake.onView = func(string, *mdv1.ViewRequest) *mdv1.ViewReply {
		return &mdv1.ViewReply{Result: &mdv1.ViewReply_Tape{Tape: &mdv1.TapePage{
			Head:   &mdv1.Cursor{ConnEpoch: 7, Seq: 2},
			Events: []*mdv1.TapeEvent{{ConnEpoch: 7, Seq: 2}, {ConnEpoch: 7, Seq: 1}},
		}}}
	}
	got := make(chan *mdv1.TapePage, 1)
	if err := r.p.Tape(42, "trade", TapeQuery{}, func(page *mdv1.TapePage, err error) {
		if err != nil {
			t.Errorf("tape %v", err)
		}
		got <- page
	}); err != nil {
		t.Fatal(err)
	}
	var page *mdv1.TapePage
	select {
	case page = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("tape callback")
	}
	// The strategy keeps a live event only when its cursor is past head (D437, D847).
	var kept []uint64
	for _, ev := range r.trades.get() {
		if afterHead(ev.ConnEpoch, ev.Seq, page.GetHead()) {
			kept = append(kept, ev.Seq)
		}
	}
	if !slices.Equal(kept, []uint64{3}) {
		t.Fatalf("kept %v", kept)
	}
	if r.trades.n() != 3 {
		t.Fatal("sdk held or dropped live")
	}
}

func TestTapeCallbacks(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		r := newRig(t, time.Second, nil)
		r.fake.listen()
		if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")))); err != nil {
			t.Fatal(err)
		}
		since := int64(50)
		var gotLimit atomic.Uint32
		var gotSince atomic.Int64
		r.fake.onView = func(_ string, req *mdv1.ViewRequest) *mdv1.ViewReply {
			gotLimit.Store(req.GetLimit())
			gotSince.Store(req.GetSinceNs())
			if req.GetBefore().GetSeq() != 3 {
				t.Errorf("before %d", req.GetBefore().GetSeq())
			}
			return tapeReply(9, 4)
		}
		page, err := awaitTape(t, r, 42, "trade", TapeQuery{Limit: 12, SinceNs: since, Before: &mdv1.Cursor{ConnEpoch: 9, Seq: 3}})
		if err != nil {
			t.Fatal(err)
		}
		if page.GetHead().GetConnEpoch() != 9 || page.GetHead().GetSeq() != 4 {
			t.Fatalf("head %+v", page.GetHead())
		}
		if gotLimit.Load() != 12 || gotSince.Load() != since {
			t.Fatalf("req limit %d since %d", gotLimit.Load(), gotSince.Load())
		}
	})
	t.Run("reply error", func(t *testing.T) {
		r := newRig(t, time.Second, nil)
		r.fake.listen()
		if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")))); err != nil {
			t.Fatal(err)
		}
		r.fake.onView = func(string, *mdv1.ViewRequest) *mdv1.ViewReply {
			return &mdv1.ViewReply{Error: "unsupported"}
		}
		_, err := awaitTape(t, r, 42, "trade", TapeQuery{})
		if err == nil || err.Error() != "mdpath: md.view: unsupported" {
			t.Fatalf("%v", err)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		r := newRig(t, 30*time.Millisecond, nil)
		r.fake.listen()
		if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")))); err != nil {
			t.Fatal(err)
		}
		r.fake.onView = func(string, *mdv1.ViewRequest) *mdv1.ViewReply { return nil }
		_, err := awaitTape(t, r, 42, "trade", TapeQuery{})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("no responder", func(t *testing.T) {
		r := newRig(t, time.Second, nil)
		if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")))); err != nil {
			t.Fatal(err)
		}
		_, err := awaitTape(t, r, 42, "trade", TapeQuery{})
		if !errors.Is(err, nats.ErrNoResponders) {
			t.Fatalf("%v", err)
		}
	})
}

func TestReadCallbacks(t *testing.T) {
	t.Run("book", func(t *testing.T) {
		r := newRig(t, time.Second, nil)
		r.fake.listen()
		if err := r.p.Apply(snap(1, 1, feedBind(topicBook, subj(topicBook)))); err != nil {
			t.Fatal(err)
		}
		var seen atomic.Value
		r.fake.onView = func(subject string, _ *mdv1.ViewRequest) *mdv1.ViewReply {
			seen.Store(subject)
			return &mdv1.ViewReply{Result: &mdv1.ViewReply_Book{Book: &mdv1.BookSnapshot{
				Uid: 42, ConnEpoch: 2, Seq: 5,
			}}}
		}
		snap, err := awaitBook(t, r, 42)
		if err != nil {
			t.Fatal(err)
		}
		if snap.GetUid() != 42 || snap.GetSeq() != 5 {
			t.Fatalf("%+v", snap)
		}
		if got, _ := seen.Load().(string); got != "md.v1.view.md-tw-1.orderbook_delta.Binance_Spot_BTCUSDT" {
			t.Fatalf("subject %q", got)
		}
	})
	t.Run("not synced", func(t *testing.T) {
		r := newRig(t, time.Second, nil)
		r.fake.listen()
		if err := r.p.Apply(snap(1, 1, feedBind(topicBook, subj(topicBook)))); err != nil {
			t.Fatal(err)
		}
		r.fake.onView = func(string, *mdv1.ViewRequest) *mdv1.ViewReply {
			return &mdv1.ViewReply{Error: "not_synced"}
		}
		_, err := awaitBook(t, r, 42)
		if err == nil || err.Error() != "mdpath: md.view: not_synced" {
			t.Fatalf("%v", err)
		}
	})
	t.Run("klines", func(t *testing.T) {
		r := newRig(t, time.Second, nil)
		r.fake.listen()
		var uid atomic.Uint32
		r.fake.onFetch = func(subject string, req *mdv1.FetchRequest) *mdv1.FetchReply {
			if subject != "md.v1.fetch.Binance" {
				t.Errorf("subject %s", subject)
			}
			uid.Store(req.GetKlines().GetUid())
			if req.GetKlines().GetInterval() != "1m" || req.GetKlines().GetLimit() != 3 {
				t.Errorf("req %+v", req.GetKlines())
			}
			return &mdv1.FetchReply{Uid: 42, Result: &mdv1.FetchReply_Klines{Klines: &mdv1.Klines{
				Klines: []*mdv1.Kline{{Interval: "1m", Close: 2}},
			}}}
		}
		kl, err := awaitKlines(t, r, 42)
		if err != nil {
			t.Fatal(err)
		}
		if uid.Load() != 42 || len(kl.GetKlines()) != 1 || kl.GetKlines()[0].GetClose() != 2 {
			t.Fatalf("%+v uid %d", kl, uid.Load())
		}
	})
	t.Run("open interest", func(t *testing.T) {
		r := newRig(t, time.Second, nil)
		r.fake.listen()
		r.fake.onFetch = func(subject string, req *mdv1.FetchRequest) *mdv1.FetchReply {
			if subject != "md.v1.fetch.Bybit" || req.GetOpenInterest().GetUid() != 7 {
				t.Errorf("%s uid %d", subject, req.GetOpenInterest().GetUid())
			}
			return &mdv1.FetchReply{Uid: 7, Result: &mdv1.FetchReply_OpenInterest{OpenInterest: &mdv1.OpenInterest{
				OpenInterest: 11,
			}}}
		}
		oi, err := awaitOI(t, r, 7)
		if err != nil {
			t.Fatal(err)
		}
		if oi.GetOpenInterest() != 11 {
			t.Fatalf("%+v", oi)
		}
	})
	t.Run("fetch timeout", func(t *testing.T) {
		r := newRig(t, 30*time.Millisecond, nil)
		r.fake.listen()
		r.fake.onFetch = func(string, *mdv1.FetchRequest) *mdv1.FetchReply { return nil }
		_, err := awaitKlines(t, r, 42)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("fetch no responder", func(t *testing.T) {
		r := newRig(t, time.Second, nil)
		_, err := awaitOI(t, r, 7)
		if !errors.Is(err, nats.ErrNoResponders) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("fetch error", func(t *testing.T) {
		r := newRig(t, time.Second, nil)
		r.fake.listen()
		r.fake.onFetch = func(string, *mdv1.FetchRequest) *mdv1.FetchReply {
			return &mdv1.FetchReply{Error: "rate_limited", Reason: "weight"}
		}
		_, err := awaitKlines(t, r, 42)
		if err == nil || err.Error() != "mdpath: md.fetch: rate_limited: weight" {
			t.Fatalf("%v", err)
		}
	})
}

func TestCallbackWaitsForTheDispatcher(t *testing.T) {
	r := newRig(t, time.Second, nil)
	r.fake.listen()
	if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")))); err != nil {
		t.Fatal(err)
	}
	r.fake.onView = func(string, *mdv1.ViewRequest) *mdv1.ViewReply { return tapeReply(1, 1) }
	release := make(chan struct{})
	var once sync.Once
	inHook := make(chan struct{})
	if err := r.d.PushTD(func() {
		once.Do(func() { close(inHook) })
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		once.Do(func() {})
		select {
		case <-release:
		default:
			close(release)
		}
	})
	wait(t, inHook)

	enqueued := make(chan struct{})
	var oncePush sync.Once
	orig := r.p.pushTD
	r.p.pushTD = func(fn func()) error {
		err := orig(fn)
		oncePush.Do(func() { close(enqueued) })
		return err
	}
	ran := make(chan struct{})
	if err := r.p.Tape(42, "trade", TapeQuery{}, func(*mdv1.TapePage, error) { close(ran) }); err != nil {
		t.Fatal(err)
	}
	wait(t, enqueued)
	select {
	case <-ran:
		t.Fatal("callback ran before the dispatcher was free")
	default:
	}
	close(release)
	wait(t, ran)
}

func TestPrehookAndBookPath(t *testing.T) {
	books := &mdbox{}
	r := newRig(t, 20*time.Millisecond, func(msg []byte) {
		_, epoch, seq, _, payload, err := envelope.Parse(msg)
		if err != nil {
			t.Errorf("book %v", err)
			return
		}
		ev := &mdv1.FeedEvent{}
		if err := proto.Unmarshal(payload, ev); err != nil {
			t.Errorf("book payload %v", err)
			return
		}
		books.add(MD{UID: ev.GetUid(), ConnEpoch: epoch, Seq: seq})
	})
	if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")), feedBind(topicBook, subj(topicBook)))); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Second).UnixNano()
	r.pub(subj("trade"), old, 1, 1, tradeEv(42))
	waitFor(t, func() bool {
		_, md, _, _ := r.d.Stats()
		return md.Dropped >= 1
	})
	if r.trades.n() != 0 {
		t.Fatal("stale trade was delivered")
	}
	fresh := time.Now().UnixNano()
	r.pub(subj(topicBook), old, 4, 6, deltaEv(42))
	waitFor(t, func() bool { return books.n() == 1 })
	_, md, _, _ := r.d.Stats()
	if md.Dropped != 1 {
		t.Fatalf("book was counted as a drop: %d", md.Dropped)
	}
	if books.get()[0].UID != 42 || books.get()[0].Seq != 6 {
		t.Fatalf("book %+v", books.get()[0])
	}
	r.pub(subj("trade"), fresh, 1, 2, tradeEv(42))
	waitFor(t, func() bool { return r.trades.n() == 1 })
}

func TestSyncErrors(t *testing.T) {
	r := newRig(t, time.Second, nil)
	if err := r.p.Apply(snap(1, 1, feedBind("trade", subj("trade")))); err != nil {
		t.Fatal(err)
	}
	if err := r.p.Tape(42, "trade", TapeQuery{}, nil); err == nil {
		t.Fatal("nil callback")
	}
	if err := r.p.Tape(99, "trade", TapeQuery{}, func(*mdv1.TapePage, error) {}); !errors.Is(err, ErrUnknownUID) {
		t.Fatalf("uid %v", err)
	}
	if err := r.p.Tape(42, "ticker", TapeQuery{}, func(*mdv1.TapePage, error) {}); !errors.Is(err, ErrNoFeed) {
		t.Fatalf("feed %v", err)
	}
	if err := r.p.BookSnapshot(42, func(*mdv1.BookSnapshot, error) {}); !errors.Is(err, ErrNoFeed) {
		t.Fatalf("book %v", err)
	}
	if err := r.p.FetchKlines(99, "1m", 1, func(*mdv1.Klines, error) {}); !errors.Is(err, ErrUnknownUID) {
		t.Fatalf("fetch %v", err)
	}
	if n := r.fake.views.Load() + r.fake.fetches.Load(); n != 0 {
		t.Fatalf("requests %d", n)
	}
	bare := "not.a.feed.subject"
	if err := r.p.Apply(snap(1, 2, feedBind("trade", bare))); err != nil {
		t.Fatal(err)
	}
	if err := r.p.Tape(42, "trade", TapeQuery{}, func(*mdv1.TapePage, error) {}); !errors.Is(err, ErrBadSubject) {
		t.Fatalf("subject %v", err)
	}
	r.p.Close()
	if err := r.p.Apply(snap(1, 3, feedBind("trade", subj("trade")))); !errors.Is(err, ErrStopping) {
		t.Fatalf("apply %v", err)
	}
	if err := r.p.FetchOpenInterest(7, func(*mdv1.OpenInterest, error) {}); !errors.Is(err, ErrStopping) {
		t.Fatalf("closed %v", err)
	}
}

func afterHead(epoch, seq uint64, head *mdv1.Cursor) bool {
	if head == nil {
		return true
	}
	if epoch != head.GetConnEpoch() {
		return epoch > head.GetConnEpoch()
	}
	return seq > head.GetSeq()
}

func awaitTape(t *testing.T, r *rig, uid uint32, topic string, q TapeQuery) (*mdv1.TapePage, error) {
	t.Helper()
	type res struct {
		page *mdv1.TapePage
		err  error
	}
	ch := make(chan res, 1)
	if err := r.p.Tape(uid, topic, q, func(page *mdv1.TapePage, err error) {
		ch <- res{page, err}
	}); err != nil {
		return nil, err
	}
	select {
	case got := <-ch:
		return got.page, got.err
	case <-time.After(5 * time.Second):
		t.Fatal("tape callback")
	}
	return nil, nil
}

func awaitBook(t *testing.T, r *rig, uid uint32) (*mdv1.BookSnapshot, error) {
	t.Helper()
	type res struct {
		snap *mdv1.BookSnapshot
		err  error
	}
	ch := make(chan res, 1)
	if err := r.p.BookSnapshot(uid, func(snap *mdv1.BookSnapshot, err error) {
		ch <- res{snap, err}
	}); err != nil {
		return nil, err
	}
	select {
	case got := <-ch:
		return got.snap, got.err
	case <-time.After(5 * time.Second):
		t.Fatal("book callback")
	}
	return nil, nil
}

func awaitKlines(t *testing.T, r *rig, uid uint32) (*mdv1.Klines, error) {
	t.Helper()
	type res struct {
		k   *mdv1.Klines
		err error
	}
	ch := make(chan res, 1)
	if err := r.p.FetchKlines(uid, "1m", 3, func(k *mdv1.Klines, err error) {
		ch <- res{k, err}
	}); err != nil {
		return nil, err
	}
	select {
	case got := <-ch:
		return got.k, got.err
	case <-time.After(5 * time.Second):
		t.Fatal("klines callback")
	}
	return nil, nil
}

func awaitOI(t *testing.T, r *rig, uid uint32) (*mdv1.OpenInterest, error) {
	t.Helper()
	type res struct {
		oi  *mdv1.OpenInterest
		err error
	}
	ch := make(chan res, 1)
	if err := r.p.FetchOpenInterest(uid, func(oi *mdv1.OpenInterest, err error) {
		ch <- res{oi, err}
	}); err != nil {
		return nil, err
	}
	select {
	case got := <-ch:
		return got.oi, got.err
	case <-time.After(5 * time.Second):
		t.Fatal("oi callback")
	}
	return nil, nil
}

type rig struct {
	t      *testing.T
	p      *Path
	d      *dispatch.Disp
	nc     *nats.Conn
	fake   *fakeMD
	logs   *logBuf
	errc   chan error
	trades *box[Trade]
	best   *box[BestQuote]
	agg    *box[AggTrade]
	kline  *box[Kline]
	tick   *box[Ticker]
	liq    *box[Liquidation]
	fund   *box[FundingRate]
	oi     *box[OpenInterest]
	greeks *box[Greeks]
	mark   *box[MarkPrice]
}

func newRig(t *testing.T, recv time.Duration, book func([]byte)) *rig {
	t.Helper()
	url := startNATS(t)
	nc := connect(t, url, "session")
	srv := connect(t, url, "md")
	logs := &logBuf{}
	r := &rig{
		t: t, d: nil, nc: nc, fake: &fakeMD{t: t, nc: srv}, logs: logs,
		trades: &box[Trade]{}, best: &box[BestQuote]{}, agg: &box[AggTrade]{}, kline: &box[Kline]{},
		tick: &box[Ticker]{}, liq: &box[Liquidation]{}, fund: &box[FundingRate]{},
		oi: &box[OpenInterest]{}, greeks: &box[Greeks]{}, mark: &box[MarkPrice]{},
	}
	var tap Tap
	d := dispatch.New(dispatch.Config{
		RecvWindow: recv,
		Feed:       func(msg []byte) { tap.Feed(msg) },
		Book:       func(msg []byte) { tap.Book(msg) },
		Log:        logs.add,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	p, err := Open(Config{
		NC:         nc,
		RecvWindow: recv,
		Disp:       d,
		Tap:        &tap,
		Resolve:    testResolve,
		Hooks:      allHooksWired(r),
		Book:       book,
		Log:        logs.add,
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	r.p = p
	r.d = d
	r.errc = errc
	t.Cleanup(func() {
		p.Close()
		cancel()
		select {
		case <-errc:
		case <-time.After(5 * time.Second):
			t.Errorf("dispatcher did not return")
		}
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = d.OnStop(stopCtx)
	})
	return r
}

func (r *rig) pub(subject string, ts int64, epoch, seq uint64, ev *mdv1.FeedEvent) {
	r.t.Helper()
	body, err := proto.Marshal(ev)
	if err != nil {
		r.t.Fatal(err)
	}
	r.pubRaw(subject, envelope.Append(nil, ts, epoch, seq, body))
}

func (r *rig) pubRaw(subject string, msg []byte) {
	r.t.Helper()
	if err := r.fake.nc.Publish(subject, msg); err != nil {
		r.t.Fatal(err)
	}
	if err := r.fake.nc.Flush(); err != nil {
		r.t.Fatal(err)
	}
}

func testResolve(uid uint32) (string, string, error) {
	switch uid {
	case 42:
		return "Binance_Spot_BTCUSDT", "Binance", nil
	case 7:
		return "Bybit_Linear_ETHUSDT", "Bybit", nil
	default:
		return "", "", ErrUnknownUID
	}
}

func subj(topic string) string {
	return "md.v1.f.md-tw-1." + topic + ".Binance_Spot_BTCUSDT"
}

func feedBind(topic, subject string) *stsctlv1.FeedBinding {
	return &stsctlv1.FeedBinding{
		Feed:       topic + ".Binance_Spot_BTCUSDT",
		MdInstance: "md-tw-1",
		Subject:    subject,
		State:      stsctlv1.BindingState_BINDING_STATE_READY,
	}
}

func snap(epoch int64, seq uint64, feeds ...*stsctlv1.FeedBinding) *stsctlv1.Bindings {
	return &stsctlv1.Bindings{
		Version: &commonv1.Version{Epoch: epoch, Seq: seq},
		Feeds:   feeds,
	}
}

func tradeEv(uid uint32) *mdv1.FeedEvent {
	return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_Trade{Trade: &mdv1.Trade{Price: 1, Qty: 1}}}
}

func bestEv(uid uint32) *mdv1.FeedEvent {
	return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_BestQuote{BestQuote: &mdv1.BestQuote{Bid: 1, Ask: 2}}}
}

func tickerEv(uid uint32) *mdv1.FeedEvent {
	return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_Ticker{Ticker: &mdv1.Ticker{}}}
}

func deltaEv(uid uint32) *mdv1.FeedEvent {
	return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_OrderBookDelta{OrderBookDelta: &mdv1.OrderBookDelta{}}}
}

func eventFor(topic string, uid uint32) *mdv1.FeedEvent {
	switch topic {
	case "bestquote":
		return bestEv(uid)
	case "trade":
		return tradeEv(uid)
	case "aggtrade":
		return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_AggTrade{AggTrade: &mdv1.AggTrade{Price: 1}}}
	case "kline_1m":
		return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_Kline{Kline: &mdv1.Kline{Interval: "1m"}}}
	case "ticker":
		return tickerEv(uid)
	case "liquidation":
		return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_Liquidation{Liquidation: &mdv1.Liquidation{Qty: 1}}}
	case "funding_rate":
		return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_FundingRate{FundingRate: &mdv1.FundingRate{Rate: 1}}}
	case "open_interest":
		return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_OpenInterest{OpenInterest: &mdv1.OpenInterest{OpenInterest: 1}}}
	case "greeks":
		return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_Greeks{Greeks: &mdv1.Greeks{}}}
	case "mark_price":
		return &mdv1.FeedEvent{Uid: uid, Event: &mdv1.FeedEvent_MarkPrice{MarkPrice: &mdv1.MarkPrice{MarkPrice: 1}}}
	default:
		return tradeEv(uid)
	}
}

func tapeReply(epoch, seq uint64) *mdv1.ViewReply {
	return &mdv1.ViewReply{Result: &mdv1.ViewReply_Tape{Tape: &mdv1.TapePage{
		Head: &mdv1.Cursor{ConnEpoch: epoch, Seq: seq},
	}}}
}

func allHooksWired(r *rig) Hooks {
	return Hooks{
		BestQuote:    func(ev BestQuote) { r.best.add(ev) },
		Trade:        func(ev Trade) { r.trades.add(ev) },
		AggTrade:     func(ev AggTrade) { r.agg.add(ev) },
		Kline:        func(ev Kline) { r.kline.add(ev) },
		Ticker:       func(ev Ticker) { r.tick.add(ev) },
		Liquidation:  func(ev Liquidation) { r.liq.add(ev) },
		FundingRate:  func(ev FundingRate) { r.fund.add(ev) },
		OpenInterest: func(ev OpenInterest) { r.oi.add(ev) },
		Greeks:       func(ev Greeks) { r.greeks.add(ev) },
		MarkPrice:    func(ev MarkPrice) { r.mark.add(ev) },
	}
}

type fakeMD struct {
	t       *testing.T
	nc      *nats.Conn
	mu      sync.Mutex
	onView  func(string, *mdv1.ViewRequest) *mdv1.ViewReply
	onFetch func(string, *mdv1.FetchRequest) *mdv1.FetchReply
	views   atomic.Int64
	fetches atomic.Int64
}

func (f *fakeMD) listen() {
	f.t.Helper()
	if _, err := f.nc.Subscribe("md.v1.view.>", f.onViewMsg); err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.nc.Subscribe("md.v1.fetch.>", f.onFetchMsg); err != nil {
		f.t.Fatal(err)
	}
	if err := f.nc.Flush(); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fakeMD) onViewMsg(m *nats.Msg) {
	f.views.Add(1)
	f.mu.Lock()
	h := f.onView
	f.mu.Unlock()
	if h == nil {
		return
	}
	req := &mdv1.ViewRequest{}
	if err := proto.Unmarshal(m.Data, req); err != nil {
		f.t.Errorf("view: %v", err)
		return
	}
	rep := h(m.Subject, req)
	if rep == nil {
		return
	}
	body, err := proto.Marshal(rep)
	if err != nil {
		f.t.Errorf("view marshal: %v", err)
		return
	}
	if err := m.Respond(body); err != nil {
		f.t.Errorf("view respond: %v", err)
	}
}

func (f *fakeMD) onFetchMsg(m *nats.Msg) {
	f.fetches.Add(1)
	f.mu.Lock()
	h := f.onFetch
	f.mu.Unlock()
	if h == nil {
		return
	}
	req := &mdv1.FetchRequest{}
	if err := proto.Unmarshal(m.Data, req); err != nil {
		f.t.Errorf("fetch: %v", err)
		return
	}
	rep := h(m.Subject, req)
	if rep == nil {
		return
	}
	body, err := proto.Marshal(rep)
	if err != nil {
		f.t.Errorf("fetch marshal: %v", err)
		return
	}
	if err := m.Respond(body); err != nil {
		f.t.Errorf("fetch respond: %v", err)
	}
}

type box[T any] struct {
	mu sync.Mutex
	v  []T
}

func (b *box[T]) add(x T) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.v = append(b.v, x)
	b.mu.Unlock()
}

func (b *box[T]) n() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.v)
}

func (b *box[T]) get() []T {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]T(nil), b.v...)
}

type mdbox struct{ box[MD] }

type logBuf struct {
	mu sync.Mutex
	v  []string
}

func (l *logBuf) add(s string) {
	l.mu.Lock()
	l.v = append(l.v, s)
	l.mu.Unlock()
}

func (l *logBuf) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.v {
		if strings.Contains(s, sub) {
			n++
		}
	}
	return n
}

func wantSubjects(t *testing.T, r *rig, want ...string) {
	t.Helper()
	got := r.p.subjects()
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("subjects got %v want %v", got, want)
	}
}

func subjectsOf(list []*feed) []string {
	out := make([]string, len(list))
	for i, f := range list {
		out[i] = f.subject
	}
	return out
}

func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cond() {
			return
		}
		if !time.Now().Before(deadline) {
			if cond() {
				return
			}
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func connect(t *testing.T, url, name string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, nats.Name(name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func startNATS(t *testing.T) string {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: false}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		t.Fatal("nats-server did not accept connections")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	addr := ns.Addr()
	if addr == nil {
		t.Fatal("nats-server has no address")
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		t.Fatal(err)
	}
	return "nats://" + host + ":" + port
}
