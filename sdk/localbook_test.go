package sdk

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/sdk/book"
	"github.com/BullionBear/mftiko/sdk/internal/bookpath"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
	"github.com/BullionBear/mftiko/sdk/internal/mdpath"
)

func TestBookWithoutKeeper(t *testing.T) {
	p := newPlane(&bookRec{}, host{})
	if _, ok := p.sc.Book(1); ok {
		t.Fatal("unsynced")
	}
}

// TestOnBookReadsMDView is the fake-MD path: snapshots are md.view replies, and OnBook
// sees a book that already includes the delta that woke it.
func TestOnBookReadsMDView(t *testing.T) {
	url := startNATS(t)
	nc := dialNATS(t, url, "session")
	md := dialNATS(t, url, "md")

	const (
		uid     = uint32(42)
		subject = "md.v1.f.md-tw-1.orderbook_delta.Binance_Spot_BTCUSDT"
		view    = "md.v1.view.md-tw-1.orderbook_delta.Binance_Spot_BTCUSDT"
	)
	var views atomic.Int32
	var mu sync.Mutex
	var subjects []string
	if _, err := md.Subscribe("md.v1.view.>", func(m *nats.Msg) {
		n := views.Add(1)
		mu.Lock()
		subjects = append(subjects, m.Subject)
		mu.Unlock()
		var snap *mdv1.BookSnapshot
		switch n {
		case 1:
			snap = bookShot(uid, 1, 1, 1)
		case 2:
			snap = bookShot(uid, 1, 4, 4)
		default:
			return
		}
		body, err := proto.Marshal(&mdv1.ViewReply{Result: &mdv1.ViewReply_Book{Book: snap}})
		if err != nil {
			return
		}
		_ = m.Respond(body)
	}); err != nil {
		t.Fatal(err)
	}
	if err := md.Flush(); err != nil {
		t.Fatal(err)
	}

	rec := &bookRec{ch: make(chan uint64, 8)}
	p := newPlane(rec, host{log: func(string) {}})
	p.recv = time.Second
	var tap mdpath.Tap
	p.tap = &tap
	p.disp = dispatch.New(dispatch.Config{
		RecvWindow: time.Second,
		Feed:       tap.Feed,
		Book:       tap.Book,
		Log:        func(string) {},
	})
	path, err := mdpath.Open(mdpath.Config{
		NC:         nc,
		RecvWindow: time.Second,
		Disp:       p.disp,
		Tap:        &tap,
		Resolve: func(u uint32) (string, string, error) {
			if u == uid {
				return "Binance_Spot_BTCUSDT", "Binance", nil
			}
			return "", "", mdpath.ErrUnknownUID
		},
		Book: func(msg []byte) {
			if p.books != nil {
				p.books.OnMessage(msg)
			}
		},
		Log: func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.md = path
	applied := make(chan struct{}, 4)
	bk, err := bookpath.Open(bookpath.Config{
		Snapshot: func(u uint32, fn func(*mdv1.BookSnapshot, error)) error {
			return path.BookSnapshot(u, func(snap *mdv1.BookSnapshot, err error) {
				fn(snap, err)
				applied <- struct{}{}
			})
		},
		Info: func(u uint32) (symbol.Info, error) {
			if u != uid {
				return symbol.Info{}, mdpath.ErrUnknownUID
			}
			return symbol.Info{UID: uid, Filters: symbol.Filters{symbol.PriceTick: 0.1}}, nil
		},
		Hook:       p.bookHook(),
		RecvWindow: time.Second,
		Schedule:   p.schedule,
		Log:        func(string) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.books = bk

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- p.disp.Run(ctx) }()
	t.Cleanup(func() {
		path.Close()
		cancel()
		select {
		case <-errc:
		case <-time.After(5 * time.Second):
			t.Errorf("dispatcher did not return")
		}
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = p.disp.OnStop(stopCtx)
		bk.Close()
	})

	if err := path.Apply(&stsctlv1.Bindings{
		Version: &commonv1.Version{Epoch: 1, Seq: 1},
		Feeds: []*stsctlv1.FeedBinding{{
			Feed:       "orderbook_delta.Binance_Spot_BTCUSDT",
			MdInstance: "md-tw-1",
			Subject:    subject,
			State:      stsctlv1.BindingState_BINDING_STATE_READY,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	pub := func(seq uint64, qty float64, ts int64) {
		t.Helper()
		body, err := proto.Marshal(&mdv1.FeedEvent{
			Uid: uid,
			Event: &mdv1.FeedEvent_OrderBookDelta{OrderBookDelta: &mdv1.OrderBookDelta{
				Meta: &mdv1.Meta{ExchTsNs: 77},
				Bids: []*mdv1.Level{{Price: 100, Qty: qty}},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := md.Publish(subject, envelope.Append(nil, ts, 1, seq, body)); err != nil {
			t.Fatal(err)
		}
		if err := md.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	waitSnap := func() {
		t.Helper()
		select {
		case <-applied:
		case <-time.After(5 * time.Second):
			t.Fatal("snapshot was not applied")
		}
	}
	waitHook := func() uint64 {
		t.Helper()
		select {
		case seq := <-rec.ch:
			return seq
		case <-time.After(5 * time.Second):
			t.Fatal("OnBook did not run")
		}
		return 0
	}

	pub(1, 1, now.UnixNano())
	waitSnap()
	pub(2, 2, now.UnixNano())
	if seq := waitHook(); seq != 2 {
		t.Fatalf("first hook seq %d", seq)
	}

	pub(4, 9, now.UnixNano())
	waitSnap()
	pub(5, 5, now.UnixNano())
	if seq := waitHook(); seq != 5 {
		t.Fatalf("after gap seq %d", seq)
	}

	pub(6, 6, now.Add(-time.Minute).UnixNano())
	pub(7, 7, now.UnixNano())
	if seq := waitHook(); seq != 7 {
		t.Fatalf("after stale seq %d", seq)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.ev) != 3 || len(rec.qty) != 3 {
		t.Fatalf("hooks %d", len(rec.ev))
	}
	wantSeq := []uint64{2, 5, 7}
	wantQty := []float64{2, 5, 7}
	for i := range wantSeq {
		if !rec.ok[i] || rec.ev[i].Seq != wantSeq[i] || rec.ev[i].UID != uid || rec.ev[i].ConnEpoch != 1 {
			t.Fatalf("event %d %+v ok %v", i, rec.ev[i], rec.ok[i])
		}
		if rec.ev[i].ExchTsNs != 77 || rec.qty[i] != wantQty[i] || rec.win != book.DefaultN {
			t.Fatalf("event %d qty %v exch %d win %d", i, rec.qty[i], rec.ev[i].ExchTsNs, rec.win)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if views.Load() != 2 || len(subjects) != 2 || subjects[0] != view || subjects[1] != view {
		t.Fatalf("views %d subjects %v", views.Load(), subjects)
	}
}

type bookRec struct {
	mu  sync.Mutex
	ev  []BookUpdate
	qty []float64
	ok  []bool
	win int
	ch  chan uint64
}

func (b *bookRec) OnInit(Context) error  { return nil }
func (b *bookRec) OnStart(Context) error { return nil }
func (b *bookRec) OnStop(Context)        {}

func (b *bookRec) OnBook(ctx Context, ev BookUpdate) {
	ob, ok := ctx.Book(ev.UID)
	var q float64
	var win int
	if ok {
		q, _ = ob.Qty(true, 100)
		win = ob.Window()
	}
	b.mu.Lock()
	b.ev = append(b.ev, ev)
	b.qty = append(b.qty, q)
	b.ok = append(b.ok, ok)
	b.win = win
	b.mu.Unlock()
	if b.ch != nil {
		b.ch <- ev.Seq
	}
}

func bookShot(uid uint32, epoch, seq uint64, qty float64) *mdv1.BookSnapshot {
	return &mdv1.BookSnapshot{
		Uid:       uid,
		ConnEpoch: epoch,
		Seq:       seq,
		Bids:      []*mdv1.Level{{Price: 100, Qty: qty}},
	}
}

func dialNATS(t *testing.T, url, name string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url, nats.Name(name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}
