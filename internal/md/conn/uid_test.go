package conn

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/exchange"
)

// TestRedundantWorkersShareUIDAndDigest is the E08-D065 acceptance for the conn
// worker. Two workers with the same active table publish one instrument: the
// payloads carry that UID, the digests match, and tape and the book snapshot
// carry the same UID. Subjects stay on the ticker.
func TestRedundantWorkersShareUIDAndDigest(t *testing.T) {
	info := tapeInfo(symbol.Tradable)
	var mu sync.Mutex
	var got []publication
	waits := map[string]chan []byte{}
	publish := func(subject string, msg []byte) error {
		if strings.HasPrefix(subject, "_INBOX_") {
			mu.Lock()
			ch := waits[subject]
			delete(waits, subject)
			mu.Unlock()
			if ch != nil {
				ch <- bytes.Clone(msg)
			}
			return nil
		}
		mu.Lock()
		got = append(got, publication{subject, bytes.Clone(msg)})
		mu.Unlock()
		return nil
	}

	now := time.Unix(0, 99)
	handlers := map[string]func(string, []byte){}
	newWorker := func(inst string) *Conn {
		c := New(Config{
			Venue: "Binance", Route: "public", Instance: inst,
			Market:  &reconMarket{},
			Symbols: stubSymbols{info: info},
			Now:     func() time.Time { return now },
			Publish: publish,
			Subscribe: func(subject string, handler func(string, []byte)) (func(), error) {
				mu.Lock()
				handlers[subject] = handler
				mu.Unlock()
				return func() {
					mu.Lock()
					delete(handlers, subject)
					mu.Unlock()
				}, nil
			},
		})
		c.SetDesired(feedsDesired(1, "trade."+tapeTicker, "orderbook_delta."+tapeTicker))
		c.mu.Lock()
		c.epoch = 3
		c.live = newLink(nil)
		c.onUpLocked()
		c.mu.Unlock()
		return c
	}
	a := newWorker("md-a")
	b := newWorker("md-b")
	trade := &exchange.Trade{ExchTsNs: 5, TradeID: "id", Price: 1.25, Qty: 2, Side: exchange.SideSell}
	snap := &exchange.BookUpdate{
		Snapshot: true, ExchTsNs: 77, Last: 1,
		Bids: []exchange.Level{{Price: 100, Qty: 1}},
		Asks: []exchange.Level{{Price: 101, Qty: 2}},
	}
	for _, c := range []*Conn{a, b} {
		c.mu.Lock()
		c.Trade(tapeCh, trade)
		c.Book(tapeCh, snap)
		c.mu.Unlock()
	}

	mu.Lock()
	pubs := append([]publication(nil), got...)
	mu.Unlock()
	bySubject := map[string]publication{}
	for _, p := range pubs {
		if _, dup := bySubject[p.subject]; dup {
			t.Fatalf("duplicate %s", p.subject)
		}
		bySubject[p.subject] = p
	}
	pairs := [][2]string{
		{"md.v1.f.md-a.trade." + tapeTicker, "md.v1.f.md-b.trade." + tapeTicker},
		{"md.v1.f.md-a.orderbook_delta." + tapeTicker, "md.v1.f.md-b.orderbook_delta." + tapeTicker},
	}
	if len(pubs) != len(pairs)*2 {
		t.Fatalf("published %d: %v", len(pubs), subjects(pubs))
	}
	for _, pair := range pairs {
		left, right := bySubject[pair[0]], bySubject[pair[1]]
		if left.msg == nil || right.msg == nil {
			t.Fatalf("missing %s or %s", pair[0], pair[1])
		}
		_, _, _, dL, pL, err := envelope.Parse(left.msg)
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, dR, pR, err := envelope.Parse(right.msg)
		if err != nil {
			t.Fatal(err)
		}
		if dL != dR || !bytes.Equal(pL, pR) {
			t.Fatalf("%s digest %#x / %#x", pair[0], dL, dR)
		}
		ev := &mdv1.FeedEvent{}
		if err := proto.Unmarshal(pL, ev); err != nil {
			t.Fatal(err)
		}
		if ev.GetUid() != tapeUID {
			t.Fatalf("%s uid %d", pair[0], ev.GetUid())
		}
	}

	ask := func(inst, topic string) *mdv1.ViewReply {
		t.Helper()
		subject := viewSubject(inst, topic, tapeTicker)
		mu.Lock()
		fn := handlers[subject]
		mu.Unlock()
		if fn == nil {
			t.Fatalf("no responder for %s", subject)
		}
		body, err := proto.Marshal(&mdv1.ViewRequest{})
		if err != nil {
			t.Fatal(err)
		}
		inbox := "_INBOX_" + inst + "." + topic
		ch := make(chan []byte, 1)
		mu.Lock()
		waits[inbox] = ch
		mu.Unlock()
		fn(inbox, body)
		select {
		case raw := <-ch:
			rep := &mdv1.ViewReply{}
			if err := proto.Unmarshal(raw, rep); err != nil {
				t.Fatal(err)
			}
			return rep
		default:
			t.Fatalf("%s did not answer", subject)
			return nil
		}
	}
	for _, inst := range []string{"md-a", "md-b"} {
		tape := ask(inst, "trade")
		evs := tape.GetTape().GetEvents()
		if tape.GetError() != "" || len(evs) != 1 || evs[0].GetEvent().GetUid() != tapeUID {
			t.Fatalf("%s tape %+v", inst, tape)
		}
		book := ask(inst, "orderbook_delta")
		if book.GetError() != "" || book.GetBook().GetUid() != tapeUID {
			t.Fatalf("%s book %+v", inst, book)
		}
	}
}

// TestPublishStampsEachFeedUID checks that one channel carrying two instruments
// does not reuse a single payload: each feed's UID is its own, and the digests
// differ.
func TestPublishStampsEachFeedUID(t *testing.T) {
	rec := newRecorder()
	c := New(Config{
		Venue: "V", Route: "r", Instance: "md", Market: &stubMarket{},
		Publish: rec.publish, Now: func() time.Time { return time.Unix(0, 1) },
	})
	c.epoch = 1
	c.live = newLink(nil)
	c.wanted = map[string][]feed{
		"ch": {
			{topic: "trade", ticker: "A", uid: 11},
			{topic: "trade", ticker: "B", uid: 22},
		},
	}
	c.Trade("ch", &exchange.Trade{ExchTsNs: 5, TradeID: "x", Price: 1, Qty: 1, Side: exchange.SideBuy})
	pubs := rec.snapshot()
	if len(pubs) != 2 {
		t.Fatalf("published %d", len(pubs))
	}
	var digests [2]uint64
	var uids [2]uint32
	for i, p := range pubs {
		_, _, _, dig, payload, err := envelope.Parse(p.msg)
		if err != nil {
			t.Fatal(err)
		}
		ev := &mdv1.FeedEvent{}
		if err := proto.Unmarshal(payload, ev); err != nil {
			t.Fatal(err)
		}
		digests[i] = dig
		uids[i] = ev.GetUid()
	}
	if uids != [2]uint32{11, 22} || digests[0] == digests[1] {
		t.Fatalf("uids %v digests %#x %#x", uids, digests[0], digests[1])
	}
}
