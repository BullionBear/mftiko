package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	mdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/mdctl/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/internal/md/conn"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

const (
	testInst   = "md-tw-1"
	testTicker = "Binance_Spot_BTCUSDT"
)

func TestTwoFeedsShareOneConn(t *testing.T) {
	book := feed("s", "k-book", "orderbook")
	delta := feed("s", "k-delta", "orderbook_delta")
	d := assign(testInst, time.Unix(1, 0), time.Second, testMarkets(8, 1), nil, 20, []feedIn{book, delta}, snapshot{})
	conn := "md/Binance/public/0"
	if d.Atoms[book.Entry.Feed].Conn != conn || d.Atoms[delta.Entry.Feed].Conn != conn || len(d.Plans) != 1 {
		t.Fatalf("placement %+v plans %d", d.Atoms, len(d.Plans))
	}
	plan := d.Plans[agent.WorkerKey(conn)]
	want := []string{book.Entry.Feed, delta.Entry.Feed}
	if plan.Spawn.BookDepth != 20 || !slices.Equal(plan.Live.Feeds, want) {
		t.Fatalf("live %+v", plan.Live.Feeds)
	}
	got := plan.Live.desired()
	if len(got.GetAtoms()) != 1 || got.GetAtoms()[0].GetAtomId() != "" || got.GetAtoms()[0].GetExchTicker() != "" {
		t.Fatalf("the worker is handed feeds, not an atom: %+v", got)
	}
	if !slices.Equal(got.GetAtoms()[0].GetFeeds(), want) {
		t.Fatalf("feeds %v", got.GetAtoms()[0].GetFeeds())
	}
}

func TestOwnerSetIsNotACounter(t *testing.T) {
	a := feed("s1", "k1", "orderbook")
	b := feed("s2", "k2", "orderbook")
	if a.Entry.Feed != b.Entry.Feed {
		t.Fatal("the two sessions must want the same feed")
	}
	now := time.Unix(1, 0)
	d := assign(testInst, now, time.Second, testMarkets(8, 1), nil, 0, []feedIn{a, b}, snapshot{})
	id := a.Entry.Feed
	if len(d.Atoms) != 1 || len(d.Atoms[id].Feeds) != 1 || len(d.Outcomes) != 2 {
		t.Fatalf("feeds %v outcomes %d", d.Atoms[id].Feeds, len(d.Outcomes))
	}
	kept := assign(testInst, now.Add(time.Minute), time.Second, testMarkets(8, 1), nil, 0, []feedIn{a}, snapOf(d))
	if len(kept.Atoms) != 1 || len(kept.Linger) != 0 || len(kept.Atoms[id].Feeds) != 1 {
		t.Fatalf("dropping one owner lingered or unsubscribed: %+v linger %v", kept.Atoms[id], kept.Linger)
	}
}

func TestLingerKeepsTheLastFeeds(t *testing.T) {
	book := feed("s", "k-book", "orderbook")
	delta := feed("s", "k-delta", "orderbook_delta")
	now := time.Unix(1_000, 0)
	linger := time.Second
	both := assign(testInst, now, linger, testMarkets(8, 1), nil, 0, []feedIn{book, delta}, snapshot{})
	one := assign(testInst, now, linger, testMarkets(8, 1), nil, 0, []feedIn{book}, snapOf(both))
	if len(one.Linger) != 1 || !one.Linger[delta.Entry.Feed].Equal(now) || one.Atoms[book.Entry.Feed].Conn == "" {
		t.Fatalf("the feed with no owners lingers, the other stays: %+v linger %v", one.Atoms, one.Linger)
	}
	plan := one.Plans[agent.WorkerKey("md/Binance/public/0")]
	if !slices.Equal(plan.Live.Feeds, []string{book.Entry.Feed, delta.Entry.Feed}) {
		t.Fatalf("linger keeps the feed on the worker: %v", plan.Live.Feeds)
	}
	held := assign(testInst, now, linger, testMarkets(8, 1), nil, 0, nil, snapOf(one))
	if len(held.Atoms) != 2 || !held.Linger[delta.Entry.Feed].Equal(now) || len(held.Linger) != 2 {
		t.Fatalf("both feeds linger once the book owner is gone too: %+v %v", held.Atoms, held.Linger)
	}
	if len(held.Plans) != 1 {
		t.Fatal("the worker stays planned during linger")
	}
	gone := assign(testInst, now.Add(linger), linger, testMarkets(8, 1), nil, 0, nil, snapOf(held))
	if len(gone.Atoms) != 0 || len(gone.Plans) != 0 {
		t.Fatalf("linger elapsed, feeds still there: %+v", gone.Atoms)
	}
}

func TestBudgetOpensTheNextConnAndDoesNotConsolidate(t *testing.T) {
	m := testMarkets(1, 1)
	m["Binance"].(*fakeMarket).atoms[exchange.TopicTrade] = exchange.AtomRef{Route: "public", Channel: "trade"}
	book := feed("s", "k-book", "orderbook")
	tr := feed("s", "k-trade", "trade")
	now := time.Unix(1_000, 0)
	d := assign(testInst, now, time.Second, m, nil, 0, []feedIn{book, tr}, snapshot{})
	if d.Atoms[book.Entry.Feed].Conn != "md/Binance/public/0" || d.Atoms[tr.Entry.Feed].Conn != "md/Binance/public/1" {
		t.Fatalf("placement %+v", d.Atoms)
	}
	held := assign(testInst, now, time.Second, m, nil, 0, []feedIn{tr}, snapOf(d))
	if held.Atoms[book.Entry.Feed].Conn != "md/Binance/public/0" || len(held.Linger) != 1 {
		t.Fatalf("the empty conn should linger, not drop in the same commit: %+v", held.Atoms)
	}
	left := assign(testInst, now.Add(time.Second), time.Second, m, nil, 0, []feedIn{tr}, snapOf(held))
	if _, ok := left.Atoms[book.Entry.Feed]; ok || left.Atoms[tr.Entry.Feed].Conn != "md/Binance/public/1" {
		t.Fatalf("trade should stay on n=1: %+v", left.Atoms)
	}
	m["Binance"].(*fakeMarket).atoms[exchange.TopicAggTrade] = exchange.AtomRef{Route: "public", Channel: "agg"}
	agg := feed("s", "k-agg", "aggtrade")
	again := assign(testInst, now.Add(time.Second), time.Second, m, nil, 0, []feedIn{tr, agg}, snapOf(left))
	if again.Atoms[agg.Entry.Feed].Conn != "md/Binance/public/0" || again.Atoms[tr.Entry.Feed].Conn != "md/Binance/public/1" {
		t.Fatalf("new feed takes the hole, the old one stays: %+v", again.Atoms)
	}
}

func TestStickyPlacementAndBudgetReject(t *testing.T) {
	book := feed("s", "k", "orderbook")
	id := book.Entry.Feed
	stuck := assign(testInst, time.Unix(1, 0), time.Second, testMarkets(8, 1), nil, 0, []feedIn{book}, snapshot{
		ByHash: map[string]string{state.WorkerHash(id): "md/Binance/public/3"},
	})
	if stuck.Atoms[id].Conn != "md/Binance/public/3" {
		t.Fatalf("conn %s", stuck.Atoms[id].Conn)
	}
	over := testMarkets(1, 5)
	kept := assign(testInst, time.Unix(2, 0), time.Second, over, nil, 0, []feedIn{book}, snapOf(stuck))
	if kept.Atoms[id].Conn != "md/Binance/public/3" || kept.Outcomes[book.Key].Reject != "" {
		t.Fatalf("sticky atom over budget: %+v %q", kept.Atoms[id], kept.Outcomes[book.Key].Reject)
	}
	rejected := assign(testInst, time.Unix(3, 0), time.Second, over, nil, 0, []feedIn{book}, snapshot{})
	if rejected.Outcomes[book.Key].Reject != "connection budget" || len(rejected.Atoms) != 0 {
		t.Fatalf("reject %+v atoms %d", rejected.Outcomes[book.Key], len(rejected.Atoms))
	}
}

func TestUnsupportedAndUnknownVenue(t *testing.T) {
	m := testMarkets(8, 1)
	m["Binance"].(*fakeMarket).bad = map[exchange.Topic]error{exchange.TopicGreeks: exchange.ErrUnsupported}
	g := feed("s", "k", "greeks")
	d := assign(testInst, time.Unix(1, 0), time.Second, m, nil, 0, []feedIn{g}, snapshot{})
	if d.Outcomes["k"].Reject != "unsupported" {
		t.Fatalf("reject %q", d.Outcomes["k"].Reject)
	}
	unknown := feed("s", "k", "orderbook")
	unknown.Entry.Ticker.Venue = "Nope"
	d = assign(testInst, time.Unix(1, 0), time.Second, m, nil, 0, []feedIn{unknown}, snapshot{})
	if d.Outcomes["k"].Reject != "unknown venue" {
		t.Fatalf("reject %q", d.Outcomes["k"].Reject)
	}
}

func TestEndpointFailure(t *testing.T) {
	book := feed("s", "k", "orderbook")
	m := testMarkets(8, 1)
	m["Binance"].(*fakeMarket).urlErr = errors.New("no host")
	d := assign(testInst, time.Unix(1, 0), time.Second, m, nil, 0, []feedIn{book}, snapshot{})
	if d.Outcomes["k"].Reject != "endpoint" || len(d.Atoms) != 0 {
		t.Fatalf("%+v", d.Outcomes["k"])
	}
	id := book.Entry.Feed
	prev := snapshot{Atoms: map[string]placed{id: {
		Conn: "md/Binance/public/0", Venue: "Binance", Route: "public",
		Feeds: []string{book.Entry.Feed}, URL: "wss://stored/ws", Cost: 1,
	}}}
	kept := assign(testInst, time.Unix(2, 0), time.Second, m, nil, 0, []feedIn{book}, prev)
	if kept.Atoms[id].URL != "wss://stored/ws" || kept.Outcomes["k"].Reject != "" {
		t.Fatalf("stored url: %+v %q", kept.Atoms[id], kept.Outcomes["k"].Reject)
	}
}

func TestDecodeDemand(t *testing.T) {
	feedKey := feedKey("orderbook")
	key := state.MDDemandKey(testInst, "s", "feed/"+feedKey)
	got, err := Decode(key, mustDemand(feedKey))
	if err != nil || got.Feed != feedKey || got.Topic != exchange.TopicOrderBook || got.Ticker.String() != testTicker {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := Decode(key, mustDemand("orderbook.Binance_Spot_ETHUSDT")); err != errFeedMismatch {
		t.Fatalf("mismatch %v", err)
	}
	if _, err := Decode(key, []byte{0xff, 0xff, 0xff, 0xff, 0xff}); err != errBadDemand {
		t.Fatalf("bad proto %v", err)
	}
	if _, err := Decode(state.MDDemandKey(testInst, "s", "select/top"), mustDemand(feedKey)); err != errNotFeed {
		t.Fatalf("selector %v", err)
	}
	bad := "orderbook.not-a-ticker"
	if _, err := Decode(state.MDDemandKey(testInst, "s", "feed/"+bad), mustDemand(bad)); err != errBadFeed {
		t.Fatalf("ticker %v", err)
	}
}

func TestAckDecision(t *testing.T) {
	atom := "Binance:public:depth"
	key := agent.WorkerKey("md/Binance/public/0")
	o := outcome{Subject: "md.f." + testInst + ".orderbook." + testTicker, Worker: key, Atom: atom}
	st, reason, write := decide(o, nil, nil)
	if !write || st != statev1.EntryState_ENTRY_STATE_PENDING || reason != "starting" {
		t.Fatalf("starting %s %s %v", st, reason, write)
	}
	lost := map[agent.WorkerKey]agent.Observed{key: {State: agent.StateLost}}
	if _, _, write = decide(o, lost, nil); write {
		t.Fatal("LOST should leave the ack alone")
	}
	ended := map[agent.WorkerKey]agent.Observed{key: {State: agent.StateExited, Incarnation: 2}}
	st, reason, write = decide(o, ended, func(agent.WorkerKey) bool { return true })
	if !write || st != statev1.EntryState_ENTRY_STATE_DEAD || reason != "gave up" {
		t.Fatalf("dead %s %s", st, reason)
	}
	st, reason, _ = decide(o, ended, func(agent.WorkerKey) bool { return false })
	if st != statev1.EntryState_ENTRY_STATE_PENDING || reason != "restarting" {
		t.Fatalf("restarting %s %s", st, reason)
	}
	rep, err := proto.Marshal(&mdctlv1.Report{Atoms: []*mdctlv1.AtomStatus{{
		AtomId: atom, State: mdctlv1.AtomState_ATOM_STATE_SUBSCRIBED,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	running := map[agent.WorkerKey]agent.Observed{key: {
		State: agent.StateRunning, Incarnation: 4,
		Report: &workerv1.Report{Contract: conn.Contract, Payload: rep},
	}}
	st, reason, write = decide(o, running, nil)
	if !write || st != statev1.EntryState_ENTRY_STATE_READY || reason != "" {
		t.Fatalf("ready %s %q", st, reason)
	}
	errRep, err := proto.Marshal(&mdctlv1.Report{Atoms: []*mdctlv1.AtomStatus{{
		AtomId: atom, State: mdctlv1.AtomState_ATOM_STATE_ERROR,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	obs := running[key]
	obs.Report = &workerv1.Report{Contract: conn.Contract, Payload: errRep}
	running[key] = obs
	st, _, _ = decide(o, running, nil)
	if st != statev1.EntryState_ENTRY_STATE_PENDING {
		t.Fatalf("atom error is still pending, got %s", st)
	}
	rej := outcome{Reject: "not found", Subject: o.Subject}
	st, reason, _ = decide(rej, running, func(agent.WorkerKey) bool { return true })
	if st != statev1.EntryState_ENTRY_STATE_REJECTED || reason != "not found" {
		t.Fatalf("reject %s %s", st, reason)
	}

	msg := statusOf(o, statev1.EntryState_ENTRY_STATE_PENDING, "starting", running, time.Unix(10, 0))
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	later := proto.Clone(msg).(*statev1.EntryStatus)
	later.UpdatedAtNs = 99
	if !sameAck(string(raw), later) {
		t.Fatal("updated_at should not churn the ack")
	}
	later.Reason = "restarting"
	if sameAck(string(raw), later) {
		t.Fatal("a new reason is a new ack")
	}
}

func TestLiveHashIgnoresVersionAndSpecHashUsesURL(t *testing.T) {
	live := Live{Feeds: []string{"orderbook.Binance_Spot_BTCUSDT"}}
	h := live.Hash()
	withVer := live.desired()
	withVer.Version = &commonv1.Version{Epoch: 4, Seq: 9}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(withVer)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:8]) == h {
		t.Fatal("version changed the live hash")
	}
	another := Live{Feeds: []string{"orderbook.Binance_Spot_BTCUSDT", "orderbook_delta.Binance_Spot_BTCUSDT"}}
	if another.Hash() == h {
		t.Fatal("another feed left the live hash unchanged")
	}
	spec := Spec{Venue: "Binance", Route: "public", N: 1, URL: "wss://a", BookDepth: 20}
	other := spec
	other.URL = "wss://b"
	shallower := spec
	shallower.BookDepth = 0
	if spec.Hash() == other.Hash() || spec.Hash() == shallower.Hash() {
		t.Fatalf("spec hash %s %s %s", spec.Hash(), other.Hash(), shallower.Hash())
	}
	if (&Program{}).CarriesLive() {
		t.Fatal("Init does not carry Desired")
	}
	epochs := &epochTable{}
	epochs.set("md/Binance/public/1", 9)
	init, err := (&Program{Instance: testInst, epochs: epochs}).Init("md/Binance/public/1", spec, Live{})
	if err != nil {
		t.Fatal(err)
	}
	var payload mdctlv1.InitPayload
	if err := proto.Unmarshal(init.GetPayload(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.GetConnEpoch() != 9 || payload.GetUrl() != spec.URL || payload.GetBookDepth() != 20 || init.GetContract() != conn.Contract {
		t.Fatalf("init %+v", &payload)
	}
	body, err := proto.Marshal(&mdctlv1.Report{ConnEpoch: 9, State: mdctlv1.ConnState_CONN_STATE_CONNECTING})
	if err != nil {
		t.Fatal(err)
	}
	n, ok := connEpoch(&workerv1.Report{Contract: conn.Contract, Payload: body})
	if !ok || n != 9 {
		t.Fatalf("epoch %d %v", n, ok)
	}
	if _, ok := connEpoch(&workerv1.Report{Contract: conn.Contract}); ok {
		t.Fatal("empty payload is not a report")
	}
}

type fakeMarket struct {
	atoms  map[exchange.Topic]exchange.AtomRef
	bad    map[exchange.Topic]error
	budget int
	cost   int
	url    string
	urlErr error
}

func (m *fakeMarket) Topics() []exchange.TopicCaps { return nil }

func (m *fakeMarket) Atom(topic exchange.Topic, _ *symbol.Info) (exchange.AtomRef, error) {
	if err, ok := m.bad[topic]; ok {
		return exchange.AtomRef{}, err
	}
	ref, ok := m.atoms[topic]
	if !ok {
		return exchange.AtomRef{}, exchange.ErrUnsupported
	}
	return ref, nil
}

func (m *fakeMarket) Routes() []string  { return []string{"public"} }
func (m *fakeMarket) Budget(string) int { return m.budget }
func (m *fakeMarket) Cost(string) int   { return m.cost }
func (m *fakeMarket) Endpoint(string, string) (string, error) {
	if m.urlErr != nil {
		return "", m.urlErr
	}
	return m.url, nil
}
func (m *fakeMarket) NewSession(string) (exchange.Session, error) {
	return nil, errors.New("no session")
}
func (m *fakeMarket) Book(string) (exchange.BookSync, bool) { return nil, false }

type venueMarkets map[string]exchange.Market

func (m venueMarkets) Market(venue string) (exchange.Market, bool) {
	mk, ok := m[venue]
	return mk, ok
}

func testMarkets(budget, cost int) venueMarkets {
	return venueMarkets{"Binance": &fakeMarket{
		atoms: map[exchange.Topic]exchange.AtomRef{
			exchange.TopicOrderBook:      {Route: "public", Channel: "depth"},
			exchange.TopicOrderBookDelta: {Route: "public", Channel: "depth"},
		},
		budget: budget, cost: cost, url: "wss://example/ws",
	}}
}

func feed(session, key, topic string) feedIn {
	t := mustParse(testTicker)
	return feedIn{
		Key: key,
		Entry: Entry{
			Session: session, Feed: topic + "." + t.String(), Topic: exchange.Topic(topic), Ticker: t,
		},
	}
}

func feedKey(topic string) string { return topic + "." + testTicker }

func snapOf(d derived) snapshot { return snapshot{Atoms: d.Atoms, Linger: d.Linger} }

func mustParse(s string) symbol.UniversalTicker {
	t, err := symbol.Parse(s)
	if err != nil {
		panic(err)
	}
	return t
}

func mustDemand(feed string) []byte {
	b, err := proto.Marshal(&statev1.DemandEntry{Target: &statev1.DemandEntry_Feed{Feed: feed}})
	if err != nil {
		panic(err)
	}
	return b
}
