package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
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

func TestTwoFeedsShareOneAtom(t *testing.T) {
	book := feed("s", "k-book", "orderbook")
	delta := feed("s", "k-delta", "orderbook_delta")
	d := assign(testInst, time.Unix(1, 0), time.Second, testMarkets(8, 1), nil, 20, []feedIn{book, delta}, snapshot{})
	id := "Binance:public:depth"
	a, ok := d.Atoms[id]
	if !ok || a.Conn != "md/Binance/public/0" || a.Record || len(d.Plans) != 1 {
		t.Fatalf("atom %+v plans %d", a, len(d.Plans))
	}
	if !slices.Equal(a.Feeds, []string{book.Entry.Feed, delta.Entry.Feed}) {
		t.Fatalf("feeds %v", a.Feeds)
	}
	if a.ExchTicker != "BTCUSDT" || a.PriceTick != 0.01 {
		t.Fatalf("symbol fields %+v", a)
	}
	plan := d.Plans[agent.WorkerKey(a.Conn)]
	if plan.Spawn.BookDepth != 20 || len(plan.Live.Atoms) != 1 {
		t.Fatalf("desired %+v", plan)
	}
	if plan.Live.Atoms[0].ExchTicker != "BTCUSDT" || plan.Live.Atoms[0].PriceTick != 0.01 {
		t.Fatalf("assignment %+v", plan.Live.Atoms[0])
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
	id := "Binance:public:depth"
	if len(d.Atoms[id].Feeds) != 1 || len(d.Outcomes) != 2 {
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
	id := "Binance:public:depth"
	if len(one.Linger) != 0 || !slices.Equal(one.Atoms[id].Feeds, []string{book.Entry.Feed}) {
		t.Fatalf("one owner: %+v linger %v", one.Atoms[id], one.Linger)
	}
	held := assign(testInst, now, linger, testMarkets(8, 1), nil, 0, nil, snapOf(one))
	if len(held.Atoms) != 1 || !held.Linger[id].Equal(now) || !slices.Equal(held.Atoms[id].Feeds, one.Atoms[id].Feeds) {
		t.Fatalf("linger should keep the feeds at the moment it starts: %+v %v", held.Atoms[id], held.Linger)
	}
	if len(held.Plans) != 1 {
		t.Fatal("the worker stays planned during linger")
	}
	gone := assign(testInst, now.Add(linger), linger, testMarkets(8, 1), nil, 0, nil, snapOf(held))
	if len(gone.Atoms) != 0 || len(gone.Plans) != 0 {
		t.Fatalf("linger elapsed, atom still there: %+v", gone.Atoms)
	}
}

func TestBudgetOpensTheNextConnAndDoesNotConsolidate(t *testing.T) {
	m := testMarkets(1, 1)
	m["Binance"].(*fakeMarket).atoms[exchange.TopicTrade] = exchange.AtomRef{Route: "public", Channel: "trade"}
	book := feed("s", "k-book", "orderbook")
	tr := feed("s", "k-trade", "trade")
	now := time.Unix(1_000, 0)
	d := assign(testInst, now, time.Second, m, nil, 0, []feedIn{book, tr}, snapshot{})
	if d.Atoms["Binance:public:depth"].Conn != "md/Binance/public/0" || d.Atoms["Binance:public:trade"].Conn != "md/Binance/public/1" {
		t.Fatalf("placement %+v", d.Atoms)
	}
	if !d.Atoms["Binance:public:trade"].Record || d.Atoms["Binance:public:depth"].Record {
		t.Fatal("tape follows the topic")
	}
	held := assign(testInst, now, time.Second, m, nil, 0, []feedIn{tr}, snapOf(d))
	if held.Atoms["Binance:public:depth"].Conn != "md/Binance/public/0" || len(held.Linger) != 1 {
		t.Fatalf("the empty conn should linger, not drop in the same commit: %+v", held.Atoms)
	}
	left := assign(testInst, now.Add(time.Second), time.Second, m, nil, 0, []feedIn{tr}, snapOf(held))
	if _, ok := left.Atoms["Binance:public:depth"]; ok || left.Atoms["Binance:public:trade"].Conn != "md/Binance/public/1" {
		t.Fatalf("trade should stay on n=1: %+v", left.Atoms)
	}
	m["Binance"].(*fakeMarket).atoms[exchange.TopicAggTrade] = exchange.AtomRef{Route: "public", Channel: "agg"}
	again := assign(testInst, now.Add(time.Second), time.Second, m, nil, 0, []feedIn{tr, feed("s", "k-agg", "aggtrade")}, snapOf(left))
	if again.Atoms["Binance:public:agg"].Conn != "md/Binance/public/0" || again.Atoms["Binance:public:trade"].Conn != "md/Binance/public/1" {
		t.Fatalf("new atom takes the hole, the old one stays: %+v", again.Atoms)
	}
}

func TestStickyPlacementAndBudgetReject(t *testing.T) {
	id := "Binance:public:depth"
	book := feed("s", "k", "orderbook")
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

func TestResolveRejectsAndHolds(t *testing.T) {
	ctx := context.Background()
	key := state.MDDemandKey(testInst, "s", "feed/"+feedKey("orderbook"))
	e := feed("s", key, "orderbook").Entry
	cases := []struct {
		st   symbol.Status
		miss bool
		want string
	}{
		{symbol.Tradable, true, "not found"},
		{symbol.Expired, false, "expired"},
		{symbol.Delisted, false, "delisted"},
		{symbol.Halted, false, "not tradable"},
	}
	for _, c := range cases {
		res := &memResolver{info: btc(c.st), miss: c.miss}
		got, skip, err := lookup(ctx, res, e)
		if err != nil || skip || got.Reject != c.want || !res.inactive {
			t.Fatalf("%s: %+v skip %v err %v inactive %v", c.st, got, skip, err, res.inactive)
		}
	}
	m := testMarkets(8, 1)
	m["Binance"].(*fakeMarket).bad = map[exchange.Topic]error{exchange.TopicGreeks: exchange.ErrUnsupported}
	g := feed("s", "k", "greeks")
	d := assign(testInst, time.Unix(1, 0), time.Second, m, nil, 0, []feedIn{g}, snapshot{})
	if d.Outcomes["k"].Reject != "unsupported" {
		t.Fatalf("reject %q", d.Outcomes["k"].Reject)
	}
	unknown := feed("s", "k", "orderbook")
	unknown.Info = &symbol.Info{UID: 1, Ticker: symbol.UniversalTicker{Venue: "Nope"}, Status: symbol.Tradable}
	d = assign(testInst, time.Unix(1, 0), time.Second, m, nil, 0, []feedIn{unknown}, snapshot{})
	if d.Outcomes["k"].Reject != "unknown venue" {
		t.Fatalf("reject %q", d.Outcomes["k"].Reject)
	}

	res := &memResolver{info: btc(symbol.Tradable)}
	p := &plane{inst: testInst, res: res, good: map[string]feedIn{}}
	items := map[string]Entry{key: e}
	feeds, good, silent, err := p.resolve(ctx, agent.Demand[Entry]{Items: items})
	if err != nil || len(feeds) != 1 || len(silent) != 0 || feeds[0].Info == nil {
		t.Fatalf("resolve %+v silent %v err %v", feeds, silent, err)
	}
	p.good = good
	res.err = errors.New("sym down")
	feeds, _, silent, err = p.resolve(ctx, agent.Demand[Entry]{Items: items})
	if err != nil || len(silent) != 0 || feeds[0].Info == nil || feeds[0].Info.UID != 7 {
		t.Fatalf("transient dropped the feed: %+v silent %v err %v", feeds, silent, err)
	}
	p.good = nil
	feeds, _, silent, err = p.resolve(ctx, agent.Demand[Entry]{Items: items})
	if err != nil || !silent[key] || len(feeds) != 0 {
		t.Fatalf("no last good should stay silent: feeds %d silent %v err %v", len(feeds), silent, err)
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
	id := "Binance:public:depth"
	prev := snapshot{Atoms: map[string]placed{id: {
		Conn: "md/Binance/public/0", Venue: "Binance", Route: "public", Channel: "depth",
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
	live := Live{Atoms: []LiveAtom{{ID: "Binance:public:depth", Feeds: []string{"a", "b"}}}}
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
	ticked := Live{Atoms: []LiveAtom{{ID: "Binance:public:depth", Feeds: []string{"a", "b"}, ExchTicker: "BTCUSDT", PriceTick: 0.01}}}
	if ticked.Hash() == h {
		t.Fatal("symbol fields left the live hash unchanged")
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

func btc(st symbol.Status) *symbol.Info {
	return &symbol.Info{
		UID: 7, Ticker: mustParse(testTicker), Status: st, ExchTicker: "BTCUSDT",
		Filters: symbol.Filters{symbol.PriceTick: 0.01},
	}
}

func TestSymbolFieldsSkipABadTick(t *testing.T) {
	exch, tick := symbolFields(&symbol.Info{ExchTicker: "BTCUSDT", Filters: symbol.Filters{symbol.PriceTick: math.NaN()}})
	if exch != "BTCUSDT" || tick != 0 {
		t.Fatalf("%s %v", exch, tick)
	}
	_, tick = symbolFields(&symbol.Info{Filters: symbol.Filters{symbol.PriceTick: 0.1}})
	if tick != 0.1 {
		t.Fatal(tick)
	}
}

func feed(session, key, topic string) feedIn {
	t := mustParse(testTicker)
	return feedIn{
		Key: key,
		Entry: Entry{
			Session: session, Feed: topic + "." + t.String(), Topic: exchange.Topic(topic), Ticker: t,
		},
		Info: btc(symbol.Tradable),
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

type memResolver struct {
	info     *symbol.Info
	miss     bool
	err      error
	inactive bool
}

func (m *memResolver) Get(_ context.Context, t symbol.UniversalTicker, includeInactive bool) (*symbol.Info, error) {
	m.inactive = includeInactive
	if m.err != nil {
		return nil, m.err
	}
	if m.miss || m.info == nil || m.info.Ticker != t {
		return nil, symbol.ErrNotFound
	}
	return m.info, nil
}

func (m *memResolver) GetByUID(_ context.Context, uid uint32, includeInactive bool) (*symbol.Info, error) {
	m.inactive = includeInactive
	if m.err != nil {
		return nil, m.err
	}
	if m.miss || m.info == nil || m.info.UID != uid {
		return nil, symbol.ErrNotFound
	}
	return m.info, nil
}
