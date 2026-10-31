package symclient

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
)

func TestDefaultCheckInterval(t *testing.T) {
	if DefaultCheckInterval != 5*time.Minute {
		t.Fatal(DefaultCheckInterval)
	}
}

func TestOpenRejects(t *testing.T) {
	if _, err := Open(t.Context(), nil, Options{}); err == nil {
		t.Fatal("nil connection was accepted")
	}
	f := newFake()
	if _, err := Open(t.Context(), f, Options{CheckInterval: -time.Second}); err == nil {
		t.Fatal("negative interval was accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Open(ctx, f, Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestResolveAndInfo(t *testing.T) {
	f := newFake()
	f.seed(spotBTC(0.1, symbol.Tradable))
	f.seed(spotETH(0.2, symbol.Tradable)) // present at SYM, never resolved
	c := openAt(t, f, time.Hour)
	c.AllowResolve(true)

	if _, err := c.Resolve(t.Context(), "nope"); !errors.Is(err, symbol.ErrInvalidTicker) {
		t.Fatalf("invalid ticker: %v", err)
	}
	if _, err := c.Resolve(t.Context(), "Bybit_Spot_SOLUSDT"); !errors.Is(err, symbol.ErrNotFound) {
		t.Fatalf("missing ticker: %v", err)
	}
	if len(f.recorded()) != 1 || f.recorded()[0].Op != "get" || f.recorded()[0].Inactive {
		t.Fatalf("missing resolve calls: %+v", f.recorded())
	}
	if subs := f.subjects(); len(subs) != 0 {
		t.Fatalf("failed resolve subscribed: %v", subs)
	}
	if _, err := c.Info(1002); !errors.Is(err, ErrUnknownUID) {
		t.Fatalf("unresolved info: %v", err)
	}
	if _, err := c.Info(0); !errors.Is(err, ErrUnknownUID) {
		t.Fatalf("uid 0: %v", err)
	}

	info, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	if info.UID != 1001 || info.Ticker.String() != "Bybit_Spot_BTCUSDT" || info.ExchTicker != "BTCUSDT" ||
		info.Status != symbol.Tradable || info.Filters[symbol.PriceTick] != 0.1 {
		t.Fatalf("resolve %+v", info)
	}
	if subs := f.subjects(); len(subs) != 1 || subs[0] != "sym.v1.changed.Bybit.Spot" {
		t.Fatalf("subjects %v", subs)
	}
	got, err := c.Info(1001)
	if err != nil || got.UID != info.UID || got.Filters[symbol.PriceTick] != 0.1 {
		t.Fatalf("info %+v %v", got, err)
	}
	got.Filters[symbol.PriceTick] = 99
	if again, err := c.Info(1001); err != nil || again.Filters[symbol.PriceTick] != 0.1 {
		t.Fatalf("info aliased the cache: %+v %v", again, err)
	}
	// A revision check that fails after Get still returns the row. The table
	// stays unconfirmed so a later check retries. This call already succeeded
	// its own sync; force the next one.
	f.failRev = 1
	c.mu.Lock()
	c.revs[tableKey{"Bybit", symbol.Spot}] = revState{}
	c.mu.Unlock()
	again, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT")
	if err != nil || again.UID != 1001 {
		t.Fatalf("resolve after a failed sync: %+v %v", again, err)
	}
	if _, err := c.Info(1002); !errors.Is(err, ErrUnknownUID) {
		t.Fatal(err)
	}
	for _, call := range f.recorded() {
		if call.UID == 1002 || call.Op == "list" {
			t.Fatalf("unresolved instrument was queried: %+v", call)
		}
	}
}

func TestResolveWrongHook(t *testing.T) {
	f := newFake()
	f.seed(spotBTC(0.1, symbol.Tradable))
	c := openAt(t, f, time.Hour)

	if _, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT"); !errors.Is(err, ErrNotInInit) {
		t.Fatalf("before init: %v", err)
	}
	if len(f.recorded()) != 0 {
		t.Fatalf("wrong hook called SYM: %+v", f.recorded())
	}
	c.AllowResolve(true)
	if _, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	n := len(f.recorded())
	c.AllowResolve(false)
	if _, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT"); !errors.Is(err, ErrNotInInit) {
		t.Fatalf("after init: %v", err)
	}
	if len(f.recorded()) != n {
		t.Fatalf("wrong hook called SYM: %+v", f.recorded()[n:])
	}
	// Info is allowed from any hook.
	if _, err := c.Info(1001); err != nil {
		t.Fatal(err)
	}
}

func TestChangedUpdatesInfo(t *testing.T) {
	f := newFake()
	f.seed(spotBTC(0.1, symbol.Tradable))
	f.seed(spotETH(0.2, symbol.Tradable))
	f.seed(perpBTC(0.5, symbol.Tradable))
	c := openAt(t, f, time.Hour)
	c.AllowResolve(true)
	ctx := t.Context()
	if _, err := c.Resolve(ctx, "Bybit_Spot_BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Resolve(ctx, "Bybit_Perp_BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	// ETH is listed and never resolved. A spot change must not look it up.
	n := len(f.recorded())
	f.setTick(1001, 0.4)
	f.bump("Bybit", symbol.Spot)
	f.publish("Bybit", symbol.Spot)

	waitFor(t, func() bool {
		info, err := c.Info(1001)
		return err == nil && info.Filters[symbol.PriceTick] == 0.4 && info.Status == symbol.Tradable
	})
	var btc, eth, perp int
	for _, call := range f.recorded()[n:] {
		if call.Op != "byuid" {
			continue
		}
		switch call.UID {
		case 1001:
			btc++
			if call.Inactive {
				t.Fatal("active refresh used include_inactive")
			}
		case 1002:
			eth++
		case 2001:
			perp++
		default:
			t.Fatalf("unexpected uid %d", call.UID)
		}
	}
	if btc == 0 || eth != 0 || perp != 0 {
		t.Fatalf("byuid btc=%d eth=%d perp=%d", btc, eth, perp)
	}
	perpInfo, err := c.Info(2001)
	if err != nil || perpInfo.Filters[symbol.PriceTick] != 0.5 {
		t.Fatalf("perp %+v %v", perpInfo, err)
	}
	if _, err := c.Info(1002); !errors.Is(err, ErrUnknownUID) {
		t.Fatal(err)
	}
}

func TestStaleNotificationSkipped(t *testing.T) {
	f := newFake()
	f.seed(spotBTC(0.1, symbol.Tradable))
	c := openAt(t, f, time.Hour)
	c.AllowResolve(true)
	if _, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	f.setTick(1001, 9)
	n := len(f.recorded())
	c.onNotify(t.Context(), note{key: tableKey{"Bybit", symbol.Spot}, rev: 1})
	if len(f.recorded()) != n {
		t.Fatalf("stale notification called SYM: %+v", f.recorded()[n:])
	}
	info, err := c.Info(1001)
	if err != nil || info.Filters[symbol.PriceTick] != 0.1 {
		t.Fatalf("info %+v %v", info, err)
	}
}

func TestDelistRefetchIncludeInactive(t *testing.T) {
	f := newFake()
	f.seed(spotBTC(0.1, symbol.Tradable))
	f.seed(spotETH(0.2, symbol.Tradable))
	f.seed(listedOnly(1003)) // never resolved
	c := openAt(t, f, time.Hour)
	c.AllowResolve(true)
	ctx := t.Context()
	if _, err := c.Resolve(ctx, "Bybit_Spot_BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Resolve(ctx, "Bybit_Spot_ETHUSDT"); err != nil {
		t.Fatal(err)
	}
	n := len(f.recorded())
	f.delist(1001, 0.7)
	f.publish("Bybit", symbol.Spot)

	waitFor(t, func() bool {
		info, err := c.Info(1001)
		return err == nil && info.Status == symbol.Delisted && info.Filters[symbol.PriceTick] == 0.7
	})
	info, err := c.Info(1001)
	if err != nil || info.Status.Active() || !info.Status.Terminal() || info.IsTradable() {
		t.Fatalf("delisted info %+v %v", info, err)
	}
	eth, err := c.Info(1002)
	if err != nil || eth.Status != symbol.Tradable || eth.Filters[symbol.PriceTick] != 0.2 {
		t.Fatalf("eth %+v %v", eth, err)
	}
	var inactive, active bool
	for _, call := range f.recorded()[n:] {
		if call.UID == 1003 {
			t.Fatal("unresolved uid was queried")
		}
		if call.Op != "byuid" || call.UID != 1001 {
			continue
		}
		if call.Inactive {
			inactive = true
		} else {
			active = true
		}
	}
	if !active || !inactive {
		t.Fatalf("btc refetch active=%v include_inactive=%v", active, inactive)
	}
	before := len(f.recorded())
	if _, err := c.Info(1003); !errors.Is(err, ErrUnknownUID) {
		t.Fatal(err)
	}
	if len(f.recorded()) != before {
		t.Fatal("Info called SYM")
	}
}

func TestRevisionCheck(t *testing.T) {
	f := newFake()
	f.seed(spotBTC(0.1, symbol.Tradable))
	c := openAt(t, f, 5*time.Millisecond)
	c.AllowResolve(true)
	if _, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	// No changed message. The loop's revision comparison is what must notice.
	f.setTick(1001, 1.5)
	f.bump("Bybit", symbol.Spot)
	waitFor(t, func() bool {
		info, err := c.Info(1001)
		return err == nil && info.Filters[symbol.PriceTick] == 1.5
	})
	var sawRev, sawBy bool
	for _, call := range f.recorded() {
		if call.Op == "revisions" {
			sawRev = true
		}
		if call.Op == "byuid" && call.UID == 1001 && !call.Inactive {
			sawBy = true
		}
	}
	if !sawRev || !sawBy {
		t.Fatalf("revision check did not refetch: %+v", f.recorded())
	}
}

func TestReconnectCheck(t *testing.T) {
	f := newFake()
	f.seed(spotBTC(0.1, symbol.Tradable))
	c := openAt(t, f, time.Hour)
	c.AllowResolve(true)
	if _, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	f.setTick(1001, 2)
	f.bump("Bybit", symbol.Spot)
	info, err := c.Info(1001)
	if err != nil || info.Filters[symbol.PriceTick] != 0.1 {
		t.Fatalf("row changed before reconnect: %+v %v", info, err)
	}
	c.Reconnect()
	waitFor(t, func() bool {
		info, err := c.Info(1001)
		return err == nil && info.Filters[symbol.PriceTick] == 2
	})
}

func TestInfoConcurrentWithRefresh(t *testing.T) {
	f := newFake()
	f.seed(spotBTC(0.1, symbol.Tradable))
	c := openAt(t, f, time.Hour)
	c.AllowResolve(true)
	if _, err := c.Resolve(t.Context(), "Bybit_Spot_BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = c.Info(1001)
				_, _ = c.Info(999)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.AllowResolve(true)
				c.Reconnect()
			}
		}
	}()
	for i := 1; i <= 20; i++ {
		f.setTick(1001, float64(i))
		f.bump("Bybit", symbol.Spot)
		f.publish("Bybit", symbol.Spot)
	}
	close(stop)
	wg.Wait()
	waitFor(t, func() bool {
		info, err := c.Info(1001)
		return err == nil && info.Filters[symbol.PriceTick] == 20
	})
}

func TestInfoProtoRoundTrip(t *testing.T) {
	call := instrument(3001, "Okx_Option_BTCUSD-261225-100000-C", "BTC-USD-261225-100000-C", 5, symbol.Tradable)
	call.ContractSize = 0.1
	call.Settle = "BTC"
	call.ExpiryNs = 1_800_000_000_000_000_000
	call.Strike = 100000
	call.OptionType = "C"
	call.Filters[symbol.MaxQty] = decimal.Absent()
	for _, info := range []symbol.Info{spotBTC(0.1, symbol.Halted), call, listedOnly(4001)} {
		got, err := InfoFromProto(InfoToProto(info))
		if err != nil {
			t.Fatal(err)
		}
		if got.Ticker != info.Ticker || got.ContractSize != info.ContractSize || got.Settle != info.Settle ||
			got.ExpiryNs != info.ExpiryNs || got.Strike != info.Strike || got.OptionType != info.OptionType ||
			got.Status != info.Status || got.UID != info.UID || got.ExchTicker != info.ExchTicker {
			t.Fatalf("round trip\n got %+v\nwant %+v", got, info)
		}
		if len(got.Filters) != len(info.Filters) {
			t.Fatalf("filters %v, want %v", got.Filters, info.Filters)
		}
		for name, v := range info.Filters {
			if g, ok := got.Filters[name]; !ok || decimal.Has(g) != decimal.Has(v) || (decimal.Has(v) && g != v) {
				t.Fatalf("filter %s: %v, want %v", name, g, v)
			}
		}
	}
	if !proto.Equal(InfoToProto(spotBTC(0.1, symbol.Tradable)), infoProto(spotBTC(0.1, symbol.Tradable))) {
		t.Fatal("InfoToProto of a spot row is not the row SYM sends")
	}
}

func TestSeedAnswersInfo(t *testing.T) {
	c := openAt(t, newFake(), time.Hour)
	c.Seed(spotBTC(0.1, symbol.Tradable))
	info, err := c.Info(1001)
	if err != nil || info.Ticker.String() != "Bybit_Spot_BTCUSDT" || info.Filters[symbol.PriceTick] != 0.1 {
		t.Fatalf("seeded info %+v %v", info, err)
	}
	if _, err := c.Info(1002); !errors.Is(err, ErrUnknownUID) {
		t.Fatalf("unseeded info: %v", err)
	}
}

// waitFor polls cond until it holds. The short gap is only the pause between
// observations; the test passes when cond is true, not when a duration elapses.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cond() {
			return
		}
		if t.Context().Err() != nil || !time.Now().Before(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func openAt(t *testing.T, bus Bus, every time.Duration) *Client {
	t.Helper()
	c, err := Open(t.Context(), bus, Options{CheckInterval: every})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Close()
		select {
		case <-c.Exited():
		case <-time.After(2 * time.Second):
			t.Error("revision loop did not exit")
		}
	})
	return c
}

func spotBTC(tick float64, st symbol.Status) symbol.Info {
	return instrument(1001, "Bybit_Spot_BTCUSDT", "BTCUSDT", tick, st)
}

func spotETH(tick float64, st symbol.Status) symbol.Info {
	return instrument(1002, "Bybit_Spot_ETHUSDT", "ETHUSDT", tick, st)
}

func perpBTC(tick float64, st symbol.Status) symbol.Info {
	return instrument(2001, "Bybit_Perp_BTCUSDT", "BTCUSDT", tick, st)
}

func listedOnly(uid uint32) symbol.Info {
	return instrument(uid, "Bybit_Spot_ADAUSDT", "ADAUSDT", 0.01, symbol.Tradable)
}

func instrument(uid uint32, ticker, exch string, tick float64, st symbol.Status) symbol.Info {
	parsed, err := symbol.Parse(ticker)
	if err != nil {
		panic(err)
	}
	return symbol.Info{
		UID:              uid,
		Ticker:           parsed,
		ExchTicker:       exch,
		Base:             "BTC",
		Quote:            "USDT",
		ContractValueCcy: "BTC",
		PriceCcy:         "USDT",
		Status:           st,
		Filters:          symbol.Filters{symbol.PriceTick: tick},
		UpdatedAtNs:      10,
	}
}

type symCall struct {
	Op       string
	Ticker   string
	UID      uint32
	Inactive bool
}

type fake struct {
	mu       sync.Mutex
	active   map[uint32]symbol.Info
	terminal map[uint32]symbol.Info
	byTicker map[string]uint32
	rev      map[string]int64
	subs     map[string][]func(string, []byte)
	calls    []symCall
	failRev  int
	drop     bool
}

func newFake() *fake {
	return &fake{
		active:   map[uint32]symbol.Info{},
		terminal: map[uint32]symbol.Info{},
		byTicker: map[string]uint32{},
		rev:      map[string]int64{},
		subs:     map[string][]func(string, []byte){},
	}
}

func (f *fake) seed(info symbol.Info) {
	f.mu.Lock()
	defer f.mu.Unlock()
	info = cloneInfo(info)
	f.active[info.UID] = info
	f.byTicker[info.Ticker.String()] = info.UID
	delete(f.terminal, info.UID)
	key := revKey(info.Ticker.Venue, info.Ticker.Category)
	if f.rev[key] == 0 {
		f.rev[key] = 1
	}
}

func (f *fake) setTick(uid uint32, tick float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	info, ok := f.active[uid]
	if !ok {
		panic("setTick of inactive uid")
	}
	info.Filters = maps.Clone(info.Filters)
	if info.Filters == nil {
		info.Filters = symbol.Filters{}
	}
	info.Filters[symbol.PriceTick] = tick
	info.UpdatedAtNs++
	f.active[uid] = info
}

func (f *fake) bump(venue string, cat symbol.Category) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := revKey(venue, cat)
	f.rev[key]++
	return f.rev[key]
}

func (f *fake) delist(uid uint32, tick float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	info, ok := f.active[uid]
	if !ok {
		panic("delist of unknown uid")
	}
	delete(f.active, uid)
	delete(f.byTicker, info.Ticker.String())
	info.Status = symbol.Delisted
	info.Filters = symbol.Filters{symbol.PriceTick: tick}
	info.UpdatedAtNs++
	f.terminal[uid] = info
	f.rev[revKey(info.Ticker.Venue, info.Ticker.Category)]++
}

func (f *fake) publish(venue string, cat symbol.Category) {
	subject := "sym.v1.changed." + venue + "." + string(cat)
	f.mu.Lock()
	rev := f.rev[revKey(venue, cat)]
	var subs []func(string, []byte)
	if !f.drop {
		subs = append(subs, f.subs[subject]...)
	}
	f.mu.Unlock()
	if len(subs) == 0 {
		return
	}
	b, err := proto.Marshal(&symv1.Changed{Venue: venue, Category: string(cat), Revision: rev})
	if err != nil {
		panic(err)
	}
	for _, fn := range subs {
		fn(subject, append([]byte(nil), b...))
	}
}

func (f *fake) recorded() []symCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]symCall(nil), f.calls...)
}

func (f *fake) subjects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.subs))
	for subject := range f.subs {
		out = append(out, subject)
	}
	return out
}

func (f *fake) Request(_ context.Context, subject string, req, resp proto.Message) error {
	if subject != subjectSym {
		return errors.New("symclient test: subject " + subject)
	}
	in, ok := req.(*symv1.Request)
	if !ok || in == nil {
		return errors.New("symclient test: request")
	}
	out, ok := resp.(*symv1.Reply)
	if !ok || out == nil {
		return errors.New("symclient test: reply")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r := in.GetReq().(type) {
	case *symv1.Request_Get:
		f.calls = append(f.calls, symCall{Op: "get", Ticker: r.Get.GetUniversalTicker(), Inactive: r.Get.GetIncludeInactive()})
		if r.Get.GetIncludeInactive() {
			return errors.New("symclient test: Resolve set include_inactive")
		}
		uid, ok := f.byTicker[r.Get.GetUniversalTicker()]
		if !ok {
			return notFound("ticker %s not found in active table (include_inactive=false)", r.Get.GetUniversalTicker())
		}
		info, ok := f.active[uid]
		if !ok {
			return notFound("ticker %s not found in active table (include_inactive=false)", r.Get.GetUniversalTicker())
		}
		*out = symv1.Reply{Result: &symv1.Reply_Info{Info: infoProto(info)}}
		return nil
	case *symv1.Request_GetByUid:
		f.calls = append(f.calls, symCall{Op: "byuid", UID: r.GetByUid.GetUid(), Inactive: r.GetByUid.GetIncludeInactive()})
		if info, ok := f.active[r.GetByUid.GetUid()]; ok {
			*out = symv1.Reply{Result: &symv1.Reply_Info{Info: infoProto(info)}}
			return nil
		}
		if r.GetByUid.GetIncludeInactive() {
			if info, ok := f.terminal[r.GetByUid.GetUid()]; ok {
				*out = symv1.Reply{Result: &symv1.Reply_Info{Info: infoProto(info)}}
				return nil
			}
			return notFound("uid %d not found in active or terminal table (include_inactive=true)", r.GetByUid.GetUid())
		}
		return notFound("uid %d not found in active table (include_inactive=false)", r.GetByUid.GetUid())
	case *symv1.Request_Revisions:
		f.calls = append(f.calls, symCall{Op: "revisions"})
		if f.failRev > 0 {
			f.failRev--
			return errors.New("revisions unavailable")
		}
		list := &symv1.RevisionList{}
		for key, rev := range f.rev {
			venue, cat, _ := splitRev(key)
			list.Tables = append(list.Tables, &symv1.TableRevision{Venue: venue, Category: cat, Revision: rev})
		}
		*out = symv1.Reply{Result: &symv1.Reply_Revisions{Revisions: list}}
		return nil
	default:
		return errors.New("symclient test: unsupported request")
	}
}

func (f *fake) Subscribe(ctx context.Context, subject string, fn func(string, []byte)) error {
	f.mu.Lock()
	f.subs[subject] = append(f.subs[subject], fn)
	f.mu.Unlock()
	go func() {
		<-ctx.Done()
		f.mu.Lock()
		delete(f.subs, subject)
		f.mu.Unlock()
	}()
	return nil
}

func infoProto(info symbol.Info) *symv1.SymbolInfo {
	out := &symv1.SymbolInfo{
		UniversalTicker:  info.Ticker.String(),
		ExchTicker:       info.ExchTicker,
		Base:             info.Base,
		Quote:            info.Quote,
		ContractValueCcy: info.ContractValueCcy,
		PriceCcy:         info.PriceCcy,
		Status:           statusProto(info.Status),
		UpdatedAtNs:      info.UpdatedAtNs,
		Uid:              info.UID,
	}
	if len(info.Filters) > 0 {
		out.Filters = make(map[string]*symv1.Bound, len(info.Filters))
		for name, v := range info.Filters {
			b := &symv1.Bound{}
			if decimal.Has(v) {
				vv := v
				b.Value = &vv
			}
			out.Filters[name] = b
		}
	}
	return out
}

func statusProto(s symbol.Status) symv1.SymbolStatus {
	switch s {
	case symbol.Prelisting:
		return symv1.SymbolStatus_SYMBOL_STATUS_PRELISTING
	case symbol.Tradable:
		return symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE
	case symbol.Halted:
		return symv1.SymbolStatus_SYMBOL_STATUS_HALTED
	case symbol.Settling:
		return symv1.SymbolStatus_SYMBOL_STATUS_SETTLING
	case symbol.Expired:
		return symv1.SymbolStatus_SYMBOL_STATUS_EXPIRED
	case symbol.Delisted:
		return symv1.SymbolStatus_SYMBOL_STATUS_DELISTED
	case symbol.Unknown:
		return symv1.SymbolStatus_SYMBOL_STATUS_UNKNOWN
	default:
		return symv1.SymbolStatus_SYMBOL_STATUS_UNSPECIFIED
	}
}

func notFound(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{symbol.ErrNotFound}, args...)...)
}

func revKey(venue string, cat symbol.Category) string {
	return venue + "/" + string(cat)
}

func splitRev(key string) (venue, cat string, ok bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:], true
		}
	}
	return "", "", false
}
