package sts_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	"github.com/BullionBear/mftiko/internal/sts"
)

func inst(name string) func(string) string {
	return func(string) string { return name }
}

func specOf(accounts []int64, feeds []string) *sessionv1.SessionSpec {
	s := &sessionv1.SessionSpec{}
	for _, id := range accounts {
		s.Accounts = append(s.Accounts, &sessionv1.AccountDecl{ApiId: id, Name: "a"})
	}
	for _, feed := range feeds {
		// md_instance on the decl is ignored. The resolver is the source (E08-D020).
		s.Feeds = append(s.Feeds, &sessionv1.FeedDecl{Feed: feed, MdInstance: "from-the-spec"})
	}
	return s
}

func perms(t *testing.T, sessionID string, spec *sessionv1.SessionSpec, feedInst func(string) string) sts.Permissions {
	t.Helper()
	got, err := sts.SessionPermissions(sessionID, spec, feedInst)
	if err != nil {
		t.Fatalf("SessionPermissions: %v", err)
	}
	return got
}

func TestSessionPermissionsOneFeedOneAccount(t *testing.T) {
	got := perms(t, "a1b2c3", specOf([]int64{7}, []string{"trade.Binance_Spot_BTCUSDT"}), inst("md-tw-1"))
	wantPub := []string{
		"md.v1.fetch.>",
		"md.v1.view.md-tw-1.trade.Binance_Spot_BTCUSDT",
		"sym",
		"td.order.7",
		"td.view.7",
	}
	wantSub := []string{
		"_INBOX_a1b2c3.>",
		"md.v1.f.md-tw-1.trade.Binance_Spot_BTCUSDT",
		"sym.v1.changed.>",
		"td.evt.7",
	}
	if !slices.Equal(got.Pub, wantPub) || !slices.Equal(got.Sub, wantSub) {
		t.Fatalf("pub %q\nsub %q", got.Pub, got.Sub)
	}
	if strings.Contains(strings.Join(got.Pub, " "), "from-the-spec") || strings.Contains(strings.Join(got.Sub, " "), "from-the-spec") {
		t.Fatal("FeedDecl.md_instance was used")
	}
}

func TestNoAccountOmitsTD(t *testing.T) {
	got := perms(t, "a1b2c3", specOf(nil, []string{"trade.Binance_Spot_BTCUSDT"}), inst("md-tw-1"))
	for _, s := range append(append([]string{}, got.Pub...), got.Sub...) {
		if strings.HasPrefix(s, "td.") {
			t.Fatalf("td subject %s", s)
		}
	}
	if !slices.Contains(got.Pub, "md.v1.view.md-tw-1.trade.Binance_Spot_BTCUSDT") {
		t.Fatalf("pub %q", got.Pub)
	}
	if !slices.Contains(got.Sub, "md.v1.f.md-tw-1.trade.Binance_Spot_BTCUSDT") {
		t.Fatalf("sub %q", got.Sub)
	}
}

func TestNoFeedOmitsMarketData(t *testing.T) {
	got := perms(t, "a1b2c3", specOf([]int64{7}, nil), inst("md-tw-1"))
	for _, s := range append(append([]string{}, got.Pub...), got.Sub...) {
		if strings.Contains(s, "md.v1.f.") || strings.Contains(s, "md.v1.view.") {
			t.Fatalf("market-data subject %s", s)
		}
	}
	if !slices.Contains(got.Pub, "md.v1.fetch.>") || !slices.Contains(got.Pub, "td.order.7") {
		t.Fatalf("pub %q", got.Pub)
	}
	if !slices.Contains(got.Sub, "td.evt.7") || !slices.Contains(got.Sub, "sym.v1.changed.>") {
		t.Fatalf("sub %q", got.Sub)
	}
}

func TestSortedUniqueAcrossOrder(t *testing.T) {
	feeds := []string{
		"trade.BinanceUM_Perp_ETHUSDT",
		"orderbook_delta.Binance_Spot_BTCUSDT",
		"trade.BinanceUM_Perp_ETHUSDT",
		"kline_1m.Okx_Option_BTCUSD-261225-100000-C",
	}
	rev := slices.Clone(feeds)
	slices.Reverse(rev)
	resolve := func(feed string) string {
		if strings.Contains(feed, "Okx_") {
			return "md-jp-1"
		}
		return "md-tw-1"
	}
	a := perms(t, "a1b2c3", specOf([]int64{11, 7, 11}, feeds), resolve)
	b := perms(t, "a1b2c3", specOf([]int64{7, 11}, rev), resolve)
	if !slices.Equal(a.Pub, b.Pub) || !slices.Equal(a.Sub, b.Sub) {
		t.Fatalf("a pub %q\nb pub %q\na sub %q\nb sub %q", a.Pub, b.Pub, a.Sub, b.Sub)
	}
	if !slices.IsSorted(a.Pub) || !slices.IsSorted(a.Sub) {
		t.Fatalf("not sorted pub %q sub %q", a.Pub, a.Sub)
	}
	wantView := []string{
		"md.v1.view.md-jp-1.kline_1m.Okx_Option_BTCUSD-261225-100000-C",
		"md.v1.view.md-tw-1.orderbook_delta.Binance_Spot_BTCUSDT",
		"md.v1.view.md-tw-1.trade.BinanceUM_Perp_ETHUSDT",
	}
	wantFeed := []string{
		"md.v1.f.md-jp-1.kline_1m.Okx_Option_BTCUSD-261225-100000-C",
		"md.v1.f.md-tw-1.orderbook_delta.Binance_Spot_BTCUSDT",
		"md.v1.f.md-tw-1.trade.BinanceUM_Perp_ETHUSDT",
	}
	for _, s := range wantView {
		if !slices.Contains(a.Pub, s) {
			t.Fatalf("missing %s in %q", s, a.Pub)
		}
	}
	for _, s := range wantFeed {
		if !slices.Contains(a.Sub, s) {
			t.Fatalf("missing %s in %q", s, a.Sub)
		}
	}
	if slices.Contains(a.Pub, "td.order.11") && slices.Contains(a.Pub, "td.order.7") &&
		slices.Contains(a.Sub, "td.evt.11") && slices.Contains(a.Sub, "td.evt.7") {
		// one of each, despite the duplicate account
		if n := countPrefix(a.Pub, "td.order."); n != 2 {
			t.Fatalf("td.order count %d in %q", n, a.Pub)
		}
	} else {
		t.Fatalf("accounts pub %q sub %q", a.Pub, a.Sub)
	}
	// String order, not numeric: "11" sorts before "7".
	if i, j := slices.Index(a.Sub, "td.evt.11"), slices.Index(a.Sub, "td.evt.7"); i > j {
		t.Fatalf("td.evt order %q", a.Sub)
	}
}

func TestOnlyTheFixedWildcards(t *testing.T) {
	got := perms(t, "a1b2c3", specOf([]int64{7}, []string{"trade.Binance_Spot_BTCUSDT"}), inst("md-tw-1"))
	allowed := map[string]bool{
		"md.v1.fetch.>":    true,
		"sym.v1.changed.>": true,
		"_INBOX_a1b2c3.>":  true,
	}
	for _, s := range append(append([]string{}, got.Pub...), got.Sub...) {
		if strings.Contains(s, "*") || (strings.Contains(s, ">") && !allowed[s]) {
			t.Fatalf("wildcard %s", s)
		}
	}
}

func TestSelectorIsAnError(t *testing.T) {
	spec := specOf([]int64{7}, []string{"trade.Binance_Spot_BTCUSDT"})
	spec.Selectors = []*sessionv1.SelectorDecl{{Name: "chain"}}
	called := false
	_, err := sts.SessionPermissions("a1b2c3", spec, func(string) string {
		called = true
		return "md-tw-1"
	})
	if !errors.Is(err, sts.ErrSelector) || called {
		t.Fatalf("err %v called %v", err, called)
	}
}

func TestBadTokens(t *testing.T) {
	goodFeed := "trade.Binance_Spot_BTCUSDT"
	cases := []struct {
		name    string
		session string
		feed    string
		inst    string
		want    error
	}{
		{"session dot", "a.b2c3", goodFeed, "md-tw-1", sts.ErrBadToken},
		{"session star", "a*b2c3", goodFeed, "md-tw-1", sts.ErrBadToken},
		{"session gt", "a>b2c3", goodFeed, "md-tw-1", sts.ErrBadToken},
		{"session space", "a b2c3", goodFeed, "md-tw-1", sts.ErrBadToken},
		{"session tab", "a\tb2c3", goodFeed, "md-tw-1", sts.ErrBadToken},
		{"session newline", "a\nb2c3", goodFeed, "md-tw-1", sts.ErrBadToken},
		{"session nbsp", "a\u00a0b", goodFeed, "md-tw-1", sts.ErrBadToken},
		{"session empty", "", goodFeed, "md-tw-1", sts.ErrBadToken},
		{"topic star", "a1b2c3", "tr*de.Binance_Spot_BTCUSDT", "md-tw-1", sts.ErrBadToken},
		{"topic space", "a1b2c3", "tr de.Binance_Spot_BTCUSDT", "md-tw-1", sts.ErrBadToken},
		{"ticker dot", "a1b2c3", "trade.Binance.Spot_BTCUSDT", "md-tw-1", sts.ErrBadToken},
		{"ticker gt", "a1b2c3", "trade.Binance_Spot_BTC>", "md-tw-1", sts.ErrBadToken},
		{"ticker star", "a1b2c3", "trade.Binance_Spot_*", "md-tw-1", sts.ErrBadToken},
		{"ticker space", "a1b2c3", "trade.Binance_Spot_ BTC", "md-tw-1", sts.ErrBadToken},
		{"instance dot", "a1b2c3", goodFeed, "md.tw-1", sts.ErrBadToken},
		{"instance gt", "a1b2c3", goodFeed, "md-tw-1>", sts.ErrBadToken},
		{"instance star", "a1b2c3", goodFeed, "md-*", sts.ErrBadToken},
		{"instance space", "a1b2c3", goodFeed, "md tw", sts.ErrBadToken},
		{"instance empty", "a1b2c3", goodFeed, "", sts.ErrBadToken},
		{"no dot", "a1b2c3", "tradeBTC", "md-tw-1", sts.ErrFeed},
		{"empty topic", "a1b2c3", ".BTC", "md-tw-1", sts.ErrFeed},
		{"empty ticker", "a1b2c3", "trade.", "md-tw-1", sts.ErrFeed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sts.SessionPermissions(tc.session, specOf([]int64{7}, []string{tc.feed}), inst(tc.inst))
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestHashIsNotASpecialCharacter(t *testing.T) {
	got := perms(t, "abc#12", specOf(nil, nil), nil)
	for _, s := range got.Pub {
		if strings.HasPrefix(s, "log.") {
			t.Fatalf("log subject %s", s)
		}
	}
	if !slices.Contains(got.Sub, "_INBOX_abc#12.>") {
		t.Fatalf("pub %q sub %q", got.Pub, got.Sub)
	}
}

func TestAPIIDMustBePositive(t *testing.T) {
	for _, id := range []int64{0, -1, -7} {
		_, err := sts.SessionPermissions("a1b2c3", specOf([]int64{id}, nil), nil)
		if !errors.Is(err, sts.ErrAPIID) {
			t.Fatalf("api_id %d: %v", id, err)
		}
	}
}

func TestNilSpecAndNilResolver(t *testing.T) {
	if _, err := sts.SessionPermissions("a1b2c3", nil, inst("md-tw-1")); !errors.Is(err, sts.ErrNilSpec) {
		t.Fatalf("nil spec: %v", err)
	}
	_, err := sts.SessionPermissions("a1b2c3", specOf(nil, []string{"trade.Binance_Spot_BTCUSDT"}), nil)
	if !errors.Is(err, sts.ErrNoResolver) {
		t.Fatalf("nil resolver: %v", err)
	}
	// A feed key that does not split is reported before the missing resolver.
	_, err = sts.SessionPermissions("a1b2c3", specOf(nil, []string{"not-a-feed"}), nil)
	if !errors.Is(err, sts.ErrFeed) {
		t.Fatalf("bad feed, nil resolver: %v", err)
	}
	// No feeds: the resolver is not required.
	if _, err := sts.SessionPermissions("a1b2c3", specOf(nil, nil), nil); err != nil {
		t.Fatal(err)
	}
}

func TestResolverSeesTheFeedKey(t *testing.T) {
	var got []string
	spec := specOf(nil, []string{"trade.Binance_Spot_BTCUSDT", "kline_1m.Okx_Perp_ETHUSDT"})
	_, err := sts.SessionPermissions("a1b2c3", spec, func(feed string) string {
		got = append(got, feed)
		return "md-tw-1"
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"trade.Binance_Spot_BTCUSDT", "kline_1m.Okx_Perp_ETHUSDT"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q", got)
	}
}

func TestManyFeedsStayExactAndStable(t *testing.T) {
	const n = 500
	feeds := make([]string, n)
	for i := range feeds {
		feeds[i] = fmt.Sprintf("trade.BinanceUM_Perp_S%04d", i)
	}
	// Reverse the input. The output is still sorted and one subject per feed.
	slices.Reverse(feeds)
	spec := specOf([]int64{7}, feeds)
	a := perms(t, "a1b2c3", spec, inst("md-tw-1"))
	b := perms(t, "a1b2c3", spec, inst("md-tw-1"))
	if !slices.Equal(a.Pub, b.Pub) || !slices.Equal(a.Sub, b.Sub) {
		t.Fatal("same input differed")
	}
	// Fixed publish subjects plus one view per feed: fetch, sym, two td, 500 views.
	// There is no log.sts (E10-D030).
	if len(a.Pub) != 4+n || len(a.Sub) != 3+n {
		t.Fatalf("pub %d sub %d", len(a.Pub), len(a.Sub))
	}
	if slices.Contains(a.Sub, "md.v1.f.md-tw-1.>") || slices.Contains(a.Pub, "md.v1.view.md-tw-1.>") {
		t.Fatal("instance wildcard")
	}
}

func TestConcurrentCalls(t *testing.T) {
	spec := specOf([]int64{7}, []string{"trade.Binance_Spot_BTCUSDT"})
	var wg sync.WaitGroup
	errc := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := sts.SessionPermissions("a1b2c3", spec, inst("md-tw-1"))
			if err != nil {
				errc <- err
				return
			}
			if !slices.Contains(got.Sub, "md.v1.f.md-tw-1.trade.Binance_Spot_BTCUSDT") {
				errc <- errors.New("missing feed subject")
			}
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Error(err)
	}
}

func countPrefix(ss []string, prefix string) int {
	n := 0
	for _, s := range ss {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}
