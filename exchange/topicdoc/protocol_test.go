package topicdoc_test

import (
	"errors"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/all"
	"github.com/BullionBear/mftiko/exchange/topicdoc"
)

// venueOrder matches the generator. A venue with a non-empty Topics that is not in this
// list is appended, sorted. #510 names BinanceUM, Binance spot, Bybit, and Okx.
var venueOrder = []string{"BinanceUM", "Binance", "Bybit", "Okx"}

var platformTopics = []exchange.Topic{
	exchange.TopicBestQuote,
	exchange.TopicTrade,
	exchange.TopicAggTrade,
	exchange.TopicOrderBookDelta,
	exchange.TopicTicker,
	exchange.TopicLiquidation,
	exchange.TopicFundingRate,
	exchange.TopicOpenInterest,
	exchange.TopicGreeks,
	exchange.TopicMarkPrice,
}

func TestProtocolMatchesGenerate(t *testing.T) {
	got, err := readSection()
	if err != nil {
		t.Fatal(err)
	}
	want, err := topicdoc.Markdown()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("docs/PROTOCOL.md topic table is stale; run: go generate ./exchange/topicdoc\n%s", firstDiff(got, want))
	}
}

// TestProtocolAgreesWithTopics reads the committed table and compares it to Topics and
// Atom directly. Changing Topics without regenerating the doc fails here.
func TestProtocolAgreesWithTopics(t *testing.T) {
	doc, err := parseDoc()
	if err != nil {
		t.Fatal(err)
	}
	venues := liveVenues()
	if !slices.Equal(doc.venues, venueDisplays(venues)) {
		t.Fatalf("columns %q, venues with Topics %q", doc.venues, venueDisplays(venues))
	}
	wantTopics := topicNames(venues)
	if !slices.Equal(doc.topics, wantTopics) {
		t.Fatalf("rows %q, want %q", doc.topics, wantTopics)
	}
	if slices.Contains(doc.topics, "kline_1m") || strings.Contains(strings.Join(doc.topics, " "), "kline_") {
		t.Fatal("kline intervals are separate rows")
	}

	for vi, v := range venues {
		caps := indexCaps(t, v.Market.Topics())
		kIntervals, kAbsent, kRoutes := klineFromCode(t, v)
		for _, topic := range doc.topics {
			cell := doc.cells[topic][vi]
			if topic == "kline" {
				checkKline(t, v.Name, cell, kIntervals, kAbsent, kRoutes)
				continue
			}
			cap, supported := caps[exchange.Topic(topic)]
			if cell.supported != supported {
				t.Errorf("%s %s supported %v, Topics %v", v.Name, topic, cell.supported, supported)
			}
			if !supported {
				if len(cell.absent) != 0 || len(cell.routes) != 0 || len(cell.intervals) != 0 {
					t.Errorf("%s %s is absent but the cell is %+v", v.Name, topic, cell)
				}
				continue
			}
			if len(cell.intervals) != 0 {
				t.Errorf("%s %s lists intervals", v.Name, topic)
			}
			if !slices.Equal(cell.absent, cap.Absent) {
				t.Errorf("%s %s Absent %q, Topics %q", v.Name, topic, cell.absent, cap.Absent)
			}
			routes := routesFromAtom(t, v, exchange.Topic(topic))
			if !slices.Equal(cell.routes, routes) {
				t.Errorf("%s %s route %q, Atom %q", v.Name, topic, cell.routes, routes)
			}
		}
	}

	// E03-D004, E03-D006: both Binance markets list trade, on the route Atom returns.
	checkTrade(t, doc, "BinanceUM", symbol.Perp, "public")
	checkTrade(t, doc, "Binance", symbol.Spot, "default")
}

func checkTrade(t *testing.T, doc parsed, venue string, cat symbol.Category, want string) {
	t.Helper()
	v, ok := all.Registry().Venue(venue)
	if !ok {
		t.Fatalf("no venue %s", venue)
	}
	ref, err := v.Market.Atom(exchange.TopicTrade, directProbe(venue, cat))
	if err != nil {
		t.Fatalf("%s trade: %v", venue, err)
	}
	if ref.Route != want {
		t.Fatalf("%s trade Atom route %q, want %q", venue, ref.Route, want)
	}
	col := slices.Index(doc.venueNames, venue)
	if col < 0 {
		t.Fatalf("doc has no %s", venue)
	}
	cell := doc.cells["trade"][col]
	if !cell.supported || !slices.Equal(cell.routes, []string{want}) {
		t.Fatalf("%s trade cell %+v, want supported route %s", venue, cell, want)
	}
}

func checkKline(t *testing.T, venue string, cell docCell, intervals, absent, routes []string) {
	t.Helper()
	supported := len(intervals) > 0
	if cell.supported != supported {
		t.Errorf("%s kline supported %v, Topics intervals %q", venue, cell.supported, intervals)
	}
	if !slices.Equal(cell.intervals, intervals) {
		t.Errorf("%s kline intervals %q, Topics %q", venue, cell.intervals, intervals)
	}
	if !slices.Equal(cell.absent, absent) {
		t.Errorf("%s kline Absent %q, Topics %q", venue, cell.absent, absent)
	}
	if !slices.Equal(cell.routes, routes) {
		t.Errorf("%s kline route %q, Atom %q", venue, cell.routes, routes)
	}
}

type docCell struct {
	supported bool
	absent    []string
	routes    []string
	intervals []string
}

type parsed struct {
	venues     []string // display names, column order
	venueNames []string // registry names
	topics     []string
	cells      map[string][]docCell // topic -> one cell per venue
}

func parseDoc() (parsed, error) {
	text, err := readSection()
	if err != nil {
		return parsed{}, err
	}
	var header []string
	var rows [][]string
	for _, line := range strings.Split(text, "\n") {
		cells := splitRow(line)
		if len(cells) == 0 {
			continue
		}
		if cells[0] == "topic" {
			header = cells
			continue
		}
		if strings.HasPrefix(cells[0], "---") {
			continue
		}
		rows = append(rows, cells)
	}
	if len(header) < 4 || (len(header)-1)%3 != 0 {
		return parsed{}, errors.New("topic table header is not topic plus support, Absent, route")
	}
	n := (len(header) - 1) / 3
	out := parsed{cells: map[string][]docCell{}}
	for i := 0; i < n; i++ {
		base := 1 + i*3
		support, absent, route := header[base], header[base+1], header[base+2]
		if !strings.HasSuffix(support, " 支援") || !strings.HasSuffix(absent, " Absent") || !strings.HasSuffix(route, " route") {
			return parsed{}, errors.New("header " + support + " / " + absent + " / " + route)
		}
		display := strings.TrimSuffix(support, " 支援")
		if strings.TrimSuffix(absent, " Absent") != display || strings.TrimSuffix(route, " route") != display {
			return parsed{}, errors.New("header columns disagree for " + display)
		}
		out.venues = append(out.venues, display)
		out.venueNames = append(out.venueNames, registryName(display))
	}
	for _, row := range rows {
		if len(row) != len(header) {
			return parsed{}, errors.New("row width " + row[0])
		}
		topic, err := unquote(row[0])
		if err != nil {
			return parsed{}, err
		}
		if _, dup := out.cells[topic]; dup {
			return parsed{}, errors.New("duplicate row " + topic)
		}
		out.topics = append(out.topics, topic)
		for i := 0; i < n; i++ {
			base := 1 + i*3
			cell, err := parseCell(topic, row[base], row[base+1], row[base+2])
			if err != nil {
				return parsed{}, err
			}
			out.cells[topic] = append(out.cells[topic], cell)
		}
	}
	return out, nil
}

func parseCell(topic, support, absent, route string) (docCell, error) {
	var cell docCell
	switch {
	case support == "沒有":
	case support == "有":
		cell.supported = true
	case strings.HasPrefix(support, "有；"):
		cell.supported = true
		iv, err := parseList(strings.TrimPrefix(support, "有；"))
		if err != nil {
			return docCell{}, err
		}
		cell.intervals = iv
	default:
		return docCell{}, errors.New(topic + " support " + support)
	}
	var err error
	if cell.absent, err = parseList(absent); err != nil {
		return docCell{}, err
	}
	if cell.routes, err = parseList(route); err != nil {
		return docCell{}, err
	}
	if topic != "kline" && len(cell.intervals) != 0 {
		return docCell{}, errors.New(topic + " lists intervals")
	}
	if !cell.supported && (len(cell.absent) != 0 || len(cell.routes) != 0 || len(cell.intervals) != 0) {
		return docCell{}, errors.New(topic + " is 沒有 but names fields")
	}
	return cell, nil
}

func parseList(s string) ([]string, error) {
	if s == "—" {
		return nil, nil
	}
	parts := strings.Split(s, "、")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		item, err := unquote(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func unquote(s string) (string, error) {
	if len(s) < 2 || s[0] != '`' || s[len(s)-1] != '`' || strings.Contains(s[1:len(s)-1], "`") {
		return "", errors.New("not quoted: " + s)
	}
	return s[1 : len(s)-1], nil
}

func splitRow(line string) []string {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "|") {
		return nil
	}
	parts := strings.Split(line, "|")
	if len(parts) < 3 {
		return nil
	}
	cells := make([]string, 0, len(parts)-2)
	for _, p := range parts[1 : len(parts)-1] {
		cells = append(cells, strings.TrimSpace(p))
	}
	return cells
}

func readSection() (string, error) {
	path, err := topicdoc.ProtocolPath()
	if err != nil {
		return "", err
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return topicdoc.Section(string(body))
}

func liveVenues() []exchange.Venue {
	reg := all.Registry()
	var have []exchange.Venue
	for _, name := range reg.Venues() {
		v, ok := reg.Venue(name)
		if ok && v.Market != nil && len(v.Market.Topics()) > 0 {
			have = append(have, v)
		}
	}
	byName := map[string]exchange.Venue{}
	for _, v := range have {
		byName[v.Name] = v
	}
	var out []exchange.Venue
	for _, name := range venueOrder {
		v, ok := byName[name]
		if !ok {
			continue
		}
		out = append(out, v)
		delete(byName, name)
	}
	rest := make([]string, 0, len(byName))
	for name := range byName {
		rest = append(rest, name)
	}
	sort.Strings(rest)
	for _, name := range rest {
		out = append(out, byName[name])
	}
	return out
}

func venueDisplays(vs []exchange.Venue) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = display(v.Name)
	}
	return out
}

func display(name string) string {
	if name == "Binance" {
		return "Binance 現貨"
	}
	return name
}

func registryName(display string) string {
	if display == "Binance 現貨" {
		return "Binance"
	}
	return display
}

func topicNames(vs []exchange.Venue) []string {
	names := make([]string, len(platformTopics))
	for i, topic := range platformTopics {
		names[i] = string(topic)
	}
	known := map[string]bool{}
	for _, topic := range names {
		known[topic] = true
	}
	var extra []string
	for _, v := range vs {
		for _, cap := range v.Market.Topics() {
			if strings.HasPrefix(string(cap.Topic), exchange.KlinePrefix) || known[string(cap.Topic)] {
				continue
			}
			known[string(cap.Topic)] = true
			extra = append(extra, string(cap.Topic))
		}
	}
	sort.Strings(extra)
	names = append(names, extra...)
	return append(names, "kline")
}

func indexCaps(t *testing.T, caps []exchange.TopicCaps) map[exchange.Topic]exchange.TopicCaps {
	t.Helper()
	out := map[exchange.Topic]exchange.TopicCaps{}
	for _, cap := range caps {
		if strings.HasPrefix(string(cap.Topic), exchange.KlinePrefix) {
			continue
		}
		if _, dup := out[cap.Topic]; dup {
			t.Fatalf("duplicate %s", cap.Topic)
		}
		out[cap.Topic] = cap
	}
	return out
}

func klineFromCode(t *testing.T, v exchange.Venue) (intervals, absent, routes []string) {
	t.Helper()
	set := false
	for _, cap := range v.Market.Topics() {
		iv, ok := strings.CutPrefix(string(cap.Topic), exchange.KlinePrefix)
		if !ok {
			continue
		}
		gotRoutes := routesFromAtom(t, v, cap.Topic)
		if !set {
			absent = cap.Absent
			routes = gotRoutes
			set = true
		} else if !slices.Equal(absent, cap.Absent) || !slices.Equal(routes, gotRoutes) {
			t.Fatalf("%s kline intervals disagree", v.Name)
		}
		intervals = append(intervals, iv)
	}
	return intervals, absent, routes
}

// routesFromAtom is the spec for the route column (E04-D063). It does not call the generator.
func routesFromAtom(t *testing.T, v exchange.Venue, topic exchange.Topic) []string {
	t.Helper()
	var routes []string
	seen := map[string]struct{}{}
	for _, cat := range v.Categories {
		ref, err := v.Market.Atom(topic, directProbe(v.Name, cat))
		if err != nil {
			if errors.Is(err, exchange.ErrUnsupported) {
				continue
			}
			t.Fatalf("%s %s %s: %v", v.Name, cat, topic, err)
		}
		if ref.Route == "" {
			t.Fatalf("%s %s %s: empty route", v.Name, cat, topic)
		}
		if _, ok := seen[ref.Route]; ok {
			continue
		}
		seen[ref.Route] = struct{}{}
		routes = append(routes, ref.Route)
	}
	if len(routes) == 0 {
		t.Fatalf("%s %s: Atom rejected every category", v.Name, topic)
	}
	return routes
}

// directProbe matches internal/md/agent.routeInfo and the generator's probe: one fixed symbol.
func directProbe(venue string, cat symbol.Category) *symbol.Info {
	info := &symbol.Info{
		Ticker:     symbol.UniversalTicker{Venue: venue, Category: cat, Symbol: "BTCUSDT"},
		ExchTicker: "BTCUSDT",
		Status:     symbol.Tradable,
	}
	if cat != symbol.Spot {
		info.ContractSize = 1
	}
	return info
}

func firstDiff(got, want string) string {
	g := strings.Split(got, "\n")
	w := strings.Split(want, "\n")
	n := len(g)
	if len(w) > n {
		n = len(w)
	}
	for i := 0; i < n; i++ {
		var gl, wl string
		if i < len(g) {
			gl = g[i]
		}
		if i < len(w) {
			wl = w[i]
		}
		if gl != wl {
			return "line " + itoa(i+1) + "\n got: " + gl + "\nwant: " + wl
		}
	}
	return "equal"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
