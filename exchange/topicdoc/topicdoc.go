// Package topicdoc renders the venue × topic table in docs/PROTOCOL.md (E04-D061–E04-D063).
//
//go:generate go run ./cmd/topicdoc
package topicdoc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/all"
)

const (
	// Begin and End bracket the generated table in docs/PROTOCOL.md. go generate rewrites
	// only the bytes between them.
	Begin = "<!-- topic-support -->"
	End   = "<!-- /topic-support -->"

	// probeSymbol is the instrument Atom is asked about. The route does not depend on which
	// symbol it is; one fixed ticker keeps the table free of sampled data.
	probeSymbol = "BTCUSDT"
)

// platformTopics is PROTOCOL §2's topic list, except kline, which is one row.
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

// venueOrder is the column order from #510: BinanceUM, Binance spot, Bybit, Okx.
// A later venue whose Topics is non-empty is appended, sorted.
var venueOrder = []string{"BinanceUM", "Binance", "Bybit", "Okx"}

// ProtocolPath is docs/PROTOCOL.md, next to this module.
func ProtocolPath() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("topicdoc: runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "docs", "PROTOCOL.md")), nil
}

// Markdown is the generated block, without the marker lines.
func Markdown() (string, error) {
	table, err := build()
	if err != nil {
		return "", err
	}
	return render(table), nil
}

// Section returns the generated block currently between the markers.
func Section(src string) (string, error) {
	i := strings.Index(src, Begin+"\n")
	if i < 0 {
		return "", errors.New("topicdoc: missing " + Begin)
	}
	rest := src[i+len(Begin)+1:]
	j := strings.Index(rest, End)
	if j < 0 {
		return "", errors.New("topicdoc: missing " + End)
	}
	return rest[:j], nil
}

// WriteProtocol rewrites the marked block in docs/PROTOCOL.md. The file is left untouched
// when the block is already current.
func WriteProtocol() error {
	path, err := ProtocolPath()
	if err != nil {
		return err
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	table, err := Markdown()
	if err != nil {
		return err
	}
	next, err := replace(string(body), table)
	if err != nil {
		return err
	}
	if next == string(body) {
		return nil
	}
	return os.WriteFile(path, []byte(next), 0o644)
}

func replace(src, table string) (string, error) {
	if !strings.HasSuffix(table, "\n") {
		table += "\n"
	}
	i := strings.Index(src, Begin+"\n")
	if i < 0 {
		return "", errors.New("topicdoc: missing " + Begin)
	}
	rest := src[i+len(Begin)+1:]
	j := strings.Index(rest, End)
	if j < 0 {
		return "", errors.New("topicdoc: missing " + End)
	}
	return src[:i] + Begin + "\n" + table + End + rest[j+len(End):], nil
}

type venueTable struct {
	name    string
	display string
	plain   map[exchange.Topic]cell
	kline   cell
}

type cell struct {
	supported bool
	absent    []string
	routes    []string
	intervals []string
}

func build() ([]venueTable, error) {
	reg := all.Registry()
	var have []exchange.Venue
	for _, name := range reg.Venues() {
		v, ok := reg.Venue(name)
		if !ok || v.Market == nil || len(v.Market.Topics()) == 0 {
			continue
		}
		have = append(have, v)
	}
	have = orderVenues(have)
	out := make([]venueTable, 0, len(have))
	for _, v := range have {
		row, err := oneVenue(v)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func orderVenues(vs []exchange.Venue) []exchange.Venue {
	byName := make(map[string]exchange.Venue, len(vs))
	for _, v := range vs {
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

func oneVenue(v exchange.Venue) (venueTable, error) {
	row := venueTable{
		name:    v.Name,
		display: display(v.Name),
		plain:   map[exchange.Topic]cell{},
	}
	seen := map[exchange.Topic]bool{}
	var klineAbsent []string
	var klineRoutes []string
	klineSet := false
	for _, cap := range v.Market.Topics() {
		if cap.Topic == "" {
			return venueTable{}, fmt.Errorf("topicdoc: %s lists an empty topic", v.Name)
		}
		if seen[cap.Topic] {
			return venueTable{}, fmt.Errorf("topicdoc: %s lists %s twice", v.Name, cap.Topic)
		}
		seen[cap.Topic] = true
		if strings.ContainsAny(string(cap.Topic), "|`") || strings.ContainsAny(strings.Join(cap.Absent, ""), "|`") {
			return venueTable{}, fmt.Errorf("topicdoc: %s %s has a character the table cannot quote", v.Name, cap.Topic)
		}
		if iv, ok := klineInterval(cap.Topic); ok {
			if iv == "" {
				return venueTable{}, fmt.Errorf("topicdoc: %s lists kline_ with no interval", v.Name)
			}
			routes, err := atomRoutes(v.Market, v.Name, v.Categories, cap.Topic)
			if err != nil {
				return venueTable{}, err
			}
			if !klineSet {
				klineAbsent = append([]string(nil), cap.Absent...)
				klineRoutes = routes
				klineSet = true
			} else if !slices.Equal(klineAbsent, cap.Absent) || !slices.Equal(klineRoutes, routes) {
				return venueTable{}, fmt.Errorf("topicdoc: %s kline intervals disagree on Absent or route", v.Name)
			}
			row.kline.intervals = append(row.kline.intervals, iv)
			continue
		}
		routes, err := atomRoutes(v.Market, v.Name, v.Categories, cap.Topic)
		if err != nil {
			return venueTable{}, err
		}
		row.plain[cap.Topic] = cell{supported: true, absent: append([]string(nil), cap.Absent...), routes: routes}
	}
	if klineSet {
		row.kline.supported = true
		row.kline.absent = klineAbsent
		row.kline.routes = klineRoutes
	}
	return row, nil
}

func klineInterval(topic exchange.Topic) (string, bool) {
	rest, ok := strings.CutPrefix(string(topic), exchange.KlinePrefix)
	return rest, ok
}

// atomRoutes is what Atom returns for topic, one entry per distinct route. Categories are
// probed in the venue's declared order. ErrUnsupported skips that category; any other error
// is returned. The probe matches internal/md/agent.routeInfo: ExchTicker is the ticker
// symbol, and a non-spot instrument has ContractSize 1. The MD agent calls Atom only to
// learn the route (E02-D002).
func atomRoutes(m exchange.Market, name string, cats []symbol.Category, topic exchange.Topic) ([]string, error) {
	var routes []string
	seen := map[string]struct{}{}
	accepted := false
	for _, cat := range cats {
		ref, err := m.Atom(topic, probe(name, cat))
		if err != nil {
			if errors.Is(err, exchange.ErrUnsupported) {
				continue
			}
			return nil, fmt.Errorf("topicdoc: %s %s %s: %w", name, cat, topic, err)
		}
		if ref.Route == "" {
			return nil, fmt.Errorf("topicdoc: %s %s %s: empty route", name, cat, topic)
		}
		accepted = true
		if _, ok := seen[ref.Route]; ok {
			continue
		}
		seen[ref.Route] = struct{}{}
		routes = append(routes, ref.Route)
	}
	if !accepted {
		return nil, fmt.Errorf("topicdoc: %s %s: Topics lists it but Atom rejected every category", name, topic)
	}
	return routes, nil
}

func probe(venue string, cat symbol.Category) *symbol.Info {
	info := &symbol.Info{
		Ticker:     symbol.UniversalTicker{Venue: venue, Category: cat, Symbol: probeSymbol},
		ExchTicker: probeSymbol,
		Status:     symbol.Tradable,
	}
	if cat != symbol.Spot {
		info.ContractSize = 1
	}
	return info
}

// display is the column name. Venue Binance is spot (E03-D001); the other names already
// say which market they are.
func display(name string) string {
	if name == "Binance" {
		return "Binance 現貨"
	}
	return name
}

// emptyTopics is every registered venue whose capability table is empty. Those columns
// are left out; the caption names them so a new Topics implementation shows up as a diff.
func emptyTopics() []string {
	reg := all.Registry()
	var names []string
	for _, name := range reg.Venues() {
		v, ok := reg.Venue(name)
		if !ok || v.Market == nil || len(v.Market.Topics()) == 0 {
			names = append(names, name)
		}
	}
	return names
}

func render(vs []venueTable) string {
	var b strings.Builder
	b.WriteString("`Binance` 是現貨。支援欄是 `Topics()` 有沒有這一列。Absent 是該列的欄位，沒有則為 —。route 是 `Atom` 對這個 venue 宣告的每個 category 回傳的路徑，依 category 順序去重；探針商品是 `BTCUSDT`，非現貨的 `ContractSize` 是 1。`Atom` 拒絕的 category 不列入。kline 是一列，interval 寫在支援欄，順序與 `Topics()` 相同。")
	if skipped := emptyTopics(); len(skipped) > 0 {
		b.WriteString("`Topics()` 為空的不列：" + quoteList(skipped) + "。")
	}
	b.WriteString("\n\n")
	b.WriteString("| topic |")
	for _, v := range vs {
		fmt.Fprintf(&b, " %s 支援 | %s Absent | %s route |", v.display, v.display, v.display)
	}
	b.WriteString("\n|---|")
	for range vs {
		b.WriteString("---|---|---|")
	}
	b.WriteString("\n")
	extra := extraTopics(vs)
	for _, topic := range platformTopics {
		writeRow(&b, vs, string(topic), topic, nil)
	}
	for _, topic := range extra {
		writeRow(&b, vs, topic, exchange.Topic(topic), nil)
	}
	writeRow(&b, vs, "kline", "", func(v venueTable) cell { return v.kline })
	return b.String()
}

func extraTopics(vs []venueTable) []string {
	known := map[exchange.Topic]bool{}
	for _, topic := range platformTopics {
		known[topic] = true
	}
	var extra []string
	for _, v := range vs {
		for topic := range v.plain {
			if known[topic] {
				continue
			}
			known[topic] = true
			extra = append(extra, string(topic))
		}
	}
	sort.Strings(extra)
	return extra
}

func writeRow(b *strings.Builder, vs []venueTable, label string, topic exchange.Topic, pick func(venueTable) cell) {
	fmt.Fprintf(b, "| `%s` |", label)
	for _, v := range vs {
		c := cell{}
		if pick != nil {
			c = pick(v)
		} else if got, ok := v.plain[topic]; ok {
			c = got
		}
		fmt.Fprintf(b, " %s | %s | %s |", supportText(c), listText(c.supported, c.absent), listText(c.supported, c.routes))
	}
	b.WriteString("\n")
}

func supportText(c cell) string {
	if !c.supported {
		return "沒有"
	}
	if len(c.intervals) == 0 {
		return "有"
	}
	return "有；" + quoteList(c.intervals)
}

func listText(supported bool, items []string) string {
	if !supported || len(items) == 0 {
		return "—"
	}
	return quoteList(items)
}

func quoteList(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = "`" + item + "`"
	}
	return strings.Join(quoted, "、")
}
