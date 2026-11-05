// Command mftiko-pms-sim is a stand-in PMS for the E21 acceptance (E21-09, #521). It builds an
// exchange account's order and trade history from only what mftiko hands a PMS (E21-D001,
// E21-D008–E21-D011):
//
//   - the API, with an ordinary bearer token: /accounts, /scopes, /session-records, /sessions;
//   - the shared SYM over NATS (publish sym; its inbox is _INBOX_pms);
//   - its own exchange key, and the exchange's order and trade history endpoints.
//
// It does not reach etcd, subscribe td.evt, or read Postgres. Accounts are grouped by
// (exchange, exch_account_uid), so several api_ids of one exchange account read its history once.
// Each scope's UID is turned into the exchange ticker through SYM, and the history is read from
// the earliest since of the group. Every order is attributed by its client_order_id: the algo and
// ts_ms it encodes must fall in exactly one session's algo_ids and [created_at, end], where end is
// last_exit.at, or sts_retired_at for a retired STS, and a session that never spawned owns nothing
// (E21-D011, E21-D083). A fill belongs to its order's session, whenever it filled.
//
//	mftiko-pms-sim -api URL -token-file F -nats URL -nats-creds F \
//	    -key binance/<uid>=<creds.json> [-rest BinanceUM=URL] [-out report.json]
//
// The report is JSON. Only BinanceUM history is implemented; other venues are listed as skipped.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

// multi is a repeatable name=value flag.
type multi map[string]string

func (m multi) String() string { return fmt.Sprint(map[string]string(m)) }

func (m multi) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" || v == "" {
		return fmt.Errorf("want name=value, got %q", s)
	}
	m[k] = v
	return nil
}

type options struct {
	api, token, natsURL, natsCreds, out string
	keys, rest                          multi
}

func main() {
	o := options{keys: multi{}, rest: multi{"BinanceUM": "https://fapi.binance.com"}}
	version := flag.Bool("version", false, "print the release and exit")
	flag.StringVar(&o.api, "api", os.Getenv("MFTIKO_API"), "API base URL, without /api/v1 (default $MFTIKO_API)")
	tokenFile := flag.String("token-file", "", "file holding the bearer token (default $MFTIKO_TOKEN)")
	flag.StringVar(&o.natsURL, "nats", os.Getenv("NATS_URL"), "NATS URL for SYM (default $NATS_URL)")
	flag.StringVar(&o.natsCreds, "nats-creds", "", "NATS credentials: publish sym, subscribe _INBOX_pms.>")
	flag.Var(o.keys, "key", "exchange/exch_account_uid=credentials JSON; repeatable")
	flag.Var(o.rest, "rest", "venue=REST base URL; repeatable (default BinanceUM=https://fapi.binance.com)")
	flag.StringVar(&o.out, "out", "-", "report file; - is stdout")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	o.token = os.Getenv("MFTIKO_TOKEN")
	if *tokenFile != "" {
		b, err := os.ReadFile(*tokenFile)
		if err != nil {
			fail(err)
		}
		o.token = strings.TrimSpace(string(b))
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	rep, err := run(ctx, o, time.Now())
	if err != nil {
		fail(err)
	}
	if err := writeReport(o.out, rep); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "mftiko-pms-sim:", err)
	os.Exit(1)
}

func writeReport(path string, rep report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if path == "-" {
		_, err = os.Stdout.Write(b)
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// report is the whole output.
type report struct {
	GeneratedAt time.Time `json:"generated_at"`
	Groups      []group   `json:"groups"`
	// Incomplete lists regions /scopes said were unavailable; the result may miss scopes (E21-D010).
	Incomplete []string `json:"incomplete,omitempty"`
}

type group struct {
	Exchange string        `json:"exchange"`
	UID      string        `json:"exch_account_uid"`
	APIIDs   []int64       `json:"api_ids"`
	Sessions []sessionLine `json:"sessions"`
	Symbols  []symbolLine  `json:"symbols"`
}

type sessionLine struct {
	RecordID  string     `json:"record_id"`
	SessionID string     `json:"session_id"`
	APIID     int64      `json:"api_id"`
	Algos     []uint32   `json:"algo_ids"`
	Start     time.Time  `json:"start"`
	End       *time.Time `json:"end,omitempty"`
	Open      bool       `json:"open,omitempty"`
	Empty     bool       `json:"empty,omitempty"`
	EndSource string     `json:"end_source"`
}

type symbolLine struct {
	Venue      string      `json:"venue"`
	ExchTicker string      `json:"exch_ticker"`
	UIDs       []uint32    `json:"uids"`
	Since      time.Time   `json:"since"`
	Skipped    string      `json:"skipped,omitempty"`
	Orders     []orderLine `json:"orders"`
	Fills      []fillLine  `json:"fills"`
}

type orderLine struct {
	OrderID       int64       `json:"order_id"`
	ClientOrderID string      `json:"client_order_id"`
	Status        string      `json:"status"`
	Side          string      `json:"side"`
	Type          string      `json:"type"`
	ExecutedQty   string      `json:"executed_qty"`
	Time          time.Time   `json:"time"`
	Attribution   attribution `json:"attribution"`
}

type fillLine struct {
	TradeID       int64       `json:"trade_id"`
	OrderID       int64       `json:"order_id"`
	ClientOrderID string      `json:"client_order_id,omitempty"`
	Side          string      `json:"side"`
	Price         string      `json:"price"`
	Qty           string      `json:"qty"`
	Time          time.Time   `json:"time"`
	Attribution   attribution `json:"attribution"`
}

// skew is how far before a scope's since the history read starts. since is the TD host's clock
// when the order was about to be sent; the venue stamps it on its own clock.
const skew = time.Minute

func run(ctx context.Context, o options, now time.Time) (report, error) {
	switch {
	case o.api == "" || o.token == "":
		return report{}, errors.New("-api and a token are required")
	case o.natsURL == "":
		return report{}, errors.New("-nats is required")
	}
	api, err := newAPI(o.api, o.token)
	if err != nil {
		return report{}, err
	}
	sym, err := dialSym(o.natsURL, o.natsCreds)
	if err != nil {
		return report{}, err
	}
	defer sym.Close()

	accounts, err := api.accounts(ctx)
	if err != nil {
		return report{}, err
	}
	scopes, incomplete, err := api.scopes(ctx)
	if err != nil {
		return report{}, err
	}
	rep := report{GeneratedAt: now.UTC(), Incomplete: incomplete}
	for _, g := range groupAccounts(accounts) {
		line, err := buildGroup(ctx, o, api, sym, g, scopes, now)
		if err != nil {
			return report{}, fmt.Errorf("%s/%s: %w", g.exchange, g.uid, err)
		}
		rep.Groups = append(rep.Groups, line)
	}
	return rep, nil
}

func buildGroup(ctx context.Context, o options, api *apiClient, sym *symClient, g accountGroup, scopes []scopeRow, now time.Time) (group, error) {
	out := group{Exchange: g.exchange, UID: g.uid, APIIDs: g.apiIDs, Sessions: []sessionLine{}, Symbols: []symbolLine{}}
	var owners []owner
	for _, id := range g.apiIDs {
		got, err := api.owners(ctx, id)
		if err != nil {
			return group{}, err
		}
		owners = append(owners, got...)
	}
	for _, ow := range owners {
		line := sessionLine{
			RecordID: ow.RecordID, SessionID: ow.SessionID, APIID: ow.APIID, Algos: ow.Algos,
			Start: ow.Interval.Start.UTC(), Open: ow.Interval.Open, Empty: ow.Interval.Empty, EndSource: ow.Interval.Source,
		}
		if !ow.Interval.Open && !ow.Interval.Empty {
			end := ow.Interval.End.UTC()
			line.End = &end
		}
		out.Sessions = append(out.Sessions, line)
	}

	// UID → exchange ticker through SYM; one line per (venue, ticker), from the earliest since.
	type symKey struct{ venue, ticker string }
	lines := map[symKey]*symbolLine{}
	var order []symKey
	for _, s := range scopes {
		if !slices.Contains(g.apiIDs, s.apiID) {
			continue
		}
		info, err := sym.byUID(ctx, s.uid)
		if err != nil {
			return group{}, err
		}
		k := symKey{venueOf(info.GetUniversalTicker()), info.GetExchTicker()}
		l := lines[k]
		if l == nil {
			l = &symbolLine{Venue: k.venue, ExchTicker: k.ticker, Since: s.since, Orders: []orderLine{}, Fills: []fillLine{}}
			lines[k] = l
			order = append(order, k)
		}
		if !slices.Contains(l.UIDs, s.uid) {
			l.UIDs = append(l.UIDs, s.uid)
		}
		if s.since.Before(l.Since) {
			l.Since = s.since
		}
	}
	for _, k := range order {
		l := lines[k]
		l.Since = l.Since.UTC()
		credsPath, ok := o.keys[g.exchange+"/"+g.uid]
		rest, venueOK := o.rest[k.venue]
		switch {
		case k.venue != "BinanceUM" || !venueOK:
			l.Skipped = "no history reader for venue " + k.venue
		case !ok:
			l.Skipped = "no -key for " + g.exchange + "/" + g.uid
		}
		if l.Skipped != "" {
			out.Symbols = append(out.Symbols, *l)
			continue
		}
		c, err := readCreds(credsPath)
		if err != nil {
			return group{}, err
		}
		venue, err := newBinanceUM(rest, c)
		if err != nil {
			return group{}, err
		}
		orders, fills, err := venue.history(ctx, k.ticker, l.Since.Add(-skew), now)
		if err != nil {
			return group{}, err
		}
		sortHistory(orders, fills)
		byOrder := map[int64]orderLine{}
		for _, ord := range orders {
			ol := orderLine{
				OrderID: ord.OrderID, ClientOrderID: ord.ClientOrderID, Status: ord.Status, Side: ord.Side,
				Type: ord.Type, ExecutedQty: ord.ExecutedQty, Time: time.UnixMilli(ord.Time).UTC(),
				Attribution: attribute(ord.ClientOrderID, owners),
			}
			byOrder[ord.OrderID] = ol
			l.Orders = append(l.Orders, ol)
		}
		for _, f := range fills {
			fl := fillLine{TradeID: f.ID, OrderID: f.OrderID, Side: f.Side, Price: f.Price, Qty: f.Qty, Time: time.UnixMilli(f.Time).UTC()}
			if ol, ok := byOrder[f.OrderID]; ok {
				fl.ClientOrderID = ol.ClientOrderID
				fl.Attribution = ol.Attribution
			} else {
				// The order is older than the read; its id is not known here.
				fl.Attribution = attribution{Kind: "order_not_read"}
			}
			l.Fills = append(l.Fills, fl)
		}
		out.Symbols = append(out.Symbols, *l)
	}
	return out, nil
}
