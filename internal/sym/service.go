package sym

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
)

// maxPageBytes is one byte under 1 MiB. A List page whose protobuf reaches
// 1 MiB is unavailable rather than a short page: the caller's offset would
// otherwise skip rows. It is the same answer on every protocol (E22-D010).
const maxPageBytes = 1<<20 - 1

// API is SYM's logic, independent of any protocol (E22-D002, E22-D003).
// Requests and results are sym.v1 messages. A failure is an *Error.
type API interface {
	Get(ctx context.Context, req *symv1.GetRequest) (*symv1.SymbolInfo, error)
	GetByUID(ctx context.Context, req *symv1.GetByUidRequest) (*symv1.SymbolInfo, error)
	List(ctx context.Context, req *symv1.ListRequest) (*symv1.SymbolList, error)
	Reverse(ctx context.Context, req *symv1.ReverseRequest) (*symv1.SymbolInfo, error)
	Revisions(ctx context.Context, req *symv1.RevisionsRequest) (*symv1.RevisionList, error)
	Refresh(ctx context.Context, req *symv1.RefreshRequest) (*symv1.RefreshResult, error)
	Watch(ctx context.Context, f WatchFilter) (*Watch, error)
}

// Reader is the store reads Service answers from. *Store implements it.
type Reader interface {
	GetByTicker(ctx context.Context, ticker string, includeInactive bool) (symbol.Info, error)
	GetByUID(ctx context.Context, uid uint32, includeInactive bool) (symbol.Info, error)
	List(ctx context.Context, venue string, category symbol.Category, offset int64, limit int, includeInactive bool) ([]symbol.Info, uint32, error)
	Reverse(ctx context.Context, key symbol.ExchKey, includeInactive bool) (symbol.Info, error)
	Revisions(ctx context.Context) ([]TableRevision, error)
}

// Trigger runs a refresh now. *Runner implements it.
type Trigger interface {
	Trigger(ctx context.Context, venue string) ([]Report, error)
}

// Service implements API over a Reader. run, when set, serves Refresh; hub, when set, serves Watch.
type Service struct {
	read Reader
	run  Trigger
	hub  *Hub
	log  zerolog.Logger
}

var _ API = (*Service)(nil)

// NewService serves reads from read. run may be nil; Refresh is then unavailable.
func NewService(read Reader, run Trigger, log zerolog.Logger) *Service {
	return &Service{read: read, run: run, log: log}
}

// WithHub serves Watch from h. Call it before the service is in use. Without it, Watch is
// unavailable.
func (sv *Service) WithHub(h *Hub) *Service {
	sv.hub = h
	return sv
}

func (sv *Service) Get(ctx context.Context, req *symv1.GetRequest) (*symv1.SymbolInfo, error) {
	out, err := sv.get(ctx, req)
	return out, sv.note(err)
}

func (sv *Service) GetByUID(ctx context.Context, req *symv1.GetByUidRequest) (*symv1.SymbolInfo, error) {
	out, err := sv.getByUID(ctx, req)
	return out, sv.note(err)
}

func (sv *Service) List(ctx context.Context, req *symv1.ListRequest) (*symv1.SymbolList, error) {
	out, err := sv.list(ctx, req)
	return out, sv.note(err)
}

func (sv *Service) Reverse(ctx context.Context, req *symv1.ReverseRequest) (*symv1.SymbolInfo, error) {
	out, err := sv.reverse(ctx, req)
	return out, sv.note(err)
}

func (sv *Service) Revisions(ctx context.Context, req *symv1.RevisionsRequest) (*symv1.RevisionList, error) {
	out, err := sv.revisions(ctx, req)
	return out, sv.note(err)
}

func (sv *Service) Refresh(ctx context.Context, req *symv1.RefreshRequest) (*symv1.RefreshResult, error) {
	out, err := sv.refresh(ctx, req)
	return out, sv.note(err)
}

func (sv *Service) get(ctx context.Context, req *symv1.GetRequest) (*symv1.SymbolInfo, error) {
	if req == nil {
		return nil, invalid("request is empty")
	}
	ticker := req.GetUniversalTicker()
	if _, err := symbol.Parse(ticker); err != nil {
		return nil, invalid(fmt.Sprintf("ticker %q is not a universal ticker", ticker))
	}
	op := "get ticker " + ticker
	info, err := sv.read.GetByTicker(ctx, ticker, req.GetIncludeInactive())
	if err != nil {
		return nil, readErr(err, op, missed("ticker "+ticker, req.GetIncludeInactive()))
	}
	return infoResult(info)
}

func (sv *Service) getByUID(ctx context.Context, req *symv1.GetByUidRequest) (*symv1.SymbolInfo, error) {
	if req == nil {
		return nil, invalid("request is empty")
	}
	uid := req.GetUid()
	if !symbol.ValidUID(uid) {
		return nil, notFound(fmt.Sprintf("uid %d not found; %d is not a valid uid", uid, uid))
	}
	op := fmt.Sprintf("get uid %d", uid)
	info, err := sv.read.GetByUID(ctx, uid, req.GetIncludeInactive())
	if err != nil {
		return nil, readErr(err, op, missed(fmt.Sprintf("uid %d", uid), req.GetIncludeInactive()))
	}
	return infoResult(info)
}

func (sv *Service) list(ctx context.Context, req *symv1.ListRequest) (*symv1.SymbolList, error) {
	if req == nil {
		return nil, invalid("request is empty")
	}
	if err := CheckVenue(req.GetVenue()); err != nil {
		return nil, err
	}
	cat, err := ParseCategory(req.GetCategory())
	if err != nil {
		return nil, err
	}
	limit, err := pageLimit(req.GetLimit())
	if err != nil {
		return nil, err
	}
	op := fmt.Sprintf("list %s/%s", req.GetVenue(), cat)
	rows, total, err := sv.read.List(ctx, req.GetVenue(), cat, int64(req.GetOffset()), limit, req.GetIncludeInactive())
	if err != nil {
		return nil, readErr(err, op, notFound(op+" not found"))
	}
	symbols := make([]*symv1.SymbolInfo, 0, len(rows))
	for _, row := range rows {
		msg, err := infoMessage(row)
		if err != nil {
			return nil, statusUnavailable(row, err)
		}
		symbols = append(symbols, msg)
	}
	page := &symv1.SymbolList{Symbols: symbols, Total: total}
	if n := proto.Size(page); n > maxPageBytes {
		return nil, unavailable(fmt.Sprintf("page of %d rows is %d bytes, exceeds 1 MiB limit; lower limit", len(symbols), n), nil)
	}
	return page, nil
}

func (sv *Service) reverse(ctx context.Context, req *symv1.ReverseRequest) (*symv1.SymbolInfo, error) {
	if req == nil {
		return nil, invalid("request is empty")
	}
	if err := CheckVenue(req.GetVenue()); err != nil {
		return nil, err
	}
	cat, err := ParseCategory(req.GetCategory())
	if err != nil {
		return nil, err
	}
	if req.GetExchTicker() == "" {
		return nil, invalid("exch_ticker is empty")
	}
	op := fmt.Sprintf("reverse %s/%s %s", req.GetVenue(), cat, req.GetExchTicker())
	info, err := sv.read.Reverse(ctx, symbol.ExchKey{
		Venue:      req.GetVenue(),
		Category:   cat,
		ExchTicker: req.GetExchTicker(),
	}, req.GetIncludeInactive())
	if err != nil {
		label := fmt.Sprintf("exch_ticker %q for %s/%s", req.GetExchTicker(), req.GetVenue(), cat)
		return nil, readErr(err, op, missed(label, req.GetIncludeInactive()))
	}
	return infoResult(info)
}

func (sv *Service) revisions(ctx context.Context, req *symv1.RevisionsRequest) (*symv1.RevisionList, error) {
	if req == nil {
		return nil, invalid("request is empty")
	}
	rows, err := sv.read.Revisions(ctx)
	if err != nil {
		return nil, readErr(err, "revisions", notFound("revisions not found"))
	}
	tables := make([]*symv1.TableRevision, 0, len(rows))
	for _, row := range rows {
		if err := CheckVenue(row.Venue); err != nil {
			return nil, unavailable(fmt.Sprintf("revisions: venue %q is not canonical", row.Venue), err)
		}
		if _, err := ParseCategory(string(row.Category)); err != nil {
			return nil, unavailable(fmt.Sprintf("revisions: category %q is not a category", row.Category), err)
		}
		tables = append(tables, &symv1.TableRevision{
			Venue:    row.Venue,
			Category: string(row.Category),
			Revision: row.Revision,
		})
	}
	return &symv1.RevisionList{Tables: tables}, nil
}

func (sv *Service) refresh(ctx context.Context, req *symv1.RefreshRequest) (*symv1.RefreshResult, error) {
	if req == nil {
		return nil, invalid("request is empty")
	}
	if sv.run == nil {
		return nil, unavailable("refresh is not served by this process", nil)
	}
	venue := req.GetVenue()
	if venue != "" {
		if err := CheckVenue(venue); err != nil {
			return nil, err
		}
	}
	reports, err := sv.run.Trigger(ctx, venue)
	if err != nil {
		if errors.Is(err, ErrUnknownVenue) {
			return nil, invalid(fmt.Sprintf("venue %s is not refreshed by this process", venue))
		}
		what := "refresh"
		if venue != "" {
			what = fmt.Sprintf("refresh venue %q", venue)
		}
		return nil, unavailable(what+": "+causeText(err), err)
	}
	return refreshResult(reports), nil
}

// note logs an unavailable failure once, with its cause, and returns err.
func (sv *Service) note(err error) error {
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeUnavailable {
		return err
	}
	ev := sv.log.Error().Str("reason", e.Reason)
	if e.Cause != nil {
		ev = ev.Err(e.Cause)
	}
	ev.Msg("sym unavailable")
	return err
}

// pageLimit turns the request limit into a row count. Zero selects the page
// cap, so a caller can omit it. A limit above the cap is invalid: it is not
// clamped, because the caller asked for a page this service will not send.
func pageLimit(n uint32) (int, error) {
	if n == 0 {
		return symbol.ListPage, nil
	}
	if n > uint32(symbol.ListPage) {
		return 0, invalid(fmt.Sprintf("limit %d exceeds max %d", n, symbol.ListPage))
	}
	return int(n), nil
}

// ParseCategory is s as a category, or an invalid *Error that names the
// canonical spelling when s differs from one only by case.
func ParseCategory(s string) (symbol.Category, error) {
	c := symbol.Category(s)
	if knownCategory(c) {
		return c, nil
	}
	if s == "" {
		return "", invalid(`category "" is empty`)
	}
	for _, name := range []symbol.Category{symbol.Spot, symbol.Perp, symbol.Inverse, symbol.Future, symbol.Option} {
		if strings.EqualFold(s, string(name)) {
			return "", invalid(fmt.Sprintf("category %q is not canonical, use %q", s, name))
		}
	}
	return "", invalid(fmt.Sprintf("category %q is not one of Spot, Perp, Inverse, Future, Option", s))
}

// CheckVenue reports, as an invalid *Error, a venue that cannot be the first
// part of a ticker. A spelling that differs only by case names the canonical form.
func CheckVenue(name string) error {
	if canonicalVenueName(name) {
		return nil
	}
	if name == "" {
		return invalid(`venue "" is empty`)
	}
	if sug, ok := suggestVenue(name); ok {
		return invalid(fmt.Sprintf("venue %q is not canonical, use %q", name, sug))
	}
	return invalid(fmt.Sprintf("venue %q is not canonical", name))
}

// suggestVenue is the CamelCase spelling of a letter-and-digit name: first
// letter upper, the rest lower. "bybit" suggests "Bybit". "okx" suggests "Okx".
func suggestVenue(name string) (string, bool) {
	rs := []rune(name)
	if len(rs) == 0 || !unicode.IsLetter(rs[0]) {
		return "", false
	}
	for _, r := range rs {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return "", false
		}
	}
	rs[0] = unicode.ToUpper(rs[0])
	for i := 1; i < len(rs); i++ {
		if unicode.IsLetter(rs[i]) {
			rs[i] = unicode.ToLower(rs[i])
		}
	}
	sug := string(rs)
	if sug == name || !canonicalVenueName(sug) {
		return "", false
	}
	return sug, true
}

// canonicalVenueName reports whether venue can be the first part of a ticker.
// Parse is the grammar; it does not consult the venue registry.
func canonicalVenueName(venue string) bool {
	t, err := symbol.Parse(venue + "_Spot_BTCUSDT")
	return err == nil && t.Venue == venue
}

func infoResult(info symbol.Info) (*symv1.SymbolInfo, error) {
	msg, err := infoMessage(info)
	if err != nil {
		return nil, statusUnavailable(info, err)
	}
	return msg, nil
}

func statusUnavailable(info symbol.Info, err error) error {
	return unavailable(fmt.Sprintf("status %q cannot be encoded", info.Status), err)
}

func infoMessage(info symbol.Info) (*symv1.SymbolInfo, error) {
	st, err := statusProto(info.Status)
	if err != nil {
		return nil, err
	}
	out := &symv1.SymbolInfo{
		UniversalTicker:  info.Ticker.String(),
		ExchTicker:       info.ExchTicker,
		Base:             info.Base,
		Quote:            info.Quote,
		ContractSize:     optNumber(info.ContractSize),
		ContractValueCcy: info.ContractValueCcy,
		PriceCcy:         info.PriceCcy,
		Settle:           opt(info.Settle),
		ExpiryNs:         info.ExpiryNs,
		Strike:           optNumber(info.Strike),
		OptionType:       info.OptionType,
		Status:           st,
		UpdatedAtNs:      info.UpdatedAtNs,
		Uid:              info.UID,
	}
	if len(info.Filters) > 0 {
		out.Filters = make(map[string]*symv1.Bound, len(info.Filters))
		for name, value := range info.Filters {
			out.Filters[name] = bound(value)
		}
	}
	return out, nil
}

func statusProto(s Status) (symv1.SymbolStatus, error) {
	switch s {
	case Prelisting:
		return symv1.SymbolStatus_SYMBOL_STATUS_PRELISTING, nil
	case Tradable:
		return symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE, nil
	case Halted:
		return symv1.SymbolStatus_SYMBOL_STATUS_HALTED, nil
	case Settling:
		return symv1.SymbolStatus_SYMBOL_STATUS_SETTLING, nil
	case Expired:
		return symv1.SymbolStatus_SYMBOL_STATUS_EXPIRED, nil
	case Delisted:
		return symv1.SymbolStatus_SYMBOL_STATUS_DELISTED, nil
	case Unknown:
		return symv1.SymbolStatus_SYMBOL_STATUS_UNKNOWN, nil
	default:
		return symv1.SymbolStatus_SYMBOL_STATUS_UNSPECIFIED, errors.New("sym: status " + string(s))
	}
}

func opt(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// optNumber is a contract size or strike: 0 is none, an unset field.
func optNumber(v float64) *float64 {
	if v == 0 {
		return nil
	}
	return &v
}

// bound carries one filter. An absent value (NaN, E00-D008) is a published
// restriction with no bound, which is a present key and an unset Bound.value.
func bound(v float64) *symv1.Bound {
	b := &symv1.Bound{}
	if decimal.Has(v) {
		b.Value = &v
	}
	return b
}

// refreshResult keys every map by venue/category. One venue has several
// categories, and a failure has to name which feed it was.
func refreshResult(reports []Report) *symv1.RefreshResult {
	res := &symv1.RefreshResult{
		Refreshed:   map[string]uint32{},
		Deactivated: map[string]uint32{},
		Failed:      map[string]string{},
	}
	for _, rep := range reports {
		key := feedKey(rep.Venue, rep.Category)
		if rep.Err != nil {
			res.Failed[key] = rep.Err.Error()
			continue
		}
		res.Refreshed[key] = fitCount(rep.Seen)
		res.Deactivated[key] = fitCount(rep.Delisted + rep.Expired)
	}
	return res
}

func fitCount(n int) uint32 {
	if n <= 0 {
		return 0
	}
	if n > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n)
}
