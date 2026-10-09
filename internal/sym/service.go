package sym

import (
	"context"
	"errors"
	"math"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
)

const (
	// Subject is the sym request subject (PROTOCOL §7). Serve joins the queue
	// group of the same name, so any replica answers.
	Subject = "sym"

	// CodeInvalid, CodeNotFound and CodeUnavailable are the business error
	// codes. A success is a symv1.Reply and nothing else. A business failure
	// is a natsx error reply (option A): natsx.CodeHandler, and a message that
	// is one of these words, then an optional ": " and a reason. Reply.error
	// and Reply.reason stay empty.
	CodeInvalid     = "invalid"
	CodeNotFound    = "not_found"
	CodeUnavailable = "unavailable"

	// maxReplyBytes is one byte under NATS's default 1 MiB payload, so a
	// published reply is accepted. A page that does not fit is unavailable
	// rather than a short page: the caller's offset would otherwise skip rows.
	maxReplyBytes = 1<<20 - 1
)

// Service answers Subject from the Postgres store. run, when set, serves Refresh.
type Service struct {
	store *Store
	run   *Runner
	log   zerolog.Logger
}

// NewService serves reads from store. run may be nil; Refresh is then unavailable.
func NewService(store *Store, run *Runner, log zerolog.Logger) *Service {
	return &Service{store: store, run: run, log: log}
}

// Serve registers the queue group and returns once the server has the interest.
// The handler stops when ctx is cancelled.
func (sv *Service) Serve(ctx context.Context, c *natsx.Conn) (*natsx.Subscription, error) {
	if sv == nil || sv.store == nil {
		return nil, errors.New("sym: nil service")
	}
	if c == nil {
		return nil, errors.New("sym: nil connection")
	}
	return c.Serve(ctx, Subject, Subject, func() proto.Message {
		return &symv1.Request{}
	}, sv.handle)
}

func (sv *Service) handle(ctx context.Context, m proto.Message) (proto.Message, error) {
	req, ok := m.(*symv1.Request)
	if !ok || req == nil {
		err := invalid("request")
		sv.note(err)
		return nil, err
	}
	var (
		reply *symv1.Reply
		err   error
	)
	switch r := req.GetReq().(type) {
	case *symv1.Request_Get:
		reply, err = sv.get(ctx, r.Get)
	case *symv1.Request_GetByUid:
		reply, err = sv.getByUID(ctx, r.GetByUid)
	case *symv1.Request_List:
		reply, err = sv.list(ctx, r.List)
	case *symv1.Request_Reverse:
		reply, err = sv.reverse(ctx, r.Reverse)
	case *symv1.Request_Refresh:
		reply, err = sv.refresh(ctx, r.Refresh)
	default:
		err = invalid("request")
	}
	if err != nil {
		sv.note(err)
		return nil, err
	}
	if err := fits(reply); err != nil {
		sv.note(err)
		return nil, err
	}
	return reply, nil
}

func (sv *Service) get(ctx context.Context, req *symv1.GetRequest) (*symv1.Reply, error) {
	if req == nil {
		return nil, invalid("request")
	}
	if _, err := symbol.Parse(req.GetUniversalTicker()); err != nil {
		return nil, invalid("ticker")
	}
	info, err := sv.store.GetByTicker(ctx, req.GetUniversalTicker(), req.GetIncludeInactive())
	if err != nil {
		return nil, mapStore(err)
	}
	return infoReply(info)
}

func (sv *Service) getByUID(ctx context.Context, req *symv1.GetByUidRequest) (*symv1.Reply, error) {
	if req == nil {
		return nil, invalid("request")
	}
	info, err := sv.store.GetByUID(ctx, req.GetUid(), req.GetIncludeInactive())
	if err != nil {
		return nil, mapStore(err)
	}
	return infoReply(info)
}

func (sv *Service) list(ctx context.Context, req *symv1.ListRequest) (*symv1.Reply, error) {
	if req == nil {
		return nil, invalid("request")
	}
	if !canonicalVenueName(req.GetVenue()) {
		return nil, invalid("venue")
	}
	cat, err := parseCategory(req.GetCategory())
	if err != nil {
		return nil, err
	}
	limit, err := pageLimit(req.GetLimit())
	if err != nil {
		return nil, err
	}
	rows, total, err := sv.store.List(ctx, req.GetVenue(), cat, int64(req.GetOffset()), limit, req.GetIncludeInactive())
	if err != nil {
		return nil, mapStore(err)
	}
	symbols := make([]*symv1.SymbolInfo, 0, len(rows))
	for _, row := range rows {
		msg, err := infoMessage(row)
		if err != nil {
			return nil, unavailable("status", err)
		}
		symbols = append(symbols, msg)
	}
	return &symv1.Reply{Result: &symv1.Reply_List{List: &symv1.SymbolList{
		Symbols: symbols,
		Total:   total,
	}}}, nil
}

func (sv *Service) reverse(ctx context.Context, req *symv1.ReverseRequest) (*symv1.Reply, error) {
	if req == nil {
		return nil, invalid("request")
	}
	if !canonicalVenueName(req.GetVenue()) {
		return nil, invalid("venue")
	}
	cat, err := parseCategory(req.GetCategory())
	if err != nil {
		return nil, err
	}
	if req.GetExchTicker() == "" {
		return nil, invalid("exch_ticker")
	}
	info, err := sv.store.Reverse(ctx, symbol.ExchKey{
		Venue:      req.GetVenue(),
		Category:   cat,
		ExchTicker: req.GetExchTicker(),
	})
	if err != nil {
		return nil, mapStore(err)
	}
	return infoReply(info)
}

func (sv *Service) refresh(ctx context.Context, req *symv1.RefreshRequest) (*symv1.Reply, error) {
	if req == nil {
		return nil, invalid("request")
	}
	if sv.run == nil {
		return nil, unavailable("refresh", nil)
	}
	venue := req.GetVenue()
	if venue != "" && !canonicalVenueName(venue) {
		return nil, invalid("venue")
	}
	reports, err := sv.run.Trigger(ctx, venue)
	if err != nil {
		if errors.Is(err, ErrUnknownVenue) {
			return nil, invalid("venue")
		}
		return nil, unavailable("refresh", err)
	}
	return refreshReply(reports), nil
}

func (sv *Service) note(err error) {
	var ce *codeError
	if !errors.As(err, &ce) || ce.code != CodeUnavailable {
		return
	}
	ev := sv.log.Error().Str("reason", ce.reason)
	if ce.cause != nil {
		ev = ev.Err(ce.cause)
	}
	ev.Msg("sym unavailable")
}

// codeError is a business failure. Error is the natsx reply text.
type codeError struct {
	code   string
	reason string
	cause  error
}

func (e *codeError) Error() string {
	if e == nil || e.code == "" {
		return CodeUnavailable
	}
	if e.reason == "" {
		return e.code
	}
	return e.code + ": " + e.reason
}

func (e *codeError) Unwrap() error { return e.cause }

func invalid(reason string) error {
	return &codeError{code: CodeInvalid, reason: reason}
}

func unavailable(reason string, cause error) error {
	return &codeError{code: CodeUnavailable, reason: reason, cause: cause}
}

func mapStore(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, symbol.ErrNotFound) {
		return &codeError{code: CodeNotFound}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return unavailable("canceled", err)
	}
	return unavailable("store", err)
}

func fits(m proto.Message) error {
	if m == nil || proto.Size(m) > maxReplyBytes {
		return unavailable("reply exceeds 1 MiB", nil)
	}
	return nil
}

// pageLimit turns the request limit into a row count. Zero selects the page
// cap, so a caller can omit it. A limit above the cap is invalid: it is not
// clamped, because the caller asked for a page this service will not send.
func pageLimit(n uint32) (int, error) {
	if n == 0 {
		return symbol.ListPage, nil
	}
	if n > uint32(symbol.ListPage) {
		return 0, invalid("limit")
	}
	return int(n), nil
}

func parseCategory(s string) (symbol.Category, error) {
	c := symbol.Category(s)
	if !knownCategory(c) {
		return "", invalid("category")
	}
	return c, nil
}

// canonicalVenueName reports whether venue can be the first part of a ticker.
// Parse is the grammar; it does not consult the venue registry.
func canonicalVenueName(venue string) bool {
	t, err := symbol.Parse(venue + "_Spot_BTCUSDT")
	return err == nil && t.Venue == venue
}

func infoReply(info symbol.Info) (*symv1.Reply, error) {
	msg, err := infoMessage(info)
	if err != nil {
		return nil, unavailable("status", err)
	}
	return &symv1.Reply{Result: &symv1.Reply_Info{Info: msg}}, nil
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
		ContractSize:     opt(info.ContractSize),
		ContractValueCcy: info.ContractValueCcy,
		PriceCcy:         info.PriceCcy,
		Settle:           opt(info.Settle),
		ExpiryNs:         info.ExpiryNs,
		Strike:           opt(info.Strike),
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

// bound carries one filter. An empty value is a published restriction with no
// bound, which is a present key and an absent Bound.value.
func bound(v string) *symv1.Bound {
	b := &symv1.Bound{}
	if v != "" {
		b.Value = &v
	}
	return b
}

// refreshReply keys every map by venue/category. One venue has several
// categories, and a failure has to name which feed it was.
func refreshReply(reports []Report) *symv1.Reply {
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
	return &symv1.Reply{Result: &symv1.Reply_Refresh{Refresh: res}}
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
