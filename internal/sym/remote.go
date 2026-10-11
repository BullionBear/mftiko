package sym

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
)

const (
	// DefaultCheckInterval is how often a remote resolver compares revisions
	// when no notification arrived. A reconnect checks immediately as well.
	DefaultCheckInterval = 5 * time.Minute

	reloadTries = 16
)

// Table is one (venue, category) active table a remote resolver keeps.
type Table struct {
	Venue    string
	Category symbol.Category
}

// RemoteOptions tunes OpenRemote. The zero value checks every DefaultCheckInterval.
type RemoteOptions struct {
	// CheckInterval is the gap between revision comparisons. Zero selects
	// DefaultCheckInterval. Negative is rejected.
	CheckInterval time.Duration
	Log           zerolog.Logger
}

// Remote is the symbol.Resolver that loads the active table from SYM over NATS
// (E2-09, D203).
//
// At startup it pages List for each configured table and swaps in one snapshot.
// Indexes match the in-process resolver: UID to Info, ticker to UID, and
// ExchKey to UID. A notification on sym.changed.{venue}.{category}, a periodic
// revision comparison, a NATS reconnect, or Invalidate reloads that table and
// swaps the whole snapshot. A row that moved to the terminal table is absent
// from the new snapshot.
//
// includeInactive does not read or fill that cache. Get, GetByUID and Reverse
// with the flag set call SYM and return what it answered. List is the cached
// active table only.
//
// Safe for concurrent use. Close, or cancelling the context passed to
// OpenRemote, stops the subscription and the periodic check.
type Remote struct {
	conn     *natsx.Conn
	tables   []Table
	interval time.Duration
	log      zerolog.Logger

	snap atomic.Pointer[snapshot]

	mu    sync.Mutex
	revs  map[revKey]revState
	dirty map[string]struct{}
	fail  map[string]error

	loadMu sync.Mutex

	notify chan note
	kick   chan struct{}
	cancel context.CancelFunc
}

type revKey struct {
	venue string
	cat   symbol.Category
}

type revState struct {
	rev int64
	ok  bool
}

type note struct {
	key revKey
	rev int64
}

// OpenRemote loads each table and starts the notification and revision loops.
// ctx is the lifetime of those loops. The connection must already be up.
// A table that SYM cannot serve fails the call; a partial snapshot is not returned.
func OpenRemote(ctx context.Context, c *natsx.Conn, tables []Table, opt RemoteOptions) (*Remote, error) {
	if c == nil {
		return nil, errors.New("sym: nil connection")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	interval := opt.CheckInterval
	if interval == 0 {
		interval = DefaultCheckInterval
	}
	if interval < 0 {
		return nil, errors.New("sym: negative check interval")
	}
	copied, err := checkTables(tables)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	r := &Remote{
		conn:     c,
		tables:   copied,
		interval: interval,
		log:      opt.Log,
		revs:     map[revKey]revState{},
		dirty:    map[string]struct{}{},
		fail:     map[string]error{},
		notify:   make(chan note, 64),
		kick:     make(chan struct{}, 1),
		cancel:   cancel,
	}
	r.snap.Store(newSnapshot())
	if _, err := c.Subscribe(runCtx, ChangedWildcard, "", r.onChanged); err != nil {
		cancel()
		return nil, err
	}
	c.OnReconnect(func() { r.kickNow() })
	for _, t := range copied {
		if err := r.reload(runCtx, t.Venue, t.Category); err != nil {
			cancel()
			return nil, err
		}
	}
	go r.loop(runCtx)
	return r, nil
}

// Close stops the subscription and the periodic check. It does not close the
// NATS connection. A second call does nothing.
func (r *Remote) Close() {
	if r == nil || r.cancel == nil {
		return
	}
	r.cancel()
}

func checkTables(tables []Table) ([]Table, error) {
	seen := map[revKey]struct{}{}
	out := make([]Table, 0, len(tables))
	for _, t := range tables {
		if err := rejectVenue(t.Venue); err != nil {
			return nil, err
		}
		if _, err := parseCategory(string(t.Category)); err != nil {
			return nil, err
		}
		key := revKey{t.Venue, t.Category}
		if _, ok := seen[key]; ok {
			return nil, invalid(fmt.Sprintf("venue %s category %s is listed twice", t.Venue, t.Category))
		}
		seen[key] = struct{}{}
		out = append(out, t)
	}
	return out, nil
}

var _ symbol.Resolver = (*Remote)(nil)

// Get resolves a UniversalTicker from the cached active table.
// includeInactive calls SYM and does not write the cache.
func (r *Remote) Get(ctx context.Context, t symbol.UniversalTicker, includeInactive bool) (*symbol.Info, error) {
	if includeInactive {
		return r.rpcInfo(ctx, "get ticker "+t.String(), &symv1.Request{Req: &symv1.Request_Get{Get: &symv1.GetRequest{
			UniversalTicker: t.String(),
			IncludeInactive: true,
		}}})
	}
	if err := r.refreshDirty(ctx); err != nil && ctxErr(err) {
		return nil, err
	}
	if err := r.failure(t.Venue); err != nil {
		return nil, err
	}
	return r.snap.Load().findTicker(t)
}

// GetByUID resolves a UID from the cached active table.
// A uid outside MinUID..MaxUID is ErrNotFound. includeInactive calls SYM.
func (r *Remote) GetByUID(ctx context.Context, uid uint32, includeInactive bool) (*symbol.Info, error) {
	if !symbol.ValidUID(uid) {
		return nil, symbol.ErrNotFound
	}
	if includeInactive {
		return r.rpcInfo(ctx, fmt.Sprintf("get uid %d", uid), &symv1.Request{Req: &symv1.Request_GetByUid{GetByUid: &symv1.GetByUidRequest{
			Uid:             uid,
			IncludeInactive: true,
		}}})
	}
	if err := r.refreshDirty(ctx); err != nil && ctxErr(err) {
		return nil, err
	}
	snap := r.snap.Load()
	info, ok := snap.findUID(uid)
	if ok {
		if err := r.failure(info.Ticker.Venue); err != nil {
			return nil, err
		}
		return &info, nil
	}
	if err := r.anyFailure(); err != nil {
		return nil, err
	}
	return nil, symbol.ErrNotFound
}

// Reverse resolves a venue spelling from the cached active table.
// includeInactive calls SYM and does not write the cache. When both tables
// match, SYM returns the active row.
func (r *Remote) Reverse(ctx context.Context, key symbol.ExchKey, includeInactive bool) (*symbol.Info, error) {
	if includeInactive {
		op := fmt.Sprintf("reverse %s/%s %s", key.Venue, key.Category, key.ExchTicker)
		return r.rpcInfo(ctx, op, &symv1.Request{Req: &symv1.Request_Reverse{Reverse: &symv1.ReverseRequest{
			Venue:           key.Venue,
			Category:        string(key.Category),
			ExchTicker:      key.ExchTicker,
			IncludeInactive: true,
		}}})
	}
	if err := r.refreshDirty(ctx); err != nil && ctxErr(err) {
		return nil, err
	}
	if err := r.failure(key.Venue); err != nil {
		return nil, err
	}
	info, ok := r.snap.Load().findExch(key)
	if !ok {
		return nil, symbol.ErrNotFound
	}
	return &info, nil
}

// List is the cached active table of one (venue, category), ordered by
// UniversalTicker. It does not call SYM.
func (r *Remote) List(ctx context.Context, venue string, c symbol.Category) ([]symbol.Info, error) {
	if err := r.refreshDirty(ctx); err != nil && ctxErr(err) {
		return nil, err
	}
	if err := r.failure(venue); err != nil {
		return nil, err
	}
	return r.snap.Load().list(venue, c), nil
}

// Invalidate marks a venue for reload, or every configured venue when venue
// is "". The next Get, GetByUID, Reverse or List reloads those tables from SYM
// and swaps the snapshot.
func (r *Remote) Invalidate(venue string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if venue == "" {
		for _, t := range r.tables {
			r.dirty[t.Venue] = struct{}{}
		}
		return
	}
	r.dirty[venue] = struct{}{}
}

func (r *Remote) loop(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.checkLog(ctx, false)
		case <-r.kick:
			r.checkLog(ctx, true)
		case n := <-r.notify:
			r.onNotify(ctx, n)
		}
	}
}

func (r *Remote) checkLog(ctx context.Context, retry bool) {
	err := r.check(ctx)
	if err == nil {
		return
	}
	if !retry {
		r.log.Error().Err(err).Msg("revision check")
		return
	}
	// A reconnect often beats the server's own resubscribe. Try again before
	// waiting out the whole interval.
	var last error
	for i := 0; i < 10 && ctx.Err() == nil; i++ {
		if err := wait(ctx, 200*time.Millisecond); err != nil {
			return
		}
		last = r.check(ctx)
		if last == nil {
			return
		}
	}
	if last != nil && ctx.Err() == nil {
		r.log.Error().Err(last).Msg("revision check")
	}
}

func (r *Remote) onNotify(ctx context.Context, n note) {
	r.mu.Lock()
	st := r.revs[n.key]
	r.mu.Unlock()
	if st.ok && n.rev <= st.rev {
		return
	}
	if err := r.reload(ctx, n.key.venue, n.key.cat); err != nil && ctx.Err() == nil {
		r.log.Error().Err(err).Str("venue", n.key.venue).Str("category", string(n.key.cat)).Msg("reload")
	}
}

func (r *Remote) onChanged(m natsx.Msg) {
	var msg symv1.Changed
	if err := natsx.Unmarshal(m.Data, &msg); err != nil {
		r.log.Warn().Err(err).Str("subject", m.Subject).Msg("sym change")
		r.kickNow()
		return
	}
	want, err := ChangedSubject(msg.GetVenue(), msg.GetCategory())
	if err != nil || want != m.Subject || msg.GetRevision() < 1 {
		r.log.Warn().Str("subject", m.Subject).Str("venue", msg.GetVenue()).Str("category", msg.GetCategory()).Int64("revision", msg.GetRevision()).Msg("sym change mismatch")
		r.kickNow()
		return
	}
	key := revKey{msg.GetVenue(), symbol.Category(msg.GetCategory())}
	if !r.tracks(key) {
		return
	}
	select {
	case r.notify <- note{key: key, rev: msg.GetRevision()}:
	default:
		r.kickNow()
	}
}

func (r *Remote) tracks(key revKey) bool {
	for _, t := range r.tables {
		if t.Venue == key.venue && t.Category == key.cat {
			return true
		}
	}
	return false
}

func (r *Remote) kickNow() {
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// check reloads every configured table whose revision differs from the one
// this process loaded. A table SYM does not list is revision 0.
func (r *Remote) check(ctx context.Context) error {
	remote, err := r.revisionMap(ctx)
	if err != nil {
		return err
	}
	var behind []Table
	r.mu.Lock()
	for _, t := range r.tables {
		key := revKey{t.Venue, t.Category}
		st := r.revs[key]
		got := remote[key]
		if !st.ok || got != st.rev {
			behind = append(behind, t)
		}
	}
	r.mu.Unlock()
	var errs []error
	for _, t := range behind {
		if err := r.reload(ctx, t.Venue, t.Category); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Remote) reload(ctx context.Context, venue string, cat symbol.Category) error {
	r.loadMu.Lock()
	defer r.loadMu.Unlock()
	rows, rev, err := r.loadTable(ctx, venue, cat)
	if err != nil {
		return err
	}
	next, err := r.snap.Load().withTable(venue, cat, rows)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.snap.Store(next)
	r.revs[revKey{venue, cat}] = revState{rev: rev, ok: true}
	r.mu.Unlock()
	return nil
}

func (r *Remote) loadTable(ctx context.Context, venue string, cat symbol.Category) ([]symbol.Info, int64, error) {
	limit := symbol.ListPage
	var last error
	for range reloadTries {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		rev1, err := r.revisionOf(ctx, venue, cat)
		if err != nil {
			return nil, 0, err
		}
		rows, err := r.pages(ctx, venue, cat, limit)
		if pageTooBig(err) && limit > 1 {
			limit /= 2
			r.log.Warn().Str("venue", venue).Str("category", string(cat)).Int("limit", limit).Msg("list page too large")
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		rev2, err := r.revisionOf(ctx, venue, cat)
		if err != nil {
			return nil, 0, err
		}
		if rev1 == rev2 {
			return rows, rev2, nil
		}
		last = fmt.Errorf("sym: %s %s revision changed from %d to %d during list", venue, cat, rev1, rev2)
	}
	if last == nil {
		last = fmt.Errorf("sym: %s %s: list did not settle", venue, cat)
	}
	return nil, 0, last
}

func (r *Remote) pages(ctx context.Context, venue string, cat symbol.Category, limit int) ([]symbol.Info, error) {
	var (
		out   []symbol.Info
		off   uint32
		total uint32
		seen  = map[uint32]struct{}{}
	)
	op := fmt.Sprintf("list %s/%s", venue, cat)
	for {
		rep, err := r.rpc(ctx, op, &symv1.Request{Req: &symv1.Request_List{List: &symv1.ListRequest{
			Venue:    venue,
			Category: string(cat),
			Limit:    uint32(limit),
			Offset:   off,
		}}})
		if err != nil {
			return nil, err
		}
		page := rep.GetList()
		if page == nil {
			return nil, fmt.Errorf("sym: unavailable: %s: empty reply", op)
		}
		total = page.GetTotal()
		if len(page.GetSymbols()) == 0 {
			break
		}
		for _, msg := range page.GetSymbols() {
			info, err := infoFromProto(msg)
			if err != nil {
				return nil, fmt.Errorf("sym: unavailable: %s: %w", op, err)
			}
			if info.Ticker.Venue != venue || info.Ticker.Category != cat {
				return nil, fmt.Errorf("sym: unavailable: %s: row %s is %s %s", op, info.Ticker, info.Ticker.Venue, info.Ticker.Category)
			}
			if !info.Status.Active() {
				return nil, fmt.Errorf("sym: unavailable: %s: status %q is not active", op, info.Status)
			}
			if _, ok := seen[info.UID]; ok {
				return nil, fmt.Errorf("sym: unavailable: %s: uid %d repeated", op, info.UID)
			}
			seen[info.UID] = struct{}{}
			out = append(out, info)
		}
		n := uint32(len(page.GetSymbols()))
		if off > ^uint32(0)-n {
			break
		}
		off += n
		if off >= total || n < uint32(limit) {
			break
		}
	}
	return out, nil
}

func (r *Remote) revisionOf(ctx context.Context, venue string, cat symbol.Category) (int64, error) {
	all, err := r.revisionMap(ctx)
	if err != nil {
		return 0, err
	}
	return all[revKey{venue, cat}], nil
}

func (r *Remote) revisionMap(ctx context.Context) (map[revKey]int64, error) {
	rep, err := r.rpc(ctx, "revisions", &symv1.Request{Req: &symv1.Request_Revisions{Revisions: &symv1.RevisionsRequest{}}})
	if err != nil {
		return nil, err
	}
	list := rep.GetRevisions()
	if list == nil {
		return nil, errors.New("sym: unavailable: revisions: empty reply")
	}
	out := make(map[revKey]int64, len(list.GetTables()))
	for _, row := range list.GetTables() {
		if row.GetRevision() < 0 {
			return nil, fmt.Errorf("sym: unavailable: revisions: revision %d is negative", row.GetRevision())
		}
		cat := symbol.Category(row.GetCategory())
		if !knownCategory(cat) {
			return nil, fmt.Errorf("sym: unavailable: revisions: category %q is not a category", row.GetCategory())
		}
		out[revKey{row.GetVenue(), cat}] = row.GetRevision()
	}
	return out, nil
}

func (r *Remote) rpcInfo(ctx context.Context, op string, req *symv1.Request) (*symbol.Info, error) {
	return rpcInfo(ctx, r.conn, op, req)
}

func (r *Remote) rpc(ctx context.Context, op string, req *symv1.Request) (*symv1.Reply, error) {
	return rpc(ctx, r.conn, op, req)
}

func rpcInfo(ctx context.Context, c *natsx.Conn, op string, req *symv1.Request) (*symbol.Info, error) {
	rep, err := rpc(ctx, c, op, req)
	if err != nil {
		return nil, err
	}
	info, err := infoFromProto(rep.GetInfo())
	if err != nil {
		return nil, fmt.Errorf("sym: unavailable: %s: %w", op, err)
	}
	return &info, nil
}

func rpc(ctx context.Context, c *natsx.Conn, op string, req *symv1.Request) (*symv1.Reply, error) {
	var rep symv1.Reply
	err := c.Request(ctx, Subject, req, &rep)
	if err == nil {
		if rep.GetError() != "" || rep.GetReason() != "" {
			return nil, fmt.Errorf("sym: unavailable: %s: error field %q", op, rep.GetError())
		}
		return &rep, nil
	}
	var re *natsx.ReplyError
	if errors.As(err, &re) && re.Code == natsx.CodeHandler {
		if strings.HasPrefix(re.Message, CodeNotFound+":") {
			return nil, fmt.Errorf("%w: %s", symbol.ErrNotFound, re.Message)
		}
		return nil, fmt.Errorf("sym: %s", re.Message)
	}
	return nil, fmt.Errorf("sym: unavailable: %s: %w", op, err)
}

func pageTooBig(err error) bool {
	return err != nil && strings.Contains(err.Error(), "exceeds 1 MiB")
}

func (r *Remote) refreshDirty(ctx context.Context) error {
	var venues []string
	r.mu.Lock()
	for v := range r.dirty {
		venues = append(venues, v)
	}
	r.mu.Unlock()
	var errs []error
	for _, venue := range venues {
		if err := r.reloadVenue(ctx, venue); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Remote) reloadVenue(ctx context.Context, venue string) error {
	var cats []symbol.Category
	for _, t := range r.tables {
		if t.Venue == venue {
			cats = append(cats, t.Category)
		}
	}
	var errs []error
	for _, cat := range cats {
		if err := r.reload(ctx, venue, cat); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		r.mu.Lock()
		delete(r.dirty, venue)
		delete(r.fail, venue)
		r.mu.Unlock()
		return nil
	}
	err := errors.Join(errs...)
	r.mu.Lock()
	r.fail[venue] = err
	r.mu.Unlock()
	return err
}

func (r *Remote) failure(venue string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail[venue]
}

func (r *Remote) anyFailure() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return errors.Join(mapsValues(r.fail)...)
}

func mapsValues(m map[string]error) []error {
	out := make([]error, 0, len(m))
	for _, err := range m {
		out = append(out, err)
	}
	return out
}

func (s *snapshot) withTable(venue string, cat symbol.Category, rows []symbol.Info) (*snapshot, error) {
	next := s.clone()
	for uid, info := range next.byUID {
		if info.Ticker.Venue == venue && info.Ticker.Category == cat {
			delete(next.byUID, uid)
			delete(next.byTicker, info.Ticker)
			delete(next.byExch, info.Exch())
		}
	}
	seenT := map[symbol.UniversalTicker]struct{}{}
	seenE := map[symbol.ExchKey]struct{}{}
	for _, info := range rows {
		if info.Ticker.Venue != venue || info.Ticker.Category != cat {
			return nil, fmt.Errorf("sym: %s %s: row %s", venue, cat, info.Ticker)
		}
		if _, ok := seenT[info.Ticker]; ok {
			return nil, fmt.Errorf("sym: duplicate ticker %s", info.Ticker)
		}
		key := info.Exch()
		if _, ok := seenE[key]; ok {
			return nil, fmt.Errorf("sym: duplicate exch ticker %s", info.ExchTicker)
		}
		if prev, ok := next.byUID[info.UID]; ok {
			return nil, fmt.Errorf("sym: uid %d is already %s", info.UID, prev.Ticker)
		}
		if _, ok := next.byTicker[info.Ticker]; ok {
			return nil, fmt.Errorf("sym: ticker %s is already loaded", info.Ticker)
		}
		if _, ok := next.byExch[key]; ok {
			return nil, fmt.Errorf("sym: exch ticker %s is already loaded", info.ExchTicker)
		}
		seenT[info.Ticker] = struct{}{}
		seenE[key] = struct{}{}
		info.Filters = maps.Clone(info.Filters)
		next.byUID[info.UID] = info
		next.byTicker[info.Ticker] = info.UID
		next.byExch[key] = info.UID
	}
	return next, nil
}

func infoFromProto(msg *symv1.SymbolInfo) (symbol.Info, error) {
	if msg == nil {
		return symbol.Info{}, errors.New("sym: empty info")
	}
	t, err := symbol.Parse(msg.GetUniversalTicker())
	if err != nil {
		return symbol.Info{}, fmt.Errorf("ticker %q is not a universal ticker", msg.GetUniversalTicker())
	}
	if !symbol.ValidUID(msg.GetUid()) {
		return symbol.Info{}, fmt.Errorf("uid %d is not a valid uid", msg.GetUid())
	}
	st, err := statusFromProto(msg.GetStatus())
	if err != nil {
		return symbol.Info{}, err
	}
	info := symbol.Info{
		UID:              msg.GetUid(),
		Ticker:           t,
		ExchTicker:       msg.GetExchTicker(),
		Base:             msg.GetBase(),
		Quote:            msg.GetQuote(),
		ContractSize:     msg.GetContractSize(),
		ContractValueCcy: msg.GetContractValueCcy(),
		PriceCcy:         msg.GetPriceCcy(),
		Settle:           msg.GetSettle(),
		ExpiryNs:         msg.GetExpiryNs(),
		Strike:           msg.GetStrike(),
		OptionType:       msg.GetOptionType(),
		Status:           st,
		UpdatedAtNs:      msg.GetUpdatedAtNs(),
	}
	if len(msg.GetFilters()) > 0 {
		info.Filters = make(symbol.Filters, len(msg.GetFilters()))
		for name, b := range msg.GetFilters() {
			if name == "" {
				return symbol.Info{}, errors.New("filter name is empty")
			}
			// An unset Bound.value is a published restriction with no bound (D008).
			v := decimal.Absent()
			if b != nil && b.Value != nil {
				v = b.GetValue()
			}
			info.Filters[name] = v
		}
	}
	return info, nil
}

func statusFromProto(s symv1.SymbolStatus) (Status, error) {
	switch s {
	case symv1.SymbolStatus_SYMBOL_STATUS_PRELISTING:
		return Prelisting, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE:
		return Tradable, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_HALTED:
		return Halted, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_SETTLING:
		return Settling, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_EXPIRED:
		return Expired, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_DELISTED:
		return Delisted, nil
	case symv1.SymbolStatus_SYMBOL_STATUS_UNKNOWN:
		return Unknown, nil
	default:
		return "", fmt.Errorf("status %q cannot be decoded", s.String())
	}
}
