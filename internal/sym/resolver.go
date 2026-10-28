package sym

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/pkg/logger"
)

// ErrRealVenue is returned when a listing source is not an in-process fake.
// A real venue's UIDs are assigned by SYM. This process must not invent them.
var ErrRealVenue = errors.New("sym: real venue; UIDs come from SYM, not this process")

// VenueListing is one fake venue the in-process Resolver loads.
// REST must implement exchange.InProcessUIDSource (exchangetest).
// Categories are the (venue, category) units read with Listing.
type VenueListing struct {
	Venue      string
	REST       exchange.Lister
	Categories []symbol.Category
}

// Resolver is the in-process symbol.Resolver (E2-05).
//
// It is for tests and local development. It loads Listing itself and
// does not call the SYM service or PostgreSQL. OpenRemote is the NATS Resolver
// (E2-09); both implement symbol.Resolver.
//
// UIDs are allocated in this process. Tests pass a Source; a nil Source uses
// RandomSource. The numbers are not written to symbol_uid and must not be sent
// to another process or plane. NewResolver rejects a REST client that does not
// implement exchange.InProcessUIDSource, so a real adapter cannot be wired here.
// Real venues wait for the UIDs SYM assigns.
//
// Safe for concurrent use. A reload builds the next snapshot in full and
// publishes it once. Readers see the previous snapshot or the next one.
type Resolver struct {
	mu      sync.Mutex
	venues  []venueSrc
	uids    Source
	snap    atomic.Pointer[snapshot]
	live    map[symbol.UniversalTicker]uint32
	used    map[uint32]struct{}
	dirty   map[string]struct{}
	fail    map[string]error
	all     bool
	log     zerolog.Logger
	skipped uint64 // listings skipped because Ticker failed; not a venue failure
}

type venueSrc struct {
	venue string
	rest  exchange.Lister
	cats  []symbol.Category
}

// snapshot is an immutable active table. Indexes are UID → Info, ticker → UID,
// and ExchKey → UID. A published snapshot is not mutated.
type snapshot struct {
	byUID    map[uint32]symbol.Info
	byTicker map[symbol.UniversalTicker]uint32
	byExch   map[symbol.ExchKey]uint32
}

type state struct {
	snap *snapshot
	live map[symbol.UniversalTicker]uint32
	used map[uint32]struct{}
}

type cached struct {
	info symbol.Info
}

// NewResolver loads each configured (venue, category) and returns a Resolver
// that serves that table. uids is the in-process sequence; nil uses RandomSource.
// The draw is not persisted. A source that is not an InProcessUIDSource is
// ErrRealVenue.
func NewResolver(ctx context.Context, sources []VenueListing, uids Source) (*Resolver, error) {
	return newResolver(ctx, sources, uids, logger.New("sym", buildinfo.Version, buildinfo.CommitHash))
}

func newResolver(ctx context.Context, sources []VenueListing, uids Source, log zerolog.Logger) (*Resolver, error) {
	venues, err := checkSources(sources)
	if err != nil {
		return nil, err
	}
	if uids == nil {
		uids = RandomSource()
	}
	r := &Resolver{
		venues: venues,
		uids:   uids,
		live:   map[symbol.UniversalTicker]uint32{},
		used:   map[uint32]struct{}{},
		dirty:  map[string]struct{}{},
		fail:   map[string]error{},
		all:    true,
		log:    log,
	}
	r.snap.Store(newSnapshot())
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func checkSources(sources []VenueListing) ([]venueSrc, error) {
	seenVenue := map[string]struct{}{}
	out := make([]venueSrc, 0, len(sources))
	for _, src := range sources {
		if src.Venue == "" {
			return nil, errors.New("sym: venue is empty")
		}
		if _, ok := seenVenue[src.Venue]; ok {
			return nil, fmt.Errorf("sym: venue %s is listed twice", src.Venue)
		}
		seenVenue[src.Venue] = struct{}{}
		if src.REST == nil {
			return nil, fmt.Errorf("sym: %s: listing source is nil", src.Venue)
		}
		if _, ok := src.REST.(exchange.InProcessUIDSource); !ok {
			return nil, fmt.Errorf("%w: %s", ErrRealVenue, src.Venue)
		}
		if len(src.Categories) == 0 {
			return nil, fmt.Errorf("sym: %s: no category", src.Venue)
		}
		seenCat := map[symbol.Category]struct{}{}
		cats := make([]symbol.Category, 0, len(src.Categories))
		for _, c := range src.Categories {
			if c == "" {
				return nil, fmt.Errorf("sym: %s: category is empty", src.Venue)
			}
			if _, ok := seenCat[c]; ok {
				return nil, fmt.Errorf("sym: %s %s is listed twice", src.Venue, c)
			}
			seenCat[c] = struct{}{}
			cats = append(cats, c)
		}
		out = append(out, venueSrc{venue: src.Venue, rest: src.REST, cats: cats})
	}
	return out, nil
}

var _ symbol.Resolver = (*Resolver)(nil)

// Get resolves a UniversalTicker from the in-process active table.
// includeInactive does not widen the result: this process keeps no terminal table.
func (r *Resolver) Get(ctx context.Context, t symbol.UniversalTicker, includeInactive bool) (*symbol.Info, error) {
	_ = includeInactive
	snap, err := r.serve(ctx, t.Venue)
	if err != nil {
		return nil, err
	}
	return snap.findTicker(t)
}

// GetByUID resolves an in-process UID. A uid outside MinUID..MaxUID is ErrNotFound.
// includeInactive does not widen the result: terminal rows are not loaded.
func (r *Resolver) GetByUID(ctx context.Context, uid uint32, includeInactive bool) (*symbol.Info, error) {
	// includeInactive does not add rows: this process keeps no terminal table.
	_ = includeInactive
	if !symbol.ValidUID(uid) {
		return nil, symbol.ErrNotFound
	}
	if err := r.refresh(ctx); err != nil && ctxErr(err) {
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

// Reverse resolves a venue spelling from the active table.
// Bybit spot and linear share an ExchTicker and differ by category.
// includeInactive does not widen the result: this process keeps no terminal table.
func (r *Resolver) Reverse(ctx context.Context, key symbol.ExchKey, includeInactive bool) (*symbol.Info, error) {
	_ = includeInactive
	snap, err := r.serve(ctx, key.Venue)
	if err != nil {
		return nil, err
	}
	info, ok := snap.findExch(key)
	if !ok {
		return nil, symbol.ErrNotFound
	}
	return &info, nil
}

// List is the active table of one (venue, category): PRELISTING, TRADABLE,
// HALTED, SETTLING and UNKNOWN. Only TRADABLE may trade. The result is ordered
// by UniversalTicker.
func (r *Resolver) List(ctx context.Context, venue string, c symbol.Category) ([]symbol.Info, error) {
	snap, err := r.serve(ctx, venue)
	if err != nil {
		return nil, err
	}
	return snap.list(venue, c), nil
}

// Invalidate marks a venue for reload, or every venue when venue is "".
// The next Get, GetByUID, Reverse or List builds the new snapshot and swaps it in.
func (r *Resolver) Invalidate(venue string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if venue == "" {
		r.all = true
		return
	}
	r.dirty[venue] = struct{}{}
}

// serve reloads when needed and returns the snapshot for venue.
// A failed reload of venue is returned; another venue's failure is not.
func (r *Resolver) serve(ctx context.Context, venue string) (*snapshot, error) {
	if err := r.refresh(ctx); err != nil && ctxErr(err) {
		return nil, err
	}
	if err := r.failure(venue); err != nil {
		return nil, err
	}
	return r.snap.Load(), nil
}

func (r *Resolver) failure(venue string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fail[venue]
}

func (r *Resolver) anyFailure() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for _, err := range r.fail {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func ctxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// refresh reloads dirty venues. Listing runs under r.mu. The source must not
// call back into the Resolver. One venue is all-or-nothing: every configured
// category is fetched, then that venue's rows replace the previous ones in a
// single snapshot. A venue whose listing fails keeps its previous rows.
// A row that passes Validate but fails Ticker is skipped, logged and counted;
// it does not fail the venue. EXPIRED, DELISTED and an invalid status are not
// cached. Successful venues are still published when another venue fails.
func (r *Resolver) refresh(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !r.all && len(r.dirty) == 0 {
		return nil
	}
	wantAll := r.all
	want := maps.Clone(r.dirty)
	cur := state{
		snap: r.snap.Load().clone(),
		live: maps.Clone(r.live),
		used: maps.Clone(r.used),
	}
	failed := map[string]error{}
	for _, src := range r.venues {
		if !wantAll {
			if _, ok := want[src.venue]; !ok {
				if err := r.fail[src.venue]; err != nil {
					failed[src.venue] = err
				}
				continue
			}
		}
		next, err := r.loadVenue(ctx, src, cur)
		if err != nil {
			failed[src.venue] = err
			continue
		}
		cur = next
	}
	r.snap.Store(cur.snap)
	r.live = cur.live
	r.used = cur.used
	r.all = false
	r.dirty = map[string]struct{}{}
	r.fail = failed
	for venue := range failed {
		r.dirty[venue] = struct{}{}
	}
	var errs []error
	for _, err := range failed {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (r *Resolver) loadVenue(ctx context.Context, src venueSrc, cur state) (state, error) {
	type batch struct {
		cat  symbol.Category
		rows []cached
	}
	batches := make([]batch, 0, len(src.cats))
	for _, cat := range src.cats {
		if err := ctx.Err(); err != nil {
			return cur, err
		}
		listed, err := src.rest.Listing(ctx, cat)
		if err != nil {
			return cur, fmt.Errorf("sym: list %s %s: %w", src.venue, cat, err)
		}
		rows, n, err := prepare(r.log, src.venue, cat, listed)
		r.skipped += uint64(n)
		if err != nil {
			return cur, fmt.Errorf("sym: list %s %s: %w", src.venue, cat, err)
		}
		batches = append(batches, batch{cat: cat, rows: rows})
	}
	next := cur.clone()
	for _, b := range batches {
		if err := next.replace(src.venue, b.cat, b.rows, r.uids); err != nil {
			for uid := range next.used {
				cur.used[uid] = struct{}{}
			}
			return cur, fmt.Errorf("sym: list %s %s: %w", src.venue, b.cat, err)
		}
	}
	return next, nil
}

// prepare keeps active-table rows that pass Validate. Asset codes are
// canonicalized with normalizeAssets, the same helper SYM refresh uses,
// before Validate. Quanto, a bad asset code, a spot settlement, an invalid
// status (including UNSPECIFIED), and EXPIRED or DELISTED are omitted.
// A row that passes Validate but fails Ticker is skipped: it is logged,
// counted, and omitted. It does not fail the venue. A venue or category
// mismatch, or a repeated ticker or exchange spelling, fails the venue reload.
// This process does not infer EXPIRED from ExpiryNs; that move is SYM refresh.
func prepare(log zerolog.Logger, venue string, cat symbol.Category, listed []symbol.Listing) ([]cached, int, error) {
	out := make([]cached, 0, len(listed))
	seenT := map[symbol.UniversalTicker]struct{}{}
	seenE := map[string]struct{}{}
	skipped := 0
	for _, l := range listed {
		if l.Venue != venue || l.Category != cat {
			return nil, skipped, fmt.Errorf("listing %s is %s %s", l.ExchTicker, l.Venue, l.Category)
		}
		if err := normalizeAssets(&l); err != nil {
			continue
		}
		if err := l.Validate(); err != nil {
			continue
		}
		if !l.Status.Active() {
			continue
		}
		ticker, err := l.Ticker()
		if err != nil {
			skipped++
			logSkipTicker(log, venue, cat, l, err)
			continue
		}
		if _, ok := seenT[ticker]; ok {
			return nil, skipped, fmt.Errorf("duplicate ticker %s", ticker)
		}
		if _, ok := seenE[l.ExchTicker]; ok {
			return nil, skipped, fmt.Errorf("duplicate exch ticker %s", l.ExchTicker)
		}
		seenT[ticker] = struct{}{}
		seenE[l.ExchTicker] = struct{}{}
		info, err := infoFrom(l, ticker)
		if err != nil {
			return nil, skipped, err
		}
		out = append(out, cached{info: info})
	}
	return out, skipped, nil
}

func logSkipTicker(log zerolog.Logger, venue string, cat symbol.Category, l symbol.Listing, err error) {
	log.Warn().
		Str("venue", venue).
		Str("category", string(cat)).
		Str("exch_ticker", l.ExchTicker).
		Err(err).
		Msg("listing skipped")
}

func infoFrom(l symbol.Listing, ticker symbol.UniversalTicker) (symbol.Info, error) {
	opt := ""
	if l.OptionType != "" {
		opt = strings.ToUpper(strings.TrimSpace(l.OptionType))
	}
	return symbol.Info{
		Ticker:           ticker,
		ExchTicker:       l.ExchTicker,
		Base:             l.Base,
		Quote:            l.Quote,
		ContractSize:     l.ContractSize,
		ContractValueCcy: l.ContractValueCcy,
		PriceCcy:         l.PriceCcy,
		Settle:           l.Settle,
		ExpiryNs:         l.ExpiryNs,
		Strike:           l.Strike,
		OptionType:       opt,
		Status:           l.Status,
		Filters:          maps.Clone(l.Filters),
	}, nil
}

// replace swaps one (venue, category) inside s. A ticker that was already live
// keeps its UID. A new ticker draws one. UIDs that leave the table stay in used.
func (s *state) replace(venue string, cat symbol.Category, rows []cached, uids Source) error {
	prev := map[symbol.UniversalTicker]uint32{}
	for ticker, uid := range s.live {
		if ticker.Venue == venue && ticker.Category == cat {
			prev[ticker] = uid
		}
	}
	for ticker, uid := range prev {
		info := s.snap.byUID[uid]
		delete(s.snap.byUID, uid)
		delete(s.snap.byTicker, ticker)
		delete(s.snap.byExch, info.Exch())
		delete(s.live, ticker)
	}
	slices.SortFunc(rows, func(a, b cached) int {
		return strings.Compare(a.info.Ticker.String(), b.info.Ticker.String())
	})
	for _, row := range rows {
		info := row.info
		uid, ok := prev[info.Ticker]
		if ok {
			delete(prev, info.Ticker)
		} else {
			var err error
			uid, err = drawUID(s.used, uids)
			if err != nil {
				return err
			}
		}
		info.UID = uid
		s.live[info.Ticker] = uid
		s.snap.byUID[uid] = info
		s.snap.byTicker[info.Ticker] = uid
		s.snap.byExch[info.Exch()] = uid
	}
	return nil
}

// drawUID takes the next in-process UID. Invalid draws and draws already used
// in this process are skipped, including UIDs retired by a reload. The value is
// recorded in used before it is returned so a discarded snapshot does not reuse it.
func drawUID(used map[uint32]struct{}, src Source) (uint32, error) {
	outside := 0
	for draws := 0; draws < maxUIDDraws; {
		uid, err := src.NextUID()
		if err != nil {
			return 0, err
		}
		if !symbol.ValidUID(uid) {
			outside++
			if outside > maxUIDDraws {
				return 0, fmt.Errorf("%w: draws were outside %d..%d", ErrUID, symbol.MinUID, symbol.MaxUID)
			}
			continue
		}
		draws++
		if _, taken := used[uid]; taken {
			continue
		}
		used[uid] = struct{}{}
		return uid, nil
	}
	return 0, fmt.Errorf("%w: in-process sequence collided", ErrUID)
}

func newSnapshot() *snapshot {
	return &snapshot{
		byUID:    map[uint32]symbol.Info{},
		byTicker: map[symbol.UniversalTicker]uint32{},
		byExch:   map[symbol.ExchKey]uint32{},
	}
}

func (s *snapshot) clone() *snapshot {
	if s == nil {
		return newSnapshot()
	}
	return &snapshot{
		byUID:    maps.Clone(s.byUID),
		byTicker: maps.Clone(s.byTicker),
		byExch:   maps.Clone(s.byExch),
	}
}

func (s state) clone() state {
	return state{
		snap: s.snap.clone(),
		live: maps.Clone(s.live),
		used: maps.Clone(s.used),
	}
}

func (s *snapshot) findUID(uid uint32) (symbol.Info, bool) {
	if s == nil {
		return symbol.Info{}, false
	}
	info, ok := s.byUID[uid]
	if !ok {
		return symbol.Info{}, false
	}
	info.Filters = maps.Clone(info.Filters)
	return info, true
}

func (s *snapshot) findTicker(t symbol.UniversalTicker) (*symbol.Info, error) {
	if s == nil {
		return nil, symbol.ErrNotFound
	}
	uid, ok := s.byTicker[t]
	if !ok {
		return nil, symbol.ErrNotFound
	}
	info, ok := s.findUID(uid)
	if !ok {
		return nil, symbol.ErrNotFound
	}
	return &info, nil
}

func (s *snapshot) findExch(key symbol.ExchKey) (symbol.Info, bool) {
	if s == nil {
		return symbol.Info{}, false
	}
	uid, ok := s.byExch[key]
	if !ok {
		return symbol.Info{}, false
	}
	return s.findUID(uid)
}

func (s *snapshot) list(venue string, c symbol.Category) []symbol.Info {
	out := []symbol.Info{}
	if s == nil {
		return out
	}
	for _, info := range s.byUID {
		if info.Ticker.Venue != venue || info.Ticker.Category != c {
			continue
		}
		info.Filters = maps.Clone(info.Filters)
		out = append(out, info)
	}
	slices.SortFunc(out, func(a, b symbol.Info) int {
		return strings.Compare(a.Ticker.String(), b.Ticker.String())
	})
	return out
}
