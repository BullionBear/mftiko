package sym

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// errDuplicate is a listing batch that names one instrument twice, or that would
// leave two active rows with the same exchange spelling. The batch is not written.
var errDuplicate = errors.New("sym: duplicate listing")

// Feed is one (venue, category) listing. Several categories of one venue share
// a reader; Listing is called with Category. A refresh of this feed does
// not delist another category. REST reads the whole listing: a venue's
// PublicREST over an HTTP transport (exchange.ListingOver), or a fake.
type Feed struct {
	Venue    string
	Category symbol.Category
	REST     exchange.Lister
}

// Report is one feed's refresh. When Err is set, nothing from this call was
// committed: a failed read, or a transaction that rolled back, leaves every
// active row and symbol_revision as they were.
type Report struct {
	Venue    string
	Category symbol.Category
	Seen     int // listings the source returned
	Rejected int // not written: validation, ticker, decimal, or the wrong unit
	Inserted int
	Updated  int
	Delisted int
	Expired  int
	Changed  bool
	Revision int64 // current revision after a successful call; 0 if this unit has none
	Err      error // set by Runner; Refresh returns the error separately
}

// Refresh reads the feed and applies it in one transaction.
//
// A listing error is returned before the transaction starts. Rows the source
// already produced are ignored, so a page that fails halfway does not delist
// the instruments that arrived. Listings that fail Listing.Validate, or whose
// ticker or decimals cannot be derived, are logged and counted and are not
// written. If that listing can be tied to an active row, by universal ticker
// or else by exchange spelling, the row is left unchanged and is not delisted:
// a bad payload is not treated as absence.
//
// Rows of this (venue, category) that the feed did not return become DELISTED,
// unless the stored row is a Future or Option already past expiry_ns, in which
// case it becomes EXPIRED. A returned Future or Option past expiry_ns becomes
// EXPIRED too, and is not inserted again on a later refresh while it stays
// expired. EXPIRED and DELISTED move to the terminal table with their filters.
// The uid and the ticker string stay. Terminal rows are not updated.
//
// Asset codes are canonicalized (uppercase, alias table) before Validate, so
// a native code such as XBT is judged and stored as BTC. The exchange spelling
// is left as published. A dated contract at or past expiry_ns is EXPIRED even
// when the listing status is still non-terminal. Any other valid status is
// stored as given: non-terminal statuses stay on the active table, and EXPIRED
// or DELISTED move. symbol_revision advances by one when any row was inserted,
// updated or moved, and does not advance when the feed matches what is stored.
func (s *Store) Refresh(ctx context.Context, f Feed, atNs int64, log zerolog.Logger) (Report, error) {
	rep := Report{Venue: f.Venue, Category: f.Category}
	if f.REST == nil || f.Venue == "" || !knownCategory(f.Category) {
		return rep, fmt.Errorf("sym: feed %s %s is incomplete", f.Venue, f.Category)
	}
	if atNs < 0 {
		return rep, fmt.Errorf("sym: refresh time %d", atNs)
	}
	rows, err := f.REST.Listing(ctx, f.Category)
	if err != nil {
		return rep, fmt.Errorf("sym: listing %s %s: %w", f.Venue, f.Category, err)
	}
	return s.apply(ctx, f.Venue, f.Category, rows, atNs, log)
}

type prepared struct {
	ticker           string
	exch             string
	base             string
	quote            string
	contractSize     *string
	contractValueCcy string
	priceCcy         string
	settle           *string
	expiry           *int64
	strike           *string
	optionType       *string
	status           Status
	filters          []Filter
}

func (p prepared) listingRow(createdAtNs int64) listingRow {
	return listingRow{
		UniversalTicker:  p.ticker,
		ExchTicker:       p.exch,
		Base:             p.base,
		Quote:            p.quote,
		ContractSize:     p.contractSize,
		ContractValueCcy: p.contractValueCcy,
		PriceCcy:         p.priceCcy,
		Settle:           p.settle,
		ExpiryNs:         p.expiry,
		Strike:           p.strike,
		OptionType:       p.optionType,
		CreatedAtNs:      createdAtNs,
	}
}

type activeRow struct {
	uid       uint32
	createdAt int64
	updatedAt int64
	prepared
}

type incoming struct {
	raw symbol.Listing
	p   *prepared
	err error
}

func (s *Store) apply(ctx context.Context, venue string, category symbol.Category, listings []symbol.Listing, atNs int64, log zerolog.Logger) (Report, error) {
	rep := Report{Venue: venue, Category: category, Seen: len(listings)}
	batch, rejected, err := splitListings(venue, category, listings, atNs)
	rep.Rejected = rejected
	for _, item := range batch {
		if item.p != nil {
			continue
		}
		logReject(log, venue, category, item.raw, item.err)
	}
	if err != nil {
		return rep, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, feedKey(venue, category)); err != nil {
		return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
	}
	active, err := loadActive(ctx, tx, venue, category)
	if err != nil {
		return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
	}
	byTicker := map[string]*activeRow{}
	byExch := map[string]*activeRow{}
	for i := range active {
		row := &active[i]
		byTicker[row.ticker] = row
		byExch[row.exch] = row
	}

	seen := map[uint32]bool{}
	var updates []prepared
	var inserts []prepared
	type terminalMove struct {
		uid    uint32
		status Status
	}
	var moves []terminalMove
	occupied := map[string]string{}
	claim := func(ticker, exch string) error {
		if prev, ok := occupied[exch]; ok && prev != ticker {
			return fmt.Errorf("%w: %s is %s and %s", errDuplicate, exch, prev, ticker)
		}
		occupied[exch] = ticker
		return nil
	}

	// Valid rows decide inserts, updates and moves. Rejected rows are applied
	// after that, and only protect a row the valid set did not already name:
	// a bad copy of a ticker must not cancel that ticker's expiry or update.
	for _, item := range batch {
		if item.p == nil {
			continue
		}
		p := *item.p
		row := byTicker[p.ticker]
		if p.status.Terminal() {
			if row == nil {
				// Already terminal, or never stored. A still-expired contract
				// must not be inserted again on every refresh.
				continue
			}
			seen[row.uid] = true
			moves = append(moves, terminalMove{uid: row.uid, status: p.status})
			continue
		}
		if row == nil {
			inserts = append(inserts, p)
			if err := claim(p.ticker, p.exch); err != nil {
				return rep, err
			}
			continue
		}
		seen[row.uid] = true
		if err := claim(p.ticker, p.exch); err != nil {
			return rep, err
		}
		if !sameContent(*row, p) {
			updates = append(updates, p)
		}
	}
	for _, item := range batch {
		if item.p != nil {
			continue
		}
		row := matchRejected(venue, category, item.raw, byTicker, byExch)
		if row == nil || seen[row.uid] {
			continue
		}
		seen[row.uid] = true
		if err := claim(row.ticker, row.exch); err != nil {
			return rep, err
		}
	}
	for i := range active {
		row := &active[i]
		if seen[row.uid] {
			continue
		}
		status := Delisted
		if rowExpired(category, row.expiry, atNs) {
			status = Expired
		}
		moves = append(moves, terminalMove{uid: row.uid, status: status})
	}

	// Moves first so a surviving row can take the exchange spelling a moved
	// row held. A unique violation still aborts the transaction: nothing is
	// committed, so a partial unique clash does not delist anyone.
	for _, m := range moves {
		if err := moveTx(ctx, tx, m.uid, m.status, atNs); err != nil {
			return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
		}
	}
	for _, p := range updates {
		row := byTicker[p.ticker]
		if err := updateActive(ctx, tx, row.uid, p, atNs); err != nil {
			return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
		}
		if !sameFilters(row.filters, p.filters) {
			if err := replaceFilters(ctx, tx, row.uid, p.filters); err != nil {
				return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
			}
		}
	}
	for _, p := range inserts {
		if _, err := s.insertPrepared(ctx, tx, p, atNs); err != nil {
			return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
		}
	}

	changed := len(updates)+len(inserts)+len(moves) > 0
	if changed {
		if err := bumpUnit(ctx, tx, venue, category, atNs); err != nil {
			return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
		}
	}
	rev, err := readRevision(ctx, tx, venue, category)
	if err != nil {
		return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return rep, fmt.Errorf("sym: refresh %s %s: %w", venue, category, err)
	}

	rep.Updated = len(updates)
	rep.Inserted = len(inserts)
	for _, m := range moves {
		if m.status == Expired {
			rep.Expired++
		} else {
			rep.Delisted++
		}
	}
	rep.Changed = changed
	rep.Revision = rev
	return rep, nil
}

// splitListings keeps the listings that can be stored and counts the rest.
// A repeated universal ticker in the valid set is errDuplicate; the caller
// writes nothing. Rejected listings stay in the slice with a nil prepared so
// the caller can still protect the active row they name.
func splitListings(venue string, category symbol.Category, listings []symbol.Listing, atNs int64) ([]incoming, int, error) {
	out := make([]incoming, 0, len(listings))
	rejected := 0
	seen := map[string]struct{}{}
	for _, l := range listings {
		p, err := prepareListing(venue, category, l, atNs)
		if err != nil {
			rejected++
			out = append(out, incoming{raw: l, err: err})
			continue
		}
		if _, ok := seen[p.ticker]; ok {
			return out, rejected, fmt.Errorf("%w: %s", errDuplicate, p.ticker)
		}
		seen[p.ticker] = struct{}{}
		out = append(out, incoming{raw: l, p: &p})
	}
	return out, rejected, nil
}

func prepareListing(venue string, category symbol.Category, l symbol.Listing, atNs int64) (prepared, error) {
	if l.Venue != venue || l.Category != category {
		return prepared{}, fmt.Errorf("sym: listing %s %s is not %s %s", l.Venue, l.Category, venue, category)
	}
	if strings.TrimSpace(l.ExchTicker) == "" {
		return prepared{}, errors.New("sym: exch ticker is empty")
	}
	// D203: uppercase and apply the alias table before quanto and the ticker.
	// Validate requires the canonical spelling, so this runs first. exch_ticker
	// stays in the venue's own spelling.
	if err := normalizeAssets(&l); err != nil {
		return prepared{}, err
	}
	if err := l.Validate(); err != nil {
		return prepared{}, err
	}
	ticker, err := l.Ticker()
	if err != nil {
		return prepared{}, err
	}
	size, err := numberText(l.ContractSize, true)
	if err != nil {
		return prepared{}, err
	}
	strike, err := numberText(l.Strike, true)
	if err != nil {
		return prepared{}, err
	}
	filters, err := canonicalFilters(l.Filters)
	if err != nil {
		return prepared{}, err
	}
	valueCcy := l.ContractValueCcy
	priceCcy := l.PriceCcy
	// The columns are NOT NULL. Spot may leave them empty and still pass
	// Validate; D203's spot row is base for qty and quote for price. Base and
	// quote are already canonical.
	if category == symbol.Spot {
		if valueCcy == "" {
			valueCcy = l.Base
		}
		if priceCcy == "" {
			priceCcy = l.Quote
		}
	}
	if valueCcy == "" || priceCcy == "" {
		return prepared{}, fmt.Errorf("%w: currency", symbol.ErrInvalidListing)
	}
	var expiry *int64
	if l.ExpiryNs != 0 {
		expiry = &l.ExpiryNs
	}
	var opt *string
	if t := strings.ToUpper(strings.TrimSpace(l.OptionType)); t != "" {
		opt = &t
	}
	return prepared{
		ticker:           ticker.String(),
		exch:             l.ExchTicker,
		base:             l.Base,
		quote:            l.Quote,
		contractSize:     strPtr(size),
		contractValueCcy: valueCcy,
		priceCcy:         priceCcy,
		settle:           strPtr(l.Settle),
		expiry:           expiry,
		strike:           strPtr(strike),
		optionType:       opt,
		status:           listingStatus(l, atNs),
		filters:          filters,
	}, nil
}

// listingStatus stores the listing's status. A dated contract at or past
// expiry_ns is EXPIRED even when that status is still non-terminal: the venue
// often keeps the row in the listing after expiry.
func listingStatus(l symbol.Listing, atNs int64) Status {
	if rowExpired(l.Category, &l.ExpiryNs, atNs) && l.ExpiryNs > 0 {
		return Expired
	}
	return l.Status
}

// normalizeAssets uppercases asset codes and applies the alias table (D203).
// Empty currency fields stay empty: spot may omit them. exch_ticker is not an
// asset code and is left alone.
func normalizeAssets(l *symbol.Listing) error {
	var err error
	if l.Base, err = symbol.CanonicalAsset(l.Base); err != nil {
		return err
	}
	if l.Quote, err = symbol.CanonicalAsset(l.Quote); err != nil {
		return err
	}
	if l.ContractValueCcy, err = canonicalAssetOrEmpty(l.ContractValueCcy); err != nil {
		return err
	}
	if l.PriceCcy, err = canonicalAssetOrEmpty(l.PriceCcy); err != nil {
		return err
	}
	if l.Settle, err = canonicalAssetOrEmpty(l.Settle); err != nil {
		return err
	}
	return nil
}

func canonicalAssetOrEmpty(code string) (string, error) {
	if strings.TrimSpace(code) == "" {
		return "", nil
	}
	return symbol.CanonicalAsset(code)
}

func rowExpired(category symbol.Category, expiry *int64, atNs int64) bool {
	if category != symbol.Future && category != symbol.Option {
		return false
	}
	return expiry != nil && *expiry > 0 && *expiry <= atNs
}

func canonicalFilters(in symbol.Filters) ([]Filter, error) {
	names := make([]string, 0, len(in))
	for name := range in {
		names = append(names, name)
	}
	slices.Sort(names)
	out := make([]Filter, 0, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("sym: filter name is empty")
		}
		c, err := numberText(in[name], false)
		if err != nil {
			return nil, err
		}
		out = append(out, Filter{Name: name, Value: strPtr(c)})
	}
	return out, nil
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// matchRejected ties a rejected payload to an active row of this same unit.
// A listing for another venue or category does not protect anything here.
// The universal ticker wins when it parses; otherwise the exchange spelling does,
// so a payload we cannot name still does not look like absence.
func matchRejected(venue string, category symbol.Category, l symbol.Listing, byTicker, byExch map[string]*activeRow) *activeRow {
	if l.Venue != venue || l.Category != category {
		return nil
	}
	if t, err := l.Ticker(); err == nil {
		if row := byTicker[t.String()]; row != nil {
			return row
		}
	}
	if l.ExchTicker == "" {
		return nil
	}
	return byExch[l.ExchTicker]
}

func logReject(log zerolog.Logger, venue string, category symbol.Category, l symbol.Listing, err error) {
	log.Warn().
		Str("venue", venue).
		Str("category", string(category)).
		Str("exch_ticker", l.ExchTicker).
		Str("listing_venue", l.Venue).
		Str("listing_category", string(l.Category)).
		Err(err).
		Msg("listing rejected")
}

func loadActive(ctx context.Context, tx pgx.Tx, venue string, category symbol.Category) ([]activeRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT uid, universal_ticker, exch_ticker, base, quote,
		       trim_scale(contract_size)::text, contract_value_ccy, price_ccy, settle,
		       expiry_ns, trim_scale(strike)::text, option_type, status,
		       created_at_ns, updated_at_ns
		FROM symbol_active
		WHERE venue = $1 AND category = $2
		FOR UPDATE`, venue, string(category))
	if err != nil {
		return nil, err
	}
	out, err := scanActive(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return out, nil
	}
	frows, err := tx.Query(ctx, `
		SELECT f.uid, f.name, trim_scale(f.value)::text
		FROM symbol_filter_active f
		JOIN symbol_active a ON a.uid = f.uid
		WHERE a.venue = $1 AND a.category = $2
		ORDER BY f.uid, f.name`, venue, string(category))
	if err != nil {
		return nil, err
	}
	defer frows.Close()
	byUID := map[uint32][]Filter{}
	for frows.Next() {
		var uid int64
		var f Filter
		if err := frows.Scan(&uid, &f.Name, &f.Value); err != nil {
			return nil, err
		}
		byUID[uint32(uid)] = append(byUID[uint32(uid)], f)
	}
	if err := frows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		out[i].filters = byUID[out[i].uid]
	}
	return out, nil
}

func scanActive(rows pgx.Rows) ([]activeRow, error) {
	var out []activeRow
	for rows.Next() {
		var row activeRow
		var uid int64
		var status string
		if err := rows.Scan(
			&uid, &row.ticker, &row.exch, &row.base, &row.quote,
			&row.contractSize, &row.contractValueCcy, &row.priceCcy, &row.settle,
			&row.expiry, &row.strike, &row.optionType, &status,
			&row.createdAt, &row.updatedAt,
		); err != nil {
			return nil, err
		}
		if uid < int64(symbol.MinUID) || uid > int64(symbol.MaxUID) {
			return nil, fmt.Errorf("%w: stored uid %d", ErrUID, uid)
		}
		row.uid = uint32(uid)
		row.status = Status(status)
		out = append(out, row)
	}
	return out, rows.Err()
}

func updateActive(ctx context.Context, tx pgx.Tx, uid uint32, p prepared, atNs int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE symbol_active SET
		    exch_ticker = $2,
		    base = $3,
		    quote = $4,
		    contract_size = $5::numeric,
		    contract_value_ccy = $6,
		    price_ccy = $7,
		    settle = $8,
		    expiry_ns = $9,
		    strike = $10::numeric,
		    option_type = $11,
		    status = $12,
		    updated_at_ns = $13
		WHERE uid = $1`,
		uid, p.exch, p.base, p.quote, p.contractSize, p.contractValueCcy, p.priceCcy,
		p.settle, p.expiry, p.strike, p.optionType, string(p.status), atNs,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("sym: update uid %d", uid)
	}
	return nil
}

func replaceFilters(ctx context.Context, tx pgx.Tx, uid uint32, filters []Filter) error {
	if _, err := tx.Exec(ctx, `DELETE FROM symbol_filter_active WHERE uid = $1`, uid); err != nil {
		return err
	}
	return insertFilters(ctx, tx, "symbol_filter_active", uid, filters)
}

func (s *Store) insertPrepared(ctx context.Context, tx pgx.Tx, p prepared, atNs int64) (uint32, error) {
	if !p.status.Active() {
		return 0, fmt.Errorf("%w: %s is not an active status", ErrStatus, p.status)
	}
	var last error
	outside := 0
	for draws := 0; draws < maxUIDDraws; {
		uid, err := s.src.NextUID()
		if err != nil {
			return 0, err
		}
		if !symbol.ValidUID(uid) {
			outside++
			if outside > maxUIDDraws {
				break
			}
			continue
		}
		draws++
		if _, err := tx.Exec(ctx, `SAVEPOINT sym_uid`); err != nil {
			return 0, err
		}
		err = insertPreparedRow(ctx, tx, uid, p, atNs)
		if err == nil {
			if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT sym_uid`); err != nil {
				return 0, err
			}
			return uid, nil
		}
		if _, rbErr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT sym_uid`); rbErr != nil {
			return 0, rbErr
		}
		if _, rbErr := tx.Exec(ctx, `RELEASE SAVEPOINT sym_uid`); rbErr != nil {
			return 0, rbErr
		}
		if isUIDCollision(err) {
			last = err
			continue
		}
		return 0, err
	}
	if last == nil {
		return 0, fmt.Errorf("%w: draws were outside %d..%d", ErrUID, symbol.MinUID, symbol.MaxUID)
	}
	return 0, fmt.Errorf("%w: %v", ErrUID, last)
}

func insertPreparedRow(ctx context.Context, tx pgx.Tx, uid uint32, p prepared, atNs int64) error {
	if _, err := tx.Exec(ctx, `INSERT INTO symbol_uid (uid) VALUES ($1)`, uid); err != nil {
		return err
	}
	if err := insertListing(ctx, tx, "symbol_active", uid, p.status, p.listingRow(atNs), atNs); err != nil {
		return err
	}
	return insertFilters(ctx, tx, "symbol_filter_active", uid, p.filters)
}

func bumpUnit(ctx context.Context, tx pgx.Tx, venue string, category symbol.Category, atNs int64) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO symbol_revision (venue, category, revision, refreshed_at_ns)
		VALUES ($1, $2, 1, $3)
		ON CONFLICT (venue, category) DO UPDATE
		SET revision = symbol_revision.revision + 1,
		    refreshed_at_ns = EXCLUDED.refreshed_at_ns`,
		venue, string(category), atNs)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("sym: revision for %s %s was not written", venue, category)
	}
	return nil
}

func readRevision(ctx context.Context, tx pgx.Tx, venue string, category symbol.Category) (int64, error) {
	var rev int64
	err := tx.QueryRow(ctx, `
		SELECT revision FROM symbol_revision
		WHERE venue = $1 AND category = $2`, venue, string(category)).Scan(&rev)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return rev, err
}

func sameContent(row activeRow, p prepared) bool {
	return row.exch == p.exch &&
		row.base == p.base &&
		row.quote == p.quote &&
		row.contractValueCcy == p.contractValueCcy &&
		row.priceCcy == p.priceCcy &&
		row.status == p.status &&
		samePtr(row.contractSize, p.contractSize) &&
		samePtr(row.settle, p.settle) &&
		samePtr(row.strike, p.strike) &&
		samePtr(row.optionType, p.optionType) &&
		sameInt(row.expiry, p.expiry) &&
		sameFilters(row.filters, p.filters)
}

func sameFilters(a, b []Filter) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || !samePtr(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

func samePtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameInt(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
