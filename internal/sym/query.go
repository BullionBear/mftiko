package sym

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
)

// GetByTicker reads one instrument by its universal ticker.
//
// The active table is the default. includeInactive also reads the terminal
// table, for a historical lookup. When the ticker is on both tables the active
// row is the one returned: that is the listing now. A terminal row is returned
// only when the active table has no such ticker. Several terminal rows and no
// active row yield the greatest updated_at_ns, then the greatest uid.
func (s *Store) GetByTicker(ctx context.Context, ticker string, includeInactive bool) (symbol.Info, error) {
	info, err := s.queryOne(ctx, listingSelect("symbol_active", "universal_ticker = $1", 0), ticker)
	if err == nil || !errors.Is(err, ErrNotFound) || !includeInactive {
		return info, err
	}
	q := listingSelect("symbol_terminal", "universal_ticker = $1", 1) + `
		ORDER BY updated_at_ns DESC, uid DESC
		LIMIT 1`
	return s.queryOne(ctx, q, ticker)
}

// GetByUID reads one instrument by the uid SYM stored on the row.
// A uid outside symbol.MinUID..symbol.MaxUID is ErrNotFound: 0 is not a UID.
// includeInactive has the same meaning as on GetByTicker. A uid is on at most
// one of the two tables.
func (s *Store) GetByUID(ctx context.Context, uid uint32, includeInactive bool) (symbol.Info, error) {
	if !symbol.ValidUID(uid) {
		return symbol.Info{}, ErrNotFound
	}
	info, err := s.queryOne(ctx, listingSelect("symbol_active", "uid = $1", 0), int64(uid))
	if err == nil || !errors.Is(err, ErrNotFound) || !includeInactive {
		return info, err
	}
	return s.queryOne(ctx, listingSelect("symbol_terminal", "uid = $1", 1), int64(uid))
}

// Reverse reads the listing for a venue spelling.
//
// The active table is the default. includeInactive falls through to the
// terminal table when the active table has no match, and that row's uid is the
// one assigned when it was created. When both tables match, the active row
// wins: a delist followed by a relist leaves both. Several terminal rows and
// no active row yield the greatest updated_at_ns, then the greatest uid.
// Category is part of the key: one spelling is not one instrument on a
// unified-account venue.
func (s *Store) Reverse(ctx context.Context, key symbol.ExchKey, includeInactive bool) (symbol.Info, error) {
	if key.Venue == "" || key.ExchTicker == "" || !knownCategory(key.Category) {
		return symbol.Info{}, fmt.Errorf("sym: reverse key")
	}
	args := []any{key.Venue, string(key.Category), key.ExchTicker}
	info, err := s.queryOne(ctx, listingSelect("symbol_active", "venue = $1 AND category = $2 AND exch_ticker = $3", 0), args...)
	if err == nil || !errors.Is(err, ErrNotFound) || !includeInactive {
		return info, err
	}
	q := listingSelect("symbol_terminal", "venue = $1 AND category = $2 AND exch_ticker = $3", 1) + `
		ORDER BY updated_at_ns DESC, uid DESC
		LIMIT 1`
	return s.queryOne(ctx, q, args...)
}

// TableRevision is one (venue, category) counter. Revision is the value
// committed with the last change of that table. A table that has never
// changed has no row; callers treat that as revision 0.
type TableRevision struct {
	Venue    string
	Category symbol.Category
	Revision int64
}

// Revisions reads every symbol_revision row, ordered by venue then category.
func (s *Store) Revisions(ctx context.Context) ([]TableRevision, error) {
	rows, err := s.db.Query(ctx, `
		SELECT venue, category, revision
		FROM symbol_revision
		ORDER BY venue, category`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TableRevision
	for rows.Next() {
		var row TableRevision
		var cat string
		if err := rows.Scan(&row.Venue, &cat, &row.Revision); err != nil {
			return nil, err
		}
		row.Category = symbol.Category(cat)
		if row.Revision < 0 {
			return nil, fmt.Errorf("sym: revision %d for %s %s", row.Revision, row.Venue, cat)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []TableRevision{}
	}
	return out, nil
}

// List reads one page of a (venue, category).
//
// limit must be in 1..symbol.ListPage. offset is the number of rows to skip.
// Rows are ordered by universal ticker, then uid, so a page is stable while
// the tables are. includeInactive appends the terminal table; the two tables
// are one sequence in that order. The total is the number of rows the filter
// matches, not the length of this page. A row that moves between pages can be
// skipped or repeated; that is accepted.
func (s *Store) List(ctx context.Context, venue string, category symbol.Category, offset int64, limit int, includeInactive bool) ([]symbol.Info, uint32, error) {
	if venue == "" || !knownCategory(category) || offset < 0 || limit < 1 || limit > symbol.ListPage {
		return nil, 0, fmt.Errorf("sym: list %s %s offset %d limit %d", venue, category, offset, limit)
	}
	var total int64
	if err := s.db.QueryRow(ctx, countSQL(includeInactive), venue, string(category)).Scan(&total); err != nil {
		return nil, 0, err
	}
	if total < 0 || total > math.MaxUint32 {
		return nil, 0, fmt.Errorf("sym: list count %d", total)
	}
	if total == 0 || offset >= total {
		return []symbol.Info{}, uint32(total), nil
	}
	rows, err := s.db.Query(ctx, pageSQL(includeInactive), venue, string(category), limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var scanned []stored
	for rows.Next() {
		var sc stored
		if err := sc.scan(rows); err != nil {
			return nil, 0, err
		}
		scanned = append(scanned, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	infos, err := s.attachFilters(ctx, scanned)
	if err != nil {
		return nil, 0, err
	}
	return infos, uint32(total), nil
}

func countSQL(includeInactive bool) string {
	if !includeInactive {
		return `SELECT count(*) FROM symbol_active WHERE venue = $1 AND category = $2`
	}
	return `SELECT
		(SELECT count(*) FROM symbol_active WHERE venue = $1 AND category = $2) +
		(SELECT count(*) FROM symbol_terminal WHERE venue = $1 AND category = $2)`
}

func pageSQL(includeInactive bool) string {
	active := listingSelect("symbol_active", "venue = $1 AND category = $2", 0)
	if !includeInactive {
		return active + `
			ORDER BY universal_ticker, uid
			LIMIT $3 OFFSET $4`
	}
	terminal := listingSelect("symbol_terminal", "venue = $1 AND category = $2", 1)
	return `SELECT uid, universal_ticker, exch_ticker, base, quote,
			contract_size, contract_value_ccy, price_ccy, settle,
			expiry_ns, strike, option_type, status, updated_at_ns, src
		FROM (
			` + active + `
			UNION ALL
			` + terminal + `
		) AS listed
		ORDER BY universal_ticker, uid
		LIMIT $3 OFFSET $4`
}

// listingSelect is one table's column list. table is a known listing table and
// where is a fixed clause written in this file; neither is caller text.
// src is 0 for the active table and 1 for the terminal table.
func listingSelect(table, where string, src int) string {
	if err := knownListingTable(table); err != nil {
		panic(err)
	}
	if src != 0 && src != 1 {
		panic("sym: listing src")
	}
	return fmt.Sprintf(`
		SELECT uid, universal_ticker, exch_ticker, base, quote,
		       contract_size::text AS contract_size, contract_value_ccy, price_ccy, settle,
		       expiry_ns, strike::text AS strike, option_type, status, updated_at_ns,
		       %d AS src
		FROM %s
		WHERE %s`, src, table, where)
}

type stored struct {
	uid              uint32
	universalTicker  string
	exchTicker       string
	base             string
	quote            string
	contractSize     *string
	contractValueCcy string
	priceCcy         string
	settle           *string
	expiryNs         *int64
	strike           *string
	optionType       *string
	status           string
	updatedAtNs      int64
	terminal         bool
	filters          []Filter
}

func (sc *stored) scan(row pgx.Row) error {
	var (
		uid int64
		src int
	)
	err := row.Scan(
		&uid,
		&sc.universalTicker,
		&sc.exchTicker,
		&sc.base,
		&sc.quote,
		&sc.contractSize,
		&sc.contractValueCcy,
		&sc.priceCcy,
		&sc.settle,
		&sc.expiryNs,
		&sc.strike,
		&sc.optionType,
		&sc.status,
		&sc.updatedAtNs,
		&src,
	)
	if err != nil {
		return err
	}
	if uid < int64(symbol.MinUID) || uid > int64(symbol.MaxUID) {
		return fmt.Errorf("sym: stored uid %d", uid)
	}
	if src != 0 && src != 1 {
		return fmt.Errorf("sym: stored table %d", src)
	}
	sc.uid = uint32(uid)
	sc.terminal = src == 1
	return nil
}

func (sc stored) info() (symbol.Info, error) {
	t, err := symbol.Parse(sc.universalTicker)
	if err != nil {
		return symbol.Info{}, fmt.Errorf("sym: stored ticker %q: %w", sc.universalTicker, err)
	}
	st := Status(sc.status)
	if !st.Valid() {
		return symbol.Info{}, fmt.Errorf("sym: stored status %q", sc.status)
	}
	info := symbol.Info{
		UID:              sc.uid,
		Ticker:           t,
		ExchTicker:       sc.exchTicker,
		Base:             sc.base,
		Quote:            sc.quote,
		ContractValueCcy: sc.contractValueCcy,
		PriceCcy:         sc.priceCcy,
		Status:           st,
		UpdatedAtNs:      sc.updatedAtNs,
	}
	if sc.contractSize != nil {
		if info.ContractSize, err = storedNumber("contract_size", *sc.contractSize); err != nil {
			return symbol.Info{}, err
		}
	}
	if sc.settle != nil {
		info.Settle = *sc.settle
	}
	if sc.expiryNs != nil {
		info.ExpiryNs = *sc.expiryNs
	}
	if sc.strike != nil {
		if info.Strike, err = storedNumber("strike", *sc.strike); err != nil {
			return symbol.Info{}, err
		}
	}
	if sc.optionType != nil {
		info.OptionType = *sc.optionType
	}
	if len(sc.filters) > 0 {
		info.Filters = make(symbol.Filters, len(sc.filters))
		for _, f := range sc.filters {
			v := decimal.Absent() // a published restriction with no bound
			if f.Value != nil {
				if v, err = storedNumber("filter "+f.Name, *f.Value); err != nil {
					return symbol.Info{}, err
				}
			}
			info.Filters[f.Name] = v
		}
	}
	return info, nil
}

// storedNumber reads a NUMERIC column's text as the float64 every plane uses (D008).
func storedNumber(column, text string) (float64, error) {
	v, err := decimal.Parse(text)
	if err != nil {
		return 0, fmt.Errorf("sym: stored %s %q: %w", column, text, err)
	}
	return v, nil
}

func (s *Store) queryOne(ctx context.Context, q string, args ...any) (symbol.Info, error) {
	var sc stored
	err := sc.scan(s.db.QueryRow(ctx, q, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return symbol.Info{}, ErrNotFound
	}
	if err != nil {
		return symbol.Info{}, err
	}
	infos, err := s.attachFilters(ctx, []stored{sc})
	if err != nil {
		return symbol.Info{}, err
	}
	return infos[0], nil
}

func (s *Store) attachFilters(ctx context.Context, rows []stored) ([]symbol.Info, error) {
	var active, terminal []int64
	for i := range rows {
		id := int64(rows[i].uid)
		if rows[i].terminal {
			terminal = append(terminal, id)
			continue
		}
		active = append(active, id)
	}
	af, err := s.readFilterSet(ctx, "symbol_filter_active", active)
	if err != nil {
		return nil, err
	}
	tf, err := s.readFilterSet(ctx, "symbol_filter_terminal", terminal)
	if err != nil {
		return nil, err
	}
	out := make([]symbol.Info, 0, len(rows))
	for i := range rows {
		f := af[rows[i].uid]
		if rows[i].terminal {
			f = tf[rows[i].uid]
		}
		rows[i].filters = f
		info, err := rows[i].info()
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

func (s *Store) readFilterSet(ctx context.Context, table string, uids []int64) (map[uint32][]Filter, error) {
	if len(uids) == 0 {
		return nil, nil
	}
	if err := knownFilterTable(table); err != nil {
		return nil, err
	}
	q := fmt.Sprintf(`SELECT uid, name, value::text FROM %s WHERE uid = ANY($1) ORDER BY uid, name`, table)
	rows, err := s.db.Query(ctx, q, uids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uint32][]Filter{}
	for rows.Next() {
		var (
			uid   int64
			f     Filter
			value *string
		)
		if err := rows.Scan(&uid, &f.Name, &value); err != nil {
			return nil, err
		}
		if uid < int64(symbol.MinUID) || uid > int64(symbol.MaxUID) {
			return nil, fmt.Errorf("sym: filter uid %d", uid)
		}
		f.Value = value
		id := uint32(uid)
		out[id] = append(out[id], f)
	}
	return out, rows.Err()
}
