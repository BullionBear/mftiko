package sym

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// Status is the provisional listing enum (D203). The contract still carries a
// bool; this is the column the tables check.
type Status string

const (
	Prelisting Status = "PRELISTING"
	Tradable   Status = "TRADABLE"
	Halted     Status = "HALTED"
	Settling   Status = "SETTLING"
	Unknown    Status = "UNKNOWN"
	Expired    Status = "EXPIRED"
	Delisted   Status = "DELISTED"
)

// Active reports whether the status stays on symbol_active.
func (s Status) Active() bool {
	switch s {
	case Prelisting, Tradable, Halted, Settling, Unknown:
		return true
	default:
		return false
	}
}

// Terminal reports whether the status belongs on symbol_terminal.
func (s Status) Terminal() bool { return s == Expired || s == Delisted }

// Filter is one trading restriction. A nil Value means the venue publishes the
// restriction and sets no bound, which is not the same as omitting the filter.
type Filter struct {
	Name  string
	Value *string
}

// Listing is one instrument to put on the active table. Insert allocates the UID.
// Decimal fields are base-10 text. A nil pointer is SQL NULL.
type Listing struct {
	UniversalTicker  string
	ExchTicker       string
	Base             string
	Quote            string
	ContractSize     *string
	ContractValueCcy string
	PriceCcy         string
	Settle           *string
	ExpiryNs         *int64
	Strike           *string
	OptionType       string // C, P, or "" (stored as NULL)
	Status           Status
	CreatedAtNs      int64
	UpdatedAtNs      int64
	Filters          []Filter
}

// ErrStatus is returned when a status is not legal for the table being written.
var ErrStatus = errors.New("sym: status")

// ErrNotFound is returned when Move does not find the uid on the active table.
var ErrNotFound = errors.New("sym: not found")

// ErrUID is returned when Insert cannot draw a uid that the tables will accept.
var ErrUID = errors.New("sym: uid")

// Store writes the symbol tables. It is the only writer.
type Store struct {
	db  *pgxpool.Pool
	src Source
}

// Open connects to the database. A nil source uses RandomSource.
func Open(ctx context.Context, dsn string, src Source) (*Store, error) {
	if src == nil {
		src = RandomSource()
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("sym: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("sym: connect: %w", err)
	}
	return &Store{db: pool, src: src}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.db.Close() }

// Insert puts a listing on the active table and allocates its uid.
//
// A candidate symbol.ValidUID rejects is discarded. Allocation inserts the uid
// into symbol_uid first. A unique violation on that registry (or on a listing
// primary key, or from symbol_reject_shared_uid) is discarded and another uid
// is drawn. A collision on the ticker or the exchange spelling is returned
// as-is: that is not a uid collision, and retrying would burn draws. The
// symbol_uid row is never deleted, so a uid stays taken after the listing is
// archived.
func (s *Store) Insert(ctx context.Context, l Listing) (uint32, error) {
	if !l.Status.Active() {
		return 0, fmt.Errorf("%w: %s is not an active status", ErrStatus, l.Status)
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
		err = s.insertActive(ctx, uid, l)
		if err == nil {
			return uid, nil
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

// Move carries the active row and its filters onto the terminal table in one
// transaction. The uid and the UniversalTicker do not change. The old active
// row is deleted in that same transaction, so the uid is never on both tables.
// status must be EXPIRED or DELISTED. atNs is the terminal row's updated_at_ns
// and the revision's refreshed_at_ns.
func (s *Store) Move(ctx context.Context, uid uint32, status Status, atNs int64) error {
	if !status.Terminal() {
		return fmt.Errorf("%w: %s is not a terminal status", ErrStatus, status)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("sym: move: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row, err := lockActive(ctx, tx, uid)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("sym: move: %w", err)
	}
	filters, err := readFilters(ctx, tx, "symbol_filter_active", uid)
	if err != nil {
		return fmt.Errorf("sym: move: %w", err)
	}
	// Delete before the terminal insert. symbol_reject_shared_uid refuses a uid
	// that is still on the other table; the registry row stays, so it does not
	// itself make the move legal. The delete rolls back with the transaction
	// if the insert fails, so a failed move leaves the active row in place.
	tag, err := tx.Exec(ctx, `DELETE FROM symbol_active WHERE uid = $1`, uid)
	if err != nil {
		return fmt.Errorf("sym: move: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	if err := insertListing(ctx, tx, "symbol_terminal", uid, status, row, atNs); err != nil {
		return fmt.Errorf("sym: move: %w", err)
	}
	if err := insertFilters(ctx, tx, "symbol_filter_terminal", uid, filters); err != nil {
		return fmt.Errorf("sym: move: %w", err)
	}
	if err := bumpRevision(ctx, tx, "symbol_terminal", uid, atNs); err != nil {
		return fmt.Errorf("sym: move: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("sym: move: %w", err)
	}
	return nil
}

func (s *Store) insertActive(ctx context.Context, uid uint32, l Listing) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := listingRow{
		UniversalTicker:  l.UniversalTicker,
		ExchTicker:       l.ExchTicker,
		Base:             l.Base,
		Quote:            l.Quote,
		ContractSize:     l.ContractSize,
		ContractValueCcy: l.ContractValueCcy,
		PriceCcy:         l.PriceCcy,
		Settle:           l.Settle,
		ExpiryNs:         l.ExpiryNs,
		Strike:           l.Strike,
		CreatedAtNs:      l.CreatedAtNs,
	}
	if l.OptionType != "" {
		row.OptionType = &l.OptionType
	}
	// The registry insert is the allocation. A listing row only references it.
	if _, err := tx.Exec(ctx, `INSERT INTO symbol_uid (uid) VALUES ($1)`, uid); err != nil {
		return err
	}
	if err := insertListing(ctx, tx, "symbol_active", uid, l.Status, row, l.UpdatedAtNs); err != nil {
		return err
	}
	if err := insertFilters(ctx, tx, "symbol_filter_active", uid, l.Filters); err != nil {
		return err
	}
	if err := bumpRevision(ctx, tx, "symbol_active", uid, l.UpdatedAtNs); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type listingRow struct {
	UniversalTicker  string
	ExchTicker       string
	Base             string
	Quote            string
	ContractSize     *string
	ContractValueCcy string
	PriceCcy         string
	Settle           *string
	ExpiryNs         *int64
	Strike           *string
	OptionType       *string
	CreatedAtNs      int64
}

func lockActive(ctx context.Context, tx pgx.Tx, uid uint32) (listingRow, error) {
	var row listingRow
	err := tx.QueryRow(ctx, `
		SELECT universal_ticker, exch_ticker, base, quote,
		       contract_size::text, contract_value_ccy, price_ccy, settle,
		       expiry_ns, strike::text, option_type, created_at_ns
		FROM symbol_active
		WHERE uid = $1
		FOR UPDATE`, uid).Scan(
		&row.UniversalTicker, &row.ExchTicker, &row.Base, &row.Quote,
		&row.ContractSize, &row.ContractValueCcy, &row.PriceCcy, &row.Settle,
		&row.ExpiryNs, &row.Strike, &row.OptionType, &row.CreatedAtNs,
	)
	return row, err
}

func insertListing(ctx context.Context, tx pgx.Tx, table string, uid uint32, status Status, row listingRow, updatedAtNs int64) error {
	if err := knownListingTable(table); err != nil {
		return err
	}
	q := fmt.Sprintf(`
		INSERT INTO %s (
		    uid, universal_ticker, exch_ticker, base, quote,
		    contract_size, contract_value_ccy, price_ccy, settle,
		    expiry_ns, strike, option_type, status,
		    created_at_ns, updated_at_ns
		) VALUES (
		    $1, $2, $3, $4, $5,
		    $6::numeric, $7, $8, $9,
		    $10, $11::numeric, $12, $13,
		    $14, $15
		)`, table)
	_, err := tx.Exec(ctx, q,
		uid, row.UniversalTicker, row.ExchTicker, row.Base, row.Quote,
		row.ContractSize, row.ContractValueCcy, row.PriceCcy, row.Settle,
		row.ExpiryNs, row.Strike, row.OptionType, string(status),
		row.CreatedAtNs, updatedAtNs,
	)
	return err
}

func insertFilters(ctx context.Context, tx pgx.Tx, table string, uid uint32, filters []Filter) error {
	if err := knownFilterTable(table); err != nil {
		return err
	}
	q := fmt.Sprintf(`INSERT INTO %s (uid, name, value) VALUES ($1, $2, $3::numeric)`, table)
	for _, f := range filters {
		if _, err := tx.Exec(ctx, q, uid, f.Name, f.Value); err != nil {
			return err
		}
	}
	return nil
}

func readFilters(ctx context.Context, tx pgx.Tx, table string, uid uint32) ([]Filter, error) {
	if err := knownFilterTable(table); err != nil {
		return nil, err
	}
	q := fmt.Sprintf(`SELECT name, value::text FROM %s WHERE uid = $1 ORDER BY name`, table)
	rows, err := tx.Query(ctx, q, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Filter
	for rows.Next() {
		var f Filter
		if err := rows.Scan(&f.Name, &f.Value); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func bumpRevision(ctx context.Context, tx pgx.Tx, table string, uid uint32, atNs int64) error {
	if err := knownListingTable(table); err != nil {
		return err
	}
	// venue and category come from the generated columns, so the revision key
	// cannot disagree with the ticker.
	q := fmt.Sprintf(`
		INSERT INTO symbol_revision (venue, category, revision, refreshed_at_ns)
		SELECT venue, category, 1, $2 FROM %s WHERE uid = $1
		ON CONFLICT (venue, category) DO UPDATE
		SET revision = symbol_revision.revision + 1,
		    refreshed_at_ns = EXCLUDED.refreshed_at_ns`, table)
	tag, err := tx.Exec(ctx, q, uid, atNs)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("sym: revision for uid %d was not written", uid)
	}
	return nil
}

func knownListingTable(table string) error {
	switch table {
	case "symbol_active", "symbol_terminal":
		return nil
	default:
		return fmt.Errorf("sym: unknown listing table %q", table)
	}
}

func knownFilterTable(table string) error {
	switch table {
	case "symbol_filter_active", "symbol_filter_terminal":
		return nil
	default:
		return fmt.Errorf("sym: unknown filter table %q", table)
	}
}

// isUIDCollision reports a unique violation on a uid. A unique violation on the
// ticker or the exchange spelling is a different failure and must not be retried.
func isUIDCollision(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return false
	}
	switch pgErr.ConstraintName {
	case "symbol_uid_pkey", "symbol_active_pkey", "symbol_terminal_pkey", "symbol_uid_across_tables":
		return true
	default:
		return false
	}
}
