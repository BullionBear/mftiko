package exchangetest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

var (
	// ErrDuplicate is returned when Add or LoadFixture is given an instrument this source already
	// lists, or when one fixture lists the same instrument twice.
	ErrDuplicate = errors.New("exchangetest: instrument already listed")

	// ErrNotFound is returned when Remove or Modify names an instrument this source does not list.
	ErrNotFound = errors.New("exchangetest: instrument not found")

	// ErrVenueMismatch is returned when an instrument names a venue other than the source's.
	ErrVenueMismatch = errors.New("exchangetest: instrument belongs to another venue")
)

// Source is an in-memory exchange.PublicREST for one venue. Listing is served from instruments the
// test adds. The other reads are not implemented. The zero Source is not usable; call New.
//
// A Source is safe for concurrent use. Listing returns copies, and Add, Remove and Modify copy what
// they are given, so a caller can reuse or mutate its own listing afterwards.
type Source struct {
	mu       sync.Mutex
	venue    string
	pageSize int // 0 = the whole category is one page
	fail     map[int]error
	items    map[symbol.Category]map[string]symbol.Listing
}

var _ exchange.PublicREST = (*Source)(nil)

// New returns a fake PublicREST for venue. venue is the venue name in a UniversalTicker, such as
// "Bybit". It must be non-empty: one Source is one venue, matching PublicREST.
func New(venue string) (*Source, error) {
	if venue == "" {
		return nil, errors.New("exchangetest: venue is empty")
	}
	return &Source{
		venue: venue,
		fail:  map[int]error{},
		items: map[symbol.Category]map[string]symbol.Listing{},
	}, nil
}

// Add inserts an instrument. Venue may be empty and is then set to the source's venue; any other
// venue is rejected. Category and ExchTicker identify the instrument and must both be non-empty. A
// second Add of the same identity is ErrDuplicate; change an existing instrument with Modify.
func (s *Source) Add(l symbol.Listing) error {
	l, err := s.prepare(l)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.has(l.Category, l.ExchTicker) {
		return fmt.Errorf("exchangetest: %s %s: %w", l.Category, l.ExchTicker, ErrDuplicate)
	}
	s.put(l)
	return nil
}

// Remove deletes the instrument identified by category and the venue's ticker. A missing instrument
// is ErrNotFound.
func (s *Source) Remove(c symbol.Category, exchTicker string) error {
	if c == "" {
		return errors.New("exchangetest: category is empty")
	}
	if exchTicker == "" {
		return errors.New("exchangetest: exch ticker is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.has(c, exchTicker) {
		return fmt.Errorf("exchangetest: %s %s: %w", c, exchTicker, ErrNotFound)
	}
	delete(s.items[c], exchTicker)
	if len(s.items[c]) == 0 {
		delete(s.items, c)
	}
	return nil
}

// Modify replaces the instrument identified by l.Category and l.ExchTicker. The identity itself does
// not move: a different category or ticker is a different instrument and is ErrNotFound. Every other
// field is taken from l, so a field left at its zero value is cleared, and Filters is replaced
// rather than merged.
func (s *Source) Modify(l symbol.Listing) error {
	l, err := s.prepare(l)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.has(l.Category, l.ExchTicker) {
		return fmt.Errorf("exchangetest: %s %s: %w", l.Category, l.ExchTicker, ErrNotFound)
	}
	s.put(l)
	return nil
}

// SetPageSize sets how many instruments Listing reads per page. Zero, the default, reads the whole
// category as one page. A negative size is rejected and leaves the current size in place. The size
// changes which page index a FailPage refers to; it does not change the instruments a successful
// Listing returns.
func (s *Source) SetPageSize(n int) error {
	if n < 0 {
		return errors.New("exchangetest: page size is negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageSize = n
	return nil
}

// FailPage arranges for Listing to fail at page, counted from zero within whichever category it is
// reading. Listing returns that error and no rows. A nil err clears the page. A page the read never
// reaches does not fail the call. An empty category is still one empty page, so page 0 is reached
// and page 1 is not. A negative page is rejected and installs nothing.
func (s *Source) FailPage(page int, err error) error {
	if page < 0 {
		return errors.New("exchangetest: page is negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		delete(s.fail, page)
		return nil
	}
	s.fail[page] = err
	return nil
}

// Listing implements exchange.PublicREST. It returns this venue's instruments in c, sorted by
// ExchTicker, read in pages of the configured size. Active is not a filter: an inactive instrument
// is returned as it was stored. If a reached page is set to fail, or ctx is done, Listing returns
// that error and no instruments. A done context is reported before the page error. Instruments of
// other categories are neither included nor affected. The returned listings are copies.
func (s *Source) Listing(ctx context.Context, c symbol.Category) ([]symbol.Listing, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := s.snapshot(c)
	if len(items) == 0 {
		if err := s.readPage(ctx, c, 0); err != nil {
			return nil, err
		}
		return []symbol.Listing{}, nil
	}
	size := s.pageSize
	if size <= 0 {
		size = len(items)
	}
	out := make([]symbol.Listing, 0, len(items))
	for page, start := 0, 0; start < len(items); page++ {
		if err := s.readPage(ctx, c, page); err != nil {
			return nil, err
		}
		end := start + size
		// end < start means size overflowed int. Either way the rest of the category is this page.
		if end > len(items) || end < start {
			end = len(items)
		}
		out = append(out, items[start:end]...)
		start = end
	}
	return out, nil
}

// Depth returns exchange.ErrNotImplemented.
func (s *Source) Depth(context.Context, *symbol.Info, int) (*exchange.BookUpdate, error) {
	return nil, exchange.ErrNotImplemented
}

// Klines returns exchange.ErrNotImplemented.
func (s *Source) Klines(context.Context, *symbol.Info, string, int) ([]exchange.Kline, error) {
	return nil, exchange.ErrNotImplemented
}

// KlinePage is zero: kline paging is not implemented.
func (s *Source) KlinePage() int { return 0 }

// OpenInterest returns exchange.ErrNotImplemented.
func (s *Source) OpenInterest(context.Context, *symbol.Info) (*exchange.OpenInterest, error) {
	return nil, exchange.ErrNotImplemented
}

// prepare checks l and returns the copy the source stores. It reads only s.venue, which New sets
// and nothing mutates.
func (s *Source) prepare(l symbol.Listing) (symbol.Listing, error) {
	if l.Venue == "" {
		l.Venue = s.venue
	} else if l.Venue != s.venue {
		return symbol.Listing{}, fmt.Errorf("exchangetest: %s belongs to %s, source is %s: %w",
			l.ExchTicker, l.Venue, s.venue, ErrVenueMismatch)
	}
	if l.Category == "" {
		return symbol.Listing{}, errors.New("exchangetest: category is empty")
	}
	if l.ExchTicker == "" {
		return symbol.Listing{}, errors.New("exchangetest: exch ticker is empty")
	}
	return cloneListing(l), nil
}

// cloneListing copies l. Filters is the only reference-typed field on symbol.Listing today, so it
// is the only field copied by hand. A new string, bool or integer field is copied with the struct.
// A new map, slice or pointer has to be copied here as well, and only here.
func cloneListing(l symbol.Listing) symbol.Listing {
	l.Filters = maps.Clone(l.Filters)
	return l
}

// put stores l. The caller holds s.mu. l must already be a copy.
func (s *Source) put(l symbol.Listing) {
	m := s.items[l.Category]
	if m == nil {
		m = map[string]symbol.Listing{}
		s.items[l.Category] = m
	}
	m[l.ExchTicker] = l
}

// has reports whether the instrument is stored. The caller holds s.mu.
func (s *Source) has(c symbol.Category, exchTicker string) bool {
	_, ok := s.items[c][exchTicker]
	return ok
}

// snapshot is this category's instruments, copied and sorted by ExchTicker. The caller holds s.mu.
func (s *Source) snapshot(c symbol.Category) []symbol.Listing {
	m := s.items[c]
	items := make([]symbol.Listing, 0, len(m))
	for _, l := range m {
		items = append(items, cloneListing(l))
	}
	slices.SortFunc(items, func(a, b symbol.Listing) int {
		return strings.Compare(a.ExchTicker, b.ExchTicker)
	})
	return items
}

// readPage fails the page when ctx is done or the test marked the page. The caller holds s.mu.
func (s *Source) readPage(ctx context.Context, c symbol.Category, page int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ferr := s.fail[page]; ferr != nil {
		return fmt.Errorf("exchangetest: listing %s page %d: %w", c, page, ferr)
	}
	return nil
}
