package sym

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// Snapshot is an immutable active table. Indexes are UID → Info, ticker → UID,
// and ExchKey → UID. A published snapshot is not mutated: WithTable returns a
// new one. The in-process Resolver and symnats.Remote both serve from it.
type Snapshot struct {
	byUID    map[uint32]symbol.Info
	byTicker map[symbol.UniversalTicker]uint32
	byExch   map[symbol.ExchKey]uint32
}

// NewSnapshot is an empty table.
func NewSnapshot() *Snapshot {
	return &Snapshot{
		byUID:    map[uint32]symbol.Info{},
		byTicker: map[symbol.UniversalTicker]uint32{},
		byExch:   map[symbol.ExchKey]uint32{},
	}
}

func (s *Snapshot) clone() *Snapshot {
	if s == nil {
		return NewSnapshot()
	}
	return &Snapshot{
		byUID:    maps.Clone(s.byUID),
		byTicker: maps.Clone(s.byTicker),
		byExch:   maps.Clone(s.byExch),
	}
}

// FindUID is the row with uid. Its Filters are a copy the caller may change.
func (s *Snapshot) FindUID(uid uint32) (symbol.Info, bool) {
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

// FindTicker is the row with ticker t, or symbol.ErrNotFound.
func (s *Snapshot) FindTicker(t symbol.UniversalTicker) (*symbol.Info, error) {
	if s == nil {
		return nil, symbol.ErrNotFound
	}
	uid, ok := s.byTicker[t]
	if !ok {
		return nil, symbol.ErrNotFound
	}
	info, ok := s.FindUID(uid)
	if !ok {
		return nil, symbol.ErrNotFound
	}
	return &info, nil
}

// FindExch is the row with the venue spelling key.
func (s *Snapshot) FindExch(key symbol.ExchKey) (symbol.Info, bool) {
	if s == nil {
		return symbol.Info{}, false
	}
	uid, ok := s.byExch[key]
	if !ok {
		return symbol.Info{}, false
	}
	return s.FindUID(uid)
}

// List is every row of (venue, c), ordered by UniversalTicker. It is never nil.
func (s *Snapshot) List(venue string, c symbol.Category) []symbol.Info {
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

// WithTable is s with every row of (venue, category) replaced by rows. A row of
// another table, or a ticker, ExchKey or UID that is already loaded, is an error.
func (s *Snapshot) WithTable(venue string, cat symbol.Category, rows []symbol.Info) (*Snapshot, error) {
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
