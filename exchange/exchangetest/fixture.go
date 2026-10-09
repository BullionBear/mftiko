package exchangetest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// LoadFixture adds every instrument in r. The fixture is a JSON array of symbol.Listing objects.
// Keys are that struct's exported field names, matched the way encoding/json matches them: a
// different case of the same name is accepted, and a name that matches no field is an error. The
// loader decodes into the struct itself, so a field added to Listing is accepted here without a
// change to this loader. An omitted Venue is set to the source's venue.
//
// Rows are applied only when every row is valid and none duplicates an instrument already in the
// source or earlier in the file. On error the source is unchanged. Row numbers in errors count
// from zero.
func (s *Source) LoadFixture(r io.Reader) error {
	rows, err := decodeFixture(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	staged := make([]symbol.Listing, 0, len(rows))
	seen := map[symbol.Category]map[string]bool{}
	for i, row := range rows {
		l, err := s.prepare(row)
		if err != nil {
			return fmt.Errorf("exchangetest: fixture row %d: %w", i, err)
		}
		if s.has(l.Category, l.ExchTicker) || seen[l.Category][l.ExchTicker] {
			return fmt.Errorf("exchangetest: fixture row %d: %s %s: %w",
				i, l.Category, l.ExchTicker, ErrDuplicate)
		}
		if seen[l.Category] == nil {
			seen[l.Category] = map[string]bool{}
		}
		seen[l.Category][l.ExchTicker] = true
		staged = append(staged, l)
	}
	for _, l := range staged {
		s.put(l)
	}
	return nil
}

// LoadFixtureFile reads a fixture from path and adds it, with the same all-or-nothing rule as
// LoadFixture.
func (s *Source) LoadFixtureFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("exchangetest: fixture: %w", err)
	}
	defer f.Close()
	return s.LoadFixture(f)
}

func decodeFixture(r io.Reader) ([]symbol.Listing, error) {
	if r == nil {
		return nil, errors.New("exchangetest: fixture reader is nil")
	}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var rows []symbol.Listing
	if err := dec.Decode(&rows); err != nil {
		return nil, fmt.Errorf("exchangetest: fixture: %w", err)
	}
	// null decodes as a nil slice. An empty array decodes as a non-nil empty slice, which is a
	// fixture that adds nothing.
	if rows == nil {
		return nil, errors.New("exchangetest: fixture must be a JSON array")
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("exchangetest: fixture has trailing data")
		}
		return nil, fmt.Errorf("exchangetest: fixture: %w", err)
	}
	return rows, nil
}
