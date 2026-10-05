package exchange

import (
	"fmt"
	"sort"
)

// Venue is one venue's adapter. A nil part means the venue does not have it.
type Venue struct {
	Name     string // UniversalTicker's <Venue> token, e.g. "BinanceUM"
	Exchange string // the exchange package that declares it, e.g. "binance"

	Market  Market     // public websocket market data
	REST    PublicREST // public REST reader
	Account AccountFactory
	History HistoryFactory
}

// Registry maps venue names to adapters. Built once at startup, read-only after.
type Registry struct {
	byName map[string]Venue
}

// NewRegistry builds a registry from each exchange package's Venues(). Duplicate names are an error.
func NewRegistry(sets ...[]Venue) (*Registry, error) {
	r := &Registry{byName: map[string]Venue{}}
	for _, set := range sets {
		for _, v := range set {
			if v.Name == "" {
				return nil, fmt.Errorf("exchange: %s declares a venue without a name", v.Exchange)
			}
			if prev, dup := r.byName[v.Name]; dup {
				return nil, fmt.Errorf("exchange: venue %s declared by both %s and %s", v.Name, prev.Exchange, v.Exchange)
			}
			r.byName[v.Name] = v
		}
	}
	return r, nil
}

func (r *Registry) Venue(name string) (Venue, bool) {
	v, ok := r.byName[name]
	return v, ok
}

// Names returns every venue name, sorted.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.byName))
	for n := range r.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
