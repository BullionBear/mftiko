package exchange

import (
	"fmt"
	"sort"

	"github.com/BullionBear/mftiko/contract/symbol"
)

// Declaration is everything one exchange package contributes: its venues (markets) and its
// account kinds (credentials).
type Declaration struct {
	Exchange     string // the exchange package, e.g. "binance"
	Venues       []Venue
	AccountKinds []AccountKind
}

// Venue is one market (D11, D26): its registry entry and its public adapters. A nil part means the
// venue does not have it.
type Venue struct {
	symbol.Venue            // Name, Categories: what Resolve and the API check tickers against
	Market       Market     // public websocket market data
	REST         PublicREST // public REST reader
}

// AccountKind is what an api_id is (D32): the credential it takes, the venues whose instruments it
// can trade, and the private adapters that trade them.
//
// Account kinds and venues are two independent enums whose names may coincide: the account kind
// BinanceUM trades the venue BinanceUM, while the account kind BinancePM trades the venues Binance,
// BinanceUM and BinanceCM and is no venue at all. There is no BinancePM_Spot_BTCUSDT; a PM account
// trades Binance_Spot_BTCUSDT, and its adapter routes the request by the ticker's venue.
type AccountKind struct {
	Name       string    // stored on the account declaration (state.v1.Account.kind)
	Label      string    // for the console
	Trades     []string  // venue names; an order for a ticker on any other venue is rejected 1xx
	KeyTypes   []KeyType // credential algorithms the adapter signs with
	Passphrase bool      // the credential includes a passphrase (Okx)
	Account    AccountFactory
	History    HistoryFactory
}

// Registry holds both enums. Built once at startup, read-only after. It is also the venue registry
// symbol.Resolve checks user input against.
type Registry struct {
	venues   map[string]Venue
	kinds    map[string]AccountKind
	exchange map[string]string // "v:"+venue or "k:"+kind -> declaring exchange, for error messages
}

// NewRegistry builds the registry from each exchange package's Declare(). It rejects duplicate
// names within an enum, venues without categories, and account kinds that trade an unknown venue.
func NewRegistry(decls ...Declaration) (*Registry, error) {
	r := &Registry{venues: map[string]Venue{}, kinds: map[string]AccountKind{}, exchange: map[string]string{}}
	for _, d := range decls {
		for _, v := range d.Venues {
			if v.Name == "" || len(v.Categories) == 0 {
				return nil, fmt.Errorf("exchange: %s declares a venue without a name or categories", d.Exchange)
			}
			if prev, dup := r.exchange["v:"+v.Name]; dup {
				return nil, fmt.Errorf("exchange: venue %s declared by both %s and %s", v.Name, prev, d.Exchange)
			}
			r.venues[v.Name] = v
			r.exchange["v:"+v.Name] = d.Exchange
		}
	}
	for _, d := range decls {
		for _, k := range d.AccountKinds {
			if k.Name == "" || len(k.Trades) == 0 {
				return nil, fmt.Errorf("exchange: %s declares an account kind without a name or venues", d.Exchange)
			}
			if prev, dup := r.exchange["k:"+k.Name]; dup {
				return nil, fmt.Errorf("exchange: account kind %s declared by both %s and %s", k.Name, prev, d.Exchange)
			}
			for _, v := range k.Trades {
				if _, ok := r.venues[v]; !ok {
					return nil, fmt.Errorf("exchange: account kind %s trades unknown venue %s", k.Name, v)
				}
			}
			r.kinds[k.Name] = k
			r.exchange["k:"+k.Name] = d.Exchange
		}
	}
	return r, nil
}

func (r *Registry) Venue(name string) (Venue, bool) {
	v, ok := r.venues[name]
	return v, ok
}

func (r *Registry) AccountKind(name string) (AccountKind, bool) {
	k, ok := r.kinds[name]
	return k, ok
}

// Lookup implements symbol.Venues: venues only, never account kinds.
func (r *Registry) Lookup(name string) (symbol.Venue, bool) {
	v, ok := r.venues[name]
	return v.Venue, ok
}

var _ symbol.Venues = (*Registry)(nil)

// Venues returns every venue name, sorted.
func (r *Registry) Venues() []string { return sortedKeys(r.venues) }

// AccountKinds returns every account kind name, sorted.
func (r *Registry) AccountKinds() []string { return sortedKeys(r.kinds) }

// CanTrade reports whether an account of kind may trade an instrument on venue. The account worker
// checks it before Submit and rejects 1xx otherwise.
func (k *AccountKind) CanTrade(venue string) bool {
	for _, v := range k.Trades {
		if v == venue {
			return true
		}
	}
	return false
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
