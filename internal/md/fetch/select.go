package fetch

import (
	"fmt"
	"strings"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// Venue is one market this process answers for. Categories are the ones the
// registry lists; the NATS resolver loads each of those active tables (D422).
type Venue struct {
	Name       string
	REST       exchange.PublicREST
	Categories []symbol.Category
}

// SelectVenues parses the deployment list of canonical venue names (D418).
// spec is comma-separated. An empty list is an error: a replica subscribes
// only to the venues it can reach, and that list is configuration, not "every
// venue". A name the registry does not have, a venue with no public REST
// reader, a dot in the name, or a duplicate is an error.
func SelectVenues(reg *exchange.Registry, spec string) ([]Venue, error) {
	if reg == nil {
		return nil, fmt.Errorf("fetch: nil registry")
	}
	if strings.TrimSpace(spec) == "" {
		return nil, fmt.Errorf("fetch: venues are empty")
	}
	var out []Venue
	seen := map[string]struct{}{}
	for _, part := range strings.Split(spec, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("fetch: empty venue")
		}
		if strings.Contains(name, ".") || strings.ContainsAny(name, " \t") {
			return nil, fmt.Errorf("fetch: venue %q is not one subject token", name)
		}
		v, ok := reg.Venue(name)
		if !ok {
			return nil, fmt.Errorf("fetch: unknown venue %s", name)
		}
		if v.REST == nil {
			return nil, fmt.Errorf("fetch: venue %s has no public REST reader", name)
		}
		if len(v.Categories) == 0 {
			return nil, fmt.Errorf("fetch: venue %s has no categories", name)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("fetch: duplicate venue %s", name)
		}
		seen[name] = struct{}{}
		cats := append([]symbol.Category(nil), v.Categories...)
		out = append(out, Venue{Name: name, REST: v.REST, Categories: cats})
	}
	return out, nil
}
