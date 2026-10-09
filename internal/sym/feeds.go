package sym

import (
	"fmt"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// SelectFeeds chooses the (venue, category) feeds this process refreshes.
//
// An empty spec is every venue category in reg that has a public REST reader,
// in registry order. Otherwise spec is a comma-separated list of Venue/Category.
// A venue the registry does not know, a category that venue does not list, a
// missing reader, a duplicate, or any other spelling is an error: the process
// should not start serving a feed it cannot name.
func SelectFeeds(reg *exchange.Registry, spec string, http exchange.Doer) ([]Feed, error) {
	if reg == nil {
		return nil, fmt.Errorf("sym: nil registry")
	}
	if strings.TrimSpace(spec) == "" {
		feeds := FeedsFromRegistry(reg, http)
		if len(feeds) == 0 {
			return nil, fmt.Errorf("sym: no feeds")
		}
		return feeds, nil
	}
	var out []Feed
	seen := map[string]struct{}{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("sym: empty feed")
		}
		venue, catName, ok := strings.Cut(part, "/")
		if !ok || venue == "" || catName == "" || strings.Contains(catName, "/") {
			return nil, fmt.Errorf("sym: feed %q", part)
		}
		cat := symbol.Category(catName)
		if !knownCategory(cat) {
			return nil, fmt.Errorf("sym: feed %q: category", part)
		}
		v, ok := reg.Venue(venue)
		if !ok {
			return nil, fmt.Errorf("sym: feed %q: unknown venue", part)
		}
		if v.REST == nil {
			return nil, fmt.Errorf("sym: feed %q: no listing source", part)
		}
		if !venueLists(v, cat) {
			return nil, fmt.Errorf("sym: feed %q: venue does not list category", part)
		}
		key := feedKey(venue, cat)
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("sym: duplicate feed %s", key)
		}
		seen[key] = struct{}{}
		out = append(out, Feed{Venue: venue, Category: cat, REST: exchange.ListingOver(v.REST, http, time.Now)})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("sym: no feeds")
	}
	return out, nil
}

func venueLists(v exchange.Venue, cat symbol.Category) bool {
	for _, c := range v.Categories {
		if c == cat {
			return true
		}
	}
	return false
}
