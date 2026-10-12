package conn

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// EnvRESTOverrides carries the agent's -rest-endpoints to the conn worker: a JSON object of venue
// to REST URL. Only the scheme and host are used, the way Market.Endpoint treats a websocket
// override. A testnet's websocket and its depth snapshot have to come from the same place: a
// production snapshot never lines up with testnet update ids.
const EnvRESTOverrides = "MFTIKO_MD_REST_ENDPOINTS"

// EncodeRESTOverrides checks each URL and returns the value of EnvRESTOverrides.
func EncodeRESTOverrides(m map[string]string) (string, error) {
	out := make(map[string]string, len(m))
	for venue, raw := range m {
		base, err := restBase(raw)
		if err != nil {
			return "", fmt.Errorf("rest endpoint for %s: %w", venue, err)
		}
		out[venue] = base
	}
	b, err := json.Marshal(out)
	return string(b), err
}

// RESTOverridesFromEnv reads EnvRESTOverrides. Unset is no overrides.
func RESTOverridesFromEnv() (map[string]string, error) {
	v := os.Getenv(EnvRESTOverrides)
	if v == "" {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(v), &m); err != nil {
		return nil, fmt.Errorf("%s: %w", EnvRESTOverrides, err)
	}
	for venue, raw := range m {
		base, err := restBase(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", EnvRESTOverrides, venue, err)
		}
		m[venue] = base
	}
	return m, nil
}

func restBase(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "", fmt.Errorf("%q is not an http(s) URL", raw)
	}
	return u.Scheme + "://" + u.Host, nil
}

// WithRESTBase sends every call of r to base (scheme://host) instead of the venue's default host.
// Paths, queries, weights and parsing stay the venue's.
func WithRESTBase(r exchange.PublicREST, base string) exchange.PublicREST {
	return rebased{PublicREST: r, base: base}
}

type rebased struct {
	exchange.PublicREST
	base string
}

func rebase[T any](c exchange.Call[T], err error, base string) (exchange.Call[T], error) {
	if err == nil {
		c.Req.Base = base
	}
	return c, err
}

func (r rebased) Listing(c symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	call, err := r.PublicREST.Listing(c)
	return rebase(call, err, r.base)
}

func (r rebased) Depth(inst *symbol.Info, limit int) (exchange.Call[*exchange.BookUpdate], error) {
	call, err := r.PublicREST.Depth(inst, limit)
	return rebase(call, err, r.base)
}

func (r rebased) Klines(inst *symbol.Info, interval string, endNs int64, limit int) (exchange.Call[[]exchange.Kline], error) {
	call, err := r.PublicREST.Klines(inst, interval, endNs, limit)
	return rebase(call, err, r.base)
}

func (r rebased) OpenInterest(inst *symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	call, err := r.PublicREST.OpenInterest(inst)
	return rebase(call, err, r.base)
}
