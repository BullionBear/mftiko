package exchangetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
)

// REST is the source as a venue's sans-I/O PublicREST (D302): Listing builds an HTTPRequest per page,
// and the Source itself is the exchange.Doer that answers it. Run the Calls with exchange.Collect or
// exchange.ListingOver, the way SYM runs a real venue's. The other reads return
// exchange.ErrNotImplemented.
func (s *Source) REST() exchange.PublicREST { return sourceREST{s} }

var (
	_ exchange.PublicREST = sourceREST{}
	_ exchange.Doer       = (*Source)(nil)
)

type sourceREST struct{ s *Source }

// listingPage is the body Source.Do answers a listing request with.
type listingPage struct {
	Items []symbol.Listing
	More  bool
}

func (r sourceREST) Listing(c symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	return r.page(c, 0), nil
}

func (r sourceREST) page(c symbol.Category, n int) exchange.Call[exchange.Page[symbol.Listing]] {
	q := url.Values{"category": {string(c)}, "page": {strconv.Itoa(n)}}
	return exchange.Call[exchange.Page[symbol.Listing]]{
		Req: exchange.HTTPRequest{
			Method: "GET", Base: "exchangetest://" + r.s.venue, Path: "/listing", Query: []byte(q.Encode()),
		},
		Parse: func(resp *exchange.HTTPResponse) (exchange.Page[symbol.Listing], error) {
			if resp.Status != 200 {
				return exchange.Page[symbol.Listing]{}, fmt.Errorf("exchangetest: status %d", resp.Status)
			}
			var body listingPage
			if err := json.Unmarshal(resp.Body, &body); err != nil {
				return exchange.Page[symbol.Listing]{}, err
			}
			p := exchange.Page[symbol.Listing]{Items: body.Items}
			if body.More {
				p.Next = func(time.Time) (exchange.Call[exchange.Page[symbol.Listing]], error) {
					return r.page(c, n+1), nil
				}
			}
			return p, nil
		},
	}
}

func (sourceREST) Depth(*symbol.Info, int) (exchange.Call[*exchange.BookUpdate], error) {
	return exchange.Call[*exchange.BookUpdate]{}, exchange.ErrNotImplemented
}

func (sourceREST) Klines(*symbol.Info, string, int64, int) (exchange.Call[[]exchange.Kline], error) {
	return exchange.Call[[]exchange.Kline]{}, exchange.ErrNotImplemented
}

func (sourceREST) KlinePage() int { return 0 }

func (sourceREST) OpenInterest(*symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	return exchange.Call[*exchange.OpenInterest]{}, exchange.ErrNotImplemented
}

func (sourceREST) Usage(*exchange.HTTPResponse, time.Time) []exchange.Used { return nil }

// Do answers the requests REST builds, as a venue would: one page of a category, sorted by
// ExchTicker, in pages of the configured size. A page marked with FailPage fails as a transport error
// would, with no response. Anything else is a 404.
func (s *Source) Do(ctx context.Context, req *exchange.HTTPRequest) (*exchange.HTTPResponse, error) {
	if req.Base != "exchangetest://"+s.venue || req.Path != "/listing" || req.Method != "GET" {
		return &exchange.HTTPResponse{Status: 404}, nil
	}
	q, err := url.ParseQuery(string(req.Query))
	if err != nil {
		return &exchange.HTTPResponse{Status: 400}, nil
	}
	c := symbol.Category(q.Get("category"))
	n, err := strconv.Atoi(q.Get("page"))
	if err != nil || n < 0 {
		return &exchange.HTTPResponse{Status: 400}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readPage(ctx, c, n); err != nil {
		return nil, err
	}
	items := s.snapshot(c)
	size := s.pageSize
	if size <= 0 {
		size = max(len(items), 1)
	}
	start := min(n*size, len(items))
	end := min(start+size, len(items))
	if n > 0 && start >= len(items) {
		return nil, errors.New("exchangetest: listing page past the end")
	}
	body, err := json.Marshal(listingPage{Items: items[start:end], More: end < len(items)})
	if err != nil {
		return nil, err
	}
	return &exchange.HTTPResponse{Status: 200, Body: body}, nil
}
