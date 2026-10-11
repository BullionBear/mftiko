package fetch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/decimal"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/transport/fasthttp"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/platform/natsx"
)

func TestFetch(t *testing.T) {
	plane := startPlane(t)
	t.Run("klines page and drop the open candle", func(t *testing.T) {
		// Five closed minutes and one still forming. The page holds two, so the
		// service walks back from the earliest open time. The boundary candle is
		// included again and must be dropped.
		candles := make([]recorded, 6)
		for i := range candles {
			candles[i] = recorded{
				OpenTimeNs: int64(i+1) * int64(time.Minute),
				Open:       float64(i + 1), High: float64(i + 2), Low: float64(i), Close: float64(i) + 0.5,
				Volume: float64(i+1) * 10, Closed: i < 5,
			}
		}
		candles[0].QuoteVolume = 4.5
		tape := newTape(t, candles, 2)
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 0)
		var reply mdv1.FetchReply
		err := client.Request(context.Background(), Subject("BinanceUM"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{
				Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1m", Limit: 5,
			}},
		}, &reply)
		if err != nil {
			t.Fatal(err)
		}
		if reply.GetError() != "" {
			t.Fatalf("error %s: %s", reply.GetError(), reply.GetReason())
		}
		got := reply.GetKlines().GetKlines()
		if len(got) != 5 {
			t.Fatalf("klines %d", len(got))
		}
		for i, row := range got {
			want := int64(i+1) * int64(time.Minute)
			if row.GetOpenTimeNs() != want || !row.GetClosed() || row.GetInterval() != "1m" {
				t.Fatalf("row %d %+v", i, row)
			}
		}
		if got[0].QuoteVolume == nil || *got[0].QuoteVolume != 4.5 {
			t.Fatalf("quote %v", got[0].QuoteVolume)
		}
		if got[1].QuoteVolume != nil {
			t.Fatal("absent quote volume was set")
		}
		if hits := tape.hits.Load(); hits != 5 {
			t.Fatalf("http calls %d, want 5", hits)
		}
	})

	t.Run("five thousand candles fit under 1 MiB", func(t *testing.T) {
		candles := make([]recorded, MaxKlines)
		for i := range candles {
			candles[i] = recorded{
				OpenTimeNs: int64(i+1) * int64(time.Minute),
				Open:       1, High: 2, Low: 0.5, Close: 1.5, Volume: 3, QuoteVolume: 4, Closed: true,
			}
		}
		tape := newTape(t, candles, MaxKlines)
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 0)
		var reply mdv1.FetchReply
		err := client.Request(context.Background(), Subject("BinanceUM"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{
				Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1m", Limit: MaxKlines,
			}},
		}, &reply)
		if err != nil {
			t.Fatal(err)
		}
		if reply.GetError() != "" || len(reply.GetKlines().GetKlines()) != MaxKlines {
			t.Fatalf("error %s klines %d", reply.GetError(), len(reply.GetKlines().GetKlines()))
		}
		if n := proto.Size(&reply); n > maxReplyBytes {
			t.Fatalf("5000 klines are %d bytes", n)
		}
		if tape.hits.Load() != 1 {
			t.Fatalf("http calls %d", tape.hits.Load())
		}
	})

	t.Run("open interest", func(t *testing.T) {
		tape := newTape(t, nil, 10)
		tape.oi = &exchange.OpenInterest{ExchTsNs: 42, OpenInterest: 1.5, OpenInterestValue: decimal.Absent()}
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 0)
		var reply mdv1.FetchReply
		err := client.Request(context.Background(), Subject("BinanceUM"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_OpenInterest{OpenInterest: &mdv1.FetchOpenInterest{Ticker: "BinanceUM_Perp_BTCUSDT"}},
		}, &reply)
		if err != nil {
			t.Fatal(err)
		}
		oi := reply.GetOpenInterest()
		if reply.GetError() != "" || oi.GetOpenInterest() != 1.5 || oi.GetMeta().GetExchTsNs() != 42 || oi.OpenInterestValue != nil {
			t.Fatalf("%+v", &reply)
		}
		if tape.hits.Load() != 1 {
			t.Fatal(tape.hits.Load())
		}
	})

	t.Run("unsupported", func(t *testing.T) {
		tape := newTape(t, nil, 10)
		tape.klineErr = exchange.ErrUnsupported
		tape.oiErr = exchange.ErrUnsupported
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 0)
		for _, req := range []*mdv1.FetchRequest{
			{Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1s", Limit: 1}}},
			{Req: &mdv1.FetchRequest_OpenInterest{OpenInterest: &mdv1.FetchOpenInterest{Ticker: "BinanceUM_Perp_BTCUSDT"}}},
		} {
			var reply mdv1.FetchReply
			if err := client.Request(context.Background(), Subject("BinanceUM"), req, &reply); err != nil {
				t.Fatal(err)
			}
			if reply.GetError() != CodeUnsupported || reply.GetResult() != nil {
				t.Fatalf("%+v", &reply)
			}
		}
		if tape.hits.Load() != 0 {
			t.Fatal("unsupported still called the venue")
		}
	})

	t.Run("rate limited by reported usage", func(t *testing.T) {
		tape := newTape(t, []recorded{{
			OpenTimeNs: int64(time.Minute), Open: 1, High: 1, Low: 1, Close: 1, Volume: 1, Closed: true,
		}}, 10)
		tape.used = 8
		tape.oi = &exchange.OpenInterest{ExchTsNs: 1, OpenInterest: 1, OpenInterestValue: decimal.Absent()}
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 8)
		var first mdv1.FetchReply
		err := client.Request(context.Background(), Subject("BinanceUM"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1m", Limit: 1}},
		}, &first)
		if err != nil || first.GetError() != "" || len(first.GetKlines().GetKlines()) != 1 {
			t.Fatalf("err %v reply %+v", err, &first)
		}
		var second mdv1.FetchReply
		err = client.Request(context.Background(), Subject("BinanceUM"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_OpenInterest{OpenInterest: &mdv1.FetchOpenInterest{Ticker: "BinanceUM_Perp_BTCUSDT"}},
		}, &second)
		if err != nil {
			t.Fatal(err)
		}
		if second.GetError() != CodeRateLimited || second.GetResult() != nil {
			t.Fatalf("%+v", &second)
		}
		if tape.hits.Load() != 1 {
			t.Fatalf("http calls %d, the second request was sent", tape.hits.Load())
		}
	})

	t.Run("venue rate limit is not retried", func(t *testing.T) {
		tape := newTape(t, nil, 10)
		tape.status = http.StatusTooManyRequests
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 0)
		var reply mdv1.FetchReply
		err := client.Request(context.Background(), Subject("BinanceUM"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1m", Limit: 1}},
		}, &reply)
		if err != nil {
			t.Fatal(err)
		}
		if reply.GetError() != CodeRateLimited {
			t.Fatalf("%+v", &reply)
		}
		if tape.hits.Load() != 1 {
			t.Fatalf("http calls %d", tape.hits.Load())
		}
	})

	t.Run("venue error is not retried", func(t *testing.T) {
		tape := newTape(t, nil, 10)
		tape.status = http.StatusBadRequest
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 0)
		var reply mdv1.FetchReply
		err := client.Request(context.Background(), Subject("BinanceUM"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1m", Limit: 1}},
		}, &reply)
		if err != nil {
			t.Fatal(err)
		}
		if reply.GetError() != CodeVenueError || reply.GetReason() == "" || reply.GetResult() != nil {
			t.Fatalf("%+v", &reply)
		}
		if tape.hits.Load() != 1 {
			t.Fatalf("http calls %d", tape.hits.Load())
		}
	})

	t.Run("inclusive page of one still walks back", func(t *testing.T) {
		candles := make([]recorded, 3)
		for i := range candles {
			candles[i] = recorded{
				OpenTimeNs: int64(i+1) * int64(time.Minute),
				Open:       1, High: 1, Low: 1, Close: 1, Volume: 1, Closed: true,
			}
		}
		tape := newTape(t, candles, 1)
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 0)
		var reply mdv1.FetchReply
		err := client.Request(context.Background(), Subject("BinanceUM"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{
				Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1m", Limit: 3,
			}},
		}, &reply)
		if err != nil {
			t.Fatal(err)
		}
		got := reply.GetKlines().GetKlines()
		if reply.GetError() != "" || len(got) != 3 {
			t.Fatalf("error %s klines %d", reply.GetError(), len(got))
		}
		for i, row := range got {
			if row.GetOpenTimeNs() != int64(i+1)*int64(time.Minute) {
				t.Fatalf("row %d %+v", i, row)
			}
		}
		if tape.hits.Load() < 3 {
			t.Fatalf("http calls %d", tape.hits.Load())
		}
	})

	t.Run("invalid", func(t *testing.T) {
		tape := newTape(t, nil, 10)
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 0)
		cases := []struct {
			name string
			req  *mdv1.FetchRequest
		}{
			{"over 5000", &mdv1.FetchRequest{Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{
				Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1m", Limit: MaxKlines + 1,
			}}}},
			{"zero", &mdv1.FetchRequest{Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{
				Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1m",
			}}}},
			{"not active", &mdv1.FetchRequest{Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{
				Ticker: "BinanceUM_Perp_ETHUSDT", Interval: "1m", Limit: 1,
			}}}},
			{"venue mismatch", &mdv1.FetchRequest{Req: &mdv1.FetchRequest_OpenInterest{OpenInterest: &mdv1.FetchOpenInterest{
				Ticker: "Okx_Spot_BTCUSDT",
			}}}},
			{"empty", &mdv1.FetchRequest{}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var reply mdv1.FetchReply
				if err := client.Request(context.Background(), Subject("BinanceUM"), tc.req, &reply); err != nil {
					t.Fatal(err)
				}
				if reply.GetError() != CodeInvalid || reply.GetReason() == "" || reply.GetResult() != nil {
					t.Fatalf("%+v", &reply)
				}
			})
		}
		if tape.hits.Load() != 0 || res.gets != 1 {
			t.Fatalf("hits %d resolver gets %d", tape.hits.Load(), res.gets)
		}
	})

	t.Run("two replicas answer once", func(t *testing.T) {
		tape := newTape(t, []recorded{{
			OpenTimeNs: int64(time.Minute), Open: 1, High: 1, Low: 1, Close: 1, Volume: 1, Closed: true,
		}}, 10)
		res := &memResolver{rows: map[string]*symbol.Info{
			"BinanceUM_Perp_BTCUSDT": instrument("BinanceUM", symbol.Perp, "BTCUSDT"),
		}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		a := dialFetch(t, plane)
		b := dialFetch(t, plane)
		svc := mustService(t, "BinanceUM", tape, res, 0)
		subsA := mustServe(t, svc, ctx, a)
		subsB := mustServe(t, svc, ctx, b)
		defer unsubscribe(t, a, subsA)
		defer unsubscribe(t, b, subsB)
		client := dialSession(t, plane)
		var reply mdv1.FetchReply
		err := client.Request(context.Background(), Subject("BinanceUM"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_Klines{Klines: &mdv1.FetchKlines{Ticker: "BinanceUM_Perp_BTCUSDT", Interval: "1m", Limit: 1}},
		}, &reply)
		if err != nil || reply.GetError() != "" || len(reply.GetKlines().GetKlines()) != 1 {
			t.Fatalf("err %v reply %+v", err, &reply)
		}
		if tape.hits.Load() != 1 {
			t.Fatalf("http calls %d, both replicas answered", tape.hits.Load())
		}
	})

	t.Run("unsubscribed venue has no responder", func(t *testing.T) {
		tape := newTape(t, nil, 10)
		res := &memResolver{rows: map[string]*symbol.Info{}}
		client := serveOne(t, plane, "BinanceUM", tape, res, 0)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var reply mdv1.FetchReply
		err := client.Request(ctx, Subject("Okx"), &mdv1.FetchRequest{
			Req: &mdv1.FetchRequest_OpenInterest{OpenInterest: &mdv1.FetchOpenInterest{Ticker: "Okx_Spot_BTCUSDT"}},
		}, &reply)
		if !errors.Is(err, natsx.ErrNoResponders) {
			t.Fatalf("err %v", err)
		}
		if tape.hits.Load() != 0 {
			t.Fatal("unsubscribed venue was called")
		}
	})
}

func TestRejectsBadService(t *testing.T) {
	res := &memResolver{}
	doer := fasthttp.New(nil, fasthttp.Options{})
	if _, err := New(nil, res, doer, Options{}); err == nil {
		t.Fatal("no venues")
	}
	if _, err := New([]Venue{{Name: "BinanceUM", REST: stubREST{}}}, nil, doer, Options{}); err == nil {
		t.Fatal("nil resolver")
	}
	if _, err := New([]Venue{{Name: "BinanceUM", REST: stubREST{}}}, res, nil, Options{}); err == nil {
		t.Fatal("nil doer")
	}
	if _, err := New([]Venue{{Name: "BinanceUM", REST: stubREST{}}}, res, doer, Options{RateLimit: -1}); err == nil {
		t.Fatal("negative rate limit")
	}
}

type recorded struct {
	OpenTimeNs  int64
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64
	QuoteVolume float64
	Closed      bool
}

// tape is a fake PublicREST whose Calls are served by httptest from a recorded
// series. The body is JSON; fasthttp is the Doer the service uses.
type tape struct {
	base     string
	candles  []recorded
	page     int
	oi       *exchange.OpenInterest
	klineErr error
	oiErr    error
	status   int
	used     int
	hits     atomic.Int32
}

func newTape(t *testing.T, candles []recorded, page int) *tape {
	t.Helper()
	tp := &tape{candles: candles, page: page, status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(tp.serve))
	t.Cleanup(srv.Close)
	tp.base = srv.URL
	return tp
}

func (tp *tape) serve(w http.ResponseWriter, r *http.Request) {
	tp.hits.Add(1)
	if tp.used > 0 {
		w.Header().Set("X-Used-Weight", strconv.Itoa(tp.used))
	}
	if tp.status != http.StatusOK {
		w.WriteHeader(tp.status)
		return
	}
	switch r.URL.Path {
	case "/klines":
		q := r.URL.Query()
		end, _ := strconv.ParseInt(q.Get("end"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))
		_ = json.NewEncoder(w).Encode(pageOf(tp.candles, end, limit))
	case "/openInterest":
		body := map[string]float64{"exch_ts_ns": float64(tp.oi.ExchTsNs), "open_interest": tp.oi.OpenInterest}
		if decimal.Has(tp.oi.OpenInterestValue) {
			body["open_interest_value"] = tp.oi.OpenInterestValue
		}
		_ = json.NewEncoder(w).Encode(body)
	default:
		http.NotFound(w, r)
	}
}

func pageOf(all []recorded, endNs int64, limit int) []recorded {
	var elig []recorded
	for _, c := range all {
		if endNs != 0 && c.OpenTimeNs > endNs {
			continue
		}
		elig = append(elig, c)
	}
	if limit > 0 && len(elig) > limit {
		elig = elig[len(elig)-limit:]
	}
	if elig == nil {
		elig = []recorded{}
	}
	return elig
}

func (tp *tape) Listing(symbol.Category) (exchange.Call[exchange.Page[symbol.Listing]], error) {
	return exchange.Call[exchange.Page[symbol.Listing]]{}, exchange.ErrUnsupported
}

func (tp *tape) Depth(*symbol.Info, int) (exchange.Call[*exchange.BookUpdate], error) {
	return exchange.Call[*exchange.BookUpdate]{}, exchange.ErrUnsupported
}

func (tp *tape) KlinePage() int { return tp.page }

func (tp *tape) Klines(inst *symbol.Info, interval string, endNs int64, limit int) (exchange.Call[[]exchange.Kline], error) {
	if tp.klineErr != nil {
		return exchange.Call[[]exchange.Kline]{}, tp.klineErr
	}
	q := url.Values{}
	q.Set("symbol", inst.ExchTicker)
	q.Set("interval", interval)
	q.Set("end", strconv.FormatInt(endNs, 10))
	q.Set("limit", strconv.Itoa(limit))
	return exchange.Call[[]exchange.Kline]{
		Req: exchange.HTTPRequest{Method: http.MethodGet, Base: tp.base, Path: "/klines", Query: []byte(q.Encode())},
		Parse: func(resp *exchange.HTTPResponse) ([]exchange.Kline, error) {
			if resp.Status == http.StatusTooManyRequests {
				return nil, &exchange.RateLimited{}
			}
			if resp.Status != http.StatusOK {
				return nil, errors.New("status " + strconv.Itoa(resp.Status))
			}
			var rows []recorded
			if err := json.Unmarshal(resp.Body, &rows); err != nil {
				return nil, err
			}
			out := make([]exchange.Kline, len(rows))
			for i, row := range rows {
				out[i] = exchange.Kline{
					ExchTsNs: row.OpenTimeNs, Interval: interval, OpenTimeNs: row.OpenTimeNs,
					Open: row.Open, High: row.High, Low: row.Low, Close: row.Close,
					Volume: row.Volume, QuoteVolume: row.QuoteVolume, Closed: row.Closed,
				}
				if row.QuoteVolume == 0 {
					out[i].QuoteVolume = decimal.Absent()
				}
			}
			return out, nil
		},
	}, nil
}

func (tp *tape) OpenInterest(inst *symbol.Info) (exchange.Call[*exchange.OpenInterest], error) {
	if tp.oiErr != nil {
		return exchange.Call[*exchange.OpenInterest]{}, tp.oiErr
	}
	q := url.Values{}
	q.Set("symbol", inst.ExchTicker)
	return exchange.Call[*exchange.OpenInterest]{
		Req: exchange.HTTPRequest{Method: http.MethodGet, Base: tp.base, Path: "/openInterest", Query: []byte(q.Encode())},
		Parse: func(resp *exchange.HTTPResponse) (*exchange.OpenInterest, error) {
			if resp.Status == http.StatusTooManyRequests {
				return nil, &exchange.RateLimited{}
			}
			if resp.Status != http.StatusOK {
				return nil, errors.New("status " + strconv.Itoa(resp.Status))
			}
			var body struct {
				ExchTsNs          int64   `json:"exch_ts_ns"`
				OpenInterest      float64 `json:"open_interest"`
				OpenInterestValue float64 `json:"open_interest_value"`
			}
			if err := json.Unmarshal(resp.Body, &body); err != nil {
				return nil, err
			}
			oi := &exchange.OpenInterest{ExchTsNs: body.ExchTsNs, OpenInterest: body.OpenInterest, OpenInterestValue: decimal.Absent()}
			if body.OpenInterestValue != 0 {
				oi.OpenInterestValue = body.OpenInterestValue
			}
			return oi, nil
		},
	}, nil
}

func (tp *tape) Usage(resp *exchange.HTTPResponse) []exchange.Used {
	if resp == nil {
		return nil
	}
	n, err := strconv.Atoi(resp.Get("X-Used-Weight"))
	if err != nil {
		return nil
	}
	return []exchange.Used{{Bucket: "weight", N: n}}
}

type memResolver struct {
	rows map[string]*symbol.Info
	gets int
}

func (m *memResolver) Get(_ context.Context, t symbol.UniversalTicker, includeInactive bool) (*symbol.Info, error) {
	m.gets++
	if includeInactive {
		return nil, errors.New("include_inactive was set")
	}
	info, ok := m.rows[t.String()]
	if !ok {
		return nil, symbol.ErrNotFound
	}
	cp := *info
	return &cp, nil
}

func (m *memResolver) GetByUID(context.Context, uint32, bool) (*symbol.Info, error) {
	return nil, symbol.ErrNotFound
}

func (m *memResolver) Reverse(context.Context, symbol.ExchKey, bool) (*symbol.Info, error) {
	return nil, symbol.ErrNotFound
}

func (m *memResolver) List(context.Context, string, symbol.Category) ([]symbol.Info, error) {
	return nil, nil
}

func (m *memResolver) Invalidate(string) {}

func instrument(venue string, cat symbol.Category, sym string) *symbol.Info {
	return &symbol.Info{
		UID:        1,
		Ticker:     symbol.UniversalTicker{Venue: venue, Category: cat, Symbol: sym},
		ExchTicker: sym,
		Status:     symbol.Tradable,
	}
}

type plane struct {
	url   string
	dir   string
	fetch natsdev.User
	lay   natsdev.Layout
}

func startPlane(t *testing.T) plane {
	t.Helper()
	dir := t.TempDir()
	if err := natsdev.Generate(dir, natsdev.Options{}); err != nil {
		t.Fatal(err)
	}
	lay, err := natsdev.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	ns := startAuthServer(t, dir, lay.Manifest.System)
	return plane{url: ns.ClientURL(), dir: dir, fetch: lay.Manifest.Fetch[0], lay: lay}
}

func startAuthServer(t *testing.T, dir, system string) *server.Server {
	t.Helper()
	conf, err := natsdev.FormatServerConfig(natsdev.ServerConfig{
		OperatorFile:  filepath.Join(dir, "operator.jwt"),
		SystemAccount: system,
		ResolverDir:   filepath.Join(dir, "resolver"),
		Host:          "127.0.0.1",
		Port:          -1,
		ServerName:    "dev",
	})
	if err != nil {
		t.Fatal(err)
	}
	confPath := filepath.Join(t.TempDir(), "nats-server.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatal(err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server did not accept connections")
	}
	return ns
}

func serveOne(t *testing.T, p plane, venue string, rest exchange.PublicREST, res *memResolver, rate int) *natsx.Conn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	nc := dialFetch(t, p)
	svc := mustService(t, venue, rest, res, rate)
	subs := mustServe(t, svc, ctx, nc)
	t.Cleanup(func() { unsubscribe(t, nc, subs) })
	return dialSession(t, p)
}

func mustService(t *testing.T, venue string, rest exchange.PublicREST, res symbol.Resolver, rate int) *Service {
	t.Helper()
	svc, err := New([]Venue{{Name: venue, REST: rest}}, res, fasthttp.New(nil, fasthttp.Options{Timeout: 5 * time.Second}), Options{RateLimit: rate})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func mustServe(t *testing.T, svc *Service, ctx context.Context, nc *natsx.Conn) []*natsx.Subscription {
	t.Helper()
	subs, err := svc.Serve(ctx, nc)
	if err != nil {
		t.Fatal(err)
	}
	return subs
}

func unsubscribe(t *testing.T, nc *natsx.Conn, subs []*natsx.Subscription) {
	t.Helper()
	for _, sub := range subs {
		if err := sub.Unsubscribe(); err != nil {
			t.Error(err)
		}
	}
	if err := nc.Flush(); err != nil {
		t.Error(err)
	}
}

func dialFetch(t *testing.T, p plane) *natsx.Conn {
	t.Helper()
	nc, err := natsx.Connect(p.url, natsx.Options{
		Name:        "mftiko-mdfetch",
		CredsFile:   filepath.Join(p.dir, filepath.FromSlash(p.fetch.Creds)),
		InboxPrefix: p.fetch.InboxPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func dialSession(t *testing.T, p plane) *natsx.Conn {
	t.Helper()
	const inbox = "_INBOX_fetchtest"
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(p.dir, filepath.FromSlash(p.lay.Manifest.SigningKeyFile)))
	if err != nil {
		t.Fatal(err)
	}
	signer, err := nkeys.FromSeed(bytes.TrimSpace(raw))
	if err != nil {
		t.Fatal(err)
	}
	uc := jwt.NewUserClaims(pub)
	uc.Name = "session-fetchtest"
	uc.Pub.Allow.Add("md.fetch.>")
	uc.Sub.Allow.Add(inbox + ".>")
	uc.IssuerAccount = p.lay.Manifest.Account
	token, err := uc.Encode(signer)
	if err != nil {
		t.Fatal(err)
	}
	body, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "session.creds")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	nc, err := natsx.Connect(p.url, natsx.Options{Name: "session-fetchtest", CredsFile: path, InboxPrefix: inbox})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}
