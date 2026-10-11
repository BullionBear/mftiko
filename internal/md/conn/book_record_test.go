package conn

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/exchange"
	"github.com/BullionBear/mftiko/exchange/binance"
	"github.com/BullionBear/mftiko/exchange/bybit"
	"github.com/BullionBear/mftiko/exchange/exchangetest/conformance"
)

// TestRecordBookFixtures dials public market data and writes the sessions the replay tests
// read. No API key. It is not part of go test: set MFTIKO_MD_RECORD=1.
func TestRecordBookFixtures(t *testing.T) {
	if os.Getenv("MFTIKO_MD_RECORD") != "1" {
		t.Skip("set MFTIKO_MD_RECORD=1 to record public book fixtures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	dir := filepath.Join("testdata", "book")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	recordBinanceUMBook(t, ctx, filepath.Join(dir, "binanceum.jsonl"))
	recordBybitBook(t, ctx, filepath.Join(dir, "bybit.jsonl"))
}

func recordBinanceUMBook(t *testing.T, ctx context.Context, path string) {
	t.Helper()
	// fapi.binance.com answers 451 from this network. The UM testnet speaks the same public
	// depth protocol and is what the adapter already uses as the UM test host. The recorded
	// symbol is BCHUSDT.
	const ch = "bchusdt@depth@100ms"
	m := umMarket(t)
	rest := umREST(t)
	call, err := rest.Depth(&symbol.Info{
		ExchTicker: "BCHUSDT",
		Ticker:     symbol.UniversalTicker{Venue: binance.VenueUM},
	}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	req := call.Req
	req.Base = "https://testnet.binancefuture.com"
	// The closing depth is taken after the socket stops, so its lastUpdateId is a little ahead of
	// the last frame. Keep a session only when the folded book still matches that snapshot.
	var last string
	for attempt := 1; attempt <= 12; attempt++ {
		f := createFixture(t, path)
		err = conformance.RecordMarket(ctx, conformance.MarketRecording{
			Venue: binance.VenueUM, Market: m, Route: "public",
			Channels:      []string{ch},
			Override:      "wss://stream.binancefuture.com",
			Dial:          dialWS,
			Out:           f,
			Duration:      4 * time.Second,
			Snapshots:     []conformance.Snapshot{{Ch: ch, Req: req}},
			SnapshotAfter: 3,
			Doer:          liveDoer{},
			Note:          "testnet BinanceUM BCHUSDT depth@100ms and REST depth limit 1000",
		})
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if last = binanceFixtureProblem(t); last == "" {
			t.Logf("wrote %s on attempt %d", path, attempt)
			return
		}
		t.Logf("attempt %d: %s", attempt, last)
	}
	t.Fatalf("no attempt matched the closing snapshot: %s", last)
}

func recordBybitBook(t *testing.T, ctx context.Context, path string) {
	t.Helper()
	reg, err := exchange.NewRegistry(bybit.Declare())
	if err != nil {
		t.Fatal(err)
	}
	v, ok := reg.Venue(bybit.Name)
	if !ok || v.Market == nil {
		t.Fatal("no bybit market")
	}
	f := createFixture(t, path)
	defer f.Close()
	const ch = "orderbook.50.BTCUSDT"
	err = conformance.RecordMarket(ctx, conformance.MarketRecording{
		Venue: bybit.Name, Market: v.Market, Route: "linear",
		Channels: []string{ch},
		Dial:     dialWS,
		Out:      f,
		Duration: 8 * time.Second,
		Note:     "mainnet Bybit linear BTCUSDT orderbook.50, stream snapshot only",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", path)
}

func createFixture(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func umREST(t *testing.T) exchange.PublicREST {
	t.Helper()
	reg, err := exchange.NewRegistry(binance.Declare())
	if err != nil {
		t.Fatal(err)
	}
	v, ok := reg.Venue(binance.VenueUM)
	if !ok || v.REST == nil {
		t.Fatal("no binance um rest")
	}
	return v.REST
}

// dialWS is the recorder's websocket. Protocol pings are answered by the library.
func dialWS(ctx context.Context, url string) (conformance.FrameConn, error) {
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	c.SetReadLimit(8 << 20)
	return wsFrame{c}, nil
}

type wsFrame struct{ c *websocket.Conn }

func (w wsFrame) Read(ctx context.Context) ([]byte, error) {
	_, b, err := w.c.Read(ctx)
	return b, err
}

func (w wsFrame) Write(ctx context.Context, frame []byte) error {
	return w.c.Write(ctx, websocket.MessageText, frame)
}

func (w wsFrame) Close() error {
	return w.c.Close(websocket.StatusNormalClosure, "")
}

// liveDoer performs the REST depth the recorder asks for. The fixture stores the body.
type liveDoer struct{}

func (liveDoer) Do(ctx context.Context, req *exchange.HTTPRequest) (*exchange.HTTPResponse, error) {
	u := req.Base + req.Path
	if len(req.Query) > 0 {
		u += "?" + string(req.Query)
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var hs []exchange.Header
	for k, vs := range resp.Header {
		for _, v := range vs {
			hs = append(hs, exchange.Header{Name: k, Value: v})
		}
	}
	return &exchange.HTTPResponse{Status: resp.StatusCode, Header: hs, Body: body}, nil
}
