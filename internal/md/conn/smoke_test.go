package conn

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
	"github.com/BullionBear/mftiko/exchange/binance"
)

// TestSmokeBinanceUM dials the public USD-M aggTrade stream. No API key. It is not part of
// go test: set MFTIKO_MD_SMOKE=1. One event is enough; the process then stops.
func TestSmokeBinanceUM(t *testing.T) {
	if os.Getenv("MFTIKO_MD_SMOKE") != "1" {
		t.Skip("set MFTIKO_MD_SMOKE=1 to dial BinanceUM public aggTrade")
	}
	const subject = "md.f.smoke.aggtrade.BinanceUM_Perp_BTCUSDT"
	var logBuf bytes.Buffer
	log := zerolog.New(&logBuf)
	got := make(chan []byte, 1)
	c := New(Config{
		Venue: binance.VenueUM, Route: "market", Instance: "smoke",
		Market: umMarket(t),
		Log:    &log,
		Publish: func(subj string, msg []byte) error {
			if subj != subject {
				t.Errorf("subject %s", subj)
				return nil
			}
			select {
			case got <- bytes.Clone(msg):
			default:
			}
			return nil
		},
	})
	c.SetDesired(desired(1, 1, atom("BinanceUM:market:btcusdt@aggTrade", "aggtrade.BinanceUM_Perp_BTCUSDT")))
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()

	select {
	case msg := <-got:
		cancel()
		_, epoch, seq, _, payload, err := envelope.Parse(msg)
		if err != nil {
			t.Fatal(err)
		}
		ev := &mdv1.FeedEvent{}
		if err := proto.Unmarshal(payload, ev); err != nil {
			t.Fatal(err)
		}
		tr := ev.GetAggTrade()
		if epoch != 1 || seq != 1 || tr.GetTradeId() == "" || tr.GetPrice() <= 0 {
			t.Fatalf("epoch %d seq %d trade %+v", epoch, seq, tr)
		}
	case err := <-errc:
		t.Fatalf("run ended: %v\n%s", err, logBuf.String())
	case <-ctx.Done():
		t.Fatalf("no aggTrade\n%s", logBuf.String())
	}
}
