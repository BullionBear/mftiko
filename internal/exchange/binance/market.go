package binance

import (
	"github.com/BullionBear/mftiko/internal/exchange"
	"github.com/BullionBear/mftiko/internal/symbol"
)

// market is the combined-stream codec shared by spot, UM, CM and EO.
type market struct{ p product }

var _ exchange.Market = (*market)(nil)

func (m *market) Topics() []exchange.TopicCaps { return nil }

// Atom: channel is the stream name built from the resolved ExchTicker, e.g. btcusdt@aggTrade,
// btcusdt@depth@100ms. On UM the stream type picks the route (public for bookTicker and depth,
// market for the rest).
func (m *market) Atom(exchange.Topic, *symbol.Info) (exchange.AtomRef, error) {
	return exchange.AtomRef{}, exchange.ErrNotImplemented
}

func (m *market) Routes() []string { return m.p.routes }

func (m *market) Budget(string) int { return m.p.budget }

// Cost: Binance counts streams.
func (m *market) Cost(string) int { return 1 }

func (m *market) Endpoint(route, override string) (string, error) {
	return "", exchange.ErrNotImplemented
}

func (m *market) NewSession(route string) (exchange.Session, error) {
	return nil, exchange.ErrNotImplemented
}

// Book: depth channels are SnapshotREST with U/u (spot) or pu (futures) chaining.
func (m *market) Book(channel string) (exchange.BookSync, bool) { return nil, false }
