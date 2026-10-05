package bybit

import "github.com/BullionBear/mftiko/internal/exchange"

type market struct{}

var _ exchange.Market = (*market)(nil)

func (m *market) Topics() []exchange.TopicCaps { return nil }

// Atom: Route is the Symbol's category, Channel the Bybit topic verbatim ("publicTrade.BTCUSDT",
// "orderbook.50.BTCUSDT").
func (m *market) Atom(exchange.Topic, exchange.Symbol) (exchange.AtomRef, error) {
	return exchange.AtomRef{}, exchange.ErrNotImplemented
}

func (m *market) Routes() []string { return []string{"spot", "linear", "inverse", "option"} }

// Budget and Cost: characters of args per public connection.
func (m *market) Budget(string) int { return 21000 }

func (m *market) Cost(channel string) int { return len(channel) }

func (m *market) Endpoint(route, override string) (string, error) {
	return "", exchange.ErrNotImplemented
}

func (m *market) NewSession(route string) (exchange.Session, error) {
	return nil, exchange.ErrNotImplemented
}

// Book: SnapshotStream; "snapshot" replaces, "delta" chains on u.
func (m *market) Book(channel string) (exchange.BookSync, bool) { return nil, false }
