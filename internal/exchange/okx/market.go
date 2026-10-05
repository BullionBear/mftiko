package okx

import "github.com/BullionBear/mftiko/internal/exchange"

type market struct{}

var _ exchange.Market = (*market)(nil)

func (m *market) Topics() []exchange.TopicCaps { return nil }

// Atom: channel is "<channel>:<instId>", e.g. "trades:BTC-USDT-SWAP", "books:BTC-USDT". Candle
// channels go to the business route.
func (m *market) Atom(exchange.Topic, exchange.Symbol) (exchange.AtomRef, error) {
	return exchange.AtomRef{}, exchange.ErrNotImplemented
}

func (m *market) Routes() []string { return []string{"public", "business"} }

// Budget: OKX publishes no per-connection channel cap.
func (m *market) Budget(string) int { return 0 }

func (m *market) Cost(string) int { return 1 }

func (m *market) Endpoint(route, override string) (string, error) {
	return "", exchange.ErrNotImplemented
}

func (m *market) NewSession(route string) (exchange.Session, error) {
	return nil, exchange.ErrNotImplemented
}

// Book: SnapshotStream, seqId/prevSeqId, and a BookChecksum.
func (m *market) Book(channel string) (exchange.BookSync, bool) { return nil, false }
