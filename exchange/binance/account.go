package binance

import (
	"fmt"
	"time"

	"github.com/BullionBear/mftiko/exchange"
)

func accountFactory(k kind) exchange.AccountFactory {
	return func(cfg exchange.AccountConfig) (exchange.Account, error) {
		if _, err := cfg.Window(); err != nil {
			return nil, err
		}
		a := &account{k: k, cfg: cfg}
		if k.name == kindUM.name {
			return &umAccount{account: a}, nil
		}
		return a, nil
	}
}

// account serves every Binance account kind. The kind picks the API family: api (spot), fapi (UM),
// dapi (CM), eapi (EO); BinancePM uses papi and picks /papi/v1/margin, /um or /cm by the ticker's
// venue. Spot and USD-M are implemented. The key is parsed once, in signer.
type account struct {
	k   kind
	cfg exchange.AccountConfig
	signerState
}

// umAccount is the USD-M account. Only it has a dead-man switch; spot does not.
type umAccount struct{ *account }

var (
	_ exchange.Account            = (*account)(nil)
	_ exchange.Account            = (*umAccount)(nil)
	_ exchange.CancelOnDisconnect = (*umAccount)(nil)
)

func (a *account) Limits() exchange.AccountLimits {
	switch a.k.name {
	case kindUM.name:
		return exchange.AccountLimits{
			ClientOrderIDMaxLen: cloidMax,
			CancelBatch:         10,
			OrderRate: []exchange.RateLimit{
				{N: 300, Per: 10 * time.Second, Bucket: orders10s},
				{N: 1200, Per: time.Minute, Bucket: orders1m},
			},
		}
	case kindSpot.name:
		return exchange.AccountLimits{
			ClientOrderIDMaxLen: cloidMax,
			CancelBatch:         1,
			OrderRate: []exchange.RateLimit{
				{N: 100, Per: 10 * time.Second, Bucket: orders10s},
				{N: 200000, Per: 24 * time.Hour, Bucket: orders1d},
			},
		}
	default:
		return exchange.AccountLimits{}
	}
}

func (a *account) NewStream() (exchange.PrivateStream, error) {
	switch a.k.name {
	case kindUM.name:
		if _, err := a.signer(); err != nil {
			return nil, err
		}
		return &privateStream{rest: a.restBase(), ws: a.privateBase(), key: a.cfg.Credentials.Key}, nil
	case kindSpot.name:
		return nil, fmt.Errorf("binance: spot events arrive on the trade stream: %w", exchange.ErrUnsupported)
	default:
		return nil, exchange.ErrNotImplemented
	}
}

// NewTradeStream is the WebSocket API. USD-M does not carry private events; spot does, after
// userDataStream.subscribe (E03-D003). HMAC is refused: this adapter signs with ed25519 only.
func (a *account) NewTradeStream() (exchange.TradeStream, error) {
	sig, err := a.signer()
	if err != nil {
		return nil, err
	}
	w, err := a.cfg.Window()
	if err != nil {
		return nil, err
	}
	switch a.k.name {
	case kindUM.name:
		return &tradeStream{url: a.tradeURL(), key: a.cfg.Credentials.Key, sig: sig, window: w.Milliseconds()}, nil
	case kindSpot.name:
		return &spotTrade{url: a.tradeURL(), key: a.cfg.Credentials.Key, sig: sig, window: w.Milliseconds()}, nil
	default:
		return nil, exchange.ErrNotImplemented
	}
}
