package binance

import (
	"time"

	"github.com/BullionBear/mftiko/exchange"
)

func accountFactory(k kind) exchange.AccountFactory {
	return func(cfg exchange.AccountConfig) (exchange.Account, error) {
		return &account{k: k, cfg: cfg}, nil
	}
}

// account serves every Binance account kind. The kind picks the API family: api (spot), fapi (UM),
// dapi (CM), eapi (EO); BinancePM uses papi and picks /papi/v1/margin, /um or /cm by the ticker's
// venue. Only BinanceUM is implemented (E3-07); the key is parsed once, in signer.
type account struct {
	k   kind
	cfg exchange.AccountConfig
	signerState
}

var (
	_ exchange.Account            = (*account)(nil)
	_ exchange.CancelOnDisconnect = (*account)(nil)
)

func (a *account) Limits() exchange.AccountLimits {
	if a.k.name != kindUM.name {
		return exchange.AccountLimits{}
	}
	return exchange.AccountLimits{
		ClientOrderIDMaxLen: cloidMax,
		CancelBatch:         10,
		OrderRate: []exchange.RateLimit{
			{N: 300, Per: 10 * time.Second},
			{N: 1200, Per: time.Minute},
		},
	}
}

func (a *account) NewStream() (exchange.PrivateStream, error) {
	if err := a.um(); err != nil {
		return nil, err
	}
	if _, err := a.signer(); err != nil {
		return nil, err
	}
	return &privateStream{rest: a.restBase(), ws: a.privateBase(), key: a.cfg.Credentials.Key}, nil
}

// NewTradeStream is the USD-M WebSocket API. Spot's logon is a later issue (E3-10); until then a
// non-UM kind has no trade socket. HMAC is refused: this adapter signs with ed25519 only.
func (a *account) NewTradeStream() (exchange.TradeStream, error) {
	if err := a.um(); err != nil {
		return nil, err
	}
	sig, err := a.signer()
	if err != nil {
		return nil, err
	}
	return &tradeStream{url: a.tradeURL(), key: a.cfg.Credentials.Key, sig: sig}, nil
}
