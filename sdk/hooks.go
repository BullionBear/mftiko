package sdk

import (
	"github.com/BullionBear/mftiko/sdk/internal/mdpath"
	"github.com/BullionBear/mftiko/sdk/internal/tdpath"
)

// tdHooks installs a callback only when the strategy implements that handler.
// tdpath updates the ledger before the callback, including when the strategy has
// no OnPosition or OnBalance (D853, D855).
func (p *plane) tdHooks() tdpath.Hooks {
	var h tdpath.Hooks
	if s, ok := p.strat.(PositionHandler); ok {
		h.Position = func(ev tdpath.PositionEvent) {
			p.inHook("OnPosition", func() {
				s.OnPosition(p.sc, PositionEvent{TD: tdOf(ev.TD), Positions: ev.Positions})
			})
		}
	}
	if s, ok := p.strat.(BalanceHandler); ok {
		h.Balance = func(ev tdpath.BalanceEvent) {
			p.inHook("OnBalance", func() {
				s.OnBalance(p.sc, BalanceEvent{TD: tdOf(ev.TD), Balances: ev.Balances})
			})
		}
	}
	if s, ok := p.strat.(ReconHandler); ok {
		h.Recon = func(ev tdpath.ReconEvent) {
			p.inHook("OnRecon", func() {
				s.OnRecon(p.sc, ReconEvent{TD: tdOf(ev.TD), Recon: ev.Recon})
			})
		}
	}
	if s, ok := p.strat.(OrderHandler); ok {
		h.Order = func(ev tdpath.OrderEvent) {
			p.inHook("OnOrder", func() {
				s.OnOrder(p.sc, OrderEvent{
					TD:     tdOf(ev.TD),
					Origin: Origin(ev.Origin),
					Order:  ev.Order,
					Cause:  ev.Cause,
					Code:   ev.Code,
					Reason: ev.Reason,
				})
			})
		}
	}
	if s, ok := p.strat.(FillHandler); ok {
		h.Fill = func(ev tdpath.FillEvent) {
			p.inHook("OnFill", func() {
				s.OnFill(p.sc, FillEvent{TD: tdOf(ev.TD), Fill: ev.Fill})
			})
		}
	}
	if s, ok := p.strat.(RejectHandler); ok {
		h.Reject = func(ev tdpath.RejectEvent) {
			p.inHook("OnReject", func() {
				s.OnReject(p.sc, RejectEvent{
					TD:             tdOf(ev.TD),
					Request:        RejectedRequest(ev.Request),
					ClientOrderIDs: ev.ClientOrderIDs,
					Code:           ev.Code,
					Reason:         ev.Reason,
				})
			})
		}
	}
	return h
}

// mdHooks installs a callback only for a topic the strategy implements. A nil
// callback makes mdpath skip that topic before decoding it (D805). orderbook_delta
// has no hook here; the local book is E8-15, so the book callback stays nil.
func (p *plane) mdHooks() mdpath.Hooks {
	var h mdpath.Hooks
	if s, ok := p.strat.(BestQuoteHandler); ok {
		h.BestQuote = func(ev mdpath.BestQuote) {
			p.inHook("OnBestQuote", func() { s.OnBestQuote(p.sc, BestQuote{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	if s, ok := p.strat.(TradeHandler); ok {
		h.Trade = func(ev mdpath.Trade) {
			p.inHook("OnTrade", func() { s.OnTrade(p.sc, Trade{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	if s, ok := p.strat.(AggTradeHandler); ok {
		h.AggTrade = func(ev mdpath.AggTrade) {
			p.inHook("OnAggTrade", func() { s.OnAggTrade(p.sc, AggTrade{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	if s, ok := p.strat.(KlineHandler); ok {
		h.Kline = func(ev mdpath.Kline) {
			p.inHook("OnKline", func() { s.OnKline(p.sc, Kline{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	if s, ok := p.strat.(TickerHandler); ok {
		h.Ticker = func(ev mdpath.Ticker) {
			p.inHook("OnTicker", func() { s.OnTicker(p.sc, Ticker{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	if s, ok := p.strat.(LiquidationHandler); ok {
		h.Liquidation = func(ev mdpath.Liquidation) {
			p.inHook("OnLiquidation", func() { s.OnLiquidation(p.sc, Liquidation{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	if s, ok := p.strat.(FundingRateHandler); ok {
		h.FundingRate = func(ev mdpath.FundingRate) {
			p.inHook("OnFundingRate", func() { s.OnFundingRate(p.sc, FundingRate{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	if s, ok := p.strat.(OpenInterestHandler); ok {
		h.OpenInterest = func(ev mdpath.OpenInterest) {
			p.inHook("OnOpenInterest", func() { s.OnOpenInterest(p.sc, OpenInterest{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	if s, ok := p.strat.(GreeksHandler); ok {
		h.Greeks = func(ev mdpath.Greeks) {
			p.inHook("OnGreeks", func() { s.OnGreeks(p.sc, Greeks{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	if s, ok := p.strat.(MarkPriceHandler); ok {
		h.MarkPrice = func(ev mdpath.MarkPrice) {
			p.inHook("OnMarkPrice", func() { s.OnMarkPrice(p.sc, MarkPrice{MD: mdOf(ev.MD), Data: ev.Data}) })
		}
	}
	return h
}

func (p *plane) inHook(name string, fn func()) {
	p.hook.Store(name)
	defer p.hook.Store("")
	fn()
}

func tdOf(t tdpath.TD) TD {
	return TD{APIID: t.APIID, Incarnation: t.Incarnation, Seq: t.Seq}
}

func mdOf(m mdpath.MD) MD {
	return MD{UID: m.UID, MdTsNs: m.MdTsNs, ConnEpoch: m.ConnEpoch, Seq: m.Seq}
}
