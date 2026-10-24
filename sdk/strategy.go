package sdk

// Strategy is the Strategy Actor a session worker hosts (DECISIONS E00-D001). Hooks run one at a time on a
// single dispatcher, so a strategy needs no locks (E08-D003).
//
// Before a hook runs, the SDK has applied its event to the local state: the book, the orders and the
// ledger that Context reads already include it (DECISIONS E08-D005, E08-D053–E08-D056). A strategy never sees the
// SDK sync or resync that state.
type Strategy interface {
	// OnInit runs right after spawn, before market data or accounts are ready (E01-D007). Parse params,
	// resolve the instruments the strategy trades (Context.Resolve), read state from the volume or
	// inputs (E09-D004). An error ends the session as INIT_FAILED.
	OnInit(ctx Context) error
	// OnStart runs once every MD and TD lease is READY (E01-D007). Orders may be placed from here on.
	OnStart(ctx Context) error
	// OnStop runs when the session ends as DONE, FAILED or CRASHED, but not after HUNG (PROTOCOL §5.3).
	// On DONE and FAILED the TD events already received are delivered first (E08-D003).
	OnStop(ctx Context)
}

// Optional hooks: a strategy implements the ones it needs. Market data of a topic no hook takes is
// skipped before it is decoded.
type (
	BestQuoteHandler interface {
		OnBestQuote(ctx Context, ev BestQuote)
	}
	TradeHandler interface {
		OnTrade(ctx Context, ev Trade)
	}
	AggTradeHandler interface {
		OnAggTrade(ctx Context, ev AggTrade)
	}
	KlineHandler interface {
		OnKline(ctx Context, ev Kline)
	}
	TickerHandler interface {
		OnTicker(ctx Context, ev Ticker)
	}
	LiquidationHandler interface {
		OnLiquidation(ctx Context, ev Liquidation)
	}
	FundingRateHandler interface {
		OnFundingRate(ctx Context, ev FundingRate)
	}
	OpenInterestHandler interface {
		OnOpenInterest(ctx Context, ev OpenInterest)
	}
	GreeksHandler interface {
		OnGreeks(ctx Context, ev Greeks)
	}
	MarkPriceHandler interface {
		OnMarkPrice(ctx Context, ev MarkPrice)
	}

	// BookHandler is called after an orderbook_delta was applied to the local book. Deltas are applied
	// however old they are; the hook is called only for one within recv_window_ms (E08-D003).
	BookHandler interface {
		OnBook(ctx Context, ev BookUpdate)
	}

	// OrderHandler receives every change of the strategy's own orders, including a Submit refused
	// locally and a Submit whose answer timed out (E08-D005).
	OrderHandler interface {
		OnOrder(ctx Context, ev OrderEvent)
	}
	// FillHandler receives fills of the strategy's own orders.
	FillHandler interface {
		OnFill(ctx Context, ev FillEvent)
	}
	// PositionHandler and BalanceHandler receive the account's changes; they are not per algo.
	PositionHandler interface {
		OnPosition(ctx Context, ev PositionEvent)
	}
	BalanceHandler interface {
		OnBalance(ctx Context, ev BalanceEvent)
	}
	// ReconHandler receives the account snapshot: once after OnStart, after every private-connection
	// reconnect or account worker incarnation, and as the answer to Context.RequestRecon (E06-D003).
	ReconHandler interface {
		OnRecon(ctx Context, ev ReconEvent)
	}
	// RejectHandler receives cancel requests refused locally (E08-D005). Strategies do not handle
	// availability (E08-D001): a broken TD shows up as rejects and, later, a new Recon.
	RejectHandler interface {
		OnReject(ctx Context, ev RejectEvent)
	}
)
