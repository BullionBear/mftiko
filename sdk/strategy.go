package sdk

// Strategy is the Strategy Actor a session worker hosts (DECISIONS D001). Hooks run one at a time on
// a single dispatcher, so a strategy needs no locks (D803).
type Strategy interface {
	// OnInit runs right after spawn, before market data or accounts are ready (D107).
	// Parse params, load models, read state from the volume or inputs (D904).
	OnInit(ctx Context) error
	// OnStart runs once every MD and TD lease is READY (D107). Orders may be placed from here on.
	OnStart(ctx Context) error
	// OnStop runs on a normal stop, a crash or a fail, but not after HUNG (PROTOCOL §5.3).
	OnStop(ctx Context)
}

// Optional hooks: a strategy implements the ones it needs.
type (
	// FeedHandler receives market data that passed the recv_window_ms prehook (D803).
	FeedHandler interface {
		OnFeed(ctx Context, ev *FeedEvent)
	}
	// ReconHandler receives the account snapshot: once after OnStart, then after every
	// private-connection reconnect or account worker incarnation (D603).
	ReconHandler interface {
		OnRecon(ctx Context, st *ReconState)
	}
	// OrderHandler receives order updates for this strategy's algo_ids.
	OrderHandler interface {
		OnOrder(ctx Context, ev *OrderEvent)
	}
	// FillHandler receives fills for this strategy's algo_ids.
	FillHandler interface {
		OnFill(ctx Context, ev *FillEvent)
	}
	// RejectHandler receives order and cancel rejects, including local 1xx rejects (D801, D603).
	RejectHandler interface {
		OnReject(ctx Context, ev *RejectEvent)
	}
)
