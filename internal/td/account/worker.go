package account

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"

	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	tdctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/tdctl/v1"
	"github.com/BullionBear/mftiko/exchange"
)

// Worker is one api_id's account: its connections, its recons, and the state it reports.
type Worker struct {
	cfg    ServeConfig
	log    zerolog.Logger
	acct   exchange.Account
	report func(*tdctlv1.Report) error
	now    func() time.Time

	retryAfter, retryMax, readyTimeout time.Duration
	onOrders                           func(up bool)

	book *book

	events chan linkEvent

	mu        sync.Mutex
	ordersUp  bool
	orderConn *conn

	// Owned by the goroutine in Start, then Run.
	ctx       context.Context
	eventsUp  bool
	needRecon bool
	// swapWait is set when an events connection finishes a planned swap, and cleared when the recon
	// that follows it starts. A swap that lands while a snapshot is already being read is not covered
	// by that snapshot (E06-D035).
	swapWait     bool
	state        tdv1.AccountState
	reason       string
	reconciledAt int64
}

func newWorker(cfg ServeConfig, log zerolog.Logger, acct exchange.Account, bk *book, report func(*tdctlv1.Report) error) *Worker {
	w := &Worker{
		cfg: cfg, log: log, acct: acct, report: report, now: cfg.Now, book: bk,
		retryAfter: cfg.RetryAfter, retryMax: cfg.RetryMax, readyTimeout: cfg.ReadyTimeout,
		onOrders: cfg.onOrders,
		events:   make(chan linkEvent, 16), needRecon: true,
	}
	if w.now == nil {
		w.now = time.Now
	}
	if w.retryAfter <= 0 {
		w.retryAfter = time.Second
	}
	if w.retryMax <= 0 {
		w.retryMax = 30 * time.Second
	}
	if w.retryMax < w.retryAfter {
		w.retryMax = w.retryAfter
	}
	if w.readyTimeout <= 0 {
		w.readyTimeout = 10 * time.Second
	}
	return w
}

// Start opens the connections and runs until the first recon has succeeded: the account is READY,
// and a Report has said so. It fails with ReasonAuth when the venue refuses the credentials, and
// with ReasonHedgeMode when the account is in hedge mode (E06-D007). Dials and recons that fail
// otherwise are retried until ctx ends: the shim's Start deadline bounds them.
//
// The connections it opens live until ctx ends.
func (w *Worker) Start(ctx context.Context) error {
	w.ctx = ctx
	links, err := w.links()
	if err != nil {
		return err
	}
	go w.queries(ctx)
	for _, l := range links {
		go l.run(ctx)
	}
	return w.loop(ctx, true)
}

// Run keeps the account until ctx ends: it reports DEGRADED while the events connection is down,
// recons once it is back, and reports READY. It ends with ReasonAuth
// when the venue refuses the credentials on a reconnect.
func (w *Worker) Run(ctx context.Context) error {
	w.ctx = ctx
	err := w.loop(ctx, false)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// links builds the connections the account needs (see the package doc).
func (w *Worker) links() ([]*link, error) {
	ts, err := w.acct.NewTradeStream()
	switch {
	case errors.Is(err, exchange.ErrUnsupported):
		ts = nil
	case err != nil:
		return nil, err
	}
	trade := func() (*codec, error) {
		t, err := w.acct.NewTradeStream()
		if err != nil {
			return nil, err
		}
		return tradeCodec(t), nil
	}
	private := func() (*codec, error) {
		s, err := w.acct.NewStream()
		if err != nil {
			return nil, err
		}
		return privateCodec(s), nil
	}
	if ts != nil && ts.Limits().Events {
		return []*link{{w: w, role: roleEvents, orders: true, name: "trade", open: trade}}, nil
	}
	out := []*link{{w: w, role: roleEvents, name: "private", open: private}}
	if ts != nil {
		out = append(out, &link{w: w, role: roleOrders, orders: true, name: "trade", open: trade})
	}
	return out, nil
}

// loop is the account's state machine. starting: return once the first recon has succeeded.
func (w *Worker) loop(ctx context.Context, starting bool) error {
	backoff := w.retryAfter
	var retry <-chan time.Time
	for {
		if w.needRecon && w.eventsUp && retry == nil {
			// This read starts now. A swap that already finished is covered by it; one that finishes
			// while it runs sets swapWait again and needs its own (E06-D035).
			w.swapWait = false
			w.book.beginRecon(ctx)
			snap, err := w.recon(ctx)
			switch {
			case err == nil:
				backoff = w.retryAfter
				if err := w.drain(starting); err != nil {
					return err
				}
				// Dropped while this recon ran: the snapshot has no live stream after it, so the next
				// recon replaces it. A planned swap that finished during the read is the same. This
				// AsOf can predate that gap (E06-D035).
				if !w.eventsUp || w.swapWait {
					if w.swapWait {
						w.needRecon = true
					}
					continue
				}
				w.needRecon = false
				if starting && snap.HedgeMode {
					return errors.New(ReasonHedgeMode)
				}
				w.apply(ctx, snap)
				if starting {
					return nil
				}
			case errors.Is(err, exchange.ErrAuth):
				return errors.New(ReasonAuth)
			case ctx.Err() != nil:
				return ctx.Err()
			default:
				wait := backoff
				if rl := (*exchange.RateLimited)(nil); errors.As(err, &rl) && rl.RetryAfterMs > 0 {
					wait = max(wait, time.Duration(rl.RetryAfterMs)*time.Millisecond)
				}
				w.log.Warn().Err(err).Dur("retry_in", wait).Msg("recon")
				retry = time.After(wait)
				backoff = min(2*backoff, w.retryMax)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-retry:
			retry = nil
		case e := <-w.events:
			if err := w.linkEvent(e, starting); err != nil {
				return err
			}
			if !e.up {
				retry = nil
				backoff = w.retryAfter
			}
		}
	}
}

// linkEvent takes a connection's coming up or going down.
func (w *Worker) linkEvent(e linkEvent, starting bool) error {
	if e.err != nil {
		return errors.New(ReasonAuth)
	}
	if e.swap {
		// The new connection is already up. Only the events role recons, and it does not report
		// DEGRADED: the role never went down (E06-D035, E06-D036).
		if e.role == roleEvents {
			w.needRecon = true
			w.swapWait = true
		}
		return nil
	}
	if e.orders {
		w.mu.Lock()
		w.ordersUp = e.up
		w.mu.Unlock()
		if w.onOrders != nil {
			w.onOrders(e.up)
		}
	}
	if e.role != roleEvents {
		return nil
	}
	w.eventsUp = e.up
	w.book.setEvents(w.ctx, e.up)
	w.needRecon = true
	w.mu.Lock()
	state, reason := w.state, w.reason
	w.mu.Unlock()
	if !e.up && !starting && !(state == tdv1.AccountState_ACCOUNT_STATE_DEGRADED && reason == ReasonStreamLost) {
		w.set(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
	}
	return nil
}

// drain takes the connection events already queued.
func (w *Worker) drain(starting bool) error {
	for {
		select {
		case e := <-w.events:
			if err := w.linkEvent(e, starting); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// recon reads the whole account (E06-D003): every Call Snapshot returns, each to its last page. AsOfNs
// is the time before the first request went out.
func (w *Worker) recon(ctx context.Context) (*exchange.AccountSnapshot, error) {
	asOf := w.now()
	calls, err := w.acct.Snapshot(asOf)
	if err != nil {
		return nil, err
	}
	out := &exchange.AccountSnapshot{AsOfNs: asOf.UnixNano()}
	for _, c := range calls {
		pages, err := exchange.Collect(ctx, w.cfg.Doer, w.now, c)
		if err != nil {
			return nil, err
		}
		for i := range pages {
			p := &pages[i]
			out.OpenOrders = append(out.OpenOrders, p.OpenOrders...)
			out.Positions = append(out.Positions, p.Positions...)
			out.Balances = append(out.Balances, p.Balances...)
			out.HedgeMode = out.HedgeMode || p.HedgeMode
		}
	}
	return out, nil
}

// apply hands a recon to the book, which publishes it, and reports the state it leaves the account
// in.
func (w *Worker) apply(ctx context.Context, snap *exchange.AccountSnapshot) {
	w.book.applyRecon(ctx, snap)
	w.reconciledAt = snap.AsOfNs
	// The position mode is checked at Start only: nobody changes it while a worker runs (E06-D016).
	w.set(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	w.log.Info().Int("open_orders", len(snap.OpenOrders)).Int("positions", len(snap.Positions)).Msg("recon")
}

// set reports the account state. A recon always reports, since reconciled_at changed.
func (w *Worker) set(state tdv1.AccountState, reason string) {
	w.mu.Lock()
	w.state, w.reason = state, reason
	w.mu.Unlock()
	w.book.setState(w.ctx, state, reason)
	if err := w.report(&tdctlv1.Report{State: state, Reason: reason, ReconciledAtNs: w.reconciledAt, FirstSubmitNs: w.book.firstCopy()}); err != nil {
		w.log.Warn().Err(err).Msg("report")
	}
}

// noteState copies a state the book decided (a query failure, or the recovery from one) so the
// connection loop does not report over it by mistake.
func (w *Worker) noteState(state tdv1.AccountState, reason string) {
	w.mu.Lock()
	w.state, w.reason = state, reason
	w.mu.Unlock()
}

func (w *Worker) post(ctx context.Context, e linkEvent) {
	select {
	case w.events <- e:
	case <-ctx.Done():
	}
}
