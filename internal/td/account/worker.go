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

// Ledger receives the account's state, in the order the worker learns it: each recon whole, and
// each private event as it arrives. Calls never overlap. What it is handed is valid only during the
// call (D302). E6-07's OMS and ledger implement it.
//
// Events from a connection that is being replaced (MaxLifetime) may arrive twice, once from each
// connection; events between a connection's Ready and the recon that follows arrive before that
// recon.
type Ledger interface {
	Recon(s *exchange.AccountSnapshot)
	Order(e *exchange.OrderUpdate)
	Fill(e *exchange.Fill)
	Balances(e []exchange.Balance)
	Positions(e []exchange.Position)
}

type nopLedger struct{}

func (nopLedger) Recon(*exchange.AccountSnapshot) {}
func (nopLedger) Order(*exchange.OrderUpdate)     {}
func (nopLedger) Fill(*exchange.Fill)             {}
func (nopLedger) Balances([]exchange.Balance)     {}
func (nopLedger) Positions([]exchange.Position)   {}

// Worker is one api_id's account: its connections, its recons, and the state it reports.
type Worker struct {
	cfg    ServeConfig
	log    zerolog.Logger
	acct   exchange.Account
	report func(*tdctlv1.Report) error
	now    func() time.Time

	retryAfter, retryMax, readyTimeout time.Duration

	ledgerMu sync.Mutex
	ledger   Ledger

	events chan linkEvent

	// Owned by the goroutine in Start, then Run.
	eventsUp     bool
	needRecon    bool
	state        tdv1.AccountState
	reason       string
	reconciledAt int64
}

func newWorker(cfg ServeConfig, log zerolog.Logger, acct exchange.Account, report func(*tdctlv1.Report) error) *Worker {
	w := &Worker{
		cfg: cfg, log: log, acct: acct, report: report, now: cfg.Now, ledger: cfg.Ledger,
		retryAfter: cfg.RetryAfter, retryMax: cfg.RetryMax, readyTimeout: cfg.ReadyTimeout,
		events: make(chan linkEvent, 16), needRecon: true,
	}
	if w.now == nil {
		w.now = time.Now
	}
	if w.ledger == nil {
		w.ledger = nopLedger{}
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
// with ReasonHedgeMode when the account is in hedge mode (D607). Dials and recons that fail
// otherwise are retried until ctx ends: the shim's Start deadline bounds them.
//
// The connections it opens live until ctx ends.
func (w *Worker) Start(ctx context.Context) error {
	links, err := w.links()
	if err != nil {
		return err
	}
	for _, l := range links {
		go l.run(ctx)
	}
	return w.loop(ctx, true)
}

// Run keeps the account until ctx ends: it reports DEGRADED while the events connection is down,
// recons once it is back, and reports READY, or UNAVAILABLE in hedge mode. It ends with ReasonAuth
// when the venue refuses the credentials on a reconnect.
func (w *Worker) Run(ctx context.Context) error {
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
		return []*link{{w: w, role: roleEvents, name: "trade", open: trade}}, nil
	}
	out := []*link{{w: w, role: roleEvents, name: "private", open: private}}
	if ts != nil {
		out = append(out, &link{w: w, role: roleOrders, name: "trade", open: trade})
	}
	return out, nil
}

// loop is the account's state machine. starting: return once the first recon has succeeded.
func (w *Worker) loop(ctx context.Context, starting bool) error {
	backoff := w.retryAfter
	var retry <-chan time.Time
	for {
		if w.needRecon && w.eventsUp && retry == nil {
			snap, err := w.recon(ctx)
			switch {
			case err == nil:
				backoff = w.retryAfter
				w.needRecon = false
				if starting && snap.HedgeMode {
					return errors.New(ReasonHedgeMode)
				}
				w.apply(snap)
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
			if e.err != nil {
				return errors.New(ReasonAuth)
			}
			if e.role != roleEvents {
				continue
			}
			w.eventsUp = e.up
			w.needRecon = true
			if !e.up {
				retry = nil
				backoff = w.retryAfter
				if !starting {
					w.set(tdv1.AccountState_ACCOUNT_STATE_DEGRADED, ReasonStreamLost)
				}
			}
		}
	}
}

// recon reads the whole account (D603): every Call Snapshot returns, each to its last page. AsOfNs
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

// apply hands a recon to the ledger and reports the state it leaves the account in.
func (w *Worker) apply(snap *exchange.AccountSnapshot) {
	w.ledgerMu.Lock()
	w.ledger.Recon(snap)
	w.ledgerMu.Unlock()
	w.reconciledAt = snap.AsOfNs
	if snap.HedgeMode {
		w.set(tdv1.AccountState_ACCOUNT_STATE_UNAVAILABLE, ReasonHedgeMode)
		return
	}
	w.set(tdv1.AccountState_ACCOUNT_STATE_READY, "")
	w.log.Info().Int("open_orders", len(snap.OpenOrders)).Int("positions", len(snap.Positions)).Msg("recon")
}

// set reports the account state. A recon always reports, since reconciled_at changed.
func (w *Worker) set(state tdv1.AccountState, reason string) {
	w.state, w.reason = state, reason
	if err := w.report(&tdctlv1.Report{State: state, Reason: reason, ReconciledAtNs: w.reconciledAt}); err != nil {
		w.log.Warn().Err(err).Msg("report")
	}
}

// event hands one private event to the ledger. Called from the connections' read loops.
func (w *Worker) event(f func(Ledger)) {
	w.ledgerMu.Lock()
	f(w.ledger)
	w.ledgerMu.Unlock()
}

func (w *Worker) post(ctx context.Context, e linkEvent) {
	select {
	case w.events <- e:
	case <-ctx.Done():
	}
}
