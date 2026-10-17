package tdpath

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
)

const (
	// flushTimeout matches the session NATS client's flush.
	flushTimeout = 5 * time.Second

	// backoffMin is the first Recon retry. It doubles until backoffMax (D842, the same
	// schedule as D622). 16s doubled is 32s, which is past the cap, so the next wait is 30s.
	backoffMin = time.Second
	backoffMax = 30 * time.Second
)

var (
	// ErrUnknownAccount means apiID is not one of this session's accounts.
	ErrUnknownAccount = errors.New("tdpath: the account is not bound to this session")
	// ErrAlgoNotDeclared means the algo is not in that account's AccountDecl.algo_ids.
	ErrAlgoNotDeclared = errors.New("tdpath: the algo_id is not declared for this account")
	// ErrNotListening means Listen has not subscribed td.evt yet.
	ErrNotListening = errors.New("tdpath: td.evt is not subscribed yet")
	// ErrNoCancelBatch means ViewReply has not yet reported a positive cancel_batch.
	ErrNoCancelBatch = errors.New("tdpath: the account's cancel_batch is not known yet")
	// ErrStopping means the session has failed or Close has been called.
	ErrStopping = errors.New("tdpath: the session is stopping")

	errNotRecon = errors.New("tdpath: view reply is not a recon")
)

// Config opens a Path. NC is not closed by the path.
//
// Push runs each delivered event and each order reply as one hook. Nil uses an in-process
// queue that runs the func before Push returns, and a func pushed from inside a running
// func waits until the running one returns. The session passes the dispatcher's PushTD.
//
// Fail is called once when the hold cannot take another event. After is the Recon backoff
// clock; nil uses time.After. Now is the Submit clock; nil uses time.Now.
type Config struct {
	SessionID  string
	Accounts   []*sessionv1.AccountDecl
	RecvWindow time.Duration
	NC         *nats.Conn
	Push       func(fn func()) error
	Fail       func(reason string)
	Now        func() time.Time
	After      func(time.Duration) <-chan time.Time
	Hooks      Hooks
	Log        func(string)
}

// Path is one session's TD path. Close cancels it. Do not call Close from a hook: the
// caller waits until the account loops and the in-flight order rounds have returned.
type Path struct {
	mu          sync.Mutex
	sessionID   string
	recv        time.Duration
	nc          *nats.Conn
	byAPI       map[int64]*account
	accounts    []*account
	listening   bool
	subscribing bool
	failed      bool
	hooks       Hooks
	push        func(fn func()) error
	fail        func(string)
	nowFn       func() time.Time
	after       func(time.Duration) <-chan time.Time
	logf        func(string)
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	settled     atomic.Uint64
	failOnce    atomic.Bool
}

type account struct {
	decl      *sessionv1.AccountDecl
	algos     map[uint32]bool
	orders    map[string]*tdv1.Order
	submitGen map[string]uint64 // sentGen at the local Submit of that client id
	// sentGen advances once per ReconRequest attempt, before that attempt is published (D854).
	sentGen     uint64
	positions   map[uint32]*tdv1.Position
	balances    map[string]*tdv1.Balance
	hold        []*tdv1.AccountEvent
	holding     bool
	pending     bool
	asking      bool
	have        bool
	haveBatch   bool
	inc         uint32
	seq         uint64
	cancelBatch uint32
	wake        chan struct{}
	sub         *nats.Subscription
}

// Open checks the session and starts one loop per account. The loops wait until Recon
// or RequestRecon. recv window is InitPayload.recv_window_ms and must be positive:
// one attempt of ReconRequest, Submit, Cancel and BatchCancel waits that long.
func Open(cfg Config) (*Path, error) {
	if cfg.NC == nil {
		return nil, errors.New("tdpath: nil nats conn")
	}
	if cfg.SessionID == "" {
		return nil, errors.New("tdpath: empty session id")
	}
	if cfg.RecvWindow <= 0 {
		return nil, errors.New("tdpath: recv window must be positive")
	}
	by := make(map[int64]*account, len(cfg.Accounts))
	accs := make([]*account, 0, len(cfg.Accounts))
	for _, d := range cfg.Accounts {
		if d == nil {
			return nil, errors.New("tdpath: nil account")
		}
		id := d.GetApiId()
		if id <= 0 {
			return nil, fmt.Errorf("tdpath: api_id %d is not positive", id)
		}
		if _, ok := by[id]; ok {
			return nil, fmt.Errorf("tdpath: duplicate api_id %d", id)
		}
		algos := make(map[uint32]bool, len(d.GetAlgoIds()))
		for _, a := range d.GetAlgoIds() {
			algos[a] = true
		}
		acc := &account{
			decl:      d,
			algos:     algos,
			orders:    map[string]*tdv1.Order{},
			submitGen: map[string]uint64{},
			positions: map[uint32]*tdv1.Position{},
			balances:  map[string]*tdv1.Balance{},
			wake:      make(chan struct{}, 1),
		}
		by[id] = acc
		accs = append(accs, acc)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Path{
		sessionID: cfg.SessionID,
		recv:      cfg.RecvWindow,
		nc:        cfg.NC,
		byAPI:     by,
		accounts:  accs,
		hooks:     cfg.Hooks,
		push:      cfg.Push,
		fail:      cfg.Fail,
		nowFn:     cfg.Now,
		after:     cfg.After,
		logf:      cfg.Log,
		ctx:       ctx,
		cancel:    cancel,
	}
	if p.push == nil {
		q := &runq{}
		p.push = q.push
	}
	if p.after == nil {
		p.after = time.After
	}
	for _, acc := range accs {
		p.wg.Add(1)
		go func(a *account) {
			defer p.wg.Done()
			p.drive(a)
		}(acc)
	}
	return p, nil
}

// Close stops the account loops and drops the td.evt subscriptions. The NATS connection
// stays open.
func (p *Path) Close() {
	p.markFailed()
	p.wg.Wait()
	p.mu.Lock()
	var subs []*nats.Subscription
	for _, acc := range p.accounts {
		if acc.sub != nil {
			subs = append(subs, acc.sub)
			acc.sub = nil
		}
	}
	p.mu.Unlock()
	for _, s := range subs {
		_ = s.Unsubscribe()
	}
}

// Listen subscribes td.evt for every account and flushes, so a message published after
// Listen returns was not missed. Events are held until the account's Recon reply.
// Submit, Cancel, BatchCancel and RequestRecon wait for Listen.
func (p *Path) Listen() error {
	p.mu.Lock()
	if p.failed {
		p.mu.Unlock()
		return ErrStopping
	}
	if p.listening {
		p.mu.Unlock()
		return nil
	}
	if p.subscribing {
		p.mu.Unlock()
		return errors.New("tdpath: listen is already in progress")
	}
	p.subscribing = true
	// Holding before Subscribe, so a callback that runs during Subscribe is held.
	for _, acc := range p.accounts {
		acc.holding = true
	}
	p.mu.Unlock()

	subs, err := p.subscribeAll()
	if err != nil {
		p.mu.Lock()
		p.subscribing = false
		for _, acc := range p.accounts {
			acc.holding = false
			acc.hold = nil
		}
		p.mu.Unlock()
		return err
	}
	p.mu.Lock()
	p.subscribing = false
	if p.failed {
		p.mu.Unlock()
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
		return ErrStopping
	}
	for i, acc := range p.accounts {
		acc.sub = subs[i]
	}
	p.listening = true
	p.mu.Unlock()
	return nil
}

// Recon asks every account for a snapshot. Events stay held until that account's reply
// has been given to the Recon hook.
func (p *Path) Recon() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed {
		return ErrStopping
	}
	if !p.listening {
		return ErrNotListening
	}
	for _, acc := range p.accounts {
		acc.holding = true
		acc.pending = true
		p.wake(acc)
	}
	return nil
}

// RequestRecon asks one account. The reply is a Recon hook with reason REQUESTED.
// An ask already in flight keeps holding: this call arms another ask when it finishes.
func (p *Path) RequestRecon(apiID int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed {
		return ErrStopping
	}
	if !p.listening {
		return ErrNotListening
	}
	acc := p.byAPI[apiID]
	if acc == nil {
		return ErrUnknownAccount
	}
	acc.holding = true
	acc.pending = true
	p.wake(acc)
	return nil
}

func (p *Path) subscribeAll() ([]*nats.Subscription, error) {
	subs := make([]*nats.Subscription, 0, len(p.accounts))
	for _, acc := range p.accounts {
		apiID := acc.decl.GetApiId()
		sub, err := p.nc.Subscribe(evtSubject(apiID), func(m *nats.Msg) {
			p.onMsg(apiID, m)
		})
		if err != nil {
			for _, s := range subs {
				_ = s.Unsubscribe()
			}
			return nil, err
		}
		// The server can deliver faster than the default client buffer. The hold, not the
		// client, is what ends the session at 65536 (D841).
		if err := sub.SetPendingLimits(-1, -1); err != nil {
			_ = sub.Unsubscribe()
			for _, s := range subs {
				_ = s.Unsubscribe()
			}
			return nil, err
		}
		subs = append(subs, sub)
	}
	if err := p.nc.FlushTimeout(flushTimeout); err != nil {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
		return nil, err
	}
	return subs, nil
}

func (p *Path) onMsg(apiID int64, m *nats.Msg) {
	raw := append([]byte(nil), m.Data...)
	ev := &tdv1.AccountEvent{}
	if err := proto.Unmarshal(raw, ev); err != nil {
		if p.logf != nil {
			p.logf("td.evt: " + err.Error())
		}
		return
	}
	p.onEvent(apiID, ev)
}

func (p *Path) onEvent(apiID int64, ev *tdv1.AccountEvent) {
	p.mu.Lock()
	acc := p.byAPI[apiID]
	if acc == nil || p.failed {
		p.mu.Unlock()
		return
	}
	if acc.holding || !acc.have {
		p.hold(acc, ev)
		return
	}
	inc := ev.GetSource().GetIncarnation()
	seq := ev.GetSeq()
	// Live stream, after a baseline exists (D844). A different incarnation, or a hole in
	// this incarnation, holds this event and the ones after it and asks again. A seq at
	// or below the cursor is already in the snapshot and is dropped.
	if inc != acc.inc || seq > acc.seq+1 {
		acc.holding = true
		acc.pending = true
		p.hold(acc, ev)
		p.wake(acc)
		return
	}
	if seq <= acc.seq {
		p.mu.Unlock()
		return
	}
	acc.seq = seq
	p.mu.Unlock()
	p.enqueue(func() { p.apply(apiID, ev) })
}

// hold appends ev or ends the session. The caller holds p.mu; hold releases it.
func (p *Path) hold(acc *account, ev *tdv1.AccountEvent) {
	if p.heldLocked() >= dispatch.TDCap {
		p.mu.Unlock()
		p.stop(dispatch.ReasonTDFull)
		return
	}
	acc.hold = append(acc.hold, ev)
	p.mu.Unlock()
}

func (p *Path) heldLocked() int {
	n := 0
	for _, acc := range p.accounts {
		n += len(acc.hold)
	}
	return n
}

func (p *Path) drive(acc *account) {
	bo := backoffMin
	for {
		if !p.waitPending(acc) {
			return
		}
		p.mu.Lock()
		acc.pending = false
		acc.asking = true
		prevInc, prevSeq, prevHave := acc.inc, acc.seq, acc.have
		p.mu.Unlock()

		err := p.ask(acc)

		p.mu.Lock()
		acc.asking = false
		failed := p.failed
		pending := acc.pending
		same := prevHave && acc.have && acc.inc == prevInc && acc.seq == prevSeq
		p.mu.Unlock()

		if p.ctx.Err() != nil || failed {
			return
		}
		if err != nil {
			p.mu.Lock()
			acc.pending = true
			acc.holding = true
			p.mu.Unlock()
			if !p.sleep(bo) {
				return
			}
			bo = grow(bo)
			continue
		}
		// The reply's watermark did not move and the hold is still waiting. Ask again
		// on the same backoff, so a snapshot that cannot release the head does not spin.
		if pending && same {
			if !p.sleep(bo) {
				return
			}
			bo = grow(bo)
			continue
		}
		bo = backoffMin
	}
}

func (p *Path) waitPending(acc *account) bool {
	p.mu.Lock()
	for !acc.pending && !p.failed {
		p.mu.Unlock()
		select {
		case <-acc.wake:
		case <-p.ctx.Done():
			return false
		}
		p.mu.Lock()
	}
	ok := !p.failed && p.ctx.Err() == nil
	p.mu.Unlock()
	return ok
}

func (p *Path) ask(acc *account) error {
	apiID := acc.decl.GetApiId()
	ctx, cancel := context.WithTimeout(p.ctx, p.recv)
	defer cancel()
	body, err := proto.Marshal(&tdv1.ViewRequest{Req: &tdv1.ViewRequest_Recon{Recon: &tdv1.ReconRequest{}}})
	if err != nil {
		return err
	}
	// D854: this attempt's generation is visible to Submit before the request is
	// published. A Submit that records a generation at least this large happened
	// after the handoff, so a snapshot that does not list it is not proof it is gone.
	p.mu.Lock()
	acc.sentGen++
	gen := acc.sentGen
	p.mu.Unlock()
	msg, err := p.nc.RequestWithContext(ctx, viewSubject(apiID), body)
	if err != nil {
		return err
	}
	rep := &tdv1.ViewReply{}
	if err := proto.Unmarshal(msg.Data, rep); err != nil {
		return err
	}
	if rep.GetRecon() == nil {
		return errNotRecon
	}
	done := make(chan struct{})
	p.enqueue(func() {
		p.beginReply(apiID, rep, gen)
		p.releaseStep(acc, done)
	})
	// The reply callback is on the dispatcher queue and has not run. Conformance
	// waits for this line so delivery cannot start with the queue still empty.
	p.mu.Lock()
	failed := p.failed
	p.mu.Unlock()
	if !failed && p.logf != nil {
		p.logf("td.view: queued")
	}
	select {
	case <-done:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

func (p *Path) beginReply(apiID int64, rep *tdv1.ViewReply, requestGen uint64) {
	body := proto.Clone(rep.GetRecon()).(*tdv1.Recon)
	body.Reason = tdv1.ReconReason_RECON_REASON_REQUESTED
	td := TD{APIID: apiID, Incarnation: rep.GetIncarnation(), Seq: rep.GetSeq()}
	p.deliverRecon(apiID, td, body, rep.GetCancelBatch(), true, requestGen)
}

// deliverRecon replaces the account's own-algo orders and its ledger, then calls the
// Recon hook. fromView is a td.view reply: its seq and incarnation become the watermark,
// a positive cancelBatch is remembered, and requestGen is that ReconRequest's generation
// (D854). A pushed td.evt Recon passes cancelBatch 0, fromView false and requestGen 0,
// which leaves cancel_batch alone and does not keep a local pending_new the snapshot
// omits. The cursor of a pushed Recon was already moved to the event's seq by the caller.
func (p *Path) deliverRecon(apiID int64, td TD, body *tdv1.Recon, cancelBatch uint32, fromView bool, requestGen uint64) {
	filtered := p.ownOrders(apiID, body.GetOpenOrders())
	p.mu.Lock()
	acc := p.byAPI[apiID]
	if acc == nil {
		p.mu.Unlock()
		return
	}
	acc.merge(filtered, requestGen, fromView)
	acc.replaceLedger(body.GetPositions(), body.GetBalances())
	if fromView {
		acc.have = true
		acc.inc = td.Incarnation
		acc.seq = td.Seq
		if cancelBatch > 0 {
			acc.cancelBatch = cancelBatch
			acc.haveBatch = true
		}
	}
	p.mu.Unlock()
	if p.hooks.Recon == nil {
		return
	}
	shown := proto.Clone(body).(*tdv1.Recon)
	shown.OpenOrders = filtered
	p.hooks.Recon(ReconEvent{TD: td, Recon: shown})
}

func (p *Path) ownOrders(apiID int64, orders []*tdv1.Order) []*tdv1.Order {
	out := make([]*tdv1.Order, 0, len(orders))
	for _, o := range orders {
		if o != nil && p.keeps(apiID, o.GetClientOrderId()) {
			out = append(out, cloneOrder(o))
		}
	}
	return out
}

const (
	classDrop = iota
	classGap
	classNewer
	classDeliver
)

// classify applies the §9.6 watermark to one held event. The same incarnation and the
// next seq are delivered. The same incarnation at or below the cursor is dropped. A
// higher seq of the same incarnation is a hole: the event stays held and another Recon
// is asked (D844). An older incarnation is dropped. A newer incarnation stays held and
// asks again, the same as a live incarnation change after the baseline (D844). Repeated
// asks that do not move the watermark wait on the D842 backoff.
func classify(acc *account, ev *tdv1.AccountEvent) int {
	inc := ev.GetSource().GetIncarnation()
	if inc < acc.inc {
		return classDrop
	}
	if inc > acc.inc {
		return classNewer
	}
	seq := ev.GetSeq()
	if seq <= acc.seq {
		return classDrop
	}
	if seq != acc.seq+1 {
		return classGap
	}
	return classDeliver
}

// releaseStep runs on the dispatcher. Drops stay in this step. Each delivered event is
// its own following step, so each one is its own hook. done closes when the hold has
// been released, or a hole or a newer incarnation has armed another ask.
func (p *Path) releaseStep(acc *account, done chan struct{}) {
	for {
		p.mu.Lock()
		if p.failed {
			p.mu.Unlock()
			close(done)
			return
		}
		if len(acc.hold) == 0 {
			if !acc.pending {
				acc.holding = false
			}
			p.mu.Unlock()
			close(done)
			return
		}
		ev := acc.hold[0]
		switch classify(acc, ev) {
		case classDrop:
			acc.hold[0] = nil
			acc.hold = acc.hold[1:]
			p.mu.Unlock()
			continue
		case classGap, classNewer:
			acc.pending = true
			acc.holding = true
			p.mu.Unlock()
			p.wake(acc)
			close(done)
			return
		default:
			acc.hold[0] = nil
			acc.hold = acc.hold[1:]
			acc.seq = ev.GetSeq()
			apiID := acc.decl.GetApiId()
			p.mu.Unlock()
			p.enqueue(func() {
				p.apply(apiID, ev)
				p.releaseStep(acc, done)
			})
			return
		}
	}
}

func (p *Path) apply(apiID int64, ev *tdv1.AccountEvent) {
	switch ev.GetEvent().(type) {
	case *tdv1.AccountEvent_OrderUpdate:
		p.applyUpdate(apiID, ev)
	case *tdv1.AccountEvent_Fill:
		p.applyFill(apiID, ev)
	case *tdv1.AccountEvent_PositionUpdate:
		p.applyPosition(apiID, ev)
	case *tdv1.AccountEvent_BalanceUpdate:
		p.applyBalance(apiID, ev)
	case *tdv1.AccountEvent_Recon:
		p.applyPushedRecon(apiID, ev)
	}
}

func (p *Path) enqueue(fn func()) {
	if err := p.push(fn); err != nil {
		if strings.Contains(err.Error(), dispatch.ReasonTDFull) {
			p.stop(dispatch.ReasonTDFull)
			return
		}
		p.markFailed()
	}
}

func (p *Path) stop(reason string) {
	first := p.failOnce.CompareAndSwap(false, true)
	p.markFailed()
	if !first {
		return
	}
	// The dispatcher logs the same words when its own buffer is the one that filled.
	// The hold fills first while events are still waiting for Recon (D841).
	if p.logf != nil && reason != "" {
		p.logf(reason)
	}
	if p.fail != nil {
		p.fail(reason)
	}
}

func (p *Path) markFailed() {
	p.mu.Lock()
	p.failed = true
	accs := make([]*account, len(p.accounts))
	copy(accs, p.accounts)
	p.mu.Unlock()
	for _, a := range accs {
		p.wake(a)
	}
	p.cancel()
}

func (p *Path) wake(a *account) {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (p *Path) sleep(d time.Duration) bool {
	select {
	case <-p.after(d):
		return p.ctx.Err() == nil
	case <-p.ctx.Done():
		return false
	}
}

func (p *Path) now() time.Time {
	if p.nowFn != nil {
		return p.nowFn()
	}
	return time.Now()
}

func (p *Path) track(fn func()) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		fn()
	}()
}

func grow(d time.Duration) time.Duration {
	n := d * 2
	if n > backoffMax {
		return backoffMax
	}
	return n
}

func orderSubject(apiID int64) string { return "td.order." + strconv.FormatInt(apiID, 10) }
func viewSubject(apiID int64) string  { return "td.view." + strconv.FormatInt(apiID, 10) }
func evtSubject(apiID int64) string   { return "td.evt." + strconv.FormatInt(apiID, 10) }

// runq is the hook queue used when Config.Push is nil. A push from a func that is
// running appends; the running func returns before the appended one starts.
type runq struct {
	mu sync.Mutex
	q  []func()
	on bool
}

func (r *runq) push(fn func()) error {
	if fn == nil {
		return nil
	}
	r.mu.Lock()
	r.q = append(r.q, fn)
	if r.on {
		r.mu.Unlock()
		return nil
	}
	r.on = true
	r.mu.Unlock()
	for {
		r.mu.Lock()
		if len(r.q) == 0 {
			r.on = false
			r.mu.Unlock()
			return nil
		}
		fn = r.q[0]
		r.q[0] = nil
		r.q = r.q[1:]
		r.mu.Unlock()
		fn()
	}
}
