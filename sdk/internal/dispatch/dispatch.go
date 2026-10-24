package dispatch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	"github.com/BullionBear/mftiko/contract/wire/c4"
	"github.com/BullionBear/mftiko/contract/wire/envelope"
)

const (
	// TDCap is how many entries the TD buffer holds (E08-D015). It is an SDK constant: it is not a
	// field of the session spec and not part of the contract. A full buffer ends the session FAILED.
	TDCap = 65536

	// MDBytes is the MD ring, one preallocated buffer (E08-D016).
	MDBytes = 32 << 20

	// SlowAfter is the soft hook limit (E08-D017). A hook that runs longer counts one hook_slow.
	// hook_blocked_ms is not this limit: it is how long the hook currently running has been running.
	SlowAfter = 100 * time.Millisecond

	// ReasonTDFull is the Exiting reason when the TD buffer cannot take another entry (E08-D015).
	ReasonTDFull = "td buffer full"
	// ReasonHung is the Exiting reason when a hook passes hook_timeout_s (E08-D003).
	ReasonHung = "hook timeout"
)

// QueueStat is one buffer's depth, dropped count, and the oldest event handed to a hook since the
// previous Stats call (PROTOCOL §6.1). Depth is entries waiting, not the hook that is running.
// Dropped accumulates. MaxAgeMs is cleared by Stats, so a report reads the interval since the last report.
type QueueStat struct {
	Depth    uint32
	Dropped  uint64
	MaxAgeMs uint32
}

// Config is the dispatcher's session inputs. Zero RingBytes selects MDBytes. Zero HookTimeout
// does not arm the hard limit; the session worker rejects a payload whose hook_timeout_s is 0
// before Run (E08-D003). Now nil means time.Now.
type Config struct {
	RecvWindow  time.Duration
	HookTimeout time.Duration
	Now         func() time.Time
	// OnStop is the strategy callback. It runs on the dispatcher goroutine, after a DONE or FAILED
	// ending has delivered the TD buffer (E08-D003). Nil does nothing.
	OnStop func(context.Context) error
	// UnsubscribeTD runs on the dispatcher goroutine before that delivery, for DONE and FAILED only.
	// It is where the session drops td.evt. Nil does nothing.
	UnsubscribeTD func()
	// Feed is called with a market-data message the prehook kept. The bytes alias the ring and are
	// valid only until Feed returns. Feed decodes; the dispatcher does not. Nil skips the message.
	Feed func([]byte)
	// Book is orderbook_delta. It is not filtered by the prehook (E08-D003); E8-15 applies it. The bytes
	// alias the ring and are valid only until Book returns. Nil skips the message without counting a drop.
	Book func([]byte)
	// Log receives the soft-limit and overflow lines. Nil discards them.
	Log func(string)
	// RingBytes is the MD ring size. Zero selects MDBytes. Tests pass a smaller ring; a session does not.
	RingBytes int
}

// Disp is one session's dispatcher. Run and OnStop are called as a c4 plane's Run and Stopper.
// The methods that enqueue are safe for concurrent use. The hook callbacks are not: they run on
// the dispatcher goroutine, one at a time.
type Disp struct {
	recv        time.Duration
	hookTimeout time.Duration
	now         func() time.Time
	onStop      func(context.Context) error
	unsub       func()
	feed        func([]byte)
	book        func([]byte)
	log         func(string)

	mu   sync.Mutex
	cond *sync.Cond

	td     tdq
	ring   *ring
	timers []*Timer

	// Terminal state. outcome is what Run returns: nil is DONE, *c4.Exit is FAILED or the value
	// the caller asked for, any other error is a hook panic (CRASHED). The watchdog's HUNG is not
	// stored here; hung is set and Run is released without waiting for the hook.
	outcome    error
	outcomeSet bool
	stopping   bool
	hung       bool
	tdClosed   bool // pushes are rejected; set once delivery has finished or been skipped
	mdClosed   bool // the ring has been discarded and MD is no longer accepted
	inStop     bool // dispatcher goroutine only: OnStop's drain is running, so the hard limit is c4's

	hookOn    bool
	hookStart time.Time
	hookSlow  uint64
	tdMax     time.Duration
	mdMax     time.Duration
	preDrop   uint64 // prehook drops; ring drops live on ring.dropped

	stopPanic error
	steps     []string

	runResult  chan error
	stopReq    chan context.Context
	stopRes    chan error
	exited     chan struct{}
	finishOnce sync.Once
}

// New builds a dispatcher. It does not start it; call Run.
func New(cfg Config) *Disp {
	d := &Disp{
		recv:        cfg.RecvWindow,
		hookTimeout: cfg.HookTimeout,
		now:         cfg.Now,
		onStop:      cfg.OnStop,
		unsub:       cfg.UnsubscribeTD,
		feed:        cfg.Feed,
		book:        cfg.Book,
		log:         cfg.Log,
		ring:        newRing(cfg.RingBytes),
		td:          tdq{buf: make([]tdItem, TDCap)},
		runResult:   make(chan error, 1),
		stopReq:     make(chan context.Context, 1),
		stopRes:     make(chan error, 1),
		exited:      make(chan struct{}),
	}
	if d.now == nil {
		d.now = time.Now
	}
	if d.log == nil {
		d.log = func(string) {}
	}
	d.cond = sync.NewCond(&d.mu)
	return d
}

// Exited is closed when the dispatcher goroutine has returned. Run may have returned earlier:
// a HUNG hook is not waited for, and the goroutine leaves once that hook does.
func (d *Disp) Exited() <-chan struct{} { return d.exited }

// HookTimeout is the duration c4 applies to OnStop when Run returns on its own (E08-D009).
func (d *Disp) HookTimeout() time.Duration { return d.hookTimeout }

// Exit ends the session DONE after the current hook. Fail ends it FAILED. The first call wins.
func (d *Disp) Exit() { d.requestStop(nil) }

// Fail ends the session FAILED with reason, after the current hook.
func (d *Disp) Fail(reason string) {
	d.requestStop(&c4.Exit{Outcome: commonv1.Outcome_OUTCOME_FAILED, Reason: reason})
}

func (d *Disp) requestStop(err error) {
	d.mu.Lock()
	d.setStopLocked(err)
	d.cond.Broadcast()
	d.mu.Unlock()
}

// setStopLocked records the first ending. hung is left to the watchdog.
func (d *Disp) setStopLocked(err error) {
	if d.outcomeSet || d.hung {
		return
	}
	d.outcome = err
	d.outcomeSet = true
	d.stopping = true
}

// PushTD queues fn on the TD buffer. errFull means the buffer is full and the session is ending
// FAILED (E08-D015); fn was not queued. errClosed means the session is no longer accepting entries.
func (d *Disp) PushTD(fn func()) error {
	if fn == nil {
		return nil
	}
	d.mu.Lock()
	if d.tdClosed || d.hung {
		d.mu.Unlock()
		return errClosed
	}
	if d.td.n == TDCap {
		d.setStopLocked(&c4.Exit{Outcome: commonv1.Outcome_OUTCOME_FAILED, Reason: ReasonTDFull})
		d.cond.Broadcast()
		d.mu.Unlock()
		d.log("td buffer full")
		return errFull
	}
	d.td.push(tdItem{fn: fn, at: d.now()})
	d.cond.Broadcast()
	d.mu.Unlock()
	return nil
}

// PushMD copies one market-data message (envelope and payload) into the ring.
// PushBook is the same for orderbook_delta, which skips the prehook.
// A dropped message returns nil: dropping is the ring's backpressure, not a failed session.
// errClosed means MD delivery has stopped.
func (d *Disp) PushMD(msg []byte) error   { return d.pushMD(msg, false) }
func (d *Disp) PushBook(msg []byte) error { return d.pushMD(msg, true) }

func (d *Disp) pushMD(msg []byte, book bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.mdClosed || d.hung {
		return errClosed
	}
	if d.ring.push(msg, book) {
		d.cond.Broadcast()
	}
	return nil
}

// Timer returns a timer whose callback runs on the TD buffer. Fires that arrive while one run of
// this timer is already queued run once (E08-D003).
func (d *Disp) Timer(fn func()) *Timer {
	t := &Timer{d: d, fn: fn}
	d.mu.Lock()
	d.timers = append(d.timers, t)
	d.mu.Unlock()
	return t
}

// Timer is one timer. After arms a real timer. Fire queues a run immediately and is what After's
// timer calls; tests use it to pile fires up without waiting.
type Timer struct {
	d    *Disp
	fn   func()
	mu   sync.Mutex
	tim  *time.Timer
	dead bool

	// queued is touched only under Disp.mu.
	queued bool
}

// After schedules one run after delay. A run already queued is not duplicated; the timer is still
// rearmed, and a later fire queues the next run once the queued one has started.
func (t *Timer) After(delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dead {
		return
	}
	if t.tim == nil {
		t.tim = time.AfterFunc(delay, t.Fire)
		return
	}
	t.tim.Reset(delay)
}

// Fire queues one run unless one is already queued or the session is no longer accepting TD.
func (t *Timer) Fire() {
	if t.fn == nil {
		return
	}
	d := t.d
	d.mu.Lock()
	full := false
	if !t.queued && !d.tdClosed && !d.hung {
		if d.td.n == TDCap {
			d.setStopLocked(&c4.Exit{Outcome: commonv1.Outcome_OUTCOME_FAILED, Reason: ReasonTDFull})
			d.cond.Broadcast()
			full = true
		} else {
			t.queued = true
			d.td.push(tdItem{fn: t.fn, at: d.now(), timer: t})
			d.cond.Broadcast()
		}
	}
	d.mu.Unlock()
	if full {
		d.log("td buffer full")
	}
}

func (t *Timer) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dead = true
	if t.tim != nil {
		t.tim.Stop()
		t.tim = nil
	}
}

// Stats reports both buffers and the hook counters. It clears each buffer's max age, which is the
// report interval (PROTOCOL §6.1). hook_blocked_ms is 0 when no hook is running (E08-D017).
func (d *Disp) Stats() (td, md QueueStat, blockedMs, slow uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	td = QueueStat{Depth: u32(d.td.n), MaxAgeMs: ms32(d.tdMax)}
	md = QueueStat{
		Depth:    u32(d.ring.waiting()),
		Dropped:  d.ring.dropped + d.preDrop,
		MaxAgeMs: ms32(d.mdMax),
	}
	d.tdMax = 0
	d.mdMax = 0
	if d.hookOn {
		blockedMs = ms64(d.now().Sub(d.hookStart))
	}
	slow = d.hookSlow
	return td, md, blockedMs, slow
}

// Run is the plane's Run. It returns when the session should send Exiting: nil is DONE, *c4.Exit
// is FAILED or HUNG, and any other error is a hook panic (CRASHED). It does not send Exiting.
// A HUNG hook is not waited for. OnStop then performs the rest of the ending on the dispatcher
// goroutine; c4 calls it after Exiting.
func (d *Disp) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		select {
		case <-ctx.Done():
			d.requestStop(nil)
		case <-d.exited:
		}
	}()
	go d.loop()
	return <-d.runResult
}

// OnStop is the plane's Stopper. c4 calls it after Exiting and before Stopped. The TD delivery
// and the strategy callback run on the dispatcher goroutine. The context is the one c4 built:
// the remaining grace, or hook_timeout_s when Run returned on its own (E08-D009).
func (d *Disp) OnStop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case d.stopReq <- ctx:
	case <-d.exited:
		return nil
	}
	select {
	case err := <-d.stopRes:
		return err
	case <-d.exited:
		return nil
	}
}

func (d *Disp) loop() {
	defer close(d.exited)
	for {
		d.mu.Lock()
		for !d.hung && !d.stopping && d.td.n == 0 && d.ring.waiting() == 0 {
			d.cond.Wait()
		}
		if d.hung {
			d.mu.Unlock()
			d.finish(hungExit())
			return
		}
		if d.stopping {
			d.mu.Unlock()
			d.phase1()
			return
		}
		if d.td.n > 0 {
			it := d.popLocked()
			if age := d.now().Sub(it.at); age > d.tdMax {
				d.tdMax = age
			}
			d.mu.Unlock()
			d.runHook(it.fn)
			continue
		}
		d.mu.Unlock()
		d.runMD()
	}
}

// phase1 stops MD, discards the ring, and lets Run return so c4 can send Exiting. It then waits
// for OnStop. The current hook has already returned: the loop only gets here between hooks.
func (d *Disp) phase1() {
	d.mu.Lock()
	d.mdClosed = true
	d.ring.discard()
	err := d.outcome
	d.mu.Unlock()
	d.stopTimers()
	d.step("stop-md")
	d.finish(err)
	ctx := <-d.stopReq
	d.phase2(ctx, delivers(err))
}

// phase2 delivers the TD buffer for DONE and FAILED, then runs the strategy OnStop. CRASHED does
// not deliver (E08-D003). The whole of this runs on the dispatcher goroutine. c4's context is the
// time limit, shared by the delivery and OnStop; this method does not arm a second one.
func (d *Disp) phase2(ctx context.Context, deliver bool) {
	d.inStop = true
	var err error
	defer func() {
		if rec := recover(); rec != nil {
			d.stopRes <- fmt.Errorf("dispatch: stop: %v", rec)
			return
		}
		d.stopRes <- err
	}()
	if ctx.Err() != nil {
		d.closeTD()
		err = ctx.Err()
		return
	}
	var delivered error
	if deliver {
		d.step("unsub")
		if d.unsub != nil {
			d.unsub()
		}
		delivered = d.drain(ctx)
	}
	d.closeTD()
	d.stopTimers()
	if ctx.Err() != nil {
		err = ctx.Err()
		return
	}
	if d.onStop != nil {
		start := d.now()
		d.mu.Lock()
		d.hookOn = true
		d.hookStart = start
		d.mu.Unlock()
		d.step("onstop")
		err = d.callOnStop(ctx)
		d.finishHook(start)
	}
	if err == nil {
		err = delivered
	}
}

// drain runs the TD entries already queued, and entries they queue, until the buffer is empty or
// ctx ends. A panic stops the rest of the delivery; the strategy OnStop still runs.
func (d *Disp) drain(ctx context.Context) error {
	for ctx.Err() == nil {
		d.mu.Lock()
		if d.td.n == 0 {
			d.mu.Unlock()
			return nil
		}
		it := d.popLocked()
		if age := d.now().Sub(it.at); age > d.tdMax {
			d.tdMax = age
		}
		d.mu.Unlock()
		d.runHook(it.fn)
		if perr := d.takeStopPanic(); perr != nil {
			return perr
		}
	}
	return ctx.Err()
}

func (d *Disp) takeStopPanic() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	err := d.stopPanic
	d.stopPanic = nil
	return err
}

func (d *Disp) closeTD() {
	d.mu.Lock()
	d.tdClosed = true
	d.mu.Unlock()
}

func (d *Disp) callOnStop(ctx context.Context) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("dispatch: onstop panic: %v", rec)
		}
	}()
	return d.onStop(ctx)
}

func (d *Disp) runMD() {
	d.mu.Lock()
	msg, book, ok := d.ring.peek()
	now := d.now()
	d.mu.Unlock()
	if !ok {
		return
	}
	defer func() {
		d.mu.Lock()
		d.ring.release()
		d.mu.Unlock()
	}()

	// A message that is not a v1 envelope is dropped here: counted, logged, and not delivered.
	// The session keeps running (E08-D049). Order of the check is length, version, reserved bytes.
	if _, err := envelope.MDTsNs(msg); err != nil {
		d.mu.Lock()
		d.preDrop++
		d.mu.Unlock()
		d.log("md envelope rejected: " + err.Error())
		return
	}
	var fn func([]byte)
	if book {
		fn = d.book
	} else if drop, age, hasTS := stale(msg, now, d.recv); hasTS && drop {
		d.mu.Lock()
		d.preDrop++
		d.mu.Unlock()
		return
	} else {
		fn = d.feed
		if fn != nil && hasTS {
			d.noteMD(age)
		}
	}
	if book && fn != nil {
		if ts, err := envelope.MDTsNs(msg); err == nil {
			age := now.Sub(time.Unix(0, ts))
			if age < 0 {
				age = 0
			}
			d.noteMD(age)
		}
	}
	if fn == nil {
		return
	}
	d.runHook(func() { fn(msg) })
}

// stale reports whether msg is past the recv window. now is the clock at dequeue (E08-D003).
// The caller has already rejected an unreadable envelope (E08-D049). ok is false only if that
// check is skipped; such a message is not a prehook drop.
func stale(msg []byte, now time.Time, window time.Duration) (drop bool, age time.Duration, ok bool) {
	ts, err := envelope.MDTsNs(msg)
	if err != nil {
		return false, 0, false
	}
	age = now.Sub(time.Unix(0, ts))
	if age < 0 {
		age = 0
	}
	// The protocol compares with `>`, so an event exactly recv_window_ms old is kept.
	return age > window, age, true
}

func (d *Disp) noteMD(age time.Duration) {
	d.mu.Lock()
	if age > d.mdMax {
		d.mdMax = age
	}
	d.mu.Unlock()
}

// runHook runs fn on this goroutine. A panic becomes CRASHED, or a delivery error when OnStop is
// already running. The hard limit is armed only before Exiting: past that, c4's OnStop context is
// the shared budget (E08-D003).
func (d *Disp) runHook(fn func()) {
	if fn == nil {
		return
	}
	start := d.now()
	d.mu.Lock()
	d.hookOn = true
	d.hookStart = start
	d.mu.Unlock()

	stopWatch := func() {}
	if !d.inStop && d.hookTimeout > 0 {
		stopWatch = d.arm(d.hookTimeout)
	}
	defer d.finishHook(start)
	defer stopWatch()
	defer func() {
		if rec := recover(); rec != nil {
			d.notePanic(rec)
		}
	}()
	fn()
}

func (d *Disp) arm(limit time.Duration) func() {
	timer := time.NewTimer(limit)
	done := make(chan struct{})
	go func() {
		select {
		case <-timer.C:
			d.mu.Lock()
			d.hung = true
			d.mu.Unlock()
			d.finish(hungExit())
		case <-done:
			timer.Stop()
		}
	}()
	return func() { close(done) }
}

func (d *Disp) finishHook(start time.Time) {
	elapsed := d.now().Sub(start)
	d.mu.Lock()
	d.hookOn = false
	slow := elapsed > SlowAfter
	if slow {
		d.hookSlow++
	}
	d.mu.Unlock()
	if slow {
		d.log("hook exceeded 100ms")
	}
}

func (d *Disp) notePanic(rec any) {
	err := fmt.Errorf("hook panic: %v", rec)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inStop {
		if d.stopPanic == nil {
			d.stopPanic = err
		}
		return
	}
	if d.hung || d.outcomeSet {
		return
	}
	d.outcome = err
	d.outcomeSet = true
	d.stopping = true
	d.cond.Broadcast()
}

func (d *Disp) finish(err error) {
	d.finishOnce.Do(func() { d.runResult <- err })
}

func (d *Disp) stopTimers() {
	d.mu.Lock()
	timers := slices.Clone(d.timers)
	d.mu.Unlock()
	for _, t := range timers {
		t.stop()
	}
}

func (d *Disp) popLocked() tdItem {
	it := d.td.pop()
	if it.timer != nil {
		it.timer.queued = false
	}
	return it
}

func (d *Disp) step(s string) {
	d.mu.Lock()
	d.steps = append(d.steps, s)
	d.mu.Unlock()
}

func (d *Disp) trace() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.steps)
}

func hungExit() error {
	return &c4.Exit{Outcome: commonv1.Outcome_OUTCOME_HUNG, Reason: ReasonHung}
}

// delivers reports whether this ending hands the TD buffer to hooks before OnStop (E08-D003).
// nil is DONE. A plain error is a panic: CRASHED does not deliver.
func delivers(err error) bool {
	if err == nil {
		return true
	}
	var x *c4.Exit
	if errors.As(err, &x) {
		switch x.Outcome {
		case commonv1.Outcome_OUTCOME_DONE, commonv1.Outcome_OUTCOME_FAILED:
			return true
		}
	}
	return false
}

func u32(n int) uint32 {
	if n <= 0 {
		return 0
	}
	if n > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n)
}

func ms32(d time.Duration) uint32 {
	if d <= 0 {
		return 0
	}
	v := d / time.Millisecond
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

func ms64(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64(d / time.Millisecond)
}

var (
	errFull   = errors.New("dispatch: td buffer full")
	errClosed = errors.New("dispatch: stopping")
)

// tdq is a fixed ring of TDCap entries (E08-D015).
type tdq struct {
	buf  []tdItem
	head int
	n    int
}

type tdItem struct {
	fn    func()
	at    time.Time
	timer *Timer
}

func (q *tdq) push(it tdItem) {
	i := (q.head + q.n) % len(q.buf)
	q.buf[i] = it
	q.n++
}

func (q *tdq) pop() tdItem {
	it := q.buf[q.head]
	q.buf[q.head] = tdItem{}
	q.head = (q.head + 1) % len(q.buf)
	q.n--
	return it
}
