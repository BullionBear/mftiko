package conformance

import (
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
	"github.com/BullionBear/mftiko/contract/wire/cloid"
)

func (r *runner) writeInit(p *proc, params Params, deadline uint32, digest string) error {
	m, err := r.bus.initDown(params, deadline, digest)
	if err != nil {
		return err
	}
	return p.write(m)
}

func (r *runner) writeStart(p *proc, deadline uint32) error {
	m, err := r.bus.startDown(deadline)
	if err != nil {
		return err
	}
	return p.write(m)
}

func (r *runner) checkFlow() (Check, error) {
	if c, err := r.flowStartBeforeInit(); c.Status == statusFail || err != nil {
		return c, err
	}
	if c, err := r.flowDupInit(); c.Status == statusFail || err != nil {
		return c, err
	}
	if c, err := r.flowStopBeforeStart(); c.Status == statusFail || err != nil {
		return c, err
	}
	return r.flowHappy()
}

func (r *runner) flowStartBeforeInit() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if _, detail := waitHello(p); detail != "" {
		return fail(detail), nil
	}
	if err := r.writeStart(p, 0); err != nil {
		return fail(err.Error()), nil
	}
	bad := ""
	ready := poll(opTimeout, func() bool {
		s := p.snap()
		if s.anyDoneOK() {
			bad = "Start before Init: Done ok"
			return true
		}
		_, _, has := s.exiting()
		return s.exited && has
	})
	if bad != "" {
		return fail(bad), nil
	}
	if !ready {
		return fail("Start before Init: no Exiting"), nil
	}
	s := p.snap()
	out, reason, ok := s.exiting()
	if !s.exited || s.code != 10 || !ok || out != commonv1.Outcome_OUTCOME_INIT_FAILED {
		return fail(fmt.Sprintf("Start before Init: exit %d outcome %s (%s)", s.code, out, reason)), nil
	}
	return pass(), nil
}

func (r *runner) flowDupInit() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if _, detail := waitHello(p); detail != "" {
		return fail(detail), nil
	}
	if err := r.writeInit(p, baseParams(""), 0, ""); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().done(workerv1.Verb_VERB_INIT, true) }) {
		return fail("duplicate Init: first Done ok missing"), nil
	}
	if err := r.writeInit(p, baseParams(""), 0, ""); err != nil {
		return fail(err.Error()), nil
	}
	secondOK := false
	poll(opTimeout, func() bool {
		s := p.snap()
		n := 0
		secondOK = false
		for _, f := range s.frames {
			if d := f.GetDone(); d != nil && d.GetVerb() == workerv1.Verb_VERB_INIT {
				n++
				if n >= 2 && d.GetOk() {
					secondOK = true
				}
			}
		}
		// A second Done(ok) is the violation and can be reported before exit.
		// A refusal still has to reach Exiting and the process exit, which
		// the reader can observe after the Done frame.
		if secondOK {
			return true
		}
		_, _, has := s.exiting()
		return n >= 2 && s.exited && has
	})
	if secondOK {
		return fail("duplicate Init: Done ok"), nil
	}
	s := p.snap()
	if !s.exited || s.code != 10 || !s.done(workerv1.Verb_VERB_INIT, false) {
		return fail(fmt.Sprintf("duplicate Init: exit %d", s.code)), nil
	}
	return pass(), nil
}

func (r *runner) flowStopBeforeStart() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if _, detail := waitHello(p); detail != "" {
		return fail(detail), nil
	}
	if err := r.writeInit(p, baseParams(""), 0, ""); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool {
		s := p.snap()
		return s.done(workerv1.Verb_VERB_INIT, true) && s.hook("OnInit")
	}) {
		return fail("Stop before Start: Init did not finish"), nil
	}
	if err := p.write(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{GraceMs: 5000}}}); err != nil {
		return fail(err.Error()), nil
	}
	poll(opTimeout, func() bool {
		s := p.snap()
		_, hasSt := s.stopped()
		_, _, hasEx := s.exiting()
		return s.exited && hasSt && hasEx
	})
	s := p.snap()
	on, hasStopped := s.stopped()
	out, _, hasEx := s.exiting()
	if s.hook("OnStart") || s.hook("OnStop") || on {
		return fail("Stop before Start called a hook"), nil
	}
	if !s.exited || s.code != 0 || !hasEx || out != commonv1.Outcome_OUTCOME_DONE || !hasStopped || on {
		return fail(fmt.Sprintf("Stop before Start: exit %d outcome %s stopped %v", s.code, out, on)), nil
	}
	return pass(), nil
}

func (r *runner) flowHappy() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if _, detail := waitHello(p); detail != "" {
		return fail(detail), nil
	}
	if err := r.writeInit(p, baseParams(""), 0, "L1"); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().done(workerv1.Verb_VERB_INIT, true) }) {
		return fail("happy: Done INIT missing"), nil
	}
	if !p.snap().appliedBeforeDoneInit("L1") {
		return fail("happy: Applied L1 was not before Done INIT"), nil
	}
	if err := r.writeStart(p, 0); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool {
		s := p.snap()
		return s.done(workerv1.Verb_VERB_START, true) && s.stage("running")
	}) {
		return fail("happy: Start did not reach running"), nil
	}
	if err := p.write(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Relay{Relay: &workerv1.Relay{
		Contract: contractSTS, CorrId: []byte("ping"),
	}}}); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().relay("ping") }) {
		return fail("happy: Relay ping was not echoed"), nil
	}
	if err := p.write(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Live{Live: &workerv1.Live{
		Contract: contractSTS, Digest: "L2", Payload: []byte{1},
	}}}); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().applied("L2") }) {
		return fail("happy: Applied L2 missing"), nil
	}
	if err := p.write(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{GraceMs: 5000}}}); err != nil {
		return fail(err.Error()), nil
	}
	poll(opTimeout, func() bool {
		s := p.snap()
		_, ok := s.exitingBeforeStopped()
		return s.exited && ok
	})
	s := p.snap()
	before, ok := s.exitingBeforeStopped()
	out, _, hasEx := s.exiting()
	on, hasSt := s.stopped()
	if !s.exited || s.code != 0 || !ok || !before || !hasEx || out != commonv1.Outcome_OUTCOME_DONE || !hasSt || !on {
		return fail(fmt.Sprintf("happy: exit %d outcome %s order %v on_stop %v", s.code, out, before, on)), nil
	}
	if !s.hook("OnInit") || !s.hook("OnStart") || !s.hook("OnStop") {
		return fail("happy: missing OnInit, OnStart, or OnStop"), nil
	}
	return pass(), nil
}

func (r *runner) checkBeat() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if _, detail := waitHello(p); detail != "" {
		return fail(detail), nil
	}
	params := baseParams("beat")
	params.HookSleepMS = 5000
	if err := r.writeInit(p, params, 0, ""); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().done(workerv1.Verb_VERB_INIT, true) }) {
		return fail("beat: Init did not finish"), nil
	}
	if err := r.writeStart(p, 0); err != nil {
		return fail(err.Error()), nil
	}
	var base int
	if !poll(opTimeout, func() bool {
		s := p.snap()
		if s.hook("OnStart") {
			base = s.beats()
			return true
		}
		return s.exited
	}) || !p.snap().hook("OnStart") {
		return fail("beat: no OnStart line"), nil
	}
	// The contract is one second. A compliant beater increments within it;
	// a worker that sends none must be watched for that whole bound.
	if !poll(time.Second, func() bool { return p.snap().beats() > base }) {
		return fail("no Beat within 1s"), nil
	}
	return pass(), nil
}

func (r *runner) checkDeadline() (Check, error) {
	if c, err := r.deadlinePhase(true); c.Status == statusFail || err != nil {
		return c, err
	}
	return r.deadlinePhase(false)
}

func (r *runner) deadlinePhase(initPhase bool) (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if _, detail := waitHello(p); detail != "" {
		return fail(detail), nil
	}
	params := baseParams("")
	var deadline uint32
	if initPhase {
		params.InitSleepMS = 30000
		deadline = 200
	} else {
		params.StartSleepMS = 30000
	}
	if err := r.writeInit(p, params, deadline, ""); err != nil {
		return fail(err.Error()), nil
	}
	if !initPhase {
		if !poll(opTimeout, func() bool {
			s := p.snap()
			return s.done(workerv1.Verb_VERB_INIT, true) || s.exited
		}) || !p.snap().done(workerv1.Verb_VERB_INIT, true) {
			return fail("deadline: Init did not finish"), nil
		}
		if err := r.writeStart(p, 200); err != nil {
			return fail(err.Error()), nil
		}
	}
	bad := ""
	poll(opTimeout, func() bool {
		s := p.snap()
		if s.anyDoneOK() && initPhase {
			bad = "deadline: Done ok"
			return true
		}
		if !initPhase && s.done(workerv1.Verb_VERB_START, true) {
			bad = "deadline: Done ok"
			return true
		}
		_, _, has := s.exiting()
		return s.exited && has
	})
	if bad != "" {
		return fail(bad), nil
	}
	s := p.snap()
	out, reason, ok := s.exiting()
	on, hasSt := s.stopped()
	if !s.exited || s.code != 10 || !ok || out != commonv1.Outcome_OUTCOME_INIT_FAILED || !strings.Contains(reason, "deadline") || !hasSt || on {
		return fail(fmt.Sprintf("deadline: exit %d outcome %s reason %q stopped %v", s.code, out, reason, on)), nil
	}
	if initPhase && s.hook("OnInit") {
		return fail("deadline: OnInit ran"), nil
	}
	if !initPhase && s.hook("OnStop") {
		return fail("deadline: OnStop ran"), nil
	}
	return pass(), nil
}

func (r *runner) checkShutdown() (Check, error) {
	if c, err := r.shutdownStop(); c.Status == statusFail || err != nil {
		return c, err
	}
	return r.shutdownSelf()
}

func (r *runner) shutdownStop() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if _, detail := waitHello(p); detail != "" {
		return fail(detail), nil
	}
	if err := r.writeInit(p, baseParams("shutdown"), 0, ""); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().done(workerv1.Verb_VERB_INIT, true) }) {
		return fail("shutdown: Init did not finish"), nil
	}
	if err := r.writeStart(p, 0); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().stage("running") }) {
		return fail("shutdown: not running"), nil
	}
	if err := p.write(&workerv1.WorkerDown{Msg: &workerv1.WorkerDown_Stop{Stop: &workerv1.Stop{GraceMs: 5000}}}); err != nil {
		return fail(err.Error()), nil
	}
	bad := ""
	poll(opTimeout, func() bool {
		s := p.snap()
		ex, st := -1, -1
		for i, f := range s.frames {
			if ex < 0 && f.GetExiting() != nil {
				ex = i
			}
			if st < 0 && f.GetStopped() != nil {
				st = i
			}
		}
		if st >= 0 && (ex < 0 || st < ex) {
			bad = "Stopped before Exiting"
			return true
		}
		return s.exited && ex >= 0 && st >= 0
	})
	if bad != "" {
		return fail(bad), nil
	}
	s := p.snap()
	before, ok := s.exitingBeforeStopped()
	out, _, hasEx := s.exiting()
	on, hasSt := s.stopped()
	if !s.exited || s.code != 0 || !ok || !before || !hasEx || out != commonv1.Outcome_OUTCOME_DONE || !hasSt || !on {
		return fail(fmt.Sprintf("shutdown: exit %d outcome %s", s.code, out)), nil
	}
	return pass(), nil
}

func (r *runner) shutdownSelf() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if _, detail := waitHello(p); detail != "" {
		return fail(detail), nil
	}
	params := baseParams("shutdown")
	params.SelfExit = true
	params.OnStopSleepMS = 30000
	if err := r.writeInit(p, params, 0, ""); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().done(workerv1.Verb_VERB_INIT, true) }) {
		return fail("shutdown: Init did not finish"), nil
	}
	if err := r.writeStart(p, 0); err != nil {
		return fail(err.Error()), nil
	}
	// OnStop sleeps past hook_timeout_s (1s). The outcome stays DONE.
	poll(longWait, func() bool {
		s := p.snap()
		_, _, has := s.exiting()
		return s.exited && has
	})
	s := p.snap()
	_, hasSt := s.stopped()
	out, _, hasEx := s.exiting()
	if hasSt || !hasEx || out != commonv1.Outcome_OUTCOME_DONE || s.code != 21 {
		return fail(fmt.Sprintf("shutdown self: exit %d outcome %s stopped %v", s.code, out, hasSt)), nil
	}
	return pass(), nil
}

func (r *runner) checkPriority() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if err := r.boot(p, baseParams("priority")); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().hook("gate") || p.snap().exited }) {
		return fail("priority: no gate"), nil
	}
	if err := r.bus.publishTrade(1, time.Now(), 0); err != nil {
		return fail(err.Error()), nil
	}
	if err := r.bus.publishAccount(1, 1, false); err != nil {
		return fail(err.Error()), nil
	}
	if err := r.bus.flush(); err != nil {
		return fail(err.Error()), nil
	}
	var hooks []string
	poll(opTimeout, func() bool {
		hooks = hooks[:0]
		for _, l := range p.snap().lines {
			if l.Hook == "OnOrder" || l.Hook == "OnTrade" {
				hooks = append(hooks, l.Hook)
			}
		}
		return len(hooks) >= 2 || p.snap().exited
	})
	if len(hooks) < 2 || hooks[0] != "OnOrder" || hooks[1] != "OnTrade" {
		return fail(fmt.Sprintf("priority: order %v", hooks)), nil
	}
	return pass(), nil
}

func (r *runner) checkPrehook() (Check, error) {
	if c, err := r.prehookStale(); c.Status == statusFail || err != nil {
		return c, err
	}
	return r.prehookRing()
}

func (r *runner) prehookStale() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	params := baseParams("prehook")
	params.UntilSeq = 2
	if err := r.boot(p, params); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().hook("gate") || p.snap().exited }) {
		return fail("prehook: no gate"), nil
	}
	if err := r.bus.publishTrade(1, time.Now().Add(-10*time.Second), 0); err != nil {
		return fail(err.Error()), nil
	}
	if err := r.bus.publishTrade(2, time.Now(), 0); err != nil {
		return fail(err.Error()), nil
	}
	if err := r.bus.flush(); err != nil {
		return fail(err.Error()), nil
	}
	// Stdout and fd 3 are not ordered against each other (PROTOCOL §9.8). Wait
	// until both the delivery line and the md drop report are visible, unless
	// the stale trade was delivered.
	poll(opTimeout, func() bool {
		s := p.snap()
		if containsSeq(s.seqs("OnTrade"), 1) || s.exited {
			return true
		}
		if !s.hook("OnStart") {
			return false
		}
		_, ok := s.mdDropped()
		return ok
	})
	s := p.snap()
	if containsSeq(s.seqs("OnTrade"), 1) {
		return fail("stale seq 1 delivered"), nil
	}
	if !containsSeq(s.seqs("OnTrade"), 2) {
		return fail("fresh seq 2 was not delivered"), nil
	}
	dropped, ok := s.mdDropped()
	if !ok || dropped < 1 {
		return fail(fmt.Sprintf("prehook: md dropped %d", dropped)), nil
	}
	return pass(), nil
}

func (r *runner) prehookRing() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	params := baseParams("ring")
	params.UntilSeq = sentinel
	if err := r.boot(p, params); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().hook("gate") || p.snap().exited }) {
		return fail("ring: no gate"), nil
	}
	now := time.Now()
	for seq := uint64(1); seq <= ringN; seq++ {
		if err := r.bus.publishTrade(seq, now, ringSize); err != nil {
			return fail(err.Error()), nil
		}
	}
	if err := r.bus.publishTrade(sentinel, time.Now(), 0); err != nil {
		return fail(err.Error()), nil
	}
	if err := r.bus.flush(); err != nil {
		return fail(err.Error()), nil
	}
	poll(longWait, func() bool {
		s := p.snap()
		if s.exited {
			return true
		}
		if !s.hook("OnStart") {
			return false
		}
		_, ok := s.mdDropped()
		return ok
	})
	s := p.snap()
	got := s.seqs("OnTrade")
	if !containsSeq(got, sentinel) || containsSeq(got, 1) {
		return fail(fmt.Sprintf("ring: delivered %d trades, has sentinel %v has seq 1 %v", len(got), containsSeq(got, sentinel), containsSeq(got, 1))), nil
	}
	dropped, ok := s.mdDropped()
	if !ok || dropped < 1 {
		return fail(fmt.Sprintf("ring: md dropped %d", dropped)), nil
	}
	return pass(), nil
}

func (r *runner) checkOverflow() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if err := r.boot(p, baseParams("overflow")); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().hook("gate") || p.snap().exited }) {
		return fail("overflow: no gate"), nil
	}
	for seq := uint64(1); seq <= tdFull; seq++ {
		if err := r.bus.publishAccount(seq, 1, false); err != nil {
			return fail(err.Error()), nil
		}
		if seq%4096 == 0 {
			if q, ok := p.snap().queued(); ok && q >= tdFull {
				return fail(fmt.Sprintf("queued %d", q)), nil
			}
		}
	}
	if err := r.bus.flush(); err != nil {
		return fail(err.Error()), nil
	}
	bad := ""
	poll(longWait, func() bool {
		s := p.snap()
		if q, ok := s.queued(); ok && q >= tdFull {
			bad = fmt.Sprintf("queued %d", q)
			return true
		}
		out, _, ok := s.exiting()
		return ok && out == commonv1.Outcome_OUTCOME_FAILED && s.exited
	})
	if bad != "" {
		return fail(bad), nil
	}
	s := p.snap()
	out, reason, ok := s.exiting()
	if !s.exited || s.code != 30 || !ok || out != commonv1.Outcome_OUTCOME_FAILED || !strings.Contains(reason, "td buffer full") {
		return fail(fmt.Sprintf("overflow: exit %d outcome %s reason %q", s.code, out, reason)), nil
	}
	return pass(), nil
}

func (r *runner) checkCloid() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	mark := r.bus.mark()
	params := baseParams("cloid")
	params.OrderCount = 2
	if err := r.boot(p, params); err != nil {
		return fail(err.Error()), nil
	}
	var ids []string
	poll(opTimeout, func() bool {
		ids = r.bus.since(mark)
		return len(ids) >= 2 || (p.snap().hook("OnStart") && len(ids) > 0 && !cloidOK(ids)) || p.snap().exited
	})
	if !cloidOK(ids) {
		return fail(fmt.Sprintf("cloid: %v", ids)), nil
	}
	return pass(), nil
}

func cloidOK(ids []string) bool {
	if len(ids) < 2 {
		return false
	}
	var prev cloid.ID
	for i, s := range ids {
		id, err := cloid.Decode(s)
		if err != nil || id.Ver != cloid.Version || id.Algo != algoID {
			return false
		}
		if i > 0 && (id.TsMs < prev.TsMs || (id.TsMs == prev.TsMs && id.Seq <= prev.Seq)) {
			return false
		}
		prev = id
	}
	return true
}

func (r *runner) checkSubject() (Check, error) {
	if err := r.bus.denied(func(nc *nats.Conn) error { return nc.Publish("td.order.8", []byte{1}) }); err != nil {
		return fail("subject: " + err.Error()), nil
	}
	other := "md.v1.f." + r.bus.inst + ".trade.Binance_Spot_ETHUSDT"
	if err := r.bus.denied(func(nc *nats.Conn) error {
		_, err := nc.Subscribe(other, func(*nats.Msg) {})
		return err
	}); err != nil {
		return fail("subject: " + err.Error()), nil
	}
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	mark := r.bus.mark()
	params := baseParams("subject")
	params.OrderCount = 1
	if err := r.boot(p, params); err != nil {
		return fail(err.Error()), nil
	}
	var sinceOnStart time.Time
	var saw bool
	ok := poll(opTimeout, func() bool {
		if p.snap().hook("OnStart") && !saw {
			sinceOnStart = time.Now()
			saw = true
		}
		if len(r.bus.since(mark)) >= 1 {
			return true
		}
		return saw && time.Since(sinceOnStart) >= time.Second
	})
	if !ok || len(r.bus.since(mark)) < 1 {
		return fail("order not on " + r.bus.orderSubject), nil
	}
	return pass(), nil
}

func (r *runner) checkRecon() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if err := r.boot(p, baseParams("recon")); err != nil {
		return fail(err.Error()), nil
	}
	// OnRecon is printed after the early deliveries and before Stage. Requiring
	// both keeps the stdout lines in the same snapshot as the stage frame.
	if !poll(opTimeout, func() bool {
		s := p.snap()
		return (s.stage("running") && s.hook("OnRecon")) || s.exited
	}) {
		return fail("recon: not running"), nil
	}
	early := reconSet(p.snap())
	if len(early) != 1 || !early[keyOf(3, 6)] {
		return fail(fmt.Sprintf("recon early: %v", keysOf(early))), nil
	}
	if err := r.bus.publishAccount(5, 3, false); err != nil {
		return fail(err.Error()), nil
	}
	if err := r.bus.publishAccount(7, 3, false); err != nil {
		return fail(err.Error()), nil
	}
	if err := r.bus.flush(); err != nil {
		return fail(err.Error()), nil
	}
	poll(opTimeout, func() bool {
		set := reconSet(p.snap())
		return set[keyOf(3, 7)] || set[keyOf(3, 5)]
	})
	got := reconSet(p.snap())
	if len(got) != 2 || !got[keyOf(3, 6)] || !got[keyOf(3, 7)] {
		return fail(fmt.Sprintf("recon: %v", keysOf(got))), nil
	}
	return pass(), nil
}

type reconKey struct {
	inc uint32
	seq uint64
}

func keyOf(inc uint32, seq uint64) reconKey { return reconKey{inc, seq} }

func reconSet(s shot) map[reconKey]bool {
	out := map[reconKey]bool{}
	for _, l := range s.lines {
		if l.Hook == "OnOrder" {
			out[keyOf(l.Incarnation, l.Seq)] = true
		}
	}
	return out
}

func keysOf(m map[reconKey]bool) []reconKey {
	var out []reconKey
	for k := range m {
		out = append(out, k)
	}
	return out
}

func (r *runner) checkCtx() (Check, error) {
	p, err := r.spawn()
	if err != nil {
		return Check{}, err
	}
	defer p.stop()
	if err := r.boot(p, baseParams("ctx")); err != nil {
		return fail(err.Error()), nil
	}
	if !poll(opTimeout, func() bool { return p.snap().hook("gate") || p.snap().exited }) {
		return fail("ctx: no gate"), nil
	}
	if err := r.bus.publishAccount(1, 1, false); err != nil {
		return fail(err.Error()), nil
	}
	if err := r.bus.publishAccount(2, 1, true); err != nil {
		return fail(err.Error()), nil
	}
	if err := r.bus.flush(); err != nil {
		return fail(err.Error()), nil
	}
	poll(opTimeout, func() bool {
		s := p.snap()
		return s.hook("OnOrder") && s.hook("OnPosition")
	})
	s := p.snap()
	if !ctxTrue(s, "OnOrder") || !ctxTrue(s, "OnPosition") {
		return fail("ctx is false"), nil
	}
	return pass(), nil
}

func ctxTrue(s shot, hook string) bool {
	for _, l := range s.lines {
		if l.Hook == hook {
			return l.Ctx != nil && *l.Ctx
		}
	}
	return false
}

// boot sends Init and waits until it succeeds. Start is the caller's when the
// check publishes after gate, which the worker emits from OnStart.
func (r *runner) boot(p *proc, params Params) error {
	if _, detail := waitHello(p); detail != "" {
		return fmt.Errorf("%s", detail)
	}
	if err := r.writeInit(p, params, 0, ""); err != nil {
		return err
	}
	if !poll(opTimeout, func() bool {
		s := p.snap()
		return s.done(workerv1.Verb_VERB_INIT, true) || s.exited
	}) || !p.snap().done(workerv1.Verb_VERB_INIT, true) {
		s := p.snap()
		return fmt.Errorf("Init did not succeed (exit %d, %s)", s.code, s.stderr)
	}
	return r.writeStart(p, 0)
}

func containsSeq(seqs []uint64, want uint64) bool {
	for _, s := range seqs {
		if s == want {
			return true
		}
	}
	return false
}
