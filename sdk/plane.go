package sdk

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	mdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/md/v1"
	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/contract/wire/c4"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/internal/bookpath"
	"github.com/BullionBear/mftiko/sdk/internal/cloidgen"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
	"github.com/BullionBear/mftiko/sdk/internal/evlog"
	"github.com/BullionBear/mftiko/sdk/internal/mdpath"
	"github.com/BullionBear/mftiko/sdk/internal/symclient"
	"github.com/BullionBear/mftiko/sdk/internal/tdpath"
)

// host is the process around one session. The zero value is production: the
// NATS URL and the volume directories come from the environment, dials use
// nats.Connect, and the report interval is five seconds. Tests replace the
// dial, the backoff clock, and the report clock so a retry does not sleep.
type host struct {
	natsURL     string
	getenv      func(string) string
	tryConn     func(url string, creds *stsctlv1.NatsCredentials, timeout time.Duration) (*nats.Conn, error)
	after       func(time.Duration) <-chan time.Time
	reportAfter func(time.Duration) <-chan time.Time
	reportEvery time.Duration
	now         func() time.Time
	log         func(string)
	c4log       func(msg string, err error)
	// settled is closed when Init returns, including an Init that c4 has already
	// stopped waiting for (E08-D007). Tests wait on it. Nil in production.
	settled chan struct{}
}

func (h host) env(key string) string {
	if h.getenv != nil {
		return h.getenv(key)
	}
	return os.Getenv(key)
}

// plane is the session worker's C4 plane. One plane is one process.
type plane struct {
	host  host
	strat Strategy
	sc    *sessCtx

	root   context.Context
	cancel context.CancelFunc

	sess *c4.Session
	nc   *nats.Conn
	sym  *symclient.Client
	// symPtr is what the NATS reconnect handler reads. It is set after Open and
	// cleared before the client is closed.
	symPtr atomic.Pointer[symclient.Client]
	td     *tdpath.Path
	md     *mdpath.Path
	books  *bookpath.Path
	disp   *dispatch.Disp
	tap    *mdpath.Tap

	sessionID   string
	params      []byte
	volumeDir   string
	inputDir    string
	hookTimeout time.Duration
	recv        time.Duration
	hookCtx     context.Context

	mu    sync.Mutex
	dead  bool
	shut  bool
	loops bool

	hook atomic.Value // string, the hook currently running; "" when none

	cmdMu   sync.Mutex
	cmdCond *sync.Cond
	cmds    []relayed
	cmdStop bool
	cmdDone chan struct{}
	repDone chan struct{}
	wake    chan struct{}

	repMu     sync.Mutex
	lastEpoch int64
	lastSeq   uint64
	lastOK    bool

	// loopStarted is set before the dispatcher loop, so OnStop after Run waits
	// on the dispatcher. A Start failure happens before that and calls the
	// strategy directly: the loop is not there to take OnStop.
	loopStarted atomic.Bool
	// asked is set when the strategy calls Exit or Fail, so Start does not
	// send the first Recon after the session has already been ended.
	asked atomic.Bool

	// rec is the event log, nil when the session spec leaves it off (E20-D007). Init sets it under
	// mu before anything that writes to it starts; shutdown closes it last.
	rec *evlog.Log
	// clock is the step clock, in unix nanoseconds: the start of the hook that is running. The
	// dispatcher sets it for its hooks and outside for the others. Context.Now reads it (E20-D004).
	clock atomic.Int64
	// seed is Context.Rand's seed. The init frame records it.
	seed    uint64
	rngOnce sync.Once
	rng     *rand.Rand
}

func newPlane(s Strategy, h host) *plane {
	ctx, cancel := context.WithCancel(context.Background())
	p := &plane{
		host:   h,
		strat:  s,
		root:   ctx,
		cancel: cancel,
	}
	p.sc = &sessCtx{p: p}
	p.cmdCond = sync.NewCond(&p.cmdMu)
	p.hook.Store("")
	return p
}

func (p *plane) Init(ctx context.Context, sess *c4.Session, payload []byte) error {
	if p.host.settled != nil {
		defer close(p.host.settled)
	}
	recvAt := p.now()
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.closing() {
		return context.Canceled
	}
	p.sess = sess
	var init stsctlv1.InitPayload
	if err := proto.Unmarshal(payload, &init); err != nil {
		return errors.New("init payload")
	}
	if init.GetSessionId() == "" {
		return errors.New("session id")
	}
	if init.GetRecvWindowMs() == 0 {
		return errors.New("recv window")
	}
	if init.GetHookTimeoutS() == 0 {
		return errors.New("hook timeout")
	}
	creds := init.GetNats()
	if creds.GetUserJwt() == "" || creds.GetSeed() == "" || creds.GetInboxPrefix() == "" {
		return errors.New("nats credentials")
	}
	url := p.host.natsURL
	if url == "" {
		url = p.host.env("MFTIKO_NATS_URL")
	}
	if url == "" {
		return errors.New("nats url")
	}

	p.sessionID = init.GetSessionId()
	p.params = append([]byte(nil), init.GetParams()...)
	p.volumeDir = p.host.env("MFTIKO_VOLUME_DIR")
	p.inputDir = p.host.env("MFTIKO_INPUT_DIR")
	p.hookTimeout = time.Duration(init.GetHookTimeoutS()) * time.Second
	p.recv = time.Duration(init.GetRecvWindowMs()) * time.Millisecond
	p.seed = rand.Uint64()

	nc, err := p.connectNATS(ctx, url, creds)
	if err != nil {
		return err
	}
	if err := p.own(func() { p.nc = nc }); err != nil {
		nc.Close()
		return err
	}
	if init.GetEventLog().GetEnabled() {
		rec := p.openRec(&init)
		if err := p.own(func() { p.rec = rec }); err != nil {
			rec.Close()
			return err
		}
	}

	p.tap = &mdpath.Tap{}
	p.disp = dispatch.New(dispatch.Config{
		RecvWindow:    p.recv,
		HookTimeout:   p.hookTimeout,
		Now:           p.host.now,
		OnStop:        p.strategyStop,
		UnsubscribeTD: p.unsubTD,
		Feed:          p.tap.Feed,
		Book:          p.tap.Book,
		Log:           p.note,
		Rec:           p.rec,
		Clock:         &p.clock,
	})
	sym, err := symclient.Open(p.root, symclient.NATS(nc), symclient.Options{Log: p.note})
	if err != nil {
		return err
	}
	if err := p.own(func() { p.sym = sym }); err != nil {
		sym.Close()
		return err
	}
	p.symPtr.Store(sym)

	td, err := tdpath.Open(tdpath.Config{
		SessionID:  p.sessionID,
		Accounts:   init.GetAccounts(),
		RecvWindow: p.recv,
		NC:         nc,
		Push:       p.disp.PushIn,
		Fail:       p.disp.Fail,
		Now:        p.host.now,
		Hooks:      p.tdHooks(),
		Log:        p.note,
		Rec:        p.rec,
	})
	if err != nil {
		return err
	}
	if err := p.own(func() { p.td = td }); err != nil {
		td.Close()
		return err
	}

	md, err := mdpath.Open(mdpath.Config{
		NC:         nc,
		RecvWindow: p.recv,
		Disp:       p.disp,
		Tap:        p.tap,
		Resolve:    p.resolveUID,
		Hooks:      p.mdHooks(),
		Book: func(msg []byte) {
			if p.books != nil {
				p.books.OnMessage(msg)
			}
		},
		Log: p.note,
		Rec: p.rec,
		Now: p.host.now,
	})
	if err != nil {
		return err
	}
	if err := p.own(func() { p.md = md }); err != nil {
		md.Close()
		return err
	}
	bk, err := bookpath.Open(bookpath.Config{
		Snapshot:   p.bookSnapshot,
		Info:       p.bookInfo,
		Hook:       p.bookHook(),
		RecvWindow: p.recv,
		// The book reads the clock only inside hooks. The step clock makes a replay keep or
		// drop the same deltas (E20-D004).
		Now:      p.stepNow,
		Schedule: p.schedule,
		Log:      p.note,
	})
	if err != nil {
		return err
	}
	if err := p.own(func() { p.books = bk }); err != nil {
		bk.Close()
		return err
	}
	p.startLoops()

	if err := ctx.Err(); err != nil {
		return err
	}
	p.sym.AllowResolve(true)
	defer p.sym.AllowResolve(false)
	p.setHookCtx(ctx)
	defer p.setHookCtx(nil)
	// The init frame is the payload without the NATS credentials, after the client order id
	// counter as Init found it (E20-D003, E20-D004).
	var head [8]byte
	var body []byte
	if p.rec != nil {
		head = eventlog.InitHead(cloidgen.Slot())
		body = withoutField(payload, natsField)
	}
	return p.outside("OnInit", eventlog.KindInit, recvAt.UnixNano(), p.seed, head[:], body, func() error {
		return p.strat.OnInit(p.sc)
	})
}

func (p *plane) Start(ctx context.Context, payload []byte) error {
	recvAt := p.now()
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.asked.Load() {
		return nil
	}
	var sp stsctlv1.StartPayload
	if err := proto.Unmarshal(payload, &sp); err != nil || sp.GetBindings() == nil {
		return errors.New("start payload")
	}
	if err := p.applyBindings(sp.GetBindings()); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.td == nil {
		return errors.New("td")
	}
	if err := p.td.Listen(); err != nil {
		return err
	}
	emitGate()
	if p.sym != nil {
		p.sym.AllowResolve(true)
		defer p.sym.AllowResolve(false)
	}
	p.setHookCtx(ctx)
	defer p.setHookCtx(nil)
	var body []byte
	if p.rec != nil {
		body = payload
	}
	err := p.outside("OnStart", eventlog.KindStart, recvAt.UnixNano(), 0, nil, body, func() error {
		return p.strat.OnStart(p.sc)
	})
	if err != nil {
		return err
	}
	if p.asked.Load() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A hold that filled during OnStart has already recorded FAILED (E08-D041). Recon
	// refuses once the path is stopping. Returning that error would end Start as
	// INIT_FAILED and hide the failure Run is waiting to report.
	if err := p.td.Recon(); errors.Is(err, tdpath.ErrStopping) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func (p *plane) Run(ctx context.Context) error {
	p.loopStarted.Store(true)
	if p.disp == nil {
		if ctx == nil {
			return errors.New("dispatcher")
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return p.disp.Run(ctx)
}

// Live applies one stsctl.Bindings snapshot (E01-D011, E08-D046). The same snapshot, or
// an older one, is success, so c4 still sends Applied. A selector is an error
// and c4 sends nothing.
func (p *plane) Live(payload []byte) error {
	var at int64
	if p.rec != nil {
		at = p.now().UnixNano()
	}
	var b stsctlv1.Bindings
	if err := proto.Unmarshal(payload, &b); err != nil {
		return errors.New("bindings")
	}
	if err := p.applyBindings(&b); err != nil {
		return err
	}
	p.recordBindings(at, 0, payload)
	return nil
}

// Relay enqueues one Command. It runs on the fd 3 reader, so it does not apply
// the snapshot itself: Apply can flush for up to five seconds.
func (p *plane) Relay(corrID, payload []byte) {
	// The shim reads this echo. Applying the command stays asynchronous.
	if p.sess != nil {
		_ = p.sess.Relay(corrID, payload)
	}
	var at int64
	if p.rec != nil {
		at = p.now().UnixNano()
	}
	p.cmdMu.Lock()
	p.cmds = append(p.cmds, relayed{at: at, b: append([]byte(nil), payload...)})
	p.cmdCond.Signal()
	p.cmdMu.Unlock()
}

func (p *plane) OnStop(ctx context.Context) error {
	if p.loopStarted.Load() && p.disp != nil {
		return p.disp.OnStop(ctx)
	}
	if p.strat != nil && p.sc != nil {
		recvAt := p.now()
		_ = p.outside("OnStop", eventlog.KindOnStop, recvAt.UnixNano(), 0, nil, nil, func() error {
			p.strat.OnStop(p.sc)
			return nil
		})
	}
	return nil
}

func (p *plane) HookTimeout() time.Duration { return p.hookTimeout }

func (p *plane) strategyStop(context.Context) error {
	if p.strat == nil || p.sc == nil {
		return nil
	}
	p.inHook("OnStop", func() { p.strat.OnStop(p.sc) })
	return nil
}

func (p *plane) unsubTD() {
	p.mu.Lock()
	td := p.td
	p.mu.Unlock()
	if td != nil {
		td.Close()
	}
}

func (p *plane) applyBindings(b *stsctlv1.Bindings) error {
	if p.md == nil || b == nil {
		return errors.New("bindings")
	}
	if err := p.md.Apply(b); err != nil {
		return err
	}
	p.wakeReport()
	return nil
}

func (p *plane) handleCommand(r relayed) {
	var cmd stsctlv1.Command
	if err := proto.Unmarshal(r.b, &cmd); err != nil {
		p.note("command")
		return
	}
	switch c := cmd.GetCmd().(type) {
	case *stsctlv1.Command_Bindings:
		if err := p.applyBindings(c.Bindings); err != nil {
			p.note("command bindings: " + err.Error())
			return
		}
		if p.rec != nil {
			if raw, ok := fieldBytes(r.b, bindingsField); ok {
				p.recordBindings(r.at, eventlog.FromCommand, raw)
			}
		}
	case *stsctlv1.Command_Fail:
		reason := ""
		if c.Fail != nil {
			reason = c.Fail.GetReason()
		}
		p.asked.Store(true)
		if p.disp != nil {
			p.disp.Fail(reason)
		}
	}
}

func (p *plane) bookSnapshot(uid uint32, fn func(*mdv1.BookSnapshot, error)) error {
	if p.md == nil {
		return mdpath.ErrStopping
	}
	return p.md.BookSnapshot(uid, fn)
}

func (p *plane) bookInfo(uid uint32) (symbol.Info, error) {
	if p.sym == nil {
		return symbol.Info{}, symclient.ErrUnknownUID
	}
	return p.sym.Info(uid)
}

// bookHook is OnBook. Nil when the strategy does not implement it; the book is
// still maintained, and other hooks can read it (E08-D005, E08-D057).
func (p *plane) bookHook() func(bookpath.Update) {
	s, ok := p.strat.(BookHandler)
	if !ok || s == nil {
		return nil
	}
	return func(ev bookpath.Update) {
		p.inHook("OnBook", func() {
			s.OnBook(p.sc, BookUpdate{
				MD:       MD{UID: ev.UID, MdTsNs: ev.MdTsNs, ConnEpoch: ev.ConnEpoch, Seq: ev.Seq},
				ExchTsNs: ev.ExchTsNs,
			})
		})
	}
}

func (p *plane) schedule(delay time.Duration, fn func()) {
	if fn == nil || p.disp == nil {
		return
	}
	p.disp.Timer(fn).After(delay)
}

func (p *plane) resolveUID(uid uint32) (string, string, error) {
	if p.sym == nil {
		return "", "", symclient.ErrUnknownUID
	}
	info, err := p.sym.Info(uid)
	if err != nil {
		return "", "", err
	}
	return info.Ticker.String(), info.Ticker.Venue, nil
}

func (p *plane) startLoops() {
	p.mu.Lock()
	if p.dead {
		p.mu.Unlock()
		return
	}
	p.wake = make(chan struct{}, 1)
	p.cmdDone = make(chan struct{})
	p.repDone = make(chan struct{})
	p.loops = true
	p.mu.Unlock()
	go p.cmdLoop()
	go p.reportLoop()
}

func (p *plane) cmdLoop() {
	defer close(p.cmdDone)
	for {
		p.cmdMu.Lock()
		for len(p.cmds) == 0 && !p.cmdStop {
			p.cmdCond.Wait()
		}
		if len(p.cmds) == 0 && p.cmdStop {
			p.cmdMu.Unlock()
			return
		}
		r := p.cmds[0]
		p.cmds = p.cmds[1:]
		p.cmdMu.Unlock()
		p.handleCommand(r)
	}
}

func (p *plane) shutdown() {
	p.mu.Lock()
	if p.shut {
		p.mu.Unlock()
		return
	}
	p.shut = true
	p.dead = true
	loops := p.loops
	p.mu.Unlock()

	p.cancel()
	p.cmdMu.Lock()
	p.cmdStop = true
	p.cmdCond.Broadcast()
	p.cmdMu.Unlock()
	if loops {
		<-p.cmdDone
		<-p.repDone
	}

	p.mu.Lock()
	nc := p.nc
	md := p.md
	td := p.td
	sym := p.sym
	books := p.books
	rec := p.rec
	p.nc, p.md, p.td, p.sym, p.books = nil, nil, nil, nil, nil
	p.mu.Unlock()
	// Last, so the frames of everything closed here are in it. The process exits after
	// shutdown returns, so this is where the ring is flushed (E20-D005).
	defer rec.Close()
	if books != nil {
		books.Close()
	}
	if md != nil {
		md.Close()
	}
	if td != nil {
		td.Close()
	}
	p.symPtr.Store(nil)
	if sym != nil {
		sym.Close()
		<-sym.Exited()
	}
	if nc != nil {
		nc.Close()
	}
}

// own records a client created during Init. A deadline can already be tearing
// the plane down; the client is then left to the caller to close (E08-D007).
func (p *plane) own(set func()) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dead {
		if err := p.root.Err(); err != nil {
			return err
		}
		return context.Canceled
	}
	set()
	return nil
}

func (p *plane) closing() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dead
}

func (p *plane) setHookCtx(ctx context.Context) {
	p.mu.Lock()
	p.hookCtx = ctx
	p.mu.Unlock()
}

func (p *plane) note(msg string) {
	if p.host.log != nil {
		p.host.log(msg)
		return
	}
	log := p.logger()
	log.Info().Msg(msg)
}
