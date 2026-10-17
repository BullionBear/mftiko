package sdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	"github.com/BullionBear/mftiko/contract/wire/c4"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
	"github.com/BullionBear/mftiko/sdk/internal/mdpath"
	"github.com/BullionBear/mftiko/sdk/internal/symclient"
	"github.com/BullionBear/mftiko/sdk/internal/tdpath"
)

// helloContracts is what a session worker lists (PROTOCOL §9.2). The C5 contract
// checked on Init, Start, and Live stays stsctl.v1.
var helloContracts = []string{"worker.v1", "stsctl.v1", "md.v1", "td.v1"}

const contractSTS = "stsctl.v1"

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
	// stopped waiting for (D807). Tests wait on it. Nil in production.
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
	disp   *dispatch.Disp
	tap    *mdpath.Tap

	sessionID   string
	params      []byte
	volumeDir   string
	inputDir    string
	hookTimeout time.Duration
	recv        time.Duration
	hookCtx     context.Context

	ledger *ledger

	mu    sync.Mutex
	dead  bool
	shut  bool
	loops bool

	hook atomic.Value // string, the hook currently running; "" when none

	cmdMu   sync.Mutex
	cmdCond *sync.Cond
	cmds    [][]byte
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
}

func newPlane(s Strategy, h host) *plane {
	ctx, cancel := context.WithCancel(context.Background())
	p := &plane{
		host:   h,
		strat:  s,
		root:   ctx,
		cancel: cancel,
		ledger: newLedger(),
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

	nc, err := p.connectNATS(ctx, url, creds)
	if err != nil {
		return err
	}
	if err := p.own(func() { p.nc = nc }); err != nil {
		nc.Close()
		return err
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
		Push:       p.disp.PushTD,
		Fail:       p.disp.Fail,
		Now:        p.host.now,
		Hooks:      p.tdHooks(),
		Log:        p.note,
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
		Log:        p.note,
	})
	if err != nil {
		return err
	}
	if err := p.own(func() { p.md = md }); err != nil {
		md.Close()
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
	p.hook.Store("OnInit")
	err = p.strat.OnInit(p.sc)
	p.hook.Store("")
	return err
}

func (p *plane) Start(ctx context.Context, payload []byte) error {
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
	p.hook.Store("OnStart")
	err := p.strat.OnStart(p.sc)
	p.hook.Store("")
	if err != nil {
		return err
	}
	if p.asked.Load() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.td.Recon()
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

// Live applies one stsctl.Bindings snapshot (D111, D846). The same snapshot, or
// an older one, is success, so c4 still sends Applied. A selector is an error
// and c4 sends nothing.
func (p *plane) Live(payload []byte) error {
	var b stsctlv1.Bindings
	if err := proto.Unmarshal(payload, &b); err != nil {
		return errors.New("bindings")
	}
	return p.applyBindings(&b)
}

// Relay enqueues one Command. It runs on the fd 3 reader, so it does not apply
// the snapshot itself: Apply can flush for up to five seconds.
func (p *plane) Relay(_, payload []byte) {
	p.cmdMu.Lock()
	p.cmds = append(p.cmds, append([]byte(nil), payload...))
	p.cmdCond.Signal()
	p.cmdMu.Unlock()
}

func (p *plane) OnStop(ctx context.Context) error {
	if p.loopStarted.Load() && p.disp != nil {
		return p.disp.OnStop(ctx)
	}
	if p.strat != nil && p.sc != nil {
		p.hook.Store("OnStop")
		p.strat.OnStop(p.sc)
		p.hook.Store("")
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

func (p *plane) handleCommand(b []byte) {
	var cmd stsctlv1.Command
	if err := proto.Unmarshal(b, &cmd); err != nil {
		p.note("command")
		return
	}
	switch c := cmd.GetCmd().(type) {
	case *stsctlv1.Command_Bindings:
		if err := p.applyBindings(c.Bindings); err != nil {
			p.note("command bindings: " + err.Error())
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
		b := p.cmds[0]
		p.cmds = p.cmds[1:]
		p.cmdMu.Unlock()
		p.handleCommand(b)
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
	p.nc, p.md, p.td, p.sym = nil, nil, nil, nil
	p.mu.Unlock()
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
// the plane down; the client is then left to the caller to close (D807).
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
	fmt.Fprintln(os.Stderr, "mftiko sdk:", msg)
}
