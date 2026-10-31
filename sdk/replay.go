package sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	stsctlv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/stsctl/v1"
	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	tdv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/td/v1"
	"github.com/BullionBear/mftiko/contract/symbol"
	"github.com/BullionBear/mftiko/sdk/eventlog"
	"github.com/BullionBear/mftiko/sdk/internal/bookpath"
	"github.com/BullionBear/mftiko/sdk/internal/cloidgen"
	"github.com/BullionBear/mftiko/sdk/internal/dispatch"
	"github.com/BullionBear/mftiko/sdk/internal/evlog"
	"github.com/BullionBear/mftiko/sdk/internal/mdpath"
	"github.com/BullionBear/mftiko/sdk/internal/symclient"
	"github.com/BullionBear/mftiko/sdk/internal/tdpath"
)

// ReplayOptions tunes Replay. The zero value replays the newest incarnation in the directory with
// no volume or input directory and discards the SDK's own lines.
type ReplayOptions struct {
	// Incarnation is the incarnation to replay. Zero is the newest one in the directory.
	Incarnation uint32
	// VolumeDir and InputDir are what Context.VolumeDir and Context.InputDir use during the replay.
	VolumeDir string
	InputDir  string
	// Log receives the SDK's own lines. Nil discards them.
	Log func(string)
}

// ReplayReport is what Replay found.
type ReplayReport struct {
	SessionID   string
	Incarnation uint32
	// SDKVersion and SDKCommit are the release that wrote the log.
	SDKVersion string
	SDKCommit  string
	// Inputs counts the recorded inputs whose hooks ran again, OnInit and OnStart included.
	Inputs int
	// Skipped counts inputs whose hook did not return in the recording: the watchdog's HUNG, or a
	// log that ends inside the hook. They are not run again (E20-D005).
	Skipped int
	// Missing counts rec_seq numbers that are not in the files: frames the ring dropped, or files
	// the keep limit removed. A replay with missing frames can differ from the recording.
	Missing uint64
	// Orders and Reads count the recorded order calls and md reads that were compared.
	Orders int
	Reads  int
	// Mismatches lists each way the replay differs from the recording: an order call, request, read
	// or Resolve it did not repeat, or a hook that ended differently. Empty means the outputs are
	// the same, call for call and client order id for client order id.
	Mismatches []string
}

// Replay runs s again over one incarnation of the event log in dir, the session's
// VolumeDir()/eventlog, without NATS or C4 (E20-D009). Each recorded input runs as the hook it
// was, in the recorded order, with Context.Now at the recorded start and Context.Rand on the
// recorded seed. Resolve, td.order, td.view, md.view and md.fetch answer as recorded, timers and
// Posts run where the log says they ran, and the client order ids are issued from the recorded
// counter and call times (E20-D004). What the strategy sends is then compared with what it sent:
// the order calls with their client order ids, the requests, and the reads.
//
// s must be a new value, as the worker would construct it. Replay does not exit and prints
// nothing. An error means the log could not be replayed at all; differences are in the report.
func Replay(s Strategy, dir string, opt ReplayOptions) (*ReplayReport, error) {
	if s == nil {
		return nil, errors.New("sdk: replay: nil strategy")
	}
	recs, h, err := readLog(dir, opt.Incarnation)
	if err != nil {
		return nil, err
	}
	logf := opt.Log
	if logf == nil {
		logf = func(string) {}
	}
	r := &replayer{
		p:     newPlane(s, host{log: logf}),
		out:   evlog.Memory(),
		recs:  recs,
		ends:  map[uint64]eventlog.Record{},
		calls: map[uint64]eventlog.Record{},
		rep: &ReplayReport{
			SessionID:   h.SessionID,
			Incarnation: h.Incarnation,
			SDKVersion:  h.SDKVersion,
			SDKCommit:   h.SDKCommit,
		},
	}
	r.p.rp = r
	r.p.volumeDir = opt.VolumeDir
	r.p.inputDir = opt.InputDir
	defer r.p.shutdown()
	if err := r.run(); err != nil {
		return nil, err
	}
	return r.rep, nil
}

// readLog reads every frame of one incarnation, oldest file first. A file that ends inside a frame
// is what a process killed during a write leaves; the frames before it are kept.
func readLog(dir string, inc uint32) ([]eventlog.Record, eventlog.Header, error) {
	files, err := eventlog.Files(dir)
	if err != nil {
		return nil, eventlog.Header{}, err
	}
	if inc == 0 {
		for _, f := range files {
			inc = max(inc, f.Incarnation)
		}
	}
	var (
		recs  []eventlog.Record
		h     eventlog.Header
		found bool
	)
	for _, f := range files {
		if f.Incarnation != inc {
			continue
		}
		rd, err := eventlog.Open(f.Path)
		if err != nil {
			return nil, h, fmt.Errorf("sdk: replay: %s: %w", f.Path, err)
		}
		if !found {
			h, found = rd.Header(), true
		}
		for {
			x, err := rd.Next()
			if errors.Is(err, io.EOF) || errors.Is(err, eventlog.ErrTruncated) {
				break
			}
			if err != nil {
				_ = rd.Close()
				return nil, h, fmt.Errorf("sdk: replay: %s: %w", f.Path, err)
			}
			recs = append(recs, x)
		}
		_ = rd.Close()
	}
	if !found {
		return nil, h, fmt.Errorf("sdk: replay: %s has no event log of incarnation %d", dir, inc)
	}
	return recs, h, nil
}

// replayer is one Replay. Everything runs on the caller's goroutine.
type replayer struct {
	p   *plane
	out *evlog.Log // what the replay sends: order_call, order_publish, read_call
	rep *ReplayReport

	recs     []eventlog.Record
	ends     map[uint64]eventlog.Record // hook_end by the rec_seq of its input
	calls    map[uint64]eventlog.Record // recorded order_call by call id
	resolves []eventlog.Record          // recorded resolve frames, in call order
	resolved int                        // resolve frames answered so far
	stopped  bool                       // the dispatcher's stop frame has been read
}

// errDiverged answers a call the recording does not have.
var errDiverged = errors.New("sdk: replay: the recording has no such call")

// replayRing is the MD ring of a replay's dispatcher. Nothing is queued in a replay.
const replayRing = 1 << 10

func (r *replayer) run() error {
	var last uint64
	for _, x := range r.recs {
		if x.Seq > last+1 {
			r.rep.Missing += x.Seq - last - 1
		}
		last = max(last, x.Seq)
		switch x.Kind {
		case eventlog.KindHookEnd:
			r.ends[x.Ref] = x
		case eventlog.KindResolve:
			r.resolves = append(r.resolves, x)
		case eventlog.KindOrderCall:
			r.calls[x.Ref] = x
		}
	}
	if len(r.recs) == 0 || r.recs[0].Kind != eventlog.KindInit {
		return errors.New("sdk: replay: the log does not start with the init frame")
	}
	for _, x := range r.recs {
		switch {
		case x.Kind == eventlog.KindInit:
			if r.p.disp != nil {
				return fmt.Errorf("sdk: replay: rec_seq %d: a second init frame", x.Seq)
			}
			if err := r.init(x); err != nil {
				return err
			}
		case x.Kind == eventlog.KindStart:
			r.start(x)
		case x.Kind == eventlog.KindBindings:
			r.bindings(x)
		case x.Kind == eventlog.KindStop:
			r.stop(x)
		case x.Kind == eventlog.KindOnStop:
			if r.stopped {
				r.p.disp.CloseTD()
			}
			r.hook(x, func() error {
				r.p.strat.OnStop(r.p.sc)
				return nil
			})
		case x.Kind.Input():
			r.step(x)
		}
	}
	if n := len(r.resolves) - r.resolved; n > 0 {
		r.mismatch("%d recorded Resolve calls were not made again", n)
	}
	r.compare()
	return nil
}

func (r *replayer) init(x eventlog.Record) error {
	slot, payload, err := eventlog.ParseInit(x.Body)
	if err != nil {
		return fmt.Errorf("sdk: replay: %w", err)
	}
	var init stsctlv1.InitPayload
	if err := proto.Unmarshal(payload, &init); err != nil {
		return fmt.Errorf("sdk: replay: init payload: %w", err)
	}
	if err := r.open(&init, slot, x.Ref); err != nil {
		return err
	}
	r.hook(x, func() error { return r.p.strat.OnInit(r.p.sc) })
	return nil
}

// open builds the plane as Init does, with every path in its replay mode.
func (r *replayer) open(init *stsctlv1.InitPayload, slot, seed uint64) error {
	p := r.p
	if init.GetRecvWindowMs() == 0 {
		return errors.New("sdk: replay: the init payload has no recv window")
	}
	p.sessionID = init.GetSessionId()
	p.params = append([]byte(nil), init.GetParams()...)
	p.hookTimeout = time.Duration(init.GetHookTimeoutS()) * time.Second
	p.recv = time.Duration(init.GetRecvWindowMs()) * time.Millisecond
	p.seed = seed
	p.tap = &mdpath.Tap{}
	p.disp = dispatch.New(dispatch.Config{
		RecvWindow: p.recv,
		Clock:      &p.clock,
		Feed:       p.tap.Feed,
		Book:       p.tap.Book,
		Log:        p.note,
		Replay:     true,
		RingBytes:  replayRing,
	})
	sym, err := symclient.Open(p.root, noSYM{}, symclient.Options{CheckInterval: 24 * time.Hour, Log: p.note})
	if err != nil {
		return err
	}
	p.sym = sym
	p.symPtr.Store(sym)
	td, err := tdpath.Open(tdpath.Config{
		SessionID:  p.sessionID,
		Accounts:   init.GetAccounts(),
		RecvWindow: p.recv,
		Push:       p.disp.PushIn,
		Fail:       p.disp.Fail,
		Now:        p.stepNow,
		Hooks:      p.tdHooks(),
		Log:        p.note,
		Rec:        r.out,
		Gen:        cloidgen.NewGen(slot),
		Replay:     &tdpath.Replay{Call: r.call},
	})
	if err != nil {
		return err
	}
	p.td = td
	md, err := mdpath.Open(mdpath.Config{
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
		Log:    p.note,
		Rec:    r.out,
		Now:    p.stepNow,
		Replay: true,
	})
	if err != nil {
		return err
	}
	p.md = md
	bk, err := bookpath.Open(bookpath.Config{
		Snapshot:   p.bookSnapshot,
		Info:       p.bookInfo,
		Hook:       p.bookHook(),
		RecvWindow: p.recv,
		Now:        p.stepNow,
		Schedule:   p.schedule,
		Log:        p.note,
	})
	if err != nil {
		return err
	}
	p.books = bk
	return nil
}

// start is Start: the bindings, the TD subscription, OnStart, and the first Recon, which a replay
// does not send. The td_view that answered it is a frame of its own.
func (r *replayer) start(x eventlog.Record) {
	var sp stsctlv1.StartPayload
	if err := proto.Unmarshal(x.Body, &sp); err != nil || sp.GetBindings() == nil {
		r.mismatch("rec_seq %d: the start payload does not decode", x.Seq)
		return
	}
	if err := r.p.md.Apply(sp.GetBindings()); err != nil {
		r.mismatch("rec_seq %d: start bindings: %v", x.Seq, err)
	}
	if err := r.p.td.Listen(); err != nil {
		r.mismatch("rec_seq %d: %v", x.Seq, err)
	}
	r.hook(x, func() error { return r.p.strat.OnStart(r.p.sc) })
	_ = r.p.td.Recon()
}

// stop is the dispatcher's ending. With StopDeliver the session dropped TD before it delivered the
// buffer, so from here on an order call is refused as it was (ErrStopping), and the buffered
// inputs that follow in the log still run.
func (r *replayer) stop(x eventlog.Record) {
	if x.Flags&eventlog.StopHung != 0 {
		return
	}
	r.stopped = true
	if x.Flags&eventlog.StopDeliver != 0 {
		r.p.td.Close()
	}
}

func (r *replayer) bindings(x eventlog.Record) {
	var b stsctlv1.Bindings
	if err := proto.Unmarshal(x.Body, &b); err != nil {
		r.mismatch("rec_seq %d: bindings do not decode", x.Seq)
		return
	}
	if err := r.p.md.Apply(&b); err != nil {
		r.mismatch("rec_seq %d: bindings: %v", x.Seq, err)
	}
}

// step runs one input the dispatcher ran.
func (r *replayer) step(x eventlog.Record) {
	p := r.p
	var fn func()
	switch x.Kind {
	case eventlog.KindMDFeed:
		fn = func() { p.tap.Feed(x.Body) }
	case eventlog.KindMDBook:
		fn = func() { p.tap.Book(x.Body) }
	case eventlog.KindTDEvent:
		fn = func() { r.check(x, p.td.ReplayEvent(int64(x.Ref), x.Body)) }
	case eventlog.KindTDView:
		fn = func() { r.check(x, p.td.ReplayView(int64(x.Ref), x.Body)) }
	case eventlog.KindOrderReply:
		fn = func() { r.check(x, p.td.ReplayReply(x.Ref, x.Flags, x.Body)) }
	case eventlog.KindReadReply:
		fn = func() { r.check(x, p.md.ReplayReply(x.Ref, x.Flags, x.Body)) }
	case eventlog.KindTimer:
		if fn = p.disp.TimerFunc(x.Ref); fn == nil && r.ended(x) {
			r.mismatch("rec_seq %d: timer %d ran in the recording; the replay did not make it", x.Seq, x.Ref)
			return
		}
	case eventlog.KindPost:
		if fn = p.disp.TakePost(x.Ref); fn == nil && r.ended(x) {
			r.mismatch("rec_seq %d: post %d ran in the recording; the replay did not make it", x.Seq, x.Ref)
			return
		}
	}
	if fn == nil {
		return
	}
	r.hook(x, func() error {
		fn()
		return nil
	})
}

// ended reports whether the hook of input x returned in the recording.
func (r *replayer) ended(x eventlog.Record) bool {
	_, ok := r.ends[x.Seq]
	return ok
}

// hook runs fn as the hook of input x, with the step clock at the recorded start, and compares
// how it ended with the recorded hook_end. An input whose hook did not return is not run.
func (r *replayer) hook(x eventlog.Record, fn func() error) {
	end, ok := r.ends[x.Seq]
	if !ok {
		r.rep.Skipped++
		return
	}
	var err error
	panicked := r.p.disp.Step(x.TNs, func() { err = fn() })
	r.rep.Inputs++
	flags, text := dispatch.EndOf(panicked, err)
	if flags != end.Flags || !bytes.Equal(text, end.Body) {
		r.mismatch("rec_seq %d %s: the hook ended %s; recorded %s", x.Seq, x.Kind, ending(flags, text), ending(end.Flags, end.Body))
	}
}

func ending(flags uint16, text []byte) string {
	switch flags {
	case 0:
		return "normally"
	case eventlog.EndPanic:
		return fmt.Sprintf("with panic %q", text)
	case eventlog.EndError:
		return fmt.Sprintf("with error %q", text)
	}
	return fmt.Sprintf("with flags %d %q", flags, text)
}

func (r *replayer) check(x eventlog.Record, err error) {
	if err != nil {
		r.mismatch("rec_seq %d %s: %v", x.Seq, x.Kind, err)
	}
}

func (r *replayer) mismatch(format string, args ...any) {
	r.rep.Mismatches = append(r.rep.Mismatches, fmt.Sprintf(format, args...))
}

// call is the recorded time and td.view generation of order call id (tdpath.Replay).
func (r *replayer) call(id uint64) (int64, uint64, bool) {
	x, ok := r.calls[id]
	if !ok {
		return 0, 0, false
	}
	c, err := eventlog.ParseOrderCall(x.Body)
	if err != nil {
		return 0, 0, false
	}
	return x.TNs, c.SubmitGen, true
}

// resolve answers Context.Resolve with the next recorded resolve frame, and keeps the row so Info
// answers as it did.
func (r *replayer) resolve(ticker string) (symbol.Info, error) {
	if r.resolved == len(r.resolves) {
		r.mismatch("Resolve(%q): the recording has no more Resolve calls", ticker)
		return symbol.Info{}, errDiverged
	}
	x := r.resolves[r.resolved]
	r.resolved++
	asked, rest, err := eventlog.ParseResolve(x.Body)
	if err != nil {
		r.mismatch("rec_seq %d: %v", x.Seq, err)
		return symbol.Info{}, errDiverged
	}
	if asked != ticker {
		r.mismatch("Resolve %d: the replay asked for %q; the recording for %q", r.resolved, ticker, asked)
		return symbol.Info{}, errDiverged
	}
	if x.Flags != eventlog.OK {
		return symbol.Info{}, resolveErr(x.Flags, rest)
	}
	msg := &symv1.SymbolInfo{}
	if err := proto.Unmarshal(rest, msg); err != nil {
		r.mismatch("rec_seq %d: SymbolInfo does not decode", x.Seq)
		return symbol.Info{}, errDiverged
	}
	info, err := symclient.InfoFromProto(msg)
	if err != nil {
		r.mismatch("rec_seq %d: %v", x.Seq, err)
		return symbol.Info{}, errDiverged
	}
	r.p.sym.Seed(info)
	return info, nil
}

// compare puts what the replay sent next to what the recording sent: the order calls one by one,
// the request of each call, and the reads one by one.
func (r *replayer) compare() {
	var calls, reads, callsAgain, readsAgain []eventlog.Record
	pubs := map[uint64]eventlog.Record{}
	for _, x := range r.recs {
		switch x.Kind {
		case eventlog.KindOrderCall:
			calls = append(calls, x)
		case eventlog.KindOrderPublish:
			pubs[x.Ref] = x
		case eventlog.KindReadCall:
			reads = append(reads, x)
		}
	}
	pubsAgain := map[uint64]eventlog.Record{}
	for _, x := range r.out.Records() {
		switch x.Kind {
		case eventlog.KindOrderCall:
			callsAgain = append(callsAgain, x)
		case eventlog.KindOrderPublish:
			pubsAgain[x.Ref] = x
		case eventlog.KindReadCall:
			readsAgain = append(readsAgain, x)
		}
	}
	r.rep.Orders = len(calls)
	r.rep.Reads = len(reads)
	for i := range max(len(calls), len(callsAgain)) {
		switch {
		case i >= len(callsAgain):
			r.mismatch("order call %d: recorded %s; the replay did not make it", calls[i].Ref, describeCall(calls[i]))
		case i >= len(calls):
			r.mismatch("order call %d: the replay made %s; the recording did not", callsAgain[i].Ref, describeCall(callsAgain[i]))
		case calls[i].Flags != callsAgain[i].Flags || calls[i].Ref != callsAgain[i].Ref || !bytes.Equal(calls[i].Body, callsAgain[i].Body):
			r.mismatch("order call %d: recorded %s; replayed %s", calls[i].Ref, describeCall(calls[i]), describeCall(callsAgain[i]))
		}
	}
	// A request the recording has no order_publish for did not reach the connection; the order
	// call comparison above already covers whether the call was made.
	for _, ref := range slices.Sorted(maps.Keys(pubs)) {
		y, ok := pubsAgain[ref]
		if !ok {
			r.mismatch("request of order call %d: recorded; the replay did not send it", ref)
			continue
		}
		a, b := &tdv1.OrderRequest{}, &tdv1.OrderRequest{}
		if proto.Unmarshal(pubs[ref].Body, a) != nil || proto.Unmarshal(y.Body, b) != nil || !proto.Equal(a, b) {
			r.mismatch("request of order call %d: recorded %v; replayed %v", ref, a, b)
		}
	}
	for i := range max(len(reads), len(readsAgain)) {
		switch {
		case i >= len(readsAgain):
			r.mismatch("read %d: recorded %s; the replay did not make it", reads[i].Ref, describeRead(reads[i]))
		case i >= len(reads):
			r.mismatch("read %d: the replay made %s; the recording did not", readsAgain[i].Ref, describeRead(readsAgain[i]))
		case reads[i].Flags != readsAgain[i].Flags || reads[i].Ref != readsAgain[i].Ref || !bytes.Equal(reads[i].Body, readsAgain[i].Body):
			r.mismatch("read %d: recorded %s; replayed %s", reads[i].Ref, describeRead(reads[i]), describeRead(readsAgain[i]))
		}
	}
}

func describeCall(x eventlog.Record) string {
	c, err := eventlog.ParseOrderCall(x.Body)
	if err != nil {
		return eventlog.OpName(x.Flags) + " (malformed)"
	}
	s := fmt.Sprintf("%s api %d", eventlog.OpName(x.Flags), c.APIID)
	if len(c.Cloids) > 0 {
		s += " [" + strings.Join(c.Cloids, " ") + "]"
	}
	return s
}

func describeRead(x eventlog.Record) string {
	subject, _, err := eventlog.ParseReadCall(x.Body)
	if err != nil {
		return eventlog.ReadName(x.Flags) + " (malformed)"
	}
	return eventlog.ReadName(x.Flags) + " " + subject
}

// noSYM is the SYM of a replay: there is none. Resolve answers from the log and Info from the rows
// it seeded, so nothing asks.
type noSYM struct{}

func (noSYM) Request(context.Context, string, proto.Message, proto.Message) error {
	return errors.New("sdk: replay: no SYM")
}

func (noSYM) Subscribe(context.Context, string, func(string, []byte)) error { return nil }
