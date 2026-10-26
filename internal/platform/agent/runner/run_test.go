package runner

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

const inst = "md-tw-1"

// h is a spec or live config that is its own hash.
type h string

func (x h) Hash() string { return string(x) }

// journal is what the fakes were asked to do, in order, across all of them.
type journal struct {
	mu      sync.Mutex
	entries []string
	changed chan struct{}
}

func newJournal() *journal { return &journal{changed: make(chan struct{}, 1)} }

func (j *journal) add(format string, args ...any) {
	j.mu.Lock()
	j.entries = append(j.entries, fmt.Sprintf(format, args...))
	j.mu.Unlock()
	select {
	case j.changed <- struct{}{}:
	default:
	}
}

func (j *journal) all() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.entries)
}

// waitFor waits until the journal holds an entry with this prefix, and returns the entries so far.
func (j *journal) waitFor(t *testing.T, prefix string) []string {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		all := j.all()
		for _, e := range all {
			if strings.HasPrefix(e, prefix) {
				return all
			}
		}
		select {
		case <-j.changed:
		case <-deadline:
			t.Fatalf("no %q in %q", prefix, all)
		}
	}
}

// never fails if an entry with this prefix shows up within d.
func (j *journal) never(t *testing.T, prefix string, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	for _, e := range j.all() {
		if strings.HasPrefix(e, prefix) {
			t.Fatalf("%q in %q", prefix, j.all())
		}
	}
}

type fakeTerm struct {
	j    *journal
	done chan struct{}
	mu   sync.Mutex
	err  error
	kv   map[string]string
	rec  *statev1.AgentRecord
}

func newTerm(j *journal) *fakeTerm {
	return &fakeTerm{j: j, done: make(chan struct{}), kv: map[string]string{}}
}

func (t *fakeTerm) Plane() state.Plane    { return state.PlaneMD }
func (t *fakeTerm) Instance() string      { return inst }
func (t *fakeTerm) Epoch() int64          { return 7 }
func (t *fakeTerm) Done() <-chan struct{} { return t.done }
func (t *fakeTerm) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}
func (t *fakeTerm) end(err error) {
	t.mu.Lock()
	t.err = err
	t.mu.Unlock()
	close(t.done)
}
func (t *fakeTerm) PutRecord(_ context.Context, val string) error {
	rec := &statev1.AgentRecord{}
	if err := proto.Unmarshal([]byte(val), rec); err != nil {
		return err
	}
	t.mu.Lock()
	t.rec = rec
	t.mu.Unlock()
	t.j.add("record epoch %d", rec.GetEpoch())
	return nil
}
func (t *fakeTerm) record() *statev1.AgentRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rec
}
func (t *fakeTerm) Put(_ context.Context, key, val string, _ ...clientv3.OpOption) error {
	st := &statev1.WorkerStatus{}
	if err := proto.Unmarshal([]byte(val), st); err != nil {
		return err
	}
	t.mu.Lock()
	t.kv[key] = val
	t.mu.Unlock()
	t.j.add("put %s %s", st.GetWorker().GetKey(), st.GetPhase())
	return nil
}
func (t *fakeTerm) Delete(_ context.Context, key string, _ ...clientv3.OpOption) error {
	t.mu.Lock()
	delete(t.kv, key)
	t.mu.Unlock()
	t.j.add("delete %s", key)
	return nil
}

// lister returns the statuses etcd held at startup.
type lister map[string]string

func (l lister) Get(_ context.Context, key string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp := &clientv3.GetResponse{}
	for _, k := range slices.Sorted(maps.Keys(l)) {
		if strings.HasPrefix(k, key) {
			resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{Key: []byte(k), Value: []byte(l[k])})
		}
	}
	return resp, nil
}

// source is a demand prefix the test edits: the value of each entry is the spec of one worker, keyed by its name.
type source struct {
	synced chan struct{}
	notify chan struct{}
	mu     sync.Mutex
	rev    int64
	view   map[string]state.Entry
	// contact is the LastContact: now, unless the test froze it
	contact time.Time
}

func newSource() *source {
	return &source{synced: make(chan struct{}), notify: make(chan struct{}, 1), view: map[string]state.Entry{}}
}

func (s *source) Synced() <-chan struct{} { return s.synced }
func (s *source) Notify() <-chan struct{} { return s.notify }
func (s *source) Snapshot() state.View {
	s.mu.Lock()
	defer s.mu.Unlock()
	return state.View{Rev: s.rev, Entries: maps.Clone(s.view)}
}
func (s *source) LastContact() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.contact.IsZero() {
		return time.Now()
	}
	return s.contact
}
func (s *source) freeze(at time.Time) {
	s.mu.Lock()
	s.contact = at
	s.mu.Unlock()
}
func (s *source) put(name, spec string) {
	s.mu.Lock()
	s.rev++
	k := state.DemandPrefix(state.PlaneMD, inst) + name
	s.view[k] = state.Entry{Key: k, Value: []byte(spec), CreateRevision: s.rev, ModRevision: s.rev}
	s.mu.Unlock()
	s.signal()
}
func (s *source) del(name string) {
	s.mu.Lock()
	s.rev++
	delete(s.view, state.DemandPrefix(state.PlaneMD, inst)+name)
	s.mu.Unlock()
	s.signal()
}
func (s *source) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// decode takes a spec; "bad" is refused.
func decode(key string, v []byte) (string, error) {
	if string(v) == "bad" {
		return "", errors.New("bad entry")
	}
	return key[strings.LastIndexByte(key, '/')+1:] + "=" + string(v), nil
}

// planner plans one worker md/<name> per entry, with the entry's spec, a fixed live config, and Start.
type planner struct{}

func (planner) Plan(d agent.Demand[string]) map[agent.WorkerKey]agent.Plan[h, h] {
	out := map[agent.WorkerKey]agent.Plan[h, h]{}
	for _, item := range d.Items {
		name, spec, _ := strings.Cut(item, "=")
		out[agent.WorkerKey("md/"+name)] = agent.Plan[h, h]{Spawn: h(spec), Live: "l", Start: true}
	}
	return out
}

// fleet is a Fleet the test moves along: an action changes what it observes at once, as a real one does.
type fleet struct {
	j         *journal
	changed   chan struct{}
	adopt     map[agent.WorkerKey]agent.Observed
	mu        sync.Mutex
	obs       map[agent.WorkerKey]agent.Observed
	block     chan struct{} // Spawn waits on it, or on its ctx, when set
	held      []string      // Unreachable
	preempted chan struct{}
}

func newFleet(j *journal) *fleet {
	return &fleet{j: j, changed: make(chan struct{}, 1), obs: map[agent.WorkerKey]agent.Observed{},
		preempted: make(chan struct{})}
}

func (f *fleet) Adopt(context.Context) error {
	f.mu.Lock()
	maps.Copy(f.obs, f.adopt)
	f.mu.Unlock()
	f.j.add("adopt %d", len(f.adopt))
	return nil
}
func (f *fleet) Observe() map[agent.WorkerKey]agent.Observed {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.obs)
}
func (f *fleet) Changed() <-chan struct{} { return f.changed }
func (f *fleet) set(k agent.WorkerKey, mod func(*agent.Observed)) {
	f.mu.Lock()
	o := f.obs[k]
	mod(&o)
	f.obs[k] = o
	f.mu.Unlock()
	select {
	case f.changed <- struct{}{}:
	default:
	}
}
func (f *fleet) Spawn(ctx context.Context, k agent.WorkerKey, spec, live h) error {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			f.j.add("spawn %s cancelled", k)
			return ctx.Err()
		}
	}
	f.mu.Lock()
	inc := f.obs[k].Incarnation + 1
	f.obs[k] = agent.Observed{Key: k, Incarnation: inc, SpecHash: string(spec), LiveHash: string(live),
		State: agent.StateInitializing, StartedAt: time.Now()}
	f.mu.Unlock()
	f.j.add("spawn %s %s", k, spec)
	return nil
}
func (f *fleet) Start(_ context.Context, k agent.WorkerKey, _ h) error {
	f.set(k, func(o *agent.Observed) { o.State = agent.StateStarting })
	f.j.add("start %s", k)
	return nil
}
func (f *fleet) Push(_ context.Context, k agent.WorkerKey, live h) error {
	f.set(k, func(o *agent.Observed) { o.LiveHash = string(live) })
	f.j.add("push %s %s", k, live)
	return nil
}
func (f *fleet) Stop(_ context.Context, k agent.WorkerKey, reason string) error {
	f.set(k, func(o *agent.Observed) { o.State = agent.StateStopping })
	f.j.add("stop %s: %s", k, reason)
	return nil
}
func (f *fleet) Preempted() <-chan struct{} { return f.preempted }
func (f *fleet) PreemptedErr() error {
	select {
	case <-f.preempted:
		return errPreempted
	default:
		return nil
	}
}

var errPreempted = errors.New("preempted")

func (f *fleet) Unreachable() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.held)
}
func (f *fleet) Release(_ context.Context, k agent.WorkerKey) error {
	f.mu.Lock()
	delete(f.obs, k)
	f.mu.Unlock()
	f.j.add("release %s", k)
	return nil
}

type harness struct {
	j     *journal
	term  *fakeTerm
	src   *source
	fleet *fleet
	cfg   Config[string, h, h]
	ret   chan error
}

func newHarness(t *testing.T, initial lister) *harness {
	j := newJournal()
	hs := &harness{j: j, term: newTerm(j), src: newSource(), fleet: newFleet(j), ret: make(chan error, 1)}
	hs.cfg = Config[string, h, h]{
		Term: hs.term, Demand: hs.src, Statuses: initial, Decode: decode, Planner: planner{},
		Policy: agent.Always{Backoff: agent.Backoff{Min: 10 * time.Millisecond, Max: 10 * time.Millisecond}},
		Fleet:  hs.fleet, Release: "test", Resync: 10 * time.Millisecond, Log: zerolog.Nop(),
		Prepare: func(_ context.Context, d agent.Demand[string], obs map[agent.WorkerKey]agent.Observed) error {
			j.add("prepare %d demand %d observed", len(d.Items), len(obs))
			return nil
		},
	}
	return hs
}

func (hs *harness) run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { hs.ret <- Run(ctx, hs.cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-hs.ret:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return")
		}
	})
}

func statusKey(k agent.WorkerKey) string {
	return state.WorkerStatusKey(state.PlaneMD, inst, state.WorkerHash(string(k)))
}

func statusVal(k agent.WorkerKey, inc uint32, phase string) string {
	return encode(&statev1.WorkerStatus{Worker: &commonv1.WorkerRef{Key: string(k), Incarnation: inc}, Phase: phase})
}

// Before the first reconcile: the demand listed, the workers adopted, WorkerStatus brought in line (rewritten
// where it differs, deleted where the worker is neither there nor wanted, kept where it is wanted), Prepare, and
// only then the AgentRecord (W7.3, W7.4).
func TestRunStartup(t *testing.T) {
	adopted := agent.Observed{Key: "md/a", Incarnation: 2, SpecHash: "s", LiveHash: "l", State: agent.StateRunning}
	same := agent.Observed{Key: "md/b", Incarnation: 1, SpecHash: "s", LiveHash: "l", State: agent.StateRunning}
	initial := lister{
		statusKey("md/a"):    statusVal("md/a", 1, PhaseRunning),     // an older incarnation: rewritten
		statusKey("md/b"):    encode(statusOf(same, true, false, 0)), // as it is: not written
		statusKey("md/gone"): statusVal("md/gone", 4, PhaseRunning),  // neither there nor wanted: deleted
		statusKey("md/want"): statusVal("md/want", 1, PhaseExited),   // wanted, not there: kept
	}
	hs := newHarness(t, initial)
	dir := t.TempDir()
	hs.cfg.WorkDir = dir
	hs.fleet.adopt = map[agent.WorkerKey]agent.Observed{"md/a": adopted, "md/b": same}
	hs.src.put("a", "s")
	hs.src.put("b", "s")
	hs.src.put("want", "s")
	hs.run(t)
	hs.j.never(t, "adopt", 50*time.Millisecond) // nothing before the demand is listed
	close(hs.src.synced)

	got := hs.j.waitFor(t, "record")
	want := []string{"adopt 2", "put md/a RUNNING", "delete " + statusKey("md/gone"), "prepare 3 demand 2 observed",
		"record epoch 7"}
	if !slices.Equal(got[:len(want)], want) {
		t.Fatalf("startup %q, want %q", got, want)
	}
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	rec := hs.term.record()
	if rec == nil {
		t.Fatal("agent record was not stored")
	}
	if rec.GetHost() != host || rec.GetWorkDir() != dir {
		t.Fatalf("agent record host %q work_dir %q", rec.GetHost(), rec.GetWorkDir())
	}
	hs.j.waitFor(t, "spawn md/want s") // then the loop
}

// The loop through a worker's life: spawned for its entry, started once initialized, stopped when the entry goes,
// released once it ended, and its status deleted. A rejected entry plans nothing.
func TestRunLifecycle(t *testing.T) {
	hs := newHarness(t, lister{})
	close(hs.src.synced)
	hs.src.put("x", "bad")
	hs.run(t)
	hs.j.waitFor(t, "record")
	hs.src.put("a", "s1")
	hs.j.waitFor(t, "spawn md/a s1")
	hs.j.waitFor(t, "put md/a STARTING")
	hs.fleet.set("md/a", func(o *agent.Observed) { o.State = agent.StateInitialized })
	hs.j.waitFor(t, "start md/a")
	hs.fleet.set("md/a", func(o *agent.Observed) { o.State = agent.StateRunning })
	hs.j.waitFor(t, "put md/a RUNNING")
	time.Sleep(50 * time.Millisecond) // five resyncs: a status that did not change is not written again (W7.2)
	if n := slices.Index(hs.j.all(), "put md/a RUNNING"); slices.Contains(hs.j.all()[n+1:], "put md/a RUNNING") {
		t.Fatalf("wrote the same status twice: %q", hs.j.all())
	}

	hs.src.del("a")
	hs.j.waitFor(t, "stop md/a: "+agent.ReasonNotPlanned)
	hs.j.waitFor(t, "put md/a STOPPING")
	hs.fleet.set("md/a", func(o *agent.Observed) { o.State = agent.StateExited; o.Exit = &commonv1.ExitInfo{} })
	hs.j.waitFor(t, "release md/a")
	got := hs.j.waitFor(t, "delete "+statusKey("md/a"))
	for _, e := range got {
		if strings.Contains(e, "md/x") {
			t.Fatalf("the rejected entry planned a worker: %q", got)
		}
	}
}

// While the demand is stale, a worker that is no longer wanted keeps running and no status is written; a kept
// worker that ended is spawned again (Always keeps). Once the demand is fresh again, the stop goes out.
func TestRunStale(t *testing.T) {
	hs := newHarness(t, lister{})
	hs.cfg.StaleAfter = 100 * time.Millisecond
	close(hs.src.synced)
	hs.src.put("a", "s")
	hs.src.put("b", "s")
	hs.run(t)
	hs.j.waitFor(t, "spawn md/a s")
	hs.j.waitFor(t, "spawn md/b s")
	hs.j.waitFor(t, "put md/b STARTING")

	hs.src.freeze(time.Now().Add(-time.Second))
	time.Sleep(50 * time.Millisecond) // a step or two at the frozen contact
	n := len(hs.j.all())
	hs.src.del("a")
	hs.fleet.set("md/b", func(o *agent.Observed) { o.State = agent.StateExited; o.Exit = &commonv1.ExitInfo{} })
	hs.j.waitFor(t, "spawn md/b s") // the kept worker came back
	hs.j.never(t, "stop", 100*time.Millisecond)
	for _, e := range hs.j.all()[n:] {
		if strings.HasPrefix(e, "put") || strings.HasPrefix(e, "delete") {
			t.Fatalf("wrote while stale: %q", hs.j.all()[n:])
		}
	}

	hs.src.freeze(time.Time{}) // fresh
	hs.j.waitFor(t, "stop md/a: "+agent.ReasonNotPlanned)
}

// A term that ends stops everything: the action in flight is cancelled, and Run returns ErrTermEnded with the
// term's cause.
func TestRunTermEnds(t *testing.T) {
	hs := newHarness(t, lister{})
	hs.fleet.block = make(chan struct{})
	close(hs.src.synced)
	hs.src.put("a", "s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { hs.ret <- Run(ctx, hs.cfg) }()
	hs.j.waitFor(t, "record")
	time.Sleep(20 * time.Millisecond) // Spawn is waiting
	hs.term.end(state.ErrSuperseded)
	select {
	case err := <-hs.ret:
		if !errors.Is(err, ErrTermEnded) || !errors.Is(err, state.ErrSuperseded) {
			t.Fatalf("Run returned %v, want ErrTermEnded and ErrSuperseded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run went on after the term ended")
	}
	hs.j.waitFor(t, "spawn md/a cancelled")
}

// Decode runs once per version of an entry.
func TestIntakeDecodesOnce(t *testing.T) {
	src := newSource()
	calls := 0
	in := NewIntake(src, func(k string, v []byte) (string, error) { calls++; return decode(k, v) })
	src.put("a", "s1")
	src.put("b", "bad")
	for range 3 {
		d := in.Demand()
		if len(d.Items) != 1 || len(d.Rejected) != 1 {
			t.Fatalf("demand %+v", d)
		}
	}
	if calls != 2 {
		t.Fatalf("decoded %d times, want 2", calls)
	}
	src.put("a", "s2")
	src.del("b")
	d := in.Demand()
	if calls != 3 || d.Items[state.DemandPrefix(state.PlaneMD, inst)+"a"] != "a=s2" || len(d.Rejected) != 0 {
		t.Fatalf("after the change: %d calls, %+v", calls, d)
	}
}

// A shim that preempts the agent for a newer term ends it, as the end of its term does: Run returns ErrTermEnded
// with the cause, and the action in flight is cancelled.
func TestRunPreempted(t *testing.T) {
	hs := newHarness(t, lister{})
	hs.fleet.block = make(chan struct{})
	close(hs.src.synced)
	hs.src.put("a", "s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { hs.ret <- Run(ctx, hs.cfg) }()
	hs.j.waitFor(t, "record")
	time.Sleep(20 * time.Millisecond) // Spawn is waiting
	close(hs.fleet.preempted)
	select {
	case err := <-hs.ret:
		if !errors.Is(err, ErrTermEnded) || !errors.Is(err, errPreempted) {
			t.Fatalf("Run returned %v, want ErrTermEnded with the preemption", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run went on after a preemption")
	}
	hs.j.waitFor(t, "spawn md/a cancelled")
}

func TestEffectiveLogRetain(t *testing.T) {
	if DefaultLogRetain != 7*24*time.Hour {
		t.Fatalf("default retain = %s", DefaultLogRetain)
	}
	if EffectiveLogRetain(0) != DefaultLogRetain || EffectiveLogRetain(-time.Hour) != DefaultLogRetain {
		t.Fatal("a non-positive retain must become the seven-day default")
	}
	if EffectiveLogRetain(time.Hour) != time.Hour {
		t.Fatal("a positive retain must be kept")
	}
}

func TestRunDeletesStaleLogs(t *testing.T) {
	hs := newHarness(t, lister{})
	hs.cfg.WorkDir = t.TempDir()
	hs.cfg.LogRetain = DefaultLogRetain
	hs.cfg.LogRetainEvery = time.Hour
	old := time.Now().Add(-8 * 24 * time.Hour)
	recent := time.Now().Add(-time.Hour)
	gone := state.WorkerHash("md/old")
	held := state.WorkerHash("md/held")
	sock := state.WorkerHash("md/sock")
	live := state.WorkerHash("md/a")
	plantLog(t, hs.cfg.WorkDir, "md", gone+".stdout", old)
	plantLog(t, hs.cfg.WorkDir, "md", gone+".shim.log", old)
	plantLog(t, hs.cfg.WorkDir, "md", gone+".shim.log.1", old)
	plantLog(t, hs.cfg.WorkDir, "md", gone+".stderr", recent)
	plantLog(t, hs.cfg.WorkDir, "md", "notes.txt", old)
	plantLog(t, hs.cfg.WorkDir, "md", live+".stdout", old)
	plantLog(t, hs.cfg.WorkDir, "md", held+".stdout", old)
	plantLog(t, hs.cfg.WorkDir, "md", sock+".shim.log", old)
	if err := os.MkdirAll(filepath.Join(hs.cfg.WorkDir, "run", "md"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hs.cfg.WorkDir, "run", "md", sock+".sock"), []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}
	hs.fleet.held = []string{held}
	hs.fleet.adopt = map[agent.WorkerKey]agent.Observed{
		"md/a": {Key: "md/a", State: agent.StateRunning},
	}
	hs.src.put("a", "s")
	hs.src.put("b", "s")
	close(hs.src.synced)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { hs.ret <- Run(ctx, hs.cfg) }()
	hs.j.waitFor(t, "spawn md/b")
	for _, name := range []string{gone + ".stdout", gone + ".shim.log", gone + ".shim.log.1"} {
		if _, err := os.Stat(filepath.Join(hs.cfg.WorkDir, "log", "md", name)); !os.IsNotExist(err) {
			t.Fatalf("%s = %v, want removed", name, err)
		}
	}
	for _, name := range []string{gone + ".stderr", "notes.txt", live + ".stdout", held + ".stdout", sock + ".shim.log"} {
		if _, err := os.Stat(filepath.Join(hs.cfg.WorkDir, "log", "md", name)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	cancel()
	<-hs.ret
}

func TestRunKeepsLogsWhenRetainOff(t *testing.T) {
	hs := newHarness(t, lister{})
	hs.cfg.WorkDir = t.TempDir()
	name := state.WorkerHash("md/old") + ".stdout"
	plantLog(t, hs.cfg.WorkDir, "md", name, time.Now().Add(-8*24*time.Hour))
	close(hs.src.synced)
	hs.src.put("a", "s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { hs.ret <- Run(ctx, hs.cfg) }()
	hs.j.waitFor(t, "spawn md/a")
	if _, err := os.Stat(filepath.Join(hs.cfg.WorkDir, "log", "md", name)); err != nil {
		t.Fatalf("a term with LogRetain 0 deleted %s: %v", name, err)
	}
	cancel()
	<-hs.ret
}

func TestRunLogCleanupRepeats(t *testing.T) {
	hs := newHarness(t, lister{})
	hs.cfg.WorkDir = t.TempDir()
	hs.cfg.LogRetain = time.Hour
	hs.cfg.LogRetainEvery = 15 * time.Millisecond
	close(hs.src.synced)
	hs.src.put("a", "s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { hs.ret <- Run(ctx, hs.cfg) }()
	hs.j.waitFor(t, "spawn md/a")
	old := time.Now().Add(-2 * time.Hour)
	gone := state.WorkerHash("md/old")
	live := state.WorkerHash("md/a")
	plantLog(t, hs.cfg.WorkDir, "md", gone+".stdout", old)
	plantLog(t, hs.cfg.WorkDir, "md", gone+".stderr", time.Now())
	plantLog(t, hs.cfg.WorkDir, "md", live+".shim.log", old)
	waitGone(t, filepath.Join(hs.cfg.WorkDir, "log", "md", gone+".stdout"))
	if _, err := os.Stat(filepath.Join(hs.cfg.WorkDir, "log", "md", gone+".stderr")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(hs.cfg.WorkDir, "log", "md", live+".shim.log")); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-hs.ret
}

func TestRunLogCleanupSkipsWhenSocketsUnreadable(t *testing.T) {
	hs := newHarness(t, lister{})
	hs.cfg.WorkDir = t.TempDir()
	hs.cfg.LogRetain = time.Hour
	var buf safeLog
	hs.cfg.Log = zerolog.New(&buf)
	if err := os.WriteFile(filepath.Join(hs.cfg.WorkDir, "run"), []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}
	name := state.WorkerHash("md/old") + ".stdout"
	plantLog(t, hs.cfg.WorkDir, "md", name, time.Now().Add(-2*time.Hour))
	close(hs.src.synced)
	hs.src.put("a", "s")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { hs.ret <- Run(ctx, hs.cfg) }()
	hs.j.waitFor(t, "spawn md/a")
	if _, err := os.Stat(filepath.Join(hs.cfg.WorkDir, "log", "md", name)); err != nil {
		t.Fatalf("cleanup deleted %s despite an unreadable socket dir: %v", name, err)
	}
	if !strings.Contains(buf.String(), "log cleanup") {
		t.Fatalf("log = %q, want the cleanup warning", buf.String())
	}
	cancel()
	<-hs.ret
}

func plantLog(t *testing.T, work, plane, name string, when time.Time) {
	t.Helper()
	path := filepath.Join(work, "log", plane, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
}

func waitGone(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was still present", path)
		}
		<-tick.C
	}
}

// safeLog is a writer the cleanup warning can share with the test.
type safeLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *safeLog) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeLog) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// Conditions ride on the same WorkerStatus write. An unchanged value is not written again.
// A worker the hook leaves empty matches statusOf, which is what TD and STS encode.
func TestConditionsRideOnWorkerStatus(t *testing.T) {
	j := newJournal()
	term := newTerm(j)
	down := true
	r := newRun(Config[string, h, h]{
		Term:  term,
		Fleet: newFleet(j),
		Log:   zerolog.Nop(),
		Conditions: func(o agent.Observed) []*commonv1.Condition {
			if o.Key != "md/a" {
				return nil
			}
			return []*commonv1.Condition{{Type: "bus_disconnected", Status: down}}
		},
	})
	a := agent.Observed{Key: "md/a", State: agent.StateRunning}
	b := agent.Observed{Key: "md/b", State: agent.StateRunning}
	obs := map[agent.WorkerKey]agent.Observed{a.Key: a, b.Key: b}
	if err := r.writeStatuses(context.Background(), nil, obs); err != nil {
		t.Fatal(err)
	}
	if n := putCount(j); n != 2 {
		t.Fatalf("puts %d, journal %q", n, j.all())
	}
	gotA := mustStatus(t, term, "md/a")
	if gotA.GetPhase() != PhaseRunning || len(gotA.GetConditions()) != 1 {
		t.Fatalf("status %+v", gotA)
	}
	cond := gotA.GetConditions()[0]
	if cond.GetType() != "bus_disconnected" || !cond.GetStatus() || cond.GetAtNs() != 0 {
		t.Fatalf("condition %+v", cond)
	}
	if term.val(statusKey("md/b")) != encode(statusOf(b, false, false, 0)) {
		t.Fatal("worker without a condition changed encoding")
	}
	if err := r.writeStatuses(context.Background(), nil, obs); err != nil {
		t.Fatal(err)
	}
	if n := putCount(j); n != 2 {
		t.Fatalf("unchanged rewrite, puts %d", n)
	}
	down = false
	if err := r.writeStatuses(context.Background(), nil, obs); err != nil {
		t.Fatal(err)
	}
	if n := putCount(j); n != 3 {
		t.Fatalf("flip puts %d", n)
	}
	gotA = mustStatus(t, term, "md/a")
	if gotA.GetPhase() != PhaseRunning || gotA.GetConditions()[0].GetStatus() {
		t.Fatalf("after reconnect %+v", gotA)
	}
}

func putCount(j *journal) int {
	n := 0
	for _, e := range j.all() {
		if strings.HasPrefix(e, "put ") {
			n++
		}
	}
	return n
}

func (t *fakeTerm) val(key string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.kv[key]
}

func mustStatus(t *testing.T, term *fakeTerm, key string) *statev1.WorkerStatus {
	t.Helper()
	st := &statev1.WorkerStatus{}
	if err := proto.Unmarshal([]byte(term.val(statusKey(agent.WorkerKey(key)))), st); err != nil {
		t.Fatal(err)
	}
	return st
}
