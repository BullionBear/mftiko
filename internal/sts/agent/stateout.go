package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/agent/fleet"
	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// pushBackoff is the state_out retry (D983, D926). It is not a deployment setting.
var pushBackoff = agent.Backoff{Min: time.Second, Max: 60 * time.Second}

// errBusy means the worker is still observed, or the session is not terminal yet.
// It is not a push failure: the volume stays, and the attempt is not counted.
var errBusy = errors.New("sts: volume still in use")

// errTerminalFile means sessions/<id>/terminal.json cannot be trusted. It is left
// in place; the fields are not invented (D982).
var errTerminalFile = errors.New("sts: terminal.json")

const terminalName = "terminal.json"

// terminalMeta is sessions/<id>/terminal.json (D982).
type terminalMeta struct {
	SessionID   string `json:"session_id"`
	Incarnation uint32 `json:"incarnation"`
	Clean       bool   `json:"clean"`
}

// exitNote is the last confirmed exit for a session. sealed is set once
// terminal.json has been written, so a later note cannot change clean (D982).
type exitNote struct {
	inc    uint32
	clean  bool
	ok     bool
	sealed bool
}

// releaseFleet notes the exit around Fleet.Release. Spawn's own release of a
// previous incarnation calls the concrete fleet and does not come through here.
type releaseFleet struct {
	*fleet.Fleet[Spec, Live]
	release func(context.Context, agent.WorkerKey) error
}

func (r *releaseFleet) Release(ctx context.Context, key agent.WorkerKey) error {
	if r.release != nil {
		return r.release(ctx, key)
	}
	return r.Fleet.Release(ctx, key)
}

var _ runner.Fleet[Spec, Live] = (*releaseFleet)(nil)

// release records the exit before the worker leaves Observe, and only then lets
// a terminal session be packed (D978). StateExited keeps the shim's ExitInfo.
// Any other successful release confirmed the process is gone without that record,
// so clean is false.
func (p *plane) release(ctx context.Context, key agent.WorkerKey) error {
	id, err := sessionOf(key)
	if err != nil || p.fleet == nil {
		return p.fleet.Release(ctx, key)
	}
	p.mu.Lock()
	obs, has := p.fleet.Observe()[key]
	if has && obs.State == agent.StateExited && obs.Exit != nil {
		p.noteExitLocked(id, obs.Incarnation, obs.Exit)
	}
	err = p.fleet.Release(ctx, key)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	if has && !(obs.State == agent.StateExited && obs.Exit != nil) {
		p.noteLostLocked(id, obs.Incarnation)
	}
	terminal := p.terminalLocked(id)
	p.mu.Unlock()
	if terminal && p.finish != nil {
		p.finish.ensure(id)
	}
	return nil
}

func (p *plane) noteExitLocked(id string, inc uint32, ex *commonv1.ExitInfo) {
	if p.exits[id].sealed {
		return
	}
	p.exits[id] = exitNote{inc: inc, clean: exitClean(ex), ok: true}
}

func (p *plane) noteLostLocked(id string, inc uint32) {
	if p.exits[id].sealed {
		return
	}
	p.exits[id] = exitNote{inc: inc, clean: false, ok: true}
}

// exitClean is the last incarnation's on_stop_completed, except a SIGKILL or a
// missing Stopped, which are false (D982).
func exitClean(ex *commonv1.ExitInfo) bool {
	if ex == nil || ex.GetSignal() == int32(syscall.SIGKILL) {
		return false
	}
	return ex.GetOnStopCompleted()
}

func (p *plane) terminalLocked(id string) bool {
	if st, ok := p.local[id]; ok {
		return terminalPhase(st.Phase)
	}
	if st, ok := p.mirrored[id]; ok {
		return terminalPhase(st.Phase)
	}
	if msg := p.saved[id]; msg != nil {
		return terminalPhase(msg.GetPhase())
	}
	return false
}

func terminalPhase(ph statev1.Phase) bool {
	return ph == statev1.Phase_PHASE_DONE || ph == statev1.Phase_PHASE_FAILED
}

// workerBusy is true while a shim for id might still have the volume or a log open.
func (p *plane) workerBusy(id string) bool {
	if p.fleet == nil {
		return false
	}
	if _, ok := p.fleet.Observe()[WorkerKey(id)]; ok {
		return true
	}
	h := state.WorkerHash(string(WorkerKey(id)))
	for _, u := range p.fleet.Unreachable() {
		if u == h {
			return true
		}
	}
	return false
}

func (p *plane) seal(id string, inc uint32, clean bool) {
	p.mu.Lock()
	p.exits[id] = exitNote{inc: inc, clean: clean, ok: true, sealed: true}
	p.mu.Unlock()
}

// finisher packs and pushes state_out outside the reconcile loop, then deletes
// the volume and the inputs. Shim logs and the session directory stay until the
// spec is gone (D983, D1016).
type finisher struct {
	ctx   context.Context
	plane *plane
	wake  chan struct{}
	done  chan struct{}

	mu       sync.Mutex
	ready    bool
	jobs     map[string]*pushJob
	finished map[string]struct{}
}

type pushJob struct {
	fails   uint32
	next    time.Time
	blocked bool
	kick    uint64 // ensure bumps this so a pass cannot hide a newer wake
	snap    *statev1.StateSnapshot
}

func newFinisher(ctx context.Context, p *plane) *finisher {
	return &finisher{
		ctx: ctx, plane: p, wake: make(chan struct{}, 1), done: make(chan struct{}),
		jobs: map[string]*pushJob{}, finished: map[string]struct{}{},
	}
}

func (f *finisher) loop() {
	defer close(f.done)
	for {
		if f.ctx.Err() != nil {
			return
		}
		f.pass()
		timer := time.NewTimer(f.nextWait())
		select {
		case <-f.ctx.Done():
			timer.Stop()
			return
		case <-f.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

func (f *finisher) adopted() {
	f.mu.Lock()
	if f.ready {
		f.mu.Unlock()
		return
	}
	f.ready = true
	f.mu.Unlock()
	f.kick()
}

func (f *finisher) ensure(id string) {
	if id == "" {
		return
	}
	f.mu.Lock()
	if _, done := f.finished[id]; done {
		f.mu.Unlock()
		return
	}
	j := f.jobs[id]
	if j == nil {
		j = &pushJob{}
		f.jobs[id] = j
	}
	j.kick++
	if j.blocked {
		j.blocked = false
		j.next = time.Time{}
	}
	ready := f.ready
	f.mu.Unlock()
	if ready {
		f.kick()
	}
}

func (f *finisher) kick() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *finisher) isReady() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready
}

func (f *finisher) pass() {
	if f.ctx.Err() != nil || !f.isReady() {
		return
	}
	f.scan()
	now := time.Now()
	for _, id := range f.due(now) {
		if f.ctx.Err() != nil {
			return
		}
		f.mu.Lock()
		j := f.jobs[id]
		if j == nil {
			f.mu.Unlock()
			continue
		}
		kick := j.kick
		f.mu.Unlock()
		err := f.step(f.ctx, id)
		f.mu.Lock()
		j = f.jobs[id]
		if j == nil {
			f.mu.Unlock()
			continue
		}
		switch {
		case err == nil:
			delete(f.jobs, id)
			f.finished[id] = struct{}{}
		case errors.Is(err, errBusy):
			// An ensure that landed during the attempt asked for another pass.
			if j.kick != kick {
				j.blocked = false
				j.next = time.Time{}
			} else {
				j.blocked = true
				j.next = time.Now().Add(time.Hour)
			}
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			f.mu.Unlock()
			return
		default:
			wait := pushBackoff.After(j.fails)
			j.fails++
			j.blocked = false
			j.next = time.Now().Add(wait)
			f.plane.log.Warn().Str("session", id).Uint32("attempt", j.fails).Dur("retry_in", wait).Err(err).Msg("state_out push")
		}
		f.mu.Unlock()
	}
	f.reap()
}

func (f *finisher) due(now time.Time) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for id, j := range f.jobs {
		if j.next.IsZero() || !now.Before(j.next) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func (f *finisher) nextWait() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	wait := time.Hour
	for _, j := range f.jobs {
		if j.next.IsZero() {
			return 0
		}
		if d := j.next.Sub(now); d < wait {
			wait = d
		}
	}
	if wait < 0 {
		return 0
	}
	return wait
}

func (f *finisher) scan() {
	if f.plane == nil || f.plane.prog == nil {
		return
	}
	dir := filepath.Join(f.plane.prog.DataDir, "sessions")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id := e.Name()
		if _, err := os.Lstat(terminalPath(f.plane.prog.DataDir, id)); err == nil {
			f.ensure(id)
		}
	}
}

func (f *finisher) snapOf(id string) *statev1.StateSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j := f.jobs[id]; j != nil {
		return j.snap
	}
	return nil
}

func (f *finisher) keepSnap(id string, snap *statev1.StateSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j := f.jobs[id]; j != nil && j.snap == nil {
		j.snap = snap
	}
}

func (f *finisher) step(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := f.plane
	p.mu.Lock()
	busy := p.workerBusy(id)
	terminal := p.terminalLocked(id)
	note := p.exits[id]
	p.mu.Unlock()
	if busy {
		return errBusy
	}
	meta, err := p.terminalMeta(id, terminal, note)
	if err != nil {
		return err
	}
	if snap := f.snapOf(id); snap != nil {
		if err := p.putStateOut(ctx, id, snap); err != nil {
			return err
		}
		return p.releaseVolume(id)
	}
	if p.existingStateOut(id) != nil {
		return p.releaseVolume(id)
	}
	vol, _ := sessionDirs(p.prog.DataDir, id)
	empty, err := artifact.DataVolumeEmpty(vol)
	if err != nil {
		return err
	}
	if empty {
		return p.releaseVolume(id)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	layer, skipped, err := p.art.PackDataFile(vol)
	if err != nil {
		return err
	}
	defer os.Remove(layer)
	for _, s := range skipped {
		p.log.Warn().Str("session", id).Str("path", s.Path).Str("reason", s.Reason).Msg("volume entry skipped")
	}
	pushed, err := p.art.PushData(ctx, artifact.DataPush{
		Repository: p.dataRepo,
		Tag:        id,
		LayerPath:  layer,
		Source: artifact.DataSource{
			SessionID: meta.SessionID, Incarnation: meta.Incarnation, Clean: meta.Clean,
		},
	})
	if err != nil {
		return err
	}
	if pushed.Size < 0 {
		return fmt.Errorf("sts: layer size %d", pushed.Size)
	}
	snap := &statev1.StateSnapshot{
		Digest: pushed.Digest, Repository: p.dataRepo, Clean: meta.Clean,
		Incarnation: meta.Incarnation, AtNs: p.clock().UnixNano(), SizeBytes: uint64(pushed.Size),
	}
	f.keepSnap(id, snap)
	if err := p.putStateOut(ctx, id, snap); err != nil {
		return err
	}
	return p.releaseVolume(id)
}

// terminalMeta reads the resume file or writes it from the confirmed exit.
// A corrupt file is returned as errTerminalFile and is not removed.
func (p *plane) terminalMeta(id string, terminal bool, note exitNote) (terminalMeta, error) {
	path := terminalPath(p.prog.DataDir, id)
	meta, err := readTerminalFile(path, id)
	if err == nil {
		p.seal(id, meta.Incarnation, meta.Clean)
		return meta, nil
	}
	if !os.IsNotExist(err) {
		return terminalMeta{}, err
	}
	if !terminal {
		return terminalMeta{}, errBusy
	}
	meta = terminalMeta{SessionID: id}
	if note.ok {
		meta.Incarnation = note.inc
		meta.Clean = note.clean
	} else if p.numbers != nil {
		if inc, ok := p.numbers.Current(string(WorkerKey(id))); ok {
			meta.Incarnation = inc
		}
	}
	if err := writeTerminalFile(path, meta); err != nil {
		return terminalMeta{}, err
	}
	if got, err := readTerminalFile(path, id); err == nil {
		meta = got
	}
	p.seal(id, meta.Incarnation, meta.Clean)
	return meta, nil
}

func terminalPath(dataDir, id string) string {
	return filepath.Join(dataDir, "sessions", id, terminalName)
}

func readTerminalFile(path, id string) (terminalMeta, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return terminalMeta{}, err
	}
	var meta terminalMeta
	if err := json.Unmarshal(b, &meta); err != nil || meta.SessionID == "" || meta.SessionID != id {
		return terminalMeta{}, fmt.Errorf("%w: %s", errTerminalFile, path)
	}
	return meta, nil
}

func writeTerminalFile(path string, meta terminalMeta) error {
	body, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, inputTempPrefix+"terminal-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	_, werr := tmp.Write(body)
	serr := tmp.Sync()
	cerr := tmp.Close()
	if werr != nil || serr != nil || cerr != nil {
		os.Remove(tmpName)
		if werr != nil {
			return werr
		}
		if serr != nil {
			return serr
		}
		return cerr
	}
	// Link does not replace an existing file, so a resume marker already on disk wins.
	if err := os.Link(tmpName, path); err != nil {
		os.Remove(tmpName)
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return err
	}
	os.Remove(tmpName)
	return fsyncDir(dir)
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	cErr := d.Close()
	if err != nil {
		return err
	}
	return cErr
}

func (p *plane) existingStateOut(id string) *statev1.StateSnapshot {
	base := p.statusBase(id)
	if base.GetStateOut() == nil {
		return nil
	}
	return base.GetStateOut()
}

func (p *plane) statusBase(id string) *statev1.SessionStatus {
	p.mu.Lock()
	msg := p.saved[id]
	p.mu.Unlock()
	if msg != nil {
		return proto.Clone(msg).(*statev1.SessionStatus)
	}
	if p.status == nil {
		return nil
	}
	e, ok := p.status.Snapshot().Entries[state.SessionStatusKey(p.inst, id)]
	if !ok {
		return nil
	}
	st, err := parseStatus(append([]byte(nil), e.Value...))
	if err != nil {
		return nil
	}
	return st
}

func (p *plane) putStateOut(ctx context.Context, id string, snap *statev1.StateSnapshot) error {
	base := p.statusBase(id)
	if base == nil {
		return fmt.Errorf("sts: status %s is not written", id)
	}
	base.StateOut = snap
	base.UpdatedAtNs = snap.GetAtNs()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(base)
	if err != nil {
		return err
	}
	if err := p.w.Put(ctx, state.SessionStatusKey(p.inst, id), string(b)); err != nil {
		return fmt.Errorf("sts: state_out %s: %w", id, err)
	}
	p.mu.Lock()
	if p.saved == nil {
		p.saved = map[string]*statev1.SessionStatus{}
	}
	p.saved[id] = base
	if p.local == nil {
		p.local = map[string]statusView{}
	}
	p.local[id] = statusView{Phase: base.GetPhase(), Reason: base.GetReason(), Gen: base.GetObservedGeneration()}
	p.mu.Unlock()
	return nil
}

// releaseVolume is phase 1: after state_out is pushed, or immediately when the
// volume is empty, only the volume and the inputs are deleted (D1016).
func (p *plane) releaseVolume(id string) error {
	vol, inputs := sessionDirs(p.prog.DataDir, id)
	if err := removeWritable(vol); err != nil {
		return err
	}
	return removeWritable(inputs)
}

// reap is phase 2. It runs on every finisher pass, which commits wake, so a
// spec that disappears while the agent is up is collected without spinning on
// "spec still held" (D1016). A pass before the first commit does not run: the
// finisher is not ready.
func (f *finisher) reap() {
	p := f.plane
	if p == nil || p.prog == nil || f.ctx.Err() != nil {
		return
	}
	dir := filepath.Join(p.prog.DataDir, "sessions")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if f.ctx.Err() != nil {
			return
		}
		if !e.IsDir() {
			continue
		}
		id := e.Name()
		if !p.hostLeftover(id) {
			continue
		}
		if err := p.releaseHost(f.ctx, id); err != nil && f.ctx.Err() == nil {
			p.log.Warn().Str("session", id).Err(err).Msg("session cleanup")
		}
	}
}

// hostLeftover reports that phase 1 has finished for a session the reconcile
// view no longer has a spec for (D1016, D1019). terminal.json is the local
// marker; a directory that never became terminal is left alone (D944).
func (p *plane) hostLeftover(id string) bool {
	if p.prog == nil || !p.hostStill(id) {
		return false
	}
	if _, err := readTerminalFile(terminalPath(p.prog.DataDir, id), id); err != nil {
		return false
	}
	vol, inputs := sessionDirs(p.prog.DataDir, id)
	if _, err := os.Lstat(vol); !os.IsNotExist(err) {
		return false
	}
	if _, err := os.Lstat(inputs); !os.IsNotExist(err) {
		return false
	}
	return true
}

// hostStill is the phase-2 gate: the first spec list has been seen, this
// session's spec key is absent from that view, and no shim still has the
// session open (D1019).
func (p *plane) hostStill(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.specReady {
		return false
	}
	if _, held := p.specs[id]; held {
		return false
	}
	return !p.workerBusy(id)
}

// releaseHost deletes one finished session in D1020's order. The first error
// stops it. Each step checks the spec view again so a spec that reappears
// (D1007) is left in place for a later pass.
func (p *plane) releaseHost(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.hostStill(id) {
		return nil
	}
	if err := removeShimLogs(p.workDir, id); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.hostStill(id) {
		return nil
	}
	if p.numbers == nil {
		return errors.New("sts: incarnations not loaded")
	}
	if err := p.numbers.Forget(ctx, string(WorkerKey(id))); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.hostStill(id) {
		return nil
	}
	if err := os.Remove(terminalPath(p.prog.DataDir, id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.hostStill(id) {
		return nil
	}
	return os.RemoveAll(filepath.Join(p.prog.DataDir, "sessions", id))
}

func removeShimLogs(workDir, id string) error {
	h := state.WorkerHash(string(WorkerKey(id)))
	dir := filepath.Join(workDir, "log", string(state.PlaneSTS))
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	prefix := h + "."
	for _, e := range entries {
		name := e.Name()
		if name != h && !strings.HasPrefix(name, prefix) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
