package agent

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/rs/zerolog"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/internal/sts"
	"github.com/BullionBear/mftiko/internal/sts/issuer"
)

// hello walks Init, a wait for start, Start, ENDED, and Stop against an
// in-process registry, etcd, and NATS (E09-D065). Phases stay PENDING while the
// artifact is pulled, WAITING until start, then RUNNING, then DONE.
func TestHelloLifecycle(t *testing.T) {
	e := setupHello(t)
	const id = "a1b2c3"
	e.putSpec(id, &sessionv1.SessionSpec{Name: "hello", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING})
	e.putDemand(id, runningDemand(false, 1))

	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()

	st := ph.wait(id, statev1.Phase_PHASE_PENDING, statev1.Phase_PHASE_WAITING)
	if st.GetPhase() != statev1.Phase_PHASE_WAITING {
		t.Fatalf("before start: %s", st.GetPhase())
	}
	e.putDemand(id, runningDemand(true, 1))
	ph.wait(id, statev1.Phase_PHASE_STARTING, statev1.Phase_PHASE_RUNNING)
	e.putDemand(id, endedDemand("ended_by_spec", 2))
	st = ph.wait(id, statev1.Phase_PHASE_STOPPING, statev1.Phase_PHASE_DONE)
	if st.GetReason() != "ended_by_spec" {
		t.Fatalf("DONE reason %q", st.GetReason())
	}
}

// After the agent is replaced, the new epoch is one higher and a bindings
// change is applied at a version strictly after the one the worker already had (E09-D065, E09-D022).
func TestHelloRotationAppliesNewerBindings(t *testing.T) {
	e := setupHello(t)
	const id = "b2c3d4"
	e.putSpec(id, &sessionv1.SessionSpec{Name: "hello", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING})
	e.putDemand(id, accountDemand(true, 1, statev1.EntryState_ENTRY_STATE_PENDING))

	ph := e.collect(e.rev())
	stop, done := e.start()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	epoch, seq := e.waitApplied(func(epoch int64, seq uint64) bool { return epoch > 0 && seq > 0 })
	rec := e.waitRecord(func(r *statev1.AgentRecord) bool { return r.GetEpoch() == epoch })
	if rec.GetEpoch() != epoch {
		t.Fatalf("record epoch %d, bindings epoch %d", rec.GetEpoch(), epoch)
	}

	stop()
	waitDone(t, done)
	e.cfg.Term.ID = t.Name() + "-next"
	stop, done = e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	next := e.waitRecord(func(r *statev1.AgentRecord) bool { return r.GetEpoch() == epoch+1 })
	if next.GetEpoch() != epoch+1 {
		t.Fatalf("epoch %d, want %d", next.GetEpoch(), epoch+1)
	}
	rotEpoch, rotSeq := e.waitApplied(func(gotEpoch int64, gotSeq uint64) bool {
		return versionNewer(epoch, seq, gotEpoch, gotSeq) && gotEpoch == epoch+1
	})
	e.putDemand(id, accountDemand(true, 1, statev1.EntryState_ENTRY_STATE_READY))
	if gotEpoch, gotSeq := e.waitApplied(func(gotEpoch int64, gotSeq uint64) bool {
		return versionNewer(rotEpoch, rotSeq, gotEpoch, gotSeq)
	}); !versionNewer(epoch, seq, gotEpoch, gotSeq) {
		t.Fatalf("applied (%d,%d), first was (%d,%d)", gotEpoch, gotSeq, epoch, seq)
	}
}

func accountDemand(start bool, issued int64, state statev1.EntryState) *statev1.SessionDemand {
	return &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, Start: start, IssuedAtNs: issued,
		Bindings: []*statev1.Binding{{
			Target:   &statev1.Binding_ApiId{ApiId: 7},
			Instance: "td-tw-1",
			Status:   &statev1.EntryStatus{State: state, Subject: "td.order.7"},
		}},
	}
}

type seenVer struct {
	epoch int64
	seq   uint64
}

type helloEnv struct {
	*env
	applied []seenVer
}

func (e *helloEnv) noteApplied(id string, epoch int64, seq uint64) {
	if id == "" {
		return
	}
	e.mu.Lock()
	e.applied = append(e.applied, seenVer{epoch, seq})
	e.mu.Unlock()
}

func (e *helloEnv) best() (epoch int64, seq uint64, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, v := range e.applied {
		if !ok || versionNewer(epoch, seq, v.epoch, v.seq) {
			epoch, seq, ok = v.epoch, v.seq, true
		}
	}
	return epoch, seq, ok
}

func (e *helloEnv) waitApplied(pred func(epoch int64, seq uint64) bool) (int64, uint64) {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		epoch, seq, ok := e.best()
		if ok && pred(epoch, seq) {
			return epoch, seq
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("applied (%d,%d) ok=%v never matched", epoch, seq, ok)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *helloEnv) waitRecord(pred func(*statev1.AgentRecord) bool) *statev1.AgentRecord {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		rec := e.record()
		if rec != nil && pred(rec) {
			return rec
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("agent record %+v", rec)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func versionNewer(oldEpoch int64, oldSeq uint64, epoch int64, seq uint64) bool {
	if epoch != oldEpoch {
		return epoch > oldEpoch
	}
	return seq > oldSeq
}

func setupHello(t *testing.T) *helloEnv {
	t.Helper()
	inst, topo, root := openCluster(t)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	img := packedHello(t)
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	t.Cleanup(reg.Close)
	pushCtx, pushCancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = artifact.Push(pushCtx, img, artifact.Destination{Repository: reg.repo(), PlainHTTP: true})
	pushCancel()
	if err != nil {
		t.Fatal(err)
	}
	ns := startHelloNATS(t)
	signer, err := natsdev.LoadSigner(ns.lay.Manifest.Account, filepath.Join(ns.lay.Dir, filepath.FromSlash(ns.lay.Manifest.SigningKeyFile)))
	if err != nil {
		t.Fatal(err)
	}
	wd, err := os.MkdirTemp("/tmp", "sts-hello-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(wd) })
	buf := &syncBuf{}
	log := zerolog.New(buf).Level(zerolog.InfoLevel)
	h := &helloEnv{env: &env{t: t, root: root, log: buf, reg: reg, digest: img.Digest, repo: "hello"}}
	h.cfg = Config{
		Topology: topo, Instance: inst, Dial: state.AuthDial(inst, etcdtest.Password(inst)),
		Term:      state.TermOptions{ID: t.Name()},
		WorkDir:   wd,
		DataDir:   t.TempDir(),
		Shim:      self,
		ShimEnv:   []string{envTestShim + "=1"},
		WorkerEnv: []string{"MFTIKO_NATS_URL=" + ns.url},
		Registry:  reg.addr(),
		PlainHTTP: true,
		Sign: func(id string, perms sts.Permissions) (string, string, error) {
			return issuer.SignSession(id, perms, signer.AccountPublic(), signer.SigningKey())
		},
		Applied:    h.noteApplied,
		Grace:      DefaultGrace,
		Resync:     50 * time.Millisecond,
		StaleAfter: 5 * time.Second,
		Mirror:     state.MirrorOptions{ProbeInterval: 50 * time.Millisecond, Log: &log},
		Release:    "test",
		Log:        log,
	}
	h.cfg.Started = func(pid int) {
		h.mu.Lock()
		h.pids = append(h.pids, pid)
		h.mu.Unlock()
	}
	t.Cleanup(func() {
		h.mu.Lock()
		pids := append([]int(nil), h.pids...)
		h.mu.Unlock()
		for _, pid := range pids {
			syscall.Kill(pid, syscall.SIGKILL) //nolint:errcheck // already exited
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", buf.String())
		}
	})
	return h
}

type helloNATS struct {
	url string
	lay natsdev.Layout
}

func startHelloNATS(t *testing.T) *helloNATS {
	t.Helper()
	dir := t.TempDir()
	if err := natsdev.Generate(dir, natsdev.Options{}); err != nil {
		t.Fatal(err)
	}
	lay, err := natsdev.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	conf, err := natsdev.FormatServerConfig(natsdev.ServerConfig{
		OperatorFile:  filepath.Join(dir, "operator.jwt"),
		SystemAccount: lay.Manifest.System,
		ResolverDir:   filepath.Join(dir, "resolver"),
		Host:          "127.0.0.1",
		Port:          -1,
		ServerName:    "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	confPath := filepath.Join(t.TempDir(), "nats-server.conf")
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatal(err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server did not accept connections")
	}
	return &helloNATS{url: ns.ClientURL(), lay: lay}
}

var (
	helloOnce sync.Once
	helloImg  artifact.Image
	helloErr  error
)

func packedHello(t *testing.T) artifact.Image {
	t.Helper()
	helloOnce.Do(func() {
		root := moduleRoot()
		if root == "" {
			helloErr = os.ErrNotExist
			return
		}
		bin := filepath.Join(os.TempDir(), "mftiko-hello-e9-06")
		cmd := exec.Command("go", "build", "-o", bin, "./examples/strategies/hello")
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		if err != nil {
			helloErr = &helloBuildError{err: err, out: string(out)}
			return
		}
		raw, err := os.ReadFile(bin)
		if err != nil {
			helloErr = err
			return
		}
		helloImg, helloErr = artifact.PackStrategy(raw, "hello", nil)
	})
	if helloErr != nil {
		t.Fatal(helloErr)
	}
	return helloImg
}

type helloBuildError struct {
	err error
	out string
}

func (e *helloBuildError) Error() string {
	if e.out == "" {
		return e.err.Error()
	}
	return e.out + ": " + e.err.Error()
}

func (e *helloBuildError) Unwrap() error { return e.err }

func moduleRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
