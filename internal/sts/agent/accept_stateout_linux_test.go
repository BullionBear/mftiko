package agent

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// The next session reads the pushed volume through state_out. Logs, terminal.json,
// and the incarnation key stay until the spec is deleted (E09-D084, E10-D016).
func TestStateOutRoundTrip(t *testing.T) {
	e := setup(t)
	const id, next = "a1b2c3", "b2c3d4"
	const body = "kept-note"
	e.setScript(id, fakeworker.Script{VolumeFile: "note", VolumeText: body})
	e.putSpec(id, sessionSpec(1, 1))
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	waitVolumeFile(t, e, id, "note", body)
	e.plantSessionLog(id)
	e.putDemand(id, endedDemand("ended_by_spec", 2))
	ph.wait(id, statev1.Phase_PHASE_DONE)
	snap := e.waitStateOut(id)
	if snap.GetRepository() != "state" || !snap.GetClean() || snap.GetIncarnation() == 0 ||
		snap.GetSizeBytes() == 0 || snap.GetAtNs() == 0 || snap.GetDigest() == "" {
		t.Fatalf("state_out %+v", snap)
	}
	if got := e.manifestDigest("state", id); got != snap.GetDigest() {
		t.Fatalf("tag digest %s, state_out %s", got, snap.GetDigest())
	}
	spec := sessionSpec(2, 1)
	spec.Inputs = []*sessionv1.InputDecl{{
		Name: "prev", Digest: snap.GetDigest(), Repository: snap.GetRepository(),
	}}
	e.putSpec(next, spec)
	e.putDemand(next, runningDemand(true, 3))
	ph.wait(next, statev1.Phase_PHASE_RUNNING)
	_, inputs := sessionDirs(e.cfg.DataDir, next)
	note := filepath.Join(inputs, "prev", "note")
	pollUntil(t, "input from state_out", func() bool {
		b, err := os.ReadFile(note)
		return err == nil && string(b) == body
	})
	pollUntil(t, "logs kept while the spec exists", func() bool { return e.logsKept(id) })
	if snap.GetDigest() != e.status(id).GetStateOut().GetDigest() {
		t.Fatalf("state_out changed before the spec was deleted")
	}
	e.delSpec(id)
	pollUntil(t, "no residue of "+id, func() bool { return e.sessionResidue(id) == "" })
}

// A stopped registry leaves the volume in place. After it answers again the push
// lands and state_out is filled in, without holding the terminal phase back (E09-D083, E09-D084).
func TestStateOutRegistryDown(t *testing.T) {
	e := setup(t)
	const id = "c3d4e5"
	const body = "kept"
	e.setScript(id, fakeworker.Script{VolumeFile: "note", VolumeText: body})
	e.putSpec(id, sessionSpec(3, 1))
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	waitVolumeFile(t, e, id, "note", body)
	e.reg.setDown(true)
	e.putDemand(id, endedDemand("ended_by_spec", 2))
	ph.wait(id, statev1.Phase_PHASE_DONE)
	var meta terminalMeta
	pollUntil(t, "volume kept while the registry is down", func() bool {
		if !strings.Contains(e.log.String(), "state_out push") {
			return false
		}
		st := e.status(id)
		if st.GetPhase() != statev1.Phase_PHASE_DONE || st.GetStateOut() != nil {
			return false
		}
		vol, _ := sessionDirs(e.cfg.DataDir, id)
		b, err := os.ReadFile(filepath.Join(vol, "note"))
		if err != nil || string(b) != body {
			return false
		}
		raw, err := os.ReadFile(terminalPath(e.cfg.DataDir, id))
		if err != nil || json.Unmarshal(raw, &meta) != nil {
			return false
		}
		return meta.SessionID == id && meta.Incarnation != 0 && meta.Clean
	})
	e.reg.setDown(false)
	snap := e.waitStateOut(id)
	if snap.GetDigest() == "" || snap.GetRepository() != "state" || !snap.GetClean() || snap.GetIncarnation() != meta.Incarnation {
		t.Fatalf("state_out %+v after %d", snap, meta.Incarnation)
	}
	pollUntil(t, "volume released and terminal.json kept", func() bool { return e.logsKept(id) })
}

// terminal.json is enough for a new process to finish the push and the cleanup (E09-D082, E09-D084).
func TestStateOutResumeAfterRestart(t *testing.T) {
	e := setup(t)
	const id = "d4e5f6"
	const body = "kept"
	e.setScript(id, fakeworker.Script{VolumeFile: "note", VolumeText: body})
	e.putSpec(id, sessionSpec(4, 1))
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	waitVolumeFile(t, e, id, "note", body)
	e.plantSessionLog(id)
	e.reg.setDown(true)
	e.putDemand(id, endedDemand("ended_by_spec", 2))
	ph.wait(id, statev1.Phase_PHASE_DONE)
	pollUntil(t, "terminal marker while the registry is down", func() bool {
		if !strings.Contains(e.log.String(), "state_out push") {
			return false
		}
		st := e.status(id)
		if st.GetPhase() != statev1.Phase_PHASE_DONE || st.GetStateOut() != nil {
			return false
		}
		_, err := os.Stat(terminalPath(e.cfg.DataDir, id))
		return err == nil
	})
	stop()
	waitDone(t, done)
	e.reg.setDown(false)
	e.cfg.Term.ID = t.Name() + "-next"
	stop, done = e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	snap := e.waitStateOut(id)
	if snap.GetRepository() != "state" || snap.GetDigest() == "" || !snap.GetClean() || snap.GetIncarnation() == 0 {
		t.Fatalf("state_out %+v", snap)
	}
	if got := e.manifestDigest("state", id); got != snap.GetDigest() {
		t.Fatalf("tag digest %s, state_out %s", got, snap.GetDigest())
	}
	pollUntil(t, "logs kept while the spec exists", func() bool { return e.logsKept(id) })
	stop()
	waitDone(t, done)
	e.delSpec(id)
	e.cfg.Term.ID = t.Name() + "-reap"
	stop, done = e.start()
	pollUntil(t, "no residue of "+id, func() bool { return e.sessionResidue(id) == "" })
}

func waitVolumeFile(t *testing.T, e *env, id, name, body string) {
	t.Helper()
	vol, _ := sessionDirs(e.cfg.DataDir, id)
	path := filepath.Join(vol, name)
	pollUntil(t, "volume file "+name, func() bool {
		b, err := os.ReadFile(path)
		return err == nil && string(b) == body
	})
}

func (e *env) waitStateOut(id string) *statev1.StateSnapshot {
	e.t.Helper()
	var snap *statev1.StateSnapshot
	pollUntil(e.t, "state_out "+id, func() bool {
		snap = e.status(id).GetStateOut()
		return snap.GetDigest() != ""
	})
	return snap
}

func (e *env) manifestDigest(repo, ref string) string {
	e.t.Helper()
	resp, err := http.Get("http://" + e.cfg.Registry + "/v2/" + repo + "/manifests/" + ref)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		e.t.Fatalf("manifest %s: %s", ref, resp.Status)
	}
	return resp.Header.Get("Docker-Content-Digest")
}

// Spec deleted while the session is still running: it ends, state_out is pushed,
// and only then do the logs go away. A running sibling and a directory that
// never became terminal stay (E10-D016, E09-D044).
func TestLogsWaitForSpec(t *testing.T) {
	e := setup(t)
	const id, keep = "e5f6a7", "f6a7b8"
	const body = "still-here"
	e.primeSTSLogDir()
	e.plantOldStdout(id)
	e.setScript(id, fakeworker.Script{VolumeFile: "note", VolumeText: body})
	rev := e.putSpec(id, sessionSpec(5, 1))
	e.putDemand(id, runningDemand(true, 1))
	e.putSpec(keep, sessionSpec(6, 2))
	e.putDemand(keep, runningDemand(true, 1))
	stray := filepath.Join(e.cfg.DataDir, "sessions", "never")
	if err := os.MkdirAll(stray, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stray, "note"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	ph.wait(keep, statev1.Phase_PHASE_RUNNING)
	waitVolumeFile(t, e, id, "note", body)
	e.assertSessionLogModes(id)
	e.reg.setDown(true)
	e.delSpec(id)
	e.putDemand(id, endedDemand("ended_by_spec", 2))
	pollUntil(t, "terminal with volume and logs kept", func() bool {
		st := e.status(id)
		if st.GetPhase() != statev1.Phase_PHASE_DONE || st.GetReason() != "ended_by_spec" || st.GetStateOut() != nil {
			return false
		}
		if st.GetObservedGeneration() != rev {
			return false
		}
		vol, _ := sessionDirs(e.cfg.DataDir, id)
		b, err := os.ReadFile(filepath.Join(vol, "note"))
		if err != nil || string(b) != body {
			return false
		}
		if _, err := os.Stat(terminalPath(e.cfg.DataDir, id)); err != nil {
			return false
		}
		return e.sessionResidue(id) != ""
	})
	e.reg.setDown(false)
	snap := e.waitStateOut(id)
	if snap.GetDigest() == "" || snap.GetRepository() != "state" {
		t.Fatalf("state_out %+v", snap)
	}
	pollUntil(t, "no residue of "+id, func() bool { return e.sessionResidue(id) == "" })
	if e.status(keep).GetPhase() != statev1.Phase_PHASE_RUNNING {
		t.Fatalf("sibling phase %s", e.status(keep).GetPhase())
	}
	if _, err := os.Stat(filepath.Join(stray, "note")); err != nil {
		t.Fatalf("stray session dir: %v", err)
	}
}

func (e *env) plantSessionLog(id string) {
	e.t.Helper()
	h := state.WorkerHash(string(WorkerKey(id)))
	dir := filepath.Join(e.cfg.WorkDir, "log", string(state.PlaneSTS))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		e.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, h+".stdout.1"), []byte("rotated"), 0o644); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) sessionResidue(id string) string {
	e.t.Helper()
	dir := filepath.Join(e.cfg.DataDir, "sessions", id)
	if _, err := os.Lstat(dir); err == nil {
		return "session dir"
	} else if !os.IsNotExist(err) {
		return err.Error()
	}
	h := state.WorkerHash(string(WorkerKey(id)))
	for _, sub := range []string{
		filepath.Join("log", string(state.PlaneSTS)),
		filepath.Join("run", string(state.PlaneSTS)),
	} {
		base := filepath.Join(e.cfg.WorkDir, sub)
		entries, err := os.ReadDir(base)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err.Error()
		}
		prefix := h + "."
		for _, ent := range entries {
			name := ent.Name()
			if name == h || strings.HasPrefix(name, prefix) {
				return filepath.Join(sub, name)
			}
		}
	}
	ctx, cancel := e.opCtx()
	defer cancel()
	resp, err := e.root.Get(ctx, state.IncarnationKey(state.PlaneSTS, testInst, h))
	if err != nil {
		return err.Error()
	}
	if len(resp.Kvs) != 0 {
		return "incarnation"
	}
	return ""
}

// logsKept reports phase 1 is done and phase 2 has not run: volume and inputs
// are gone, and terminal.json, a shim log, and the incarnation key remain.
func (e *env) logsKept(id string) bool {
	vol, inputs := sessionDirs(e.cfg.DataDir, id)
	if _, err := os.Lstat(vol); !os.IsNotExist(err) {
		return false
	}
	if _, err := os.Lstat(inputs); !os.IsNotExist(err) {
		return false
	}
	if _, err := os.Lstat(terminalPath(e.cfg.DataDir, id)); err != nil {
		return false
	}
	if !e.hasShimLog(id) {
		return false
	}
	h := state.WorkerHash(string(WorkerKey(id)))
	ctx, cancel := e.opCtx()
	defer cancel()
	resp, err := e.root.Get(ctx, state.IncarnationKey(state.PlaneSTS, testInst, h))
	return err == nil && len(resp.Kvs) != 0
}

func (e *env) hasShimLog(id string) bool {
	h := state.WorkerHash(string(WorkerKey(id)))
	dir := filepath.Join(e.cfg.WorkDir, "log", string(state.PlaneSTS))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	prefix := h + "."
	for _, ent := range entries {
		name := ent.Name()
		if name == h || strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// primeSTSLogDir creates ${WORK_DIR}/log/sts at 0700 with setgid, and the parent
// log directory setgid, so a later chmod to 0750 has a bit to keep (E10-D015, E10-D022).
func (e *env) primeSTSLogDir() {
	e.t.Helper()
	dir := filepath.Join(e.cfg.WorkDir, "log", string(state.PlaneSTS))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		e.t.Fatal(err)
	}
	if gid, ok := otherGroup(); ok {
		if err := os.Chown(filepath.Dir(dir), os.Geteuid(), gid); err != nil {
			e.t.Fatal(err)
		}
		if err := os.Chown(dir, os.Geteuid(), gid); err != nil {
			e.t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Dir(dir), 0o750|os.ModeSetgid); err != nil {
		e.t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o700|os.ModeSetgid); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) plantOldStdout(id string) {
	e.t.Helper()
	h := state.WorkerHash(string(WorkerKey(id)))
	path := filepath.Join(e.cfg.WorkDir, "log", string(state.PlaneSTS), h+".stdout")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		e.t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) assertSessionLogModes(id string) {
	e.t.Helper()
	h := state.WorkerHash(string(WorkerKey(id)))
	dir := filepath.Join(e.cfg.WorkDir, "log", string(state.PlaneSTS))
	stdout := filepath.Join(dir, h+".stdout")
	stderr := filepath.Join(dir, h+".stderr")
	shimLog := filepath.Join(dir, h+".shim.log")
	pollUntil(e.t, "stderr and shim log", func() bool {
		_, err1 := os.Stat(stderr)
		_, err2 := os.Stat(shimLog)
		return err1 == nil && err2 == nil
	})
	dirMode := mustMode(e.t, dir)
	if dirMode.Perm() != 0o750 || dirMode&os.ModeSetgid == 0 {
		e.t.Fatalf("log dir mode %v, want 0750 with setgid", dirMode)
	}
	if mode := mustMode(e.t, stdout); mode.Perm() != 0o600 {
		e.t.Fatalf("old stdout mode %v, want 0600", mode)
	}
	if mode := mustMode(e.t, stderr); mode.Perm() != 0o640 {
		e.t.Fatalf("stderr mode %v, want 0640", mode)
	}
	if mode := mustMode(e.t, shimLog); mode.Perm() != 0o640 {
		e.t.Fatalf("shim log mode %v, want 0640", mode)
	}
	if got, want := fileGID(e.t, stderr), fileGID(e.t, dir); got != want {
		e.t.Fatalf("stderr gid %d, dir gid %d", got, want)
	}
	if got, want := fileGID(e.t, shimLog), fileGID(e.t, dir); got != want {
		e.t.Fatalf("shim log gid %d, dir gid %d", got, want)
	}
}

func mustMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode()
}

func otherGroup() (int, bool) {
	egid := os.Getegid()
	groups, err := os.Getgroups()
	if err != nil {
		return 0, false
	}
	for _, g := range groups {
		if g != egid && g != 0 {
			return g, true
		}
	}
	return 0, false
}

func fileGID(t *testing.T, path string) uint32 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("%s: stat %T", path, fi.Sys())
	}
	return st.Gid
}
