package agent

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// The next session reads the pushed volume through state_out, and the host keeps
// nothing of the finished session (D984).
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
	pollUntil(t, "no residue of "+id, func() bool { return e.sessionResidue(id) == "" })
}

// A stopped registry leaves the volume in place. After it answers again the push
// lands and state_out is filled in, without holding the terminal phase back (D983, D984).
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
}

// terminal.json is enough for a new process to finish the push and the cleanup (D982, D984).
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
