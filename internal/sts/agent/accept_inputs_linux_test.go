package agent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
)

func gzipDataLayer(t *testing.T, name, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	if err := tw.WriteHeader(&tar.Header{Name: "sub", Mode: 0o755, Typeflag: tar.TypeDir, Format: tar.FormatPAX}); err != nil {
		t.Fatal(err)
	}
	hdr := &tar.Header{Name: name, Mode: 0o644, Typeflag: tar.TypeReg, Size: int64(len(body)), Format: tar.FormatPAX}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func (e *env) pushData(name, body string) (manifestDigest, layerDigest string) {
	e.t.Helper()
	layer := gzipDataLayer(e.t, name, body)
	cfg := []byte(`{"schema":"mftiko.data.v1"}`)
	layerDesc := e.putBlob(layer)
	cfgDesc := e.putBlob(cfg)
	raw, err := json.Marshal(ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: artifact.MediaTypeDataArtifact,
		Config: ocispec.Descriptor{
			MediaType: artifact.MediaTypeDataConfig, Digest: digest.Digest(cfgDesc), Size: int64(len(cfg)),
		},
		Layers: []ocispec.Descriptor{{
			MediaType: artifact.MediaTypeDataLayer, Digest: digest.Digest(layerDesc), Size: int64(len(layer)),
		}},
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return e.putManifest(raw), layerDesc
}

func (e *env) putBlob(body []byte) string {
	e.t.Helper()
	base := "http://" + e.reg.addr() + "/v2/hello/blobs/uploads/"
	resp, err := http.Post(base, "application/octet-stream", nil)
	if err != nil {
		e.t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		e.t.Fatalf("start upload: %s", resp.Status)
	}
	sum := digest.FromBytes(body).String()
	req, err := http.NewRequest(http.MethodPut, base+"1?digest="+sum, bytes.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		e.t.Fatalf("put blob: %s", resp.Status)
	}
	return sum
}

func (e *env) putManifest(body []byte) string {
	e.t.Helper()
	sum := digest.FromBytes(body).String()
	req, err := http.NewRequest(http.MethodPut, "http://"+e.reg.addr()+"/v2/hello/manifests/"+sum, bytes.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", ocispec.MediaTypeImageManifest)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		e.t.Fatalf("put manifest: %s", resp.Status)
	}
	return sum
}

func pollUntil(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if ok() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A good input is unpacked read-only, the volume is 0700, and the data layer
// is collected once the unpack has finished. The strategy layer stays.
// Unpack limits are unset, so this input (a directory plus one file) is still accepted (E15-D012).
func TestInputReadOnlyAndLayerGC(t *testing.T) {
	e := setup(t)
	const id = "a1b2c3"
	manifest, layer := e.pushData("note.txt", "hello-input")
	spec := sessionSpec(1, 1)
	spec.Inputs = []*sessionv1.InputDecl{{Name: "model", Digest: manifest, Repository: e.repo}}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	vol, inputs := sessionDirs(e.cfg.DataDir, id)
	note := filepath.Join(inputs, "model", "note.txt")
	if string(mustReadFile(t, note)) != "hello-input" {
		t.Fatal("input body")
	}
	assertPerm(t, vol, 0o700)
	assertPerm(t, filepath.Join(inputs, "model"), 0o555)
	assertPerm(t, filepath.Join(inputs, "model", "sub"), 0o555)
	assertPerm(t, note, 0o444)
	var env map[string]string
	for _, en := range e.journalEntries(id) {
		if en.Event == "env" {
			env = en.Env
		}
	}
	if env["MFTIKO_VOLUME_DIR"] != vol || env["MFTIKO_INPUT_DIR"] != inputs {
		t.Fatalf("env %+v", env)
	}
	pollUntil(t, "data layer collected", func() bool {
		_, err := os.Stat(filepath.Join(e.cfg.DataDir, "layers", layer))
		return os.IsNotExist(err)
	})
	rootfs := filepath.Join(e.cfg.DataDir, "artifacts", e.digest, "rootfs")
	if _, err := os.Stat(rootfs); err != nil {
		t.Fatalf("strategy rootfs: %v", err)
	}
}

// An input over the unpack entry limit ends the session before spawn.
// The rootfs is one file, so the same limit still lets a session with no inputs run.
func TestInputOverUnpackLimit(t *testing.T) {
	e := setup(t)
	e.cfg.UnpackLimits.MaxFiles = 1
	const id, sib = "a1b2c3", "b2c3d4"
	manifest, _ := e.pushData("note.txt", "aaaa")
	spec := sessionSpec(1, 1)
	spec.Inputs = []*sessionv1.InputDecl{{Name: "model", Digest: manifest, Repository: e.repo}}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	e.putSpec(sib, sessionSpec(2, 1))
	e.putDemand(sib, runningDemand(true, 2))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	st := ph.wait(id, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != artifact.ReasonUnsafeLayer {
		t.Fatalf("reason %q", st.GetReason())
	}
	if n := e.starts(id); n != 0 {
		t.Fatalf("spawned %d times", n)
	}
	ph.wait(sib, statev1.Phase_PHASE_RUNNING)
	if strings.Contains(e.log.String(), "artifact rejected") {
		t.Fatalf("rootfs was rejected:\n%s", e.log.String())
	}
	e.waitLog("input rejected")
	if !strings.Contains(e.log.String(), artifact.ReasonUnsafeLayer) {
		t.Fatalf("log missing reason:\n%s", e.log.String())
	}
	_, inputs := sessionDirs(e.cfg.DataDir, id)
	if _, err := os.Stat(filepath.Join(inputs, "model", "note.txt")); !os.IsNotExist(err) {
		t.Fatalf("input file: %v", err)
	}
}

// Two inputs that each fit the entry limit still run. The counts are not added together.
func TestInputsUnpackLimitsNotSummed(t *testing.T) {
	e := setup(t)
	e.cfg.UnpackLimits.MaxFiles = 2
	const id = "a1b2c3"
	one, _ := e.pushData("a.txt", "aaaa")
	two, _ := e.pushData("b.txt", "bbbb")
	spec := sessionSpec(1, 1)
	spec.Inputs = []*sessionv1.InputDecl{
		{Name: "one", Digest: one, Repository: e.repo},
		{Name: "two", Digest: two, Repository: e.repo},
	}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	_, inputs := sessionDirs(e.cfg.DataDir, id)
	if string(mustReadFile(t, filepath.Join(inputs, "one", "a.txt"))) != "aaaa" {
		t.Fatal("one")
	}
	if string(mustReadFile(t, filepath.Join(inputs, "two", "b.txt"))) != "bbbb" {
		t.Fatal("two")
	}
}

// A rootfs over the unpacked size limit fails the same way, with no inputs.
func TestRootfsOverUnpackLimit(t *testing.T) {
	e := setup(t)
	e.cfg.UnpackLimits.MaxBytes = 1
	const id = "a1b2c3"
	e.putSpec(id, sessionSpec(1, 1))
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	st := ph.wait(id, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != artifact.ReasonUnsafeLayer {
		t.Fatalf("reason %q", st.GetReason())
	}
	if n := e.starts(id); n != 0 {
		t.Fatalf("spawned %d times", n)
	}
	e.waitLog("artifact rejected")
}

// An input whose manifest is not a data artifact ends the session and does not spawn.
func TestInputRejectedDoesNotSpawn(t *testing.T) {
	e := setup(t)
	const id = "a1b2c3"
	spec := sessionSpec(1, 1)
	spec.Inputs = []*sessionv1.InputDecl{{Name: "bad", Digest: e.digest, Repository: e.repo}}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	st := ph.wait(id, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != artifact.ReasonWrongMediaType {
		t.Fatalf("reason %q", st.GetReason())
	}
	if n := e.starts(id); n != 0 {
		t.Fatalf("spawned %d times", n)
	}
}

// The volume written by an incarnation is still there after that worker crashes.
func TestVolumeSurvivesCrash(t *testing.T) {
	e := setup(t)
	e.cfg.BackoffMin = 20 * time.Millisecond
	e.cfg.BackoffMax = 20 * time.Millisecond
	const id = "a1b2c3"
	e.setScript(id, fakeworker.Script{
		Exit:        fakeworker.ExitCrashed,
		ExitAfterMS: 500,
		VolumeFile:  "note",
		VolumeText:  "kept",
	})
	spec := sessionSpec(1, 1)
	spec.Restart = &sessionv1.RestartPolicy{
		Mode: sessionv1.RestartMode_RESTART_MODE_ON_FAILURE, MaxRestarts: 5, WindowS: 300,
	}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	// ExitAfterMS is long enough for a reconcile to publish RUNNING before the crash.
	// The latest phase does not have to still be RUNNING: the next incarnation follows.
	pollUntil(t, "running then a later incarnation", func() bool {
		ran := false
		for _, p := range ph.history(id) {
			if p == statev1.Phase_PHASE_RUNNING {
				ran = true
			}
		}
		return ran && e.starts(id) >= 2
	})
	pollUntil(t, "restarted incarnation read the volume", func() bool {
		for _, en := range e.journalEntries(id) {
			if en.Event == "volume" && en.Detail == "kept" {
				return true
			}
		}
		return false
	})
	vol, _ := sessionDirs(e.cfg.DataDir, id)
	if string(mustReadFile(t, filepath.Join(vol, "note"))) != "kept" {
		t.Fatal("volume file")
	}
}

// A volume over the limit is a normal Stop: grace, OnStop, FAILED, and no restart.
func TestVolumeOverLimit(t *testing.T) {
	e := setup(t)
	e.cfg.VolumeInterval = 15 * time.Millisecond
	const id, sib = "a1b2c3", "b2c3d4"
	e.setScript(id, fakeworker.Script{VolumeFile: "big", VolumeBytes: 2 << 20, OnStopMS: 40})
	spec := sessionSpec(1, 1)
	spec.Limits = &sessionv1.Limits{VolumeMb: 1}
	spec.GraceMs = 2500
	spec.Restart = &sessionv1.RestartPolicy{
		Mode: sessionv1.RestartMode_RESTART_MODE_ON_FAILURE, MaxRestarts: 5, WindowS: 300,
	}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	e.putSpec(sib, sessionSpec(2, 1))
	e.putDemand(sib, runningDemand(true, 2))
	ph := e.collect(e.rev())
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	st := ph.wait(id, statev1.Phase_PHASE_FAILED)
	if st.GetReason() != ReasonVolumeOverLimit {
		t.Fatalf("reason %q", st.GetReason())
	}
	ph.wait(sib, statev1.Phase_PHASE_RUNNING)
	if got := e.stopGrace(id); got != 2500 {
		t.Fatalf("grace %d", got)
	}
	pollUntil(t, "OnStop completed", func() bool {
		for _, en := range e.journalEntries(id) {
			if en.Event != "sent" {
				continue
			}
			up, err := en.Up()
			if err != nil || up.GetStopped() == nil {
				continue
			}
			if up.GetStopped().GetOnStopCompleted() {
				return true
			}
		}
		return false
	})
	if n := e.starts(id); n != 1 {
		t.Fatalf("starts %d", n)
	}
	if got := e.status(id); got.GetPhase() != statev1.Phase_PHASE_FAILED || got.GetReason() != ReasonVolumeOverLimit {
		t.Fatalf("status %+v", got)
	}
}

// An agent restart unpacks an input again when the directory is gone, and does
// not spawn while that directory is missing.
func TestInputUnpackedAgainWhenMissing(t *testing.T) {
	e := setup(t)
	const id = "a1b2c3"
	manifest, _ := e.pushData("note.txt", "hello-input")
	spec := sessionSpec(1, 1)
	spec.Inputs = []*sessionv1.InputDecl{{Name: "model", Digest: manifest, Repository: e.repo}}
	e.putSpec(id, spec)
	e.putDemand(id, runningDemand(true, 1))
	ph := e.collect(e.rev())
	stop, done := e.start()
	ph.wait(id, statev1.Phase_PHASE_RUNNING)
	stop()
	waitDone(t, done)
	_, inputs := sessionDirs(e.cfg.DataDir, id)
	if err := artifact.RemoveWritable(filepath.Join(inputs, "model")); err != nil {
		t.Fatal(err)
	}
	before := e.starts(id)
	e.cfg.Term.ID = t.Name() + "-next"
	stop, done = e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()
	note := filepath.Join(inputs, "model", "note.txt")
	pollUntil(t, "input unpacked again", func() bool {
		b, err := os.ReadFile(note)
		if err == nil && string(b) == "hello-input" {
			return true
		}
		if e.starts(id) > before {
			t.Fatalf("spawned before the input was unpacked: starts %d", e.starts(id))
		}
		return false
	})
	assertPerm(t, note, 0o444)
	if got := e.status(id); got.GetPhase() == statev1.Phase_PHASE_FAILED {
		t.Fatalf("failed: %+v", got)
	}
}
