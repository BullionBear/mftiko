package agent

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
	"github.com/BullionBear/mftiko/internal/platform/state"
	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
	"github.com/BullionBear/mftiko/internal/sts"
	"github.com/BullionBear/mftiko/internal/sts/issuer"
)

// TestE2EInputs is E15-05: a directory pushed with mftiko-pack data reaches a Go strategy
// read-only through the API, the controller, STS, and the shim. The strategy is
// examples/strategies/inputs, packed with mftiko-pack, and the registry is registry:2
// (E10-D061). Nothing writes etcd directly.
func TestE2EInputs(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes through 0444 and 0555; the strategy checks for EACCES")
	}
	reg := registry2(t)
	root := reg + "/mftiko-test/e15-05"
	bins := buildInputsBins(t)
	dockerConfig := t.TempDir()
	pack := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(bins.pack, args...)
		cmd.Env = append(os.Environ(), "DOCKER_CONFIG="+dockerConfig)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("mftiko-pack %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
		}
		return strings.TrimSpace(string(out))
	}
	stratRepo := root + "/strategies/inputs"
	strat := pack("-bin", bins.strategy, "-name", "inputs", "-repo", stratRepo, "-plain-http")

	// The model is random, so gzip cannot shrink it below max_input_bytes; big is over it.
	const maxInputBytes = 6 << 20
	dataRepo := root + "/data/model"
	model, files := inputTree(t, 4<<20)
	modelDigest := pack("data", "-dir", model, "-repo", dataRepo, "-tag", "e15-05", "-plain-http")
	other, _ := inputTree(t, 4<<10)
	otherDigest := pack("data", "-dir", other, "-repo", dataRepo, "-plain-http")
	big, _ := inputTree(t, 8<<20)
	bigDigest := pack("data", "-dir", big, "-repo", dataRepo, "-plain-http")

	e := setupInputsSTS(t, reg, root+"/state")
	base := e.startAPI(map[string]any{
		"allow":            []string{root},
		"plain_http_hosts": []string{reg},
		"max_input_bytes":  maxInputBytes,
	})
	e.startController()
	stop, done := e.start()
	defer func() {
		stop()
		waitDone(t, done)
	}()

	create := func(repo, digest string) (int, []byte) {
		t.Helper()
		body, err := json.Marshal(map[string]any{
			"name":         "inputs",
			"sts_instance": e.inst(),
			"strategy":     map[string]string{"repository": stratRepo, "ref": strat},
			"params":       map[string]any{"input": "model", "sha256": files},
			"inputs":       []map[string]string{{"name": "model", "repository": repo, "digest": digest}},
		})
		if err != nil {
			t.Fatal(err)
		}
		code, _, raw := sessionPost(t, base+"/api/v1/sessions", string(body))
		return code, raw
	}

	t.Run("rejected", func(t *testing.T) {
		for _, c := range []struct {
			name, repo, digest, path, reason string
		}{
			{"not allowed", reg + "/mftiko-test/e15-05-denied/model", modelDigest, "/inputs/0/repository", "is not allowed"},
			{"not data", stratRepo, strat, "/inputs/0/digest", "is not a data artifact"},
			{"too large", dataRepo, bigDigest, "/inputs/0/digest", "is too large"},
		} {
			code, raw := create(c.repo, c.digest)
			var p apiv1.Problem
			if err := json.Unmarshal(raw, &p); err != nil {
				t.Fatalf("%s: %d %s", c.name, code, raw)
			}
			if code != http.StatusBadRequest || p.Code != apiv1.ErrorCodeValidationFailed || p.Errors == nil ||
				!slices.Contains(*p.Errors, apiv1.FieldError{Path: c.path, Reason: c.reason}) {
				t.Errorf("%s: %d %s", c.name, code, raw)
			}
		}
	})

	t.Run("read", func(t *testing.T) {
		code, raw := create(dataRepo, modelDigest)
		if code != http.StatusCreated {
			t.Fatalf("create %d %s", code, raw)
		}
		var created apiv1.Session
		if err := json.Unmarshal(raw, &created); err != nil {
			t.Fatal(err)
		}
		waitSession(t, base, created.Id, "running", func(st *apiv1.SessionStatus) bool {
			return st.Phase == apiv1.PhaseRunning
		})
		if code, _, raw := sessionPost(t, base+"/api/v1/sessions/"+created.Id+"/end", `{}`); code != http.StatusOK {
			t.Fatalf("end %d %s", code, raw)
		}
		st := waitSession(t, base, created.Id, "done", func(st *apiv1.SessionStatus) bool {
			return st.Phase == apiv1.PhaseDone || st.Phase == apiv1.PhaseFailed
		})
		if st.Phase != apiv1.PhaseDone || st.Reason != "ended_by_spec" {
			t.Fatalf("status %+v", st)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		code, raw := create(dataRepo, otherDigest)
		if code != http.StatusCreated {
			t.Fatalf("create %d %s", code, raw)
		}
		var created apiv1.Session
		if err := json.Unmarshal(raw, &created); err != nil {
			t.Fatal(err)
		}
		st := waitSession(t, base, created.Id, "failed", func(st *apiv1.SessionStatus) bool {
			return st.Phase == apiv1.PhaseDone || st.Phase == apiv1.PhaseFailed
		})
		if st.Phase != apiv1.PhaseFailed || st.LastExit == nil || st.LastExit.Outcome != apiv1.OutcomeInitFailed ||
			!strings.Contains(st.LastExit.Reason, ".pt: sha256") {
			t.Fatalf("status %+v exit %+v", st, st.LastExit)
		}
	})
}

// registry2 is the CI registry:2 service, or the one MFTIKO_REGISTRY names. Without
// either the test skips, unless MFTIKO_REQUIRE_REGISTRY is set (E10-D061).
func registry2(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("MFTIKO_REGISTRY")
	if addr == "" {
		addr = "127.0.0.1:5000"
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/v2/")
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return addr
		}
	}
	if os.Getenv("MFTIKO_REQUIRE_REGISTRY") != "" {
		t.Fatalf("MFTIKO_REQUIRE_REGISTRY is set and no registry answers on %s: %v", addr, err)
	}
	t.Skipf("no registry on %s; set MFTIKO_REGISTRY", addr)
	return ""
}

type inputsBins struct {
	pack     string // mftiko-pack
	strategy string // examples/strategies/inputs
}

// buildInputsBins builds mftiko-pack and the inputs strategy without the race detector.
func buildInputsBins(t *testing.T) inputsBins {
	t.Helper()
	root := moduleRoot()
	if root == "" {
		t.Fatal("module root not found")
	}
	dir := t.TempDir()
	out := inputsBins{pack: filepath.Join(dir, "mftiko-pack"), strategy: filepath.Join(dir, "inputs")}
	for bin, pkg := range map[string]string{out.pack: "./cmd/mftiko-pack", out.strategy: "./examples/strategies/inputs"} {
		cmd := exec.Command("go", "build", "-o", bin, pkg)
		cmd.Dir = root
		cmd.Env = workerBuildEnv()
		if msg, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", pkg, err, msg)
		}
	}
	return out
}

// inputTree writes model.pt with size random bytes, sub/meta.json, and latest.pt, a
// symlink to model.pt. files is the strategy's sha256 params for that tree.
func inputTree(t *testing.T, size int) (dir string, files map[string]string) {
	t.Helper()
	dir = t.TempDir()
	weights := make([]byte, size)
	if _, err := rand.Read(weights); err != nil {
		t.Fatal(err)
	}
	meta := []byte(`{"layers":12}`)
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{"model.pt": weights, "sub/meta.json": meta} {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("model.pt", filepath.Join(dir, "latest.pt")); err != nil {
		t.Fatal(err)
	}
	sum := func(b []byte) string {
		h := sha256.Sum256(b)
		return hex.EncodeToString(h[:])
	}
	return dir, map[string]string{"model.pt": sum(weights), "latest.pt": sum(weights), "sub/meta.json": sum(meta)}
}

// setupInputsSTS is an STS agent that runs real workers against registry:2 and an
// in-process NATS, as setupHello does against the in-process registry.
func setupInputsSTS(t *testing.T, reg, dataRepo string) *env {
	t.Helper()
	inst, topo, root := openCluster(t)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	ns := startHelloNATS(t)
	signer, err := natsdev.LoadSigner(ns.lay.Manifest.Account, filepath.Join(ns.lay.Dir, filepath.FromSlash(ns.lay.Manifest.SigningKeyFile)))
	if err != nil {
		t.Fatal(err)
	}
	wd, err := os.MkdirTemp("/tmp", "sts-inputs-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(wd) })
	buf := &syncBuf{}
	log := zerolog.New(buf).Level(zerolog.InfoLevel)
	e := &env{t: t, root: root, log: buf}
	e.cfg = Config{
		Topology: topo, Instance: inst, Dial: state.AuthDial(inst, etcdtest.Password(inst)),
		Term:           state.TermOptions{ID: t.Name()},
		WorkDir:        wd,
		DataDir:        t.TempDir(),
		Shim:           self,
		ShimEnv:        []string{envTestShim + "=1"},
		WorkerEnv:      []string{"MFTIKO_NATS_URL=" + ns.url},
		DataRepository: dataRepo,
		PlainHTTPHosts: []string{reg},
		Sign: func(id string, perms sts.Permissions) (string, string, error) {
			return issuer.SignSession(id, perms, signer.AccountPublic(), signer.SigningKey())
		},
		Grace:      DefaultGrace,
		Resync:     50 * time.Millisecond,
		StaleAfter: 5 * time.Second,
		Mirror:     state.MirrorOptions{ProbeInterval: 50 * time.Millisecond, Log: &log},
		Release:    "test",
		Log:        log,
	}
	e.cfg.Started = func(pid int) {
		e.mu.Lock()
		e.pids = append(e.pids, pid)
		e.mu.Unlock()
	}
	t.Cleanup(func() {
		// Inputs are mode 0555 after unpack (E09-D075). TempDir cannot remove them until they are writable.
		_ = artifact.RemoveWritable(filepath.Join(e.cfg.DataDir, "sessions"))
		e.mu.Lock()
		pids := append([]int(nil), e.pids...)
		e.mu.Unlock()
		for _, pid := range pids {
			syscall.Kill(pid, syscall.SIGKILL) //nolint:errcheck // already exited
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", buf.String())
		}
	})
	return e
}

// waitSession polls GET /api/v1/sessions/{id} until ok accepts its status.
func waitSession(t *testing.T, base, id, what string, ok func(*apiv1.SessionStatus) bool) *apiv1.SessionStatus {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	var last apiv1.Session
	deadline := time.Now().Add(60 * time.Second)
	for {
		req, err := http.NewRequest(http.MethodGet, base+"/api/v1/sessions/"+id, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+sessionAPIToken)
		resp, err := client.Do(req)
		if err == nil {
			var s apiv1.Session
			if resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&s) == nil {
				last = s
			}
			resp.Body.Close()
		}
		if last.Status != nil && ok(last.Status) {
			return last.Status
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s %s: status %+v", id, what, last.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
