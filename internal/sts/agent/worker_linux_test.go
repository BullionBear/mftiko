package agent

import (
	"bytes"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/sdk"
)

// prepareWorker builds the testdata worker without the race detector and packs
// it once. Acceptance tests share that image. The race detector stays on the
// test binary, which is still the shim and the agent.
func prepareWorker() error {
	root := moduleRoot()
	if root == "" {
		return errors.New("sts: module root not found")
	}
	dir, err := os.MkdirTemp("", "sts-worker-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	bin := filepath.Join(dir, "worker")
	cmd := exec.Command("go", "build", "-ldflags", "-s -w", "-o", bin, "./internal/sts/agent/testdata/worker")
	cmd.Dir = root
	cmd.Env = workerBuildEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			return fmt.Errorf("sts: build worker: %w", err)
		}
		return fmt.Errorf("sts: build worker: %w\n%s", err, msg)
	}
	raw, err := os.ReadFile(bin)
	if err != nil {
		return err
	}
	if err := checkWorkerBuild(raw); err != nil {
		return err
	}
	img, err := artifact.PackStrategy(raw, "fake", nil)
	if err != nil {
		return fmt.Errorf("sts: pack worker: %w", err)
	}
	var manifest struct {
		Contracts []string `json:"contracts"`
	}
	if err := json.Unmarshal(img.Config, &manifest); err != nil {
		return fmt.Errorf("sts: worker manifest: %w", err)
	}
	if !slices.Equal(manifest.Contracts, sdk.Contracts()) {
		return fmt.Errorf("sts: worker contracts %q, sdk %q", manifest.Contracts, sdk.Contracts())
	}
	packImg = img
	return nil
}

// checkWorkerBuild refuses a race-instrumented worker. The race detector
// belongs on the shim and the agent, which are the test binary.
func checkWorkerBuild(raw []byte) error {
	info, err := buildinfo.Read(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("sts: worker buildinfo: %w", err)
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "CGO_ENABLED":
			if s.Value != "0" {
				return fmt.Errorf("sts: worker CGO_ENABLED=%s, want 0", s.Value)
			}
		case "-race":
			if s.Value == "true" {
				return errors.New("sts: worker was built with -race")
			}
		}
	}
	return nil
}

// workerBuildEnv is the test process environment with the race detector
// removed. -race requires cgo, so CGO_ENABLED=0 keeps the worker uninstrumented
// even when the parent was started with -race. A GOFLAGS=-race is dropped too.
func workerBuildEnv() []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, kv := range os.Environ() {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		switch k {
		case "GOFLAGS":
			kept := dropRaceFlag(v)
			if kept == "" {
				continue
			}
			env = append(env, "GOFLAGS="+kept)
		case "CGO_ENABLED":
			continue
		default:
			env = append(env, kv)
		}
	}
	env = append(env, "CGO_ENABLED=0")
	return env
}

func dropRaceFlag(flags string) string {
	fields := strings.Fields(flags)
	kept := make([]string, 0, len(fields))
	for _, f := range fields {
		if f == "-race" || f == "--race" {
			continue
		}
		kept = append(kept, f)
	}
	return strings.Join(kept, " ")
}
