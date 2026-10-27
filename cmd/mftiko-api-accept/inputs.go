package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// inputRepository is where cli inputs pushes its data artifact. setup's allow covers data/ (E15-D007).
const inputRepository = "registry:5000/data/accept-model"

// inputSteps is E15-07: an uploaded directory reaches a session through the CLI alone. mftiko-pack
// data pushes it, hello runs with it as input "model", and sts1 unpacked it read-only (E09-D075).
// An input repository outside allow is a validation_failed before anything is written.
func (c *cli) inputSteps(pack, stsData string) error {
	src := filepath.Join(c.dir, "model")
	sums, err := writeModel(src)
	if err != nil {
		return err
	}
	digest, err := packData(c.f.ctx, pack, src)
	if err != nil {
		return err
	}
	say("mftiko-pack data: %s@%s", inputRepository, digest)

	outside, err := c.sessionFile("inputs-outside", inputsFile("accept-cli-inputs-outside", "registry:5000/private/accept-model", digest))
	if err != nil {
		return err
	}
	o, err := c.fails(exitValidation, "validation_failed", "session", "create", "-f", outside)
	if err != nil {
		return err
	}
	if !strings.Contains(o.stderr, "/inputs/0/repository") {
		return fmt.Errorf("validation_failed does not name /inputs/0/repository: %s", o)
	}

	file, err := c.sessionFile("inputs", inputsFile("accept-cli-inputs", inputRepository, digest))
	if err != nil {
		return err
	}
	var created apiv1.Session
	if err := c.okJSON(&created, "session", "create", "-f", file, "--wait", "running", "--wait-timeout", "2m"); err != nil {
		return err
	}
	id := created.Id
	say("mftiko session create --wait running: %s on %s with input model", id, stsInst)
	shown, err := c.show(id)
	if err != nil {
		return err
	}
	if in := shown.Spec.Inputs; len(in) != 1 || in[0].Name != "model" || in[0].Repository != inputRepository || in[0].Digest != digest {
		return fmt.Errorf("session show: inputs %+v, want model %s@%s", in, inputRepository, digest)
	}

	dest := filepath.Join(stsData, "sessions", id, "inputs", "model")
	if err := checkInput(dest, sums); err != nil {
		return err
	}
	say("input %s: %d files match, directories 0555, files 0444, latest.pt -> model.pt", dest, len(sums))

	if _, err := c.ok("session", "end", id, "--wait", "--yes"); err != nil {
		return err
	}
	s, err := c.show(id)
	if err != nil {
		return err
	}
	if phaseOf(s) != apiv1.PhaseDone || s.Status.Reason != "ended_by_spec" {
		return fmt.Errorf("after end --wait: phase %q reason %q", phaseOf(s), s.Status.Reason)
	}
	if _, err := c.ok("session", "delete", id, "--yes"); err != nil {
		return err
	}
	say("session %s done and deleted", id)
	return nil
}

// inputsFile is hello with no params on sts1, reading one input named model.
func inputsFile(name, repository, digest string) string {
	return idleFile(name, stsInst, 0, 0) + fmt.Sprintf("inputs:\n  - name: model\n    repository: %s\n    digest: %s\n", repository, digest)
}

// writeModel writes model.pt (1 MiB of random bytes), sub/meta.json, and latest.pt, a symlink to
// model.pt. It returns the SHA-256 of each regular file.
func writeModel(dir string) (map[string]string, error) {
	weights := make([]byte, 1<<20)
	if _, err := rand.Read(weights); err != nil {
		return nil, err
	}
	files := map[string][]byte{"model.pt": weights, "sub/meta.json": []byte(`{"layers":12}` + "\n")}
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		return nil, err
	}
	sums := make(map[string]string, len(files))
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), body, 0o644); err != nil {
			return nil, err
		}
		h := sha256.Sum256(body)
		sums[name] = hex.EncodeToString(h[:])
	}
	return sums, os.Symlink("model.pt", filepath.Join(dir, "latest.pt"))
}

// packData runs mftiko-pack data and returns the digest it prints on stdout.
func packData(ctx context.Context, pack, dir string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, pack, "data", "-dir", dir, "-repo", inputRepository, "-tag", strategyTag, "-plain-http")
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("mftiko-pack data: %w\n%s", err, strings.TrimSpace(se.String()))
	}
	digest := strings.TrimSpace(so.String())
	if !strings.HasPrefix(digest, "sha256:") || strings.ContainsAny(digest, " \n") {
		return "", fmt.Errorf("mftiko-pack data printed %q, want one digest", so.String())
	}
	return digest, nil
}

// checkInput compares the unpacked input with sums and checks its modes.
func checkInput(dest string, sums map[string]string) error {
	for name, want := range sums {
		p := filepath.Join(dest, filepath.FromSlash(name))
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if h := sha256.Sum256(b); hex.EncodeToString(h[:]) != want {
			return fmt.Errorf("%s: content differs from what was pushed", p)
		}
		if err := wantMode(p, 0o444); err != nil {
			return err
		}
	}
	for _, d := range []string{".", "sub"} {
		if err := wantMode(filepath.Join(dest, d), fs.ModeDir|0o555); err != nil {
			return err
		}
	}
	link := filepath.Join(dest, "latest.pt")
	target, err := os.Readlink(link)
	if err != nil {
		return err
	}
	if target != "model.pt" {
		return fmt.Errorf("%s -> %s, want model.pt", link, target)
	}
	return nil
}

func wantMode(path string, want fs.FileMode) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if got := st.Mode() & (fs.ModeType | fs.ModePerm); got != want {
		return fmt.Errorf("%s: mode %s, want %s", path, got, want)
	}
	return nil
}
