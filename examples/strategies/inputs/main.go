// Command inputs reads a data input from the session spec and checks it (E15-05, PROTOCOL §8.4).
//
// Params are the strategy's own JSON. The platform does not interpret them.
//
//	{"input":"model","sha256":{"model.pt":"<hex>","sub/meta.json":"<hex>"}}
//
// OnInit reads each listed file under ctx.InputDir(input) and compares its SHA-256. It then
// checks that the input is read-only: creating a file and opening a listed file for writing must
// both fail with EACCES. Any mismatch is an error, so the session ends INIT_FAILED. A matching
// input is one JSON line on stderr through ctx.Log (E15-D022). After that the strategy runs until
// its session is ended.
//
// The input is read-only only by its mode bits (0444 and 0555, E09-D075). A worker running as
// root can still write, and this strategy then fails OnInit.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/BullionBear/mftiko/sdk"
)

// inputsParams is this strategy's own configuration. Field names are not a platform contract.
type inputsParams struct {
	Input  string            `json:"input"`
	SHA256 map[string]string `json:"sha256"` // slash-separated path under the input: hex SHA-256
}

type inputs struct{}

func (s *inputs) OnInit(ctx sdk.Context) error {
	var p inputsParams
	if err := json.Unmarshal(ctx.Params(), &p); err != nil {
		return fmt.Errorf("params: %w", err)
	}
	if p.Input == "" || len(p.SHA256) == 0 {
		return errors.New("params: input and sha256 are required")
	}
	dir := ctx.InputDir(p.Input)
	if dir == "" {
		return fmt.Errorf("input %q has no directory", p.Input)
	}
	names := make([]string, 0, len(p.SHA256))
	for name := range p.SHA256 {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if !filepath.IsLocal(filepath.FromSlash(name)) {
			return fmt.Errorf("%s: not a path under the input", name)
		}
		got, err := fileSHA256(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			return err
		}
		if got != p.SHA256[name] {
			return fmt.Errorf("%s: sha256 %s, want %s", name, got, p.SHA256[name])
		}
	}
	if err := refusesWrites(dir, filepath.Join(dir, filepath.FromSlash(names[0]))); err != nil {
		return err
	}
	log := ctx.Log()
	log.Info().Str("hook", "OnInit").Str("input", p.Input).Int("files", len(names)).Msg("inputs")
	return nil
}

func (s *inputs) OnStart(sdk.Context) error { return nil }

func (s *inputs) OnStop(sdk.Context) {}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// refusesWrites checks that a new file in dir and a write to file both fail with EACCES.
func refusesWrites(dir, file string) error {
	probe := filepath.Join(dir, ".inputs-probe")
	f, err := os.OpenFile(probe, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err == nil {
		f.Close()
		os.Remove(probe)
		return fmt.Errorf("%s: created a file in the input", dir)
	}
	if !errors.Is(err, syscall.EACCES) {
		return fmt.Errorf("%s: create: %w, want EACCES", dir, err)
	}
	f, err = os.OpenFile(file, os.O_WRONLY, 0)
	if err == nil {
		f.Close()
		return fmt.Errorf("%s: opened for writing", file)
	}
	if !errors.Is(err, syscall.EACCES) {
		return fmt.Errorf("%s: open for writing: %w, want EACCES", file, err)
	}
	return nil
}

var _ sdk.Strategy = (*inputs)(nil)

func main() { sdk.Run(&inputs{}) }
