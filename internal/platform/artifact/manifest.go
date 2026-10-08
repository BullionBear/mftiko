package artifact

import (
	"errors"
	"fmt"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
)

// Schema is the manifest version this package reads.
const Schema = "mftiko.worker.v1"

// Kinds of worker artifact (PROTOCOL §8.2). An agent accepts only its own.
const (
	KindStrategy  = "strategy"
	KindMDConn    = "md.conn"
	KindTDAccount = "td.account"
)

// ErrBadManifest is a manifest no worker is started from.
var ErrBadManifest = errors.New("artifact: bad manifest")

// Manifest is the mftiko manifest, the config blob of a worker artifact (PROTOCOL §8.2).
type Manifest struct {
	Schema     string            `json:"schema"`
	Kind       string            `json:"kind"`
	Name       string            `json:"name"`
	Runtime    string            `json:"runtime"`
	OS         string            `json:"os"`
	Arch       string            `json:"arch"`
	Entrypoint []string          `json:"entrypoint"`
	Env        map[string]string `json:"env,omitempty"`
	SDK        SDK               `json:"sdk"`
	Contracts  []string          `json:"contracts"`
}

// SDK is the language and version of the SDK a worker was built with.
type SDK struct {
	Lang    string `json:"lang"`
	Version string `json:"version"`
}

// CheckLaunchFields checks what Launch takes from the manifest: an entrypoint relative to the rootfs that stays in
// it, and an environment that sets nothing of the platform's.
func (m Manifest) CheckLaunchFields() error {
	if len(m.Entrypoint) == 0 || m.Entrypoint[0] == "" {
		return fmt.Errorf("%w: no entrypoint", ErrBadManifest)
	}
	exe := m.Entrypoint[0]
	if strings.HasPrefix(exe, "/") || slices.Contains(strings.Split(exe, "/"), "..") {
		return fmt.Errorf("%w: entrypoint %q is not a path inside the rootfs", ErrBadManifest, exe)
	}
	for k := range m.Env {
		switch {
		case k == "" || strings.Contains(k, "="):
			return fmt.Errorf("%w: env name %q", ErrBadManifest, k)
		case strings.HasPrefix(k, "MFTIKO_"):
			return fmt.Errorf("%w: env sets %s: MFTIKO_ variables are the platform's", ErrBadManifest, k)
		}
	}
	return nil
}

// Launch fills the fields of a shim Launch that come from the manifest (PROTOCOL §4.6): dir is the rootfs, path
// the rootfs and entrypoint[0], args the rest of the entrypoint, env the manifest's (sorted by name) and then
// platform's, the §5.4 variables. rootfs is the absolute path the artifact was unpacked to. The caller sets the
// rest: plane, agent instance, worker, spec hash and credential.
func (m Manifest) Launch(rootfs string, platform []string) (*shimv1.Launch, error) {
	if err := m.CheckLaunchFields(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(rootfs) {
		return nil, fmt.Errorf("%w: rootfs %q is not absolute", ErrBadManifest, rootfs)
	}
	rootfs = filepath.Clean(rootfs)
	env := make([]string, 0, len(m.Env)+len(platform))
	for _, k := range slices.Sorted(maps.Keys(m.Env)) {
		env = append(env, k+"="+m.Env[k])
	}
	env = append(env, platform...)
	return &shimv1.Launch{
		Dir:  rootfs,
		Path: filepath.Join(rootfs, path.Clean(m.Entrypoint[0])),
		Args: slices.Clone(m.Entrypoint[1:]),
		Env:  env,
	}, nil
}
