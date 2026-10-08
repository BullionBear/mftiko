package artifact

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/shim"
)

const rootfs = "/var/lib/mftiko/artifacts/sha256:ab12/rootfs"

var platformEnv = []string{"MFTIKO_WORKER_KEY=td/42", "MFTIKO_INCARNATION=3"}

// The two examples of PROTOCOL §8.2, mapped as in the table of §4.6: what comes out is a Launch the shim takes.
func TestManifestLaunch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		manifest string
		path     string
		args     []string
		env      []string
	}{
		{
			name:     "go",
			manifest: `{"schema":"mftiko.worker.v1","kind":"strategy","entrypoint":["bin/strategy"],"env":{}}`,
			path:     rootfs + "/bin/strategy",
			env:      platformEnv,
		},
		{
			name: "python",
			manifest: `{"schema":"mftiko.worker.v1","kind":"strategy","runtime":"python",
				"entrypoint":["python/bin/python3","-m","sdk.run","app.chase:Chase"],
				"env":{"PYTHONPATH":"app","PYTHONDONTWRITEBYTECODE":"1"}}`,
			path: rootfs + "/python/bin/python3",
			args: []string{"-m", "sdk.run", "app.chase:Chase"},
			env:  append([]string{"PYTHONDONTWRITEBYTECODE=1", "PYTHONPATH=app"}, platformEnv...),
		},
		{
			name:     "a ./ entrypoint",
			manifest: `{"entrypoint":["./bin/worker"]}`,
			path:     rootfs + "/bin/worker",
			env:      platformEnv,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var m Manifest
			if err := json.Unmarshal([]byte(tc.manifest), &m); err != nil {
				t.Fatal(err)
			}
			l, err := m.Launch(rootfs+"/", platformEnv)
			if err != nil {
				t.Fatal(err)
			}
			if l.GetDir() != rootfs || l.GetPath() != tc.path || !slices.Equal(l.GetArgs(), tc.args) || !slices.Equal(l.GetEnv(), tc.env) {
				t.Fatalf("launch dir %q path %q args %q env %q, want %q %q %q %q",
					l.GetDir(), l.GetPath(), l.GetArgs(), l.GetEnv(), rootfs, tc.path, tc.args, tc.env)
			}
			if err := shim.CheckLaunch(l); err != nil {
				t.Fatalf("the shim refuses it: %v", err)
			}
		})
	}
}

func TestManifestLaunchRefused(t *testing.T) {
	for name, m := range map[string]Manifest{
		"no entrypoint":    {},
		"empty entrypoint": {Entrypoint: []string{""}},
		"absolute":         {Entrypoint: []string{"/bin/sh"}},
		"climbs out":       {Entrypoint: []string{"bin/../../etc/w"}},
		"platform env":     {Entrypoint: []string{"bin/w"}, Env: map[string]string{"MFTIKO_CTL_FD": "4"}},
		"empty env name":   {Entrypoint: []string{"bin/w"}, Env: map[string]string{"": "x"}},
		"= in env name":    {Entrypoint: []string{"bin/w"}, Env: map[string]string{"A=B": "x"}},
	} {
		if _, err := m.Launch(rootfs, platformEnv); !errors.Is(err, ErrBadManifest) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := (Manifest{Entrypoint: []string{"bin/w"}}).Launch("rootfs", nil); !errors.Is(err, ErrBadManifest) {
		t.Errorf("relative rootfs: %v", err)
	}
}
