package fleet

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/internal/platform/shim"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

const envFleetLogMode = "MFTIKO_TEST_FLEET_LOGMODE"

// startShim leaves an existing shim log alone and creates a new one at 0640
// under a umask that would otherwise clear the group bit. The plane log
// directory becomes 0750 and keeps setgid (E10-D021, E10-D022).
func TestStartShimLogMode(t *testing.T) {
	if os.Getenv(envFleetLogMode) == "1" {
		runStartShimLogModeChild()
		return
	}
	wd := t.TempDir()
	dir := shim.LogDir(wd, "td")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o700|os.ModeSetgid); err != nil {
		t.Fatal(err)
	}
	oldKey := "td/7"
	old := filepath.Join(dir, state.WorkerHash(oldKey)+".shim.log")
	if err := os.WriteFile(old, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(old, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestStartShimLogMode$")
	cmd.Env = append(os.Environ(), envFleetLogMode+"=1", "MFTIKO_TEST_WORKDIR="+wd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o750 || fi.Mode()&os.ModeSetgid == 0 {
		t.Fatalf("dir mode %v, want 0750 with setgid", fi.Mode())
	}
	fi, err = os.Stat(old)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("existing shim log %v, want 0600", fi.Mode())
	}
	created := filepath.Join(dir, state.WorkerHash("td/8")+".shim.log")
	fi, err = os.Stat(created)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o640 {
		t.Fatalf("new shim log %v, want 0640", fi.Mode())
	}
}

func runStartShimLogModeChild() {
	syscall.Umask(0o077)
	wd := os.Getenv("MFTIKO_TEST_WORKDIR")
	f := New(Config[h, h]{
		Plane: state.PlaneTD, Instance: "td-tw-1", Epoch: 1, WorkDir: wd, Shim: "/bin/cat", Log: zerolog.Nop(),
	})
	for _, key := range []string{"td/7", "td/8"} {
		done, err := f.startShim(&shimv1.Launch{Worker: &commonv1.WorkerRef{Key: key, Incarnation: 1}})
		if err != nil {
			fmt.Fprintf(os.Stderr, "start %s: %v\n", key, err)
			os.Exit(2)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			fmt.Fprintf(os.Stderr, "timeout %s\n", key)
			os.Exit(3)
		}
	}
	os.Exit(0)
}
