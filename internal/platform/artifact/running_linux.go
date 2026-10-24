//go:build linux

package artifact

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
)

// RunningDigests reports image digests of processes whose executable is under
// ${DATA}/artifacts/<digest>/rootfs (E12-D014, E12-D008). A process whose /proc entry cannot be
// read is skipped. An unreadable /proc is an error: the caller then skips GC and deletes nothing.
func RunningDigests(dataDir string) ([]string, error) {
	dataDir = filepath.Clean(dataDir)
	// /proc/pid/exe is the path with symlinks resolved. A data directory reached
	// through a symlink still names the same rootfs.
	if resolved, err := filepath.EvalSymlinks(dataDir); err == nil {
		dataDir = resolved
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("artifact: running: %w", err)
	}
	set := map[string]struct{}{}
	for _, e := range entries {
		pid := e.Name()
		if !pidName(pid) {
			continue
		}
		target, err := os.Readlink(filepath.Join("/proc", pid, "exe"))
		if err != nil {
			continue
		}
		if dig, ok := digestOfExe(dataDir, target); ok {
			set[dig] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(set)), nil
}

func pidName(s string) bool {
	if s == "" || s[0] < '1' || s[0] > '9' {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
