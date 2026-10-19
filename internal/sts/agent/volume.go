package agent

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
)

// inputTempPrefix is the artifact package's unfinished-unpack prefix. A crash
// leaves these under sessions/<id>/inputs; startup removes them (D955, D975).
const inputTempPrefix = ".tmp-"

// volTarget is one non-terminal session and the volume size that ends it.
type volTarget struct {
	id    string
	limit int64
}

// volumer measures session volumes on a ticker (D976). A session that has gone
// over stays over for the life of this process.
type volumer struct {
	dataDir  string
	interval time.Duration
	log      zerolog.Logger

	mu      sync.Mutex
	targets []volTarget
	over    map[string]struct{}
}

func newVolumer(dataDir string, interval time.Duration, log zerolog.Logger) *volumer {
	if interval <= 0 {
		interval = DefaultVolumeInterval
	}
	return &volumer{dataDir: dataDir, interval: interval, log: log}
}

func (v *volumer) setTargets(t []volTarget) {
	v.mu.Lock()
	v.targets = t
	v.mu.Unlock()
}

func (v *volumer) exceeded(id string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.over[id]
	return ok
}

// run samples once at start, then on every interval, until ctx is done.
func (v *volumer) run(ctx context.Context) {
	t := time.NewTicker(v.interval)
	defer t.Stop()
	for {
		v.measure()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (v *volumer) measure() {
	v.mu.Lock()
	targets := append([]volTarget(nil), v.targets...)
	v.mu.Unlock()
	var over []string
	for _, tg := range targets {
		vol, _ := sessionDirs(v.dataDir, tg.id)
		n, err := diskBytes(vol)
		if err != nil {
			if !os.IsNotExist(err) {
				v.log.Warn().Str("session", tg.id).Err(err).Msg("volume measure")
			}
			continue
		}
		if n > tg.limit {
			over = append(over, tg.id)
		}
	}
	if len(over) == 0 {
		return
	}
	v.mu.Lock()
	if v.over == nil {
		v.over = map[string]struct{}{}
	}
	for _, id := range over {
		v.over[id] = struct{}{}
	}
	v.mu.Unlock()
}

// volumeLimit is spec volume_mb in bytes. 0 uses the deployment default (D918, D976).
func volumeLimit(spec *sessionv1.SessionSpec, defaultMB int) int64 {
	mb := spec.GetLimits().GetVolumeMb()
	if mb == 0 {
		mb = uint32(defaultMB)
	}
	if mb == 0 {
		mb = DefaultVolumeMB
	}
	return int64(mb) * 1024 * 1024
}

func volumeTargets(sessions map[string]*sess, defaultMB int) []volTarget {
	var out []volTarget
	for id, s := range sessions {
		if s.terminal() || s.parsed == nil {
			continue
		}
		out = append(out, volTarget{id: id, limit: volumeLimit(s.parsed, defaultMB)})
	}
	return out
}

type inodeKey struct {
	dev uint64
	ino uint64
}

// diskBytes is the space root occupies, the same total du reports: st_blocks
// of 512-byte units, each inode once, without following symlinks (D976).
func diskBytes(root string) (int64, error) {
	var total int64
	seen := map[inodeKey]struct{}{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || st == nil {
			return fmtSys(path)
		}
		key := inodeKey{dev: uint64(st.Dev), ino: st.Ino}
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		total += int64(st.Blocks) * 512
		return nil
	})
	return total, err
}

func fmtSys(path string) error {
	return &os.PathError{Op: "stat", Path: path, Err: errors.ErrUnsupported}
}

// chmodReadOnly makes files 0444 and directories 0555. Symlinks are left alone
// so the target outside this tree is not changed (D975). Calling it again is safe.
func chmodReadOnly(root string) error {
	var dirs []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, path)
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		return os.Chmod(path, 0o444)
	})
	if err != nil {
		return err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chmod(dirs[i], 0o555); err != nil {
			return err
		}
	}
	return nil
}

// removeWritable makes a tree deletable and removes it (D975). Directories
// become 0700 and files 0600 first, so a read-only unpack can still be removed.
func removeWritable(root string) error {
	st, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !st.IsDir() {
		if err := os.Chmod(root, 0o600); err != nil && !os.IsNotExist(err) {
			return err
		}
		return os.Remove(root)
	}
	var dirs []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if d.IsDir() {
			dirs = append(dirs, path)
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		return os.Chmod(path, 0o600)
	})
	if err != nil {
		return err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chmod(dirs[i], 0o700); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.RemoveAll(root)
}

// cleanInputTemps removes unpack leftovers under every session's inputs directory.
func cleanInputTemps(dataDir string) error {
	sessions := filepath.Join(dataDir, "sessions")
	entries, err := os.ReadDir(sessions)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		inputs := filepath.Join(sessions, e.Name(), "inputs")
		subs, err := os.ReadDir(inputs)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, s := range subs {
			if !strings.HasPrefix(s.Name(), inputTempPrefix) {
				continue
			}
			if err := removeWritable(filepath.Join(inputs, s.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}
