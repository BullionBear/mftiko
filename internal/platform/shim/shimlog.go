package shim

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// shimLog is {h}.shim.log (E12-D042). It rotates at ShimLogMaxBytes and keeps ShimLogKeep files,
// the one being written included, using the same rotator as worker stdout. Writes are serialized:
// zerolog calls Write from more than one goroutine, and the rotator is not.
//
// After each open, fd 1 and fd 2 are pointed at the current file. The agent attached that file,
// not a pipe, and a write that still uses stderr then follows a rotation. The rotator file itself
// is close-on-exec (os.OpenFile), so the worker does not inherit it; Spawn replaces the worker's
// own fd 1 and 2 with its stdout and stderr pipes.
type shimLog struct {
	mu  sync.Mutex
	r   *rotator
	gen uint64
}

// openShimLog rotates the shim log the agent already created. A missing file is not an error and
// not a signal to create one: the parent kept stderr for itself (a test capturing it). A file that
// exists but cannot be opened is an error; the shim exits before spawn.
func openShimLog(workDir, plane, workerKey string) (*shimLog, error) {
	return openShimLogLimits(workDir, plane, workerKey, ShimLogMaxBytes, ShimLogKeep)
}

func openShimLogLimits(workDir, plane, workerKey string, maxBytes int64, keep int) (*shimLog, error) {
	path := filepath.Join(LogDir(workDir, plane), workerHash(workerKey)+".shim.log")
	if _, err := os.Lstat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	r, err := openRotator(path, maxBytes, keep)
	if err != nil {
		return nil, err
	}
	s := &shimLog{r: r}
	if err := s.sync(); err != nil {
		r.Close()
		return nil, err
	}
	return s, nil
}

func (s *shimLog) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.r.Write(p)
	if derr := s.sync(); derr != nil && err == nil {
		err = derr
	}
	return n, err
}

func (s *shimLog) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.r.Close()
}

// sync points fd 1 and fd 2 at the file the rotator currently has open. gen distinguishes a
// rotation that reused the same descriptor number.
func (s *shimLog) sync() error {
	if s.r.f == nil || s.r.gen == s.gen {
		return nil
	}
	if err := dupStdio(int(s.r.f.Fd())); err != nil {
		return err
	}
	s.gen = s.r.gen
	return nil
}
