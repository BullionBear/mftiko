package shim

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// logFileName matches the files E12-D041 may delete: {h}.stdout, {h}.stderr, {h}.shim.log, and
// rotations .{n} where n has no leading zero. h is 16 hex digits (PROTOCOL §2).
var logFileName = regexp.MustCompile(`^([0-9a-f]{16})\.(?:stdout|stderr|shim\.log)(?:\.[1-9][0-9]*)?$`)

// socketName matches a live shim socket, {h}.sock (PROTOCOL §4.1).
var socketName = regexp.MustCompile(`^([0-9a-f]{16})\.sock$`)

// CleanAbsentLogs deletes regular files in dir whose names match logFileName, whose hash is not
// in keep, and whose modification time is strictly older than now-retain (E12-D041). A missing
// directory is not an error. Other names, directories, and symlinks stay. retain <= 0 deletes
// nothing.
func CleanAbsentLogs(dir string, retain time.Duration, now time.Time, keep map[string]struct{}) error {
	if retain <= 0 {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	cutoff := now.Add(-retain)
	for _, e := range entries {
		m := logFileName.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		if _, ok := keep[m[1]]; ok {
			continue
		}
		info, err := e.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if !info.ModTime().Before(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// SocketHashes returns the worker hashes of {h}.sock names in dir (E12-D041). A missing directory
// is an empty set. Any other read error is returned so the caller can skip the cleanup: it cannot
// tell which workers are still alive.
func SocketHashes(dir string) (map[string]struct{}, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]struct{}{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]struct{}{}
	for _, e := range entries {
		m := socketName.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		out[m[1]] = struct{}{}
	}
	return out, nil
}
