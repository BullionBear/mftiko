//go:build unix

package logread

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// maxSuffix is the highest rotation suffix the shim writes. keep includes the
// live file and is at most 100 (PROTOCOL §4.5 S4), so the oldest name is
// path.99. Listing stops there instead of walking the directory, which also
// ignores unrelated names.
const maxSuffix = 99

// errRetry means a path changed between stat and open. The caller lists again.
var errRetry = errors.New("logread: retry")

// listHook, when set, runs in listFiles after the numbered names are stat'ed
// and before the live one. Tests rotate there (E14-D042). It is not a setting.
var listHook func()

// fileEnt is one regular file in a rotation, oldest first when listed.
type fileEnt struct {
	path string
	dev  uint64
	ino  uint64
	size int64
}

// listFiles returns path.99 … path.1 and then path, skipping names that are
// absent or not regular files. A gap in the numbering is kept: rotation
// renames through that gap, and stopping at the first hole would hide a file.
// The stats are not one snapshot: a rotation between them can leave a file
// out under both its old and its new name (E14-D041).
func listFiles(live string) ([]fileEnt, error) {
	out := make([]fileEnt, 0, 4)
	for i := maxSuffix; i >= 1; i-- {
		ent, ok, err := statRegular(live + "." + strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, ent)
		}
	}
	if listHook != nil {
		listHook()
	}
	ent, ok, err := statRegular(live)
	if err != nil {
		return nil, err
	}
	if ok {
		out = append(out, ent)
	}
	return out, nil
}

func statRegular(path string) (fileEnt, bool, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fileEnt{}, false, nil
		}
		return fileEnt{}, false, err
	}
	if !fi.Mode().IsRegular() {
		return fileEnt{}, false, nil
	}
	dev, ino, err := fileID(fi)
	if err != nil {
		return fileEnt{}, false, fmt.Errorf("logread: %s: %w", path, err)
	}
	return fileEnt{path: path, dev: dev, ino: ino, size: fi.Size()}, true, nil
}

func fileID(fi os.FileInfo) (dev, ino uint64, err error) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st == nil {
		return 0, 0, errors.ErrUnsupported
	}
	return uint64(st.Dev), uint64(st.Ino), nil
}

// linkCount is how many names fi has. A file still open after its last name
// was removed has none; a rename keeps the count.
func linkCount(fi os.FileInfo) (uint64, error) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st == nil {
		return 0, errors.ErrUnsupported
	}
	return uint64(st.Nlink), nil
}

// openNoFollow opens a regular file without following a symlink planted in its
// place. Symlinks are not part of the rotation.
func openNoFollow(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(fd), path), nil
}

// hashFile is the cursor hash at off: FNV-1a of the bytes in [off-n, off),
// where n is at most prefixN. The file may have been replaced; the caller
// compares the result with the cursor.
func hashFile(path string, off int64) (uint64, error) {
	if off <= 0 {
		return fnv64(nil), nil
	}
	f, err := openNoFollow(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return hashAt(f, off)
}

// hashAt is hashFile on a file that is already open.
func hashAt(r io.ReaderAt, off int64) (uint64, error) {
	if off <= 0 {
		return fnv64(nil), nil
	}
	n := off
	if n > prefixN {
		n = prefixN
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(io.NewSectionReader(r, off-n, n), buf); err != nil {
		return 0, err
	}
	return fnv64(buf), nil
}

// numbered reports whether path is live's rotation suffix (.1, .2, …), not the
// live file itself.
func numbered(live, path string) bool {
	rest, ok := strings.CutPrefix(path, live+".")
	if !ok || rest == "" {
		return false
	}
	n, err := strconv.Atoi(rest)
	return err == nil && n >= 1 && strconv.Itoa(n) == rest
}

func hasNumbered(live string, ents []fileEnt) bool {
	for _, e := range ents {
		if numbered(live, e.path) {
			return true
		}
	}
	return false
}
