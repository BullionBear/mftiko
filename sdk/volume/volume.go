package volume

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// beforeRename runs after the temp file is synced and closed, and before rename.
// Tests set it to crash the process. Production leaves it nil.
var beforeRename func()

var errName = errors.New("volume: name must be a relative path inside the directory")

// Write atomically replaces dir/name with data (E08-D052). dir is the directory itself,
// not a parent the name is allowed to escape. name is a relative path with no ".."
// element.
func Write(dir, name string, data []byte) error {
	path, err := within(dir, name)
	if err != nil {
		return err
	}
	// The temp file lives in the destination's own directory, so rename stays in
	// that directory and the fsync covers the directory entry (E08-D052).
	destDir := filepath.Dir(path)
	f, err := os.CreateTemp(destDir, ".vol-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	closed := false
	renamed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
		if !renamed {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	closed = true
	if beforeRename != nil {
		beforeRename()
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	renamed = true
	d, err := os.Open(destDir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Read returns the file name from volumeDir when it is there, otherwise from
// inputDir (E09-D004, E08-D051). ok is false when both are missing. A missing directory
// is a missing file. An empty directory is not the current directory.
func Read(volumeDir, inputDir, name string) (data []byte, ok bool, err error) {
	if err := checkName(name); err != nil {
		return nil, false, err
	}
	data, ok, err = readIn(volumeDir, name)
	if err != nil || ok {
		return data, ok, err
	}
	return readIn(inputDir, name)
}

func readIn(dir, name string) ([]byte, bool, error) {
	if dir == "" {
		return nil, false, nil
	}
	path, err := within(dir, name)
	if err != nil {
		return nil, false, err
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func within(dir, name string) (string, error) {
	if dir == "" {
		return "", errors.New("volume: empty directory")
	}
	if err := checkName(name); err != nil {
		return "", err
	}
	root := filepath.Clean(dir)
	path := filepath.Join(root, name)
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errName
	}
	return path, nil
}

func checkName(name string) error {
	if name == "" || name == "." || filepath.IsAbs(name) || filepath.Clean(name) != name {
		return errName
	}
	for _, p := range strings.Split(name, string(filepath.Separator)) {
		if p == "" || p == ".." {
			return errName
		}
	}
	return nil
}
