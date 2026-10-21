package artifact

import (
	"io/fs"
	"os"
	"path/filepath"
)

// MakeReadOnly makes files 0444 and directories 0555 (D975). Symlinks are left alone
// so a target outside this tree is not changed. Calling it again is safe.
func MakeReadOnly(root string) error {
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

// RemoveWritable makes a tree deletable and removes it (D975). Directories become
// 0700 and files 0600 first, so a read-only tree can still be removed. A missing
// root is not an error.
func RemoveWritable(root string) error {
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
