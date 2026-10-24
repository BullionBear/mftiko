package artifact

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Skipped is one volume entry PackData left out of the layer (E09-D080). The push still proceeds.
type Skipped struct {
	Path   string
	Reason string
}

type packEntry struct {
	name string
	mode int64
	typ  byte
	link string
	path string
	size int64
}

// DataVolumeEmpty reports whether root has no regular file and no symlink (E09-D081).
// A missing directory is empty. Directories, sockets, devices and fifos are not content.
// A regular file is content even when unpack would reject its name.
func DataVolumeEmpty(root string) (bool, error) {
	st, err := os.Lstat(root)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !st.IsDir() {
		return false, fmt.Errorf("artifact: %s is not a directory", root)
	}
	empty := true
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		mode := d.Type()
		if mode.IsRegular() || mode&os.ModeSymlink != 0 {
			empty = false
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return empty, nil
}

// PackData writes a reproducible data layer for the tree at root (E09-D080, E09-D051).
// Paths are sorted. Members are mtime 0, uid 0, gid 0. Files keep only the execute
// bit (0644 or 0755); directories are 0755. Symlinks that unpack would accept are
// stored as the original relative text. Anything else is skipped, not a failure.
// Hard links are written as independent regular files. dest is created or truncated.
func PackData(root, dest string) ([]Skipped, error) {
	st, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("artifact: %s is not a directory", root)
	}
	var skipped []Skipped
	var entries []packEntry
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		name, err := safeRel(rel)
		if err != nil {
			skipped = append(skipped, Skipped{Path: rel, Reason: err.Error()})
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if name == "." {
			return nil
		}
		mode := d.Type()
		switch {
		case mode&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if _, err := symlinkTarget(target); err != nil {
				skipped = append(skipped, Skipped{Path: rel, Reason: "symlink: " + err.Error()})
				return nil
			}
			entries = append(entries, packEntry{name: name, mode: 0o777, typ: tar.TypeSymlink, link: target})
		case mode.IsRegular():
			info, err := d.Info()
			if err != nil {
				return err
			}
			perm := int64(0o644)
			if info.Mode().Perm()&0o111 != 0 {
				perm = 0o755
			}
			entries = append(entries, packEntry{
				name: name, mode: perm, typ: tar.TypeReg, path: path, size: info.Size(),
			})
		case d.IsDir():
			entries = append(entries, packEntry{name: name, mode: 0o755, typ: tar.TypeDir})
		default:
			skipped = append(skipped, Skipped{Path: rel, Reason: skipKind(mode)})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b packEntry) int { return strings.Compare(a.name, b.name) })
	slices.SortFunc(skipped, func(a, b Skipped) int { return strings.Compare(a.Path, b.Path) })
	if err := writeDataLayer(dest, entries); err != nil {
		return nil, err
	}
	return skipped, nil
}

func skipKind(mode fs.FileMode) string {
	switch {
	case mode&os.ModeSocket != 0:
		return "socket"
	case mode&os.ModeDevice != 0:
		return "device"
	case mode&os.ModeNamedPipe != 0:
		return "fifo"
	default:
		return "unsupported"
	}
}

// PackDataFile packs root into a new file under ${DATA}/layers (E09-D079). The name uses
// the unfinished-download prefix, so GC and the next Open ignore it. The caller removes it.
func (c *Client) PackDataFile(root string) (string, []Skipped, error) {
	if err := os.MkdirAll(c.layers, 0o755); err != nil {
		return "", nil, err
	}
	f, err := os.CreateTemp(c.layers, tempPrefix+"*")
	if err != nil {
		return "", nil, err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", nil, err
	}
	skipped, err := PackData(root, path)
	if err != nil {
		os.Remove(path)
		return "", nil, err
	}
	return path, skipped, nil
}

func writeDataLayer(dest string, entries []packEntry) error {
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	err = writeDataGzip(out, entries)
	syncErr := out.Sync()
	closeErr := out.Close()
	if err != nil {
		return err
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func writeDataGzip(out *os.File, entries []packEntry) error {
	zw, err := gzip.NewWriterLevel(out, gzip.DefaultCompression)
	if err != nil {
		return err
	}
	zw.Name = ""
	zw.ModTime = time.Time{}
	zw.OS = 255
	tw := tar.NewWriter(zw)
	for _, e := range entries {
		hdr := &tar.Header{
			Name:     e.name,
			Mode:     e.mode,
			Uid:      0,
			Gid:      0,
			Size:     e.size,
			ModTime:  time.Unix(0, 0),
			Typeflag: e.typ,
			Linkname: e.link,
			Format:   tar.FormatPAX,
		}
		if e.typ != tar.TypeReg {
			hdr.Size = 0
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if e.typ != tar.TypeReg {
			continue
		}
		if err := copyPackFile(tw, e.path, e.size); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return zw.Close()
}

func copyPackFile(tw *tar.Writer, path string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	n, err := io.Copy(tw, f)
	cErr := f.Close()
	if err != nil {
		return err
	}
	if cErr != nil {
		return cErr
	}
	if n != size {
		return fmt.Errorf("artifact: %s changed while packing", path)
	}
	return nil
}
