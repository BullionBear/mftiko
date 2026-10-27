package artifact

import (
	"archive/tar"
	"compress/gzip"
	"errors"
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
	tree, err := walkData(root, walkOptions{})
	if err != nil {
		return nil, err
	}
	if err := writeDataLayer(dest, tree.entries); err != nil {
		return nil, err
	}
	return tree.skipped, nil
}

// dataTree is what one walk of a directory would put in a data layer.
// bytes is the sum of the regular file sizes, the number unpack checks (E09-D053).
type dataTree struct {
	entries []packEntry
	skipped []Skipped
	bytes   int64
}

// walkOptions changes a walk for an upload (E15-D004, E15-D005). The zero value is the
// state_out walk: symlinks are stored, and there is no limit.
type walkOptions struct {
	follow   bool  // store what each symlink points at instead of the link
	maxFiles int   // stop once the entries pass this; zero is no limit
	maxBytes int64 // stop once the file sizes pass this; zero is no limit
}

type dataWalker struct {
	opt  walkOptions
	tree dataTree
}

// walkData lists the layer entries for root, sorted by name, and the entries a layer
// cannot carry. With follow set, root and every symlink are resolved, and a directory
// reached again through a symlink below itself is skipped as a loop.
func walkData(root string, opt walkOptions) (*dataTree, error) {
	stat := os.Lstat
	if opt.follow {
		stat = os.Stat
	}
	st, err := stat(root)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("artifact: %s is not a directory", root)
	}
	w := &dataWalker{opt: opt}
	if err := w.dir(root, "", []fs.FileInfo{st}); err != nil {
		return nil, err
	}
	slices.SortFunc(w.tree.entries, func(a, b packEntry) int { return strings.Compare(a.name, b.name) })
	slices.SortFunc(w.tree.skipped, func(a, b Skipped) int { return strings.Compare(a.Path, b.Path) })
	return &w.tree, nil
}

// dir visits the children of the directory at abs, whose layer path is rel ("" for the root).
// ancestors are the directories from the root down to abs; only a follow walk reads them.
func (w *dataWalker) dir(abs, rel string, ancestors []fs.FileInfo) error {
	children, err := os.ReadDir(abs)
	if err != nil {
		return err
	}
	for _, d := range children {
		childRel := d.Name()
		if rel != "" {
			childRel = rel + "/" + d.Name()
		}
		if err := w.visit(filepath.Join(abs, d.Name()), childRel, d, ancestors); err != nil {
			return err
		}
	}
	return nil
}

func (w *dataWalker) visit(abs, rel string, d fs.DirEntry, ancestors []fs.FileInfo) error {
	name, err := safeRel(rel)
	if err != nil {
		w.skip(rel, err.Error())
		return nil
	}
	mode := d.Type()
	switch {
	case mode&os.ModeSymlink != 0:
		if w.opt.follow {
			return w.follow(abs, rel, name, ancestors)
		}
		target, err := os.Readlink(abs)
		if err != nil {
			return err
		}
		if _, err := symlinkTarget(target); err != nil {
			w.skip(rel, "symlink: "+err.Error())
			return nil
		}
		return w.add(packEntry{name: name, mode: 0o777, typ: tar.TypeSymlink, link: target})
	case mode.IsRegular():
		info, err := d.Info()
		if err != nil {
			return err
		}
		return w.addFile(abs, name, info)
	case d.IsDir():
		var info fs.FileInfo
		if w.opt.follow {
			if info, err = d.Info(); err != nil {
				return err
			}
		}
		return w.addDir(abs, rel, name, info, ancestors)
	default:
		w.skip(rel, skipKind(mode))
		return nil
	}
}

// follow stores what the symlink at abs resolves to (E15-D004). A missing target, a chain
// that never ends, a directory that is one of its own ancestors, and a target that is not
// a file or a directory are skipped with the reason.
func (w *dataWalker) follow(abs, rel, name string, ancestors []fs.FileInfo) error {
	info, err := os.Stat(abs)
	if errors.Is(err, fs.ErrNotExist) {
		w.skip(rel, "symlink: target does not exist")
		return nil
	}
	if err != nil {
		w.skip(rel, "symlink: "+err.Error())
		return nil
	}
	switch {
	case info.Mode().IsRegular():
		return w.addFile(abs, name, info)
	case info.IsDir():
		for _, a := range ancestors {
			if os.SameFile(a, info) {
				w.skip(rel, "symlink: loop")
				return nil
			}
		}
		return w.addDir(abs, rel, name, info, ancestors)
	default:
		w.skip(rel, "symlink: "+skipKind(info.Mode()))
		return nil
	}
}

func (w *dataWalker) addFile(path, name string, info fs.FileInfo) error {
	perm := int64(0o644)
	if info.Mode().Perm()&0o111 != 0 {
		perm = 0o755
	}
	size := info.Size()
	if w.opt.maxBytes > 0 && (size > w.opt.maxBytes || w.tree.bytes > w.opt.maxBytes-size) {
		return fmt.Errorf("%w: unpacked size exceeds %d bytes", ErrUploadLimit, w.opt.maxBytes)
	}
	w.tree.bytes += size
	return w.add(packEntry{name: name, mode: perm, typ: tar.TypeReg, path: path, size: size})
}

func (w *dataWalker) addDir(abs, rel, name string, info fs.FileInfo, ancestors []fs.FileInfo) error {
	if err := w.add(packEntry{name: name, mode: 0o755, typ: tar.TypeDir}); err != nil {
		return err
	}
	if w.opt.follow {
		ancestors = append(ancestors[:len(ancestors):len(ancestors)], info)
	}
	return w.dir(abs, rel, ancestors)
}

func (w *dataWalker) add(e packEntry) error {
	w.tree.entries = append(w.tree.entries, e)
	if w.opt.maxFiles > 0 && len(w.tree.entries) > w.opt.maxFiles {
		return fmt.Errorf("%w: more than %d entries", ErrUploadLimit, w.opt.maxFiles)
	}
	return nil
}

func (w *dataWalker) skip(rel, reason string) {
	w.tree.skipped = append(w.tree.skipped, Skipped{Path: rel, Reason: reason})
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
