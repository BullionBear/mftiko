package artifact

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/opencontainers/go-digest"
)

// DefaultMaxUnpackedBytes is the uncompressed size of one unpack when the deployment sets no limit (D953).
const DefaultMaxUnpackedBytes int64 = 4 << 30

// DefaultMaxFiles is the number of tar entries in one unpack when the deployment sets no limit (D953).
const DefaultMaxFiles = 100_000

// whiteoutPrefix is the OCI whiteout marker. A path component with this prefix is rejected (D951).
const whiteoutPrefix = ".wh."

// layersName is the file next to rootfs listing the layer digests that unpack used, one per line (D950).
// GC treats those lines as the rootfs's references (D956).
const layersName = "layers"

// UnpackLimits is the deployment cap on one unpack (D953). A zero field means the default.
type UnpackLimits struct {
	// MaxBytes is the total uncompressed size, the sum of every entry's declared size.
	MaxBytes int64
	// MaxFiles is the number of tar entries, including directories, symlinks and hardlinks.
	MaxFiles int
}

// Unpack extracts gzip-compressed tar layers, in order, into dest (D951–D953, D957).
// dest's parent must already exist and dest must not. Bytes land in a temporary directory
// beside dest and are renamed into place, so a failure leaves nothing at dest.
// #286 calls this for sessions/<id>/inputs/<name>/; the rejection rules are the same.
func Unpack(ctx context.Context, layers []string, dest string, limits UnpackLimits) error {
	return unpack(ctx, layers, dest, limits, false)
}

// UnpackReadOnly is Unpack for a session input: the tree is made read-only (files 0444,
// directories 0555, MakeReadOnly) before it is renamed into place, so dest never appears with
// the permissions of the extraction (D975, D996). The temporary directory is a sibling of dest,
// and renaming a directory within the same parent does not need write permission on it.
func UnpackReadOnly(ctx context.Context, layers []string, dest string, limits UnpackLimits) error {
	return unpack(ctx, layers, dest, limits, true)
}

func unpack(ctx context.Context, layers []string, dest string, limits UnpackLimits, readOnly bool) error {
	limits, err := normalizeLimits(limits)
	if err != nil {
		return err
	}
	if dest == "" {
		return badRequest("destination is required")
	}
	parent := filepath.Dir(dest)
	pst, err := os.Stat(parent)
	if err != nil || !pst.IsDir() {
		return badRequest("destination parent %q is not a directory", parent)
	}
	if _, err := os.Lstat(dest); err == nil {
		return badRequest("destination %q exists", dest)
	} else if !os.IsNotExist(err) {
		return temporary(ReasonUnavailable, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, tempPrefix)
	if err != nil {
		return temporary(ReasonUnavailable, err)
	}
	keep := false
	defer func() {
		if !keep {
			// RemoveWritable, not RemoveAll: a read-only tree cannot be emptied as it is.
			RemoveWritable(tmp)
		}
	}()
	if err := extractAll(ctx, tmp, layers, limits); err != nil {
		return err
	}
	if readOnly {
		if err := MakeReadOnly(tmp); err != nil {
			return temporary(ReasonUnavailable, err)
		}
	}
	if err := os.Rename(tmp, dest); err != nil {
		return temporary(ReasonUnavailable, err)
	}
	keep = true
	if err := syncDir(parent); err != nil {
		return temporary(ReasonUnavailable, err)
	}
	return nil
}

func normalizeLimits(l UnpackLimits) (UnpackLimits, error) {
	if l.MaxBytes < 0 || l.MaxFiles < 0 {
		return UnpackLimits{}, badRequest("unpack limits bytes %d files %d", l.MaxBytes, l.MaxFiles)
	}
	if l.MaxBytes == 0 {
		l.MaxBytes = DefaultMaxUnpackedBytes
	}
	if l.MaxFiles == 0 {
		l.MaxFiles = DefaultMaxFiles
	}
	return l, nil
}

func extractAll(ctx context.Context, dest string, layers []string, limits UnpackLimits) error {
	root, err := os.OpenRoot(dest)
	if err != nil {
		return temporary(ReasonUnavailable, err)
	}
	defer root.Close()
	b := &budget{maxFiles: limits.MaxFiles, maxBytes: limits.MaxBytes}
	for _, layer := range layers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := extractLayer(ctx, root, layer, b); err != nil {
			return err
		}
	}
	return nil
}

func extractLayer(ctx context.Context, root *os.Root, layer string, b *budget) (err error) {
	f, err := os.Open(layer)
	if err != nil {
		return temporary(ReasonUnavailable, err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return unsafe(fmt.Errorf("%s: %w", filepath.Base(layer), err))
	}
	defer func() {
		cErr := zr.Close()
		if err == nil && cErr != nil {
			err = unsafe(cErr)
		}
	}()
	tr := tar.NewReader(zr)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return unsafe(err)
		}
		if err := validateHeader(hdr); err != nil {
			return err
		}
		if err := b.consume(hdr.Size); err != nil {
			return err
		}
		if err := applyHeader(ctx, root, hdr, tr); err != nil {
			return err
		}
	}
}

// budget counts entries and declared sizes across every layer of one unpack (D953).
type budget struct {
	files    int
	bytes    int64
	maxFiles int
	maxBytes int64
}

func (b *budget) consume(size int64) error {
	b.files++
	if b.files > b.maxFiles {
		return unsafe(fmt.Errorf("%d entries, limit %d", b.files, b.maxFiles))
	}
	if size < 0 {
		return unsafe(fmt.Errorf("negative size %d", size))
	}
	if size > b.maxBytes || b.bytes > b.maxBytes-size {
		return unsafe(fmt.Errorf("unpacked size exceeds %d", b.maxBytes))
	}
	b.bytes += size
	return nil
}

func validateHeader(hdr *tar.Header) error {
	if hdr.Mode&0o6000 != 0 {
		return unsafe(fmt.Errorf("%s: setuid or setgid", hdr.Name))
	}
	switch hdr.Typeflag {
	case tar.TypeReg, tar.TypeRegA, tar.TypeDir, tar.TypeSymlink, tar.TypeLink:
	default:
		return unsafe(fmt.Errorf("%s: type %q", hdr.Name, hdr.Typeflag))
	}
	name, err := safeRel(hdr.Name)
	if err != nil {
		return unsafe(fmt.Errorf("%s: %w", hdr.Name, err))
	}
	if name == "." && hdr.Typeflag != tar.TypeDir {
		return unsafe(fmt.Errorf("%s: not a directory", hdr.Name))
	}
	switch hdr.Typeflag {
	case tar.TypeSymlink:
		if _, err := symlinkTarget(hdr.Linkname); err != nil {
			return unsafe(fmt.Errorf("%s: symlink target %q: %w", hdr.Name, hdr.Linkname, err))
		}
	case tar.TypeLink:
		target, err := safeRel(hdr.Linkname)
		if err != nil || target == "." {
			return unsafe(fmt.Errorf("%s: hardlink target %q", hdr.Name, hdr.Linkname))
		}
	}
	return nil
}

func applyHeader(ctx context.Context, root *os.Root, hdr *tar.Header, r io.Reader) error {
	name, err := safeRel(hdr.Name)
	if err != nil {
		return unsafe(fmt.Errorf("%s: %w", hdr.Name, err))
	}
	if name == "." {
		return nil
	}
	switch hdr.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		return writeFile(ctx, root, name, hdr, r)
	case tar.TypeDir:
		return mapRootErr(root.MkdirAll(name, modePerm(hdr, 0o755)))
	case tar.TypeSymlink:
		if err := mkdirParent(root, name); err != nil {
			return err
		}
		if err := removeExisting(root, name); err != nil {
			return err
		}
		// The target string is stored as it was checked. os.Root does not interpret it (D952).
		return mapRootErr(root.Symlink(hdr.Linkname, name))
	case tar.TypeLink:
		return writeHardlink(root, name, hdr.Linkname)
	default:
		return unsafe(fmt.Errorf("%s: type %q", hdr.Name, hdr.Typeflag))
	}
}

func writeFile(ctx context.Context, root *os.Root, name string, hdr *tar.Header, r io.Reader) error {
	if err := mkdirParent(root, name); err != nil {
		return err
	}
	// A symlink at the final component would be followed by OpenFile. Drop it, and drop a directory
	// occupying the name, so the new file is created here (D952). Parents stay as they are: os.Root
	// refuses a parent symlink that leaves the root.
	fi, err := root.Lstat(name)
	if err == nil && !fi.Mode().IsRegular() {
		if err := removeExisting(root, name); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return mapRootErr(err)
	}
	f, err := root.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, modePerm(hdr, 0o644))
	if err != nil {
		return mapRootErr(err)
	}
	n, copyErr := io.Copy(f, &ctxReader{ctx: ctx, r: io.LimitReader(r, hdr.Size)})
	closeErr := f.Close()
	if copyErr != nil {
		if errors.Is(copyErr, context.Canceled) || errors.Is(copyErr, context.DeadlineExceeded) {
			return copyErr
		}
		return unsafe(fmt.Errorf("%s: %w", name, copyErr))
	}
	if closeErr != nil {
		return temporary(ReasonUnavailable, closeErr)
	}
	if n != hdr.Size {
		return unsafe(fmt.Errorf("%s: read %d bytes, header says %d", name, n, hdr.Size))
	}
	return nil
}

func writeHardlink(root *os.Root, name, linkname string) error {
	target, err := safeRel(linkname)
	if err != nil || target == "." {
		return unsafe(fmt.Errorf("%s: hardlink target %q", name, linkname))
	}
	// "前面已出現檔案": the target is already in this rootfs, and it is a regular file.
	// Lstat does not follow a final symlink, so a hardlink to a symlink is refused.
	fi, err := root.Lstat(target)
	if err != nil || !fi.Mode().IsRegular() {
		return unsafe(fmt.Errorf("%s: hardlink target %q is not a file already in the rootfs", name, linkname))
	}
	if err := mkdirParent(root, name); err != nil {
		return err
	}
	if err := removeExisting(root, name); err != nil {
		return err
	}
	return mapRootErr(root.Link(target, name))
}

func mkdirParent(root *os.Root, name string) error {
	dir := path.Dir(name)
	if dir == "." || dir == "" {
		return nil
	}
	return mapRootErr(root.MkdirAll(dir, 0o755))
}

func removeExisting(root *os.Root, name string) error {
	fi, err := root.Lstat(name)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return mapRootErr(err)
	}
	if fi.IsDir() {
		return mapRootErr(root.RemoveAll(name))
	}
	return mapRootErr(root.Remove(name))
}

func modePerm(hdr *tar.Header, fallback os.FileMode) os.FileMode {
	perm := os.FileMode(hdr.Mode) & 0o777
	if perm == 0 {
		return fallback
	}
	return perm
}

// safeRel is the entry's path inside the rootfs. Absolute paths, "..", and OCI whiteouts are rejected
// (D951). os.Root is what stops a write from leaving the directory (D952); this check is the rejection
// itself, so the entry fails the unpack instead of being skipped.
func safeRel(name string) (string, error) {
	if name == "" || strings.IndexByte(name, 0) >= 0 {
		return "", errBadPath
	}
	if strings.HasPrefix(name, "/") || filepath.IsAbs(name) {
		return "", fmt.Errorf("%w: absolute", errBadPath)
	}
	parts := strings.Split(name, "/")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" || p == "." {
			continue
		}
		if err := badComponent(p); err != nil {
			return "", err
		}
		clean = append(clean, p)
	}
	if len(clean) == 0 {
		return ".", nil
	}
	return strings.Join(clean, "/"), nil
}

// symlinkTarget accepts a relative link text that cannot leave the rootfs. ".." is rejected even when
// the cleaned path would stay inside. An absolute target is rejected even when it points at a file in
// the rootfs (D951). The text is not rewritten.
func symlinkTarget(target string) (string, error) {
	if target == "" || strings.IndexByte(target, 0) >= 0 {
		return "", errBadPath
	}
	if strings.HasPrefix(target, "/") || filepath.IsAbs(target) {
		return "", fmt.Errorf("%w: absolute", errBadPath)
	}
	for _, p := range strings.Split(target, "/") {
		if p == "" || p == "." {
			continue
		}
		if err := badComponent(p); err != nil {
			return "", err
		}
	}
	return target, nil
}

func badComponent(p string) error {
	if p == ".." {
		return fmt.Errorf("%w: ..", errBadPath)
	}
	if strings.HasPrefix(p, whiteoutPrefix) {
		return fmt.Errorf("%w: whiteout", errBadPath)
	}
	return nil
}

var errBadPath = errors.New("path not allowed")

func unsafe(err error) error { return permanent(ReasonUnsafeLayer, err) }

func mapRootErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, syscall.ENAMETOOLONG) || errors.Is(err, syscall.EINVAL) || errors.Is(err, os.ErrInvalid) {
		return unsafe(err)
	}
	var pe *os.PathError
	if errors.As(err, &pe) && pe.Err != nil {
		msg := pe.Err.Error()
		if strings.Contains(msg, "escapes") || strings.Contains(msg, "unsupported file mode") {
			return unsafe(err)
		}
	}
	return temporary(ReasonUnavailable, err)
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// unpackFlight is one in-progress unpack of an image digest. Waiters share its result (D955, D930).
type unpackFlight struct {
	done   chan struct{}
	rootfs string
	err    error
}

var (
	unpackMu sync.Mutex
	unpacks  = map[string]*unpackFlight{}
)

// unpackWait, when set, runs as an unpack joins one that is already in flight. Tests use it.
// unpackEnter, when set, runs inside the leader before it writes. Tests use it.
// Neither is safe for t.Parallel: they are process-wide.
var (
	unpackWait  func()
	unpackEnter func()
)

// UnpackImage extracts layers into ${DATA}/artifacts/<digest>/rootfs (D950, D955).
// The same digest is unpacked once per process. A rootfs directory that is already there is complete
// and is reused without reading the layers again. The layer digests are recorded next to the rootfs
// for GC. A rejection removes the temporary directory and does not delete the cached layers (D954).
func (c *Client) UnpackImage(ctx context.Context, image string, layers []Layer, limits UnpackLimits) (string, error) {
	if !digestOK(image) {
		return "", permanent(ReasonDigestMismatch, fmt.Errorf("digest %q is not sha256", image))
	}
	if _, err := normalizeLimits(limits); err != nil {
		return "", err
	}
	for _, layer := range layers {
		if !digestOK(layer.Digest) {
			return "", permanent(ReasonDigestMismatch, fmt.Errorf("layer digest %q is not sha256", layer.Digest))
		}
		if layer.Path == "" {
			return "", badRequest("layer path is required")
		}
	}
	rootfs := c.rootfsPath(image)
	key := c.data + "\x00" + image
	for {
		if rootfsReady(rootfs) {
			return rootfs, nil
		}
		unpackMu.Lock()
		if rootfsReady(rootfs) {
			unpackMu.Unlock()
			return rootfs, nil
		}
		if f, ok := unpacks[key]; ok {
			unpackMu.Unlock()
			if unpackWait != nil {
				unpackWait()
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-f.done:
				if f.err != nil {
					if ctx.Err() == nil && (errors.Is(f.err, context.Canceled) || errors.Is(f.err, context.DeadlineExceeded)) {
						continue
					}
					return "", f.err
				}
				if rootfsReady(rootfs) {
					return rootfs, nil
				}
				return "", unsafe(errors.New("rootfs missing after unpack"))
			}
		}
		f := &unpackFlight{done: make(chan struct{})}
		unpacks[key] = f
		unpackMu.Unlock()

		path, err := c.unpackImage(ctx, image, layers, limits)
		f.rootfs, f.err = path, err
		unpackMu.Lock()
		delete(unpacks, key)
		unpackMu.Unlock()
		close(f.done)
		return path, err
	}
}

func (c *Client) unpackImage(ctx context.Context, image string, layers []Layer, limits UnpackLimits) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if unpackEnter != nil {
		unpackEnter()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	dir := filepath.Join(c.data, "artifacts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	final := filepath.Join(dir, image)
	rootfs := filepath.Join(final, "rootfs")
	if rootfsReady(rootfs) {
		return rootfs, nil
	}
	tmp, err := os.MkdirTemp(dir, tempPrefix)
	if err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	keep := false
	defer func() {
		if !keep {
			os.RemoveAll(tmp)
		}
	}()
	paths := make([]string, len(layers))
	var list strings.Builder
	for i, layer := range layers {
		paths[i] = layer.Path
		list.WriteString(layer.Digest)
		list.WriteByte('\n')
	}
	if err := Unpack(ctx, paths, filepath.Join(tmp, "rootfs"), limits); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(tmp, layersName), []byte(list.String()), 0o644); err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	// A digest directory without a rootfs is not complete (D955). Replace it. A directory that gained
	// a rootfs while this unpack ran is the one to use.
	if rootfsReady(rootfs) {
		return rootfs, nil
	}
	if _, err := os.Lstat(final); err == nil {
		if err := os.RemoveAll(final); err != nil {
			return "", temporary(ReasonUnavailable, err)
		}
	}
	if err := os.Rename(tmp, final); err != nil {
		if rootfsReady(rootfs) {
			return rootfs, nil
		}
		return "", temporary(ReasonUnavailable, err)
	}
	keep = true
	if err := syncDir(dir); err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	return rootfs, nil
}

func (c *Client) rootfsPath(image string) string {
	return filepath.Join(c.data, "artifacts", image, "rootfs")
}

func rootfsReady(path string) bool {
	st, err := os.Lstat(path)
	return err == nil && st.IsDir()
}

func digestOK(s string) bool {
	d, err := digest.Parse(s)
	return err == nil && d.Algorithm() == digest.SHA256 && d.String() == s
}
