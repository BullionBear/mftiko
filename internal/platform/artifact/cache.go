package artifact

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"
)

// layerFlight is one in-progress download of a layer. Waiters share its result (D930).
type layerFlight struct {
	done chan struct{}
	path string
	err  error
}

var (
	flightsMu sync.Mutex
	flights   = map[string]*layerFlight{}
)

// layerWait, when set, runs as a pull joins a download that is already in flight. Tests use it.
// layerCreated, when set, runs with the temporary file's name before its bytes are read. Tests use it.
// Neither is safe for t.Parallel: they are process-wide.
var (
	layerWait    func()
	layerCreated func(name string)
)

// layersDir is ${DATA}/layers.
func layersDir(dataDir string) string { return filepath.Join(dataDir, "layers") }

// cleanTemps removes leftover download files from a previous process (D930). A name that is a
// verified digest is left alone: those are written only after the hash matches.
func cleanTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// cached reports a layer reused from disk. The file name exists only after verification, so the check is
// the size (D931): the bytes are not hashed again.
func cached(dir string, desc ocispec.Descriptor) (string, bool) {
	p := filepath.Join(dir, desc.Digest.String())
	st, err := os.Stat(p)
	if err != nil || !st.Mode().IsRegular() || st.Size() != desc.Size {
		return "", false
	}
	return p, true
}

// ensureLayer returns the cache path of desc, downloading it once per process when it is missing (D930).
// The download runs under the first caller's context: if that caller cancels, the waiters see the same error
// and the caller decides whether to retry (D928).
func ensureLayer(ctx context.Context, repo *remote.Repository, dir string, desc ocispec.Descriptor) (string, error) {
	if p, ok := cached(dir, desc); ok {
		return p, nil
	}
	key := dir + "\x00" + desc.Digest.String()
	flightsMu.Lock()
	if p, ok := cached(dir, desc); ok {
		flightsMu.Unlock()
		return p, nil
	}
	if f, ok := flights[key]; ok {
		flightsMu.Unlock()
		if layerWait != nil {
			layerWait()
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-f.done:
			if f.err != nil {
				return "", f.err
			}
			if p, ok := cached(dir, desc); ok {
				return p, nil
			}
			return "", permanent(ReasonDigestMismatch, fmt.Errorf("%s: cached layer is %d bytes in the manifest and a different size on disk", desc.Digest, desc.Size))
		}
	}
	f := &layerFlight{done: make(chan struct{})}
	flights[key] = f
	flightsMu.Unlock()

	path, err := downloadLayer(ctx, repo, dir, desc)
	f.path, f.err = path, err
	close(f.done)
	flightsMu.Lock()
	delete(flights, key)
	flightsMu.Unlock()
	return path, err
}

// downloadLayer writes the blob to a temporary file, hashing as it reads, and renames it to the digest only
// when the hash and the size both match (D930, D932). A failure deletes the temporary file.
func downloadLayer(ctx context.Context, repo *remote.Repository, dir string, desc ocispec.Descriptor) (string, error) {
	rc, err := repo.Blobs().Fetch(ctx, desc)
	if err != nil {
		return "", classify(err)
	}
	defer rc.Close()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	name := tmp.Name()
	if layerCreated != nil {
		layerCreated(name)
	}
	keep := false
	defer func() {
		if !keep {
			tmp.Close()
			os.Remove(name)
		}
	}()

	got, n, err := hashReader(tmp, rc, desc.Size)
	if err != nil {
		return "", err
	}
	if n > desc.Size {
		return "", permanent(ReasonTooLarge, fmt.Errorf("%s: read %d bytes, manifest says %d", desc.Digest, n, desc.Size))
	}
	if n != desc.Size || got != desc.Digest {
		return "", permanent(ReasonDigestMismatch, fmt.Errorf("%s: got %s, %d bytes", desc.Digest, got, n))
	}
	if err := tmp.Sync(); err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	if err := tmp.Close(); err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	final := filepath.Join(dir, desc.Digest.String())
	if err := os.Rename(name, final); err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	keep = true
	if err := syncDir(dir); err != nil {
		return "", temporary(ReasonUnavailable, err)
	}
	return final, nil
}

// hashReader copies r to w up to one byte past limit, hashing with sha256. n > limit means the caller must
// stop: the blob is bigger than it was declared (D932).
func hashReader(w io.Writer, r io.Reader, limit int64) (digest.Digest, int64, error) {
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, h), io.LimitReader(r, limit+1))
	if err != nil {
		return "", n, classify(err)
	}
	return digest.NewDigestFromBytes(digest.SHA256, h.Sum(nil)), n, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	cErr := d.Close()
	if err != nil {
		return err
	}
	return cErr
}
