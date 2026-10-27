package artifact

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"oras.land/oras-go/v2/registry/remote"
)

// ErrUploadLimit is an upload over the size or entry limit (E15-D005). Nothing was pushed.
var ErrUploadLimit = errors.New("artifact: upload limit")

// UnsupportedError lists the entries an upload cannot carry (E15-D004): unpack would reject
// them (E09-D051). Nothing was packed or pushed.
type UnsupportedError struct {
	Entries []Skipped
}

func (e *UnsupportedError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "artifact: %d entries cannot be uploaded", len(e.Entries))
	for _, s := range e.Entries {
		fmt.Fprintf(&b, "\n  %s: %s", s.Path, s.Reason)
	}
	return b.String()
}

// Symlinks reports whether any listed entry is a symlink, which following symlinks would change.
func (e *UnsupportedError) Symlinks() bool {
	for _, s := range e.Entries {
		if strings.HasPrefix(s.Reason, "symlink: ") {
			return true
		}
	}
	return false
}

// DataUpload is an operator upload: one local directory packed into a data artifact and pushed
// (E15-D001–E15-D008). Destination is the same as a strategy push: Docker's config.json for
// credentials and one PlainHTTP switch (E09-D040, E10-D126). An empty Tag pushes by digest.
type DataUpload struct {
	Root        string
	Destination Destination
	// FollowSymlinks packs what each symlink points at instead of the link (E15-D004).
	FollowSymlinks bool
	// MaxBytes limits the config plus the compressed layer; zero means DefaultMaxBytes (E09-D013).
	MaxBytes int64
	// Limits is the unpack limit the STS applies (E09-D053, E15-D012); zero fields mean the defaults.
	// mftiko-pack data sets it from -max-unpacked-bytes and -max-unpacked-files (E15-D015).
	Limits UnpackLimits
	// TempDir holds the packed layer until the push ends; empty means os.TempDir().
	TempDir string
}

// UploadedData is a data artifact that landed in the registry. Size is the compressed layer.
// Unpacked and Entries are what unpack will count (E09-D053).
type UploadedData struct {
	Digest   string
	Size     int64
	Unpacked int64
	Entries  int
}

// UploadData packs req.Root and pushes it (E15-D001–E15-D008). The config has no source.
// Every limit is checked before the first blob is sent, and an entry unpack would reject is an
// UnsupportedError rather than a skip.
func UploadData(ctx context.Context, req DataUpload) (UploadedData, error) {
	if req.Root == "" {
		return UploadedData{}, badRequest("directory is required")
	}
	if _, _, err := SplitRepository(req.Destination.Repository); err != nil {
		return UploadedData{}, err
	}
	limits, err := normalizeLimits(req.Limits)
	if err != nil {
		return UploadedData{}, err
	}
	maxBytes := req.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if maxBytes < 0 {
		return UploadedData{}, badRequest("max bytes %d", maxBytes)
	}
	tree, err := walkData(req.Root, walkOptions{
		follow: req.FollowSymlinks, maxFiles: limits.MaxFiles, maxBytes: limits.MaxBytes,
	})
	if err != nil {
		return UploadedData{}, err
	}
	if len(tree.skipped) > 0 {
		return UploadedData{}, &UnsupportedError{Entries: tree.skipped}
	}
	tmp, err := os.CreateTemp(req.TempDir, "mftiko-data-*.tar.gz")
	if err != nil {
		return UploadedData{}, err
	}
	path := tmp.Name()
	defer os.Remove(path)
	if err := tmp.Close(); err != nil {
		return UploadedData{}, err
	}
	if err := writeDataLayer(path, tree.entries); err != nil {
		return UploadedData{}, err
	}
	repo, err := remote.NewRepository(req.Destination.Repository)
	if err != nil {
		return UploadedData{}, badRequest("repository: %v", err)
	}
	repo.PlainHTTP = req.Destination.PlainHTTP
	if repo.Client, err = dockerClient(); err != nil {
		return UploadedData{}, err
	}
	pushed, err := pushDataFile(ctx, repo, dataFile{path: path, ref: req.Destination.Tag, maxBytes: maxBytes})
	if err != nil {
		return UploadedData{}, err
	}
	return UploadedData{
		Digest: pushed.Digest, Size: pushed.Size, Unpacked: tree.bytes, Entries: len(tree.entries),
	}, nil
}
