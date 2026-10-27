package artifact

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestUploadDataRejectsFifoAndSocket(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	reg := newMemRegistry()
	defer reg.Close()
	root := uploadTree(t)
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A socket path must fit in sun_path; t.TempDir can be too long, so bind in a short dir and move it.
	short, err := os.MkdirTemp("", "s")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(short)
	ln, err := net.Listen("unix", filepath.Join(short, "s"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := os.Rename(filepath.Join(short, "s"), filepath.Join(root, "sock")); err != nil {
		t.Fatal(err)
	}
	for _, follow := range []bool{false, true} {
		req := uploadTo(t, reg, root, "")
		req.FollowSymlinks = follow
		_, err := UploadData(context.Background(), req)
		var unsupported *UnsupportedError
		if !errors.As(err, &unsupported) || len(unsupported.Entries) != 2 ||
			unsupported.Entries[0] != (Skipped{Path: "pipe", Reason: "fifo"}) ||
			unsupported.Entries[1] != (Skipped{Path: "sock", Reason: "socket"}) {
			t.Fatalf("follow %v: err %v", follow, err)
		}
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.blobPuts != 0 {
		t.Fatalf("pushed %d blobs", reg.blobPuts)
	}
}
