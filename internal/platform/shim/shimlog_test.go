package shim

import (
	"os"
	"path/filepath"
	"testing"
)

func TestShimLogLimits(t *testing.T) {
	if ShimLogMaxBytes != 10<<20 || ShimLogKeep != 3 {
		t.Fatalf("shim log limits %d bytes, keep %d", ShimLogMaxBytes, ShimLogKeep)
	}
}

func TestOpenShimLogMissing(t *testing.T) {
	dir := t.TempDir()
	sl, err := openShimLog(dir, "md", "md/a")
	if err != nil || sl != nil {
		t.Fatalf("missing file: %v %v", sl, err)
	}
	if _, err := os.Lstat(filepath.Join(LogDir(dir, "md"), workerHash("md/a")+".shim.log")); !os.IsNotExist(err) {
		t.Fatalf("created a file: %v", err)
	}
}
