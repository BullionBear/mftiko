//go:build !linux

package artifact

import "testing"

func TestRunningDigestsNotLinux(t *testing.T) {
	if _, err := RunningDigests(t.TempDir()); err == nil {
		t.Fatal("running digests without /proc")
	}
}
