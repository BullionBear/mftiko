package shim

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

// Running tells a process from a later one with the same pid, and a zombie from a live process (E1-16).
func TestRunning(t *testing.T) {
	self := os.Getpid()
	st, err := procStartTime(self)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := Running(self, st); !ok || err != nil {
		t.Fatalf("this process: %v %v, want running", ok, err)
	}
	if ok, err := Running(self, st+1); ok || err != nil {
		t.Fatalf("this pid, another start time (a reused pid): %v %v, want gone", ok, err)
	}
	if _, err := Running(0, 0); err == nil {
		t.Fatal("pid 0 is no process")
	}

	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	zst, err := procStartTime(pid)
	if err != nil { // it may already be a zombie, which still has its stat
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for { // not waited for: a zombie once it exits
		state, _, err := procStat(pid)
		if err != nil {
			t.Fatal(err)
		}
		if state == 'Z' {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d still in state %c", pid, state)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ok, err := Running(pid, zst); ok || err != nil {
		t.Fatalf("a zombie: %v %v, want gone", ok, err)
	}
	cmd.Wait() //nolint:errcheck
	if ok, err := Running(pid, zst); ok || err != nil {
		t.Fatalf("a reaped process: %v %v, want gone", ok, err)
	}
}
