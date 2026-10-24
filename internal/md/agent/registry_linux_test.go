package agent

import (
	"context"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent/latest"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/artifact/artifacttest"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// selfImage is the test binary, which is also the fake conn worker, with tail appended so that each tail is a
// different image digest. Bytes after the ELF sections do not change what runs.
func selfImage(t *testing.T, tail string) []byte {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(self)
	if err != nil {
		t.Fatal(err)
	}
	return append(b, tail...)
}

func registrySource(t *testing.T, e *env, reg *artifacttest.Registry) *latest.Source {
	t.Helper()
	src, err := latest.New(latest.Config{
		Registry: artifact.Registry{Address: reg.Addr(), PlainHTTP: true}, DataDir: t.TempDir(),
		Repository: latest.RepoMDConn, Kind: artifact.KindMDConn, Poll: 100 * time.Millisecond, Log: e.cfg.Log,
	})
	if err != nil {
		t.Fatal(err)
	}
	return src
}

// exe is the image digest a process runs from: ${DATA}/artifacts/<digest>/rootfs/bin/worker.
func exe(t *testing.T, pid int) string {
	t.Helper()
	p, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func hexOf(dig string) string { return strings.TrimPrefix(dig, "sha256:") }

// The conn worker comes from the latest artifact (E12-02). A new latest does not restart the running worker; the
// next spawn uses it (P9).
func TestConnFromRegistry(t *testing.T) {
	e := setup(t)
	reg := artifacttest.New(t)
	a := reg.Push(t, latest.RepoMDConn, latest.Tag, artifact.KindMDConn, selfImage(t, "a"), nil)
	src := registrySource(t, e, reg)
	e.cfg.Conn, e.cfg.Worker = "", src
	stop, done, ret := e.start()

	e.put("s1", "mark_price")
	path := e.waitJournal()
	e.waitLive(path, 1)
	e.waitRunning("md/Binance/public/0", 1)
	pid := journalPID(t, path)
	if p := exe(t, pid); !strings.Contains(p, hexOf(a)) || !strings.HasSuffix(p, "/rootfs/bin/worker") {
		t.Fatalf("worker runs %s, want image %s", p, a)
	}

	b := reg.Push(t, latest.RepoMDConn, latest.Tag, artifact.KindMDConn, selfImage(t, "b"), nil)
	e.wait(func() bool { return src.Digest() == b })
	time.Sleep(300 * time.Millisecond)
	if !alive(pid) || countEvent(t, path, "start") != 1 {
		t.Fatalf("a new latest restarted the worker: alive %v, starts %d", alive(pid), countEvent(t, path, "start"))
	}
	if p := exe(t, pid); !strings.Contains(p, hexOf(a)) {
		t.Fatalf("the running worker moved to %s", p)
	}

	// The worker goes once linger ends; a new owner spawns it again, from b.
	e.del("s1", "mark_price")
	e.waitEvent(path, "stop")
	e.waitAcks(0)
	e.wait(func() bool { return !alive(pid) })
	e.put("s1", "mark_price")
	e.wait(func() bool { return countEvent(t, path, "start") == 2 })
	e.waitRunning("md/Binance/public/0", 2)
	if p := exe(t, lastPID(t, path)); !strings.Contains(p, hexOf(b)) {
		t.Fatalf("respawned worker runs %s, want image %s", p, b)
	}

	stop()
	waitDone(t, done, ret)
}

// With no artifact ready, the ack stays PENDING with the artifact's reason, and the worker starts once the
// registry has one (E12-D009).
func TestPendingWithoutArtifact(t *testing.T) {
	e := setup(t)
	reg := artifacttest.New(t)
	reg.SetDown(true)
	src := registrySource(t, e, reg)
	e.cfg.Conn, e.cfg.Worker = "", src
	stop, done, ret := e.start()

	e.put("s1", "mark_price")
	e.wait(func() bool { return ackReason(t, e) == artifact.ReasonUnavailable })

	reg.SetDown(false)
	reg.Push(t, latest.RepoMDConn, latest.Tag, artifact.KindMDConn, selfImage(t, "a"), nil)
	path := e.waitJournal()
	e.waitLive(path, 1)
	e.waitRunning("md/Binance/public/0", 1)

	stop()
	waitDone(t, done, ret)
	syscall.Kill(lastPID(t, path), syscall.SIGKILL) //nolint:errcheck // cleanup also kills it
}

// lastPID is the pid of the newest incarnation in the journal.
func lastPID(t *testing.T, path string) int {
	t.Helper()
	pid := 0
	for _, line := range readJournal(t, path) {
		if line.Event == "start" && line.PID != 0 {
			pid = line.PID
		}
	}
	if pid == 0 {
		t.Fatal("no start pid")
	}
	return pid
}

// ackReason is the reason of the one ack, or "" when there is none or it is not PENDING.
func ackReason(t *testing.T, e *env) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := e.root.Get(ctx, state.MDAckPrefix(testInst), clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 1 {
		return ""
	}
	st := &statev1.EntryStatus{}
	if err := proto.Unmarshal(resp.Kvs[0].Value, st); err != nil {
		t.Fatal(err)
	}
	if st.GetState() != statev1.EntryState_ENTRY_STATE_PENDING {
		return ""
	}
	return st.GetReason()
}
