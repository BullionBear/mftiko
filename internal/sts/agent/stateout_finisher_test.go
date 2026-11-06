package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	clientv3 "go.etcd.io/etcd/client/v3"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

type putRecorder struct {
	mu   sync.Mutex
	keys []string
}

func (w *putRecorder) Put(_ context.Context, key, _ string, _ ...clientv3.OpOption) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.keys = append(w.keys, key)
	return nil
}

func (w *putRecorder) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.keys)
}

// finisherPlane is a plane with one terminal session whose volume holds a file.
func finisherPlane(t *testing.T, id string) (*plane, *bytes.Buffer, string) {
	t.Helper()
	data := t.TempDir()
	writeTestTerminal(t, data, id)
	vol, _ := sessionDirs(data, id)
	if err := os.MkdirAll(vol, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(vol, "note"), []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	p := &plane{
		inst: "sts-test", prog: &Program{DataDir: data}, workDir: t.TempDir(),
		log: zerolog.New(&logs), exits: map[string]exitNote{},
		saved: map[string]*statev1.SessionStatus{id: {Phase: statev1.Phase_PHASE_DONE}},
		w:     &putRecorder{},
	}
	return p, &logs, data
}

func readyFinisher(p *plane) *finisher {
	f := newFinisher(context.Background(), p)
	f.ready = true
	p.finish = f
	return f
}

func woken(f *finisher) bool {
	select {
	case <-f.wake:
		return true
	default:
		return false
	}
}

func tempLayers(t *testing.T, data string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(data, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), ".tmp-") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// No data repository: the volume is kept, nothing is packed, the hold is logged once,
// and a pass does not wake the loop again (E09-D100).
func TestFinisherNoDataRepoHoldsVolume(t *testing.T) {
	const id = "a1b2c3"
	p, logs, data := finisherPlane(t, id)
	f := readyFinisher(p) // p.art is nil: packing would panic

	f.pass()
	if woken(f) {
		t.Fatal("a pass woke the loop: it would run pass after pass")
	}
	j := f.jobs[id]
	if j == nil || j.blocked || time.Until(j.next) < noRepoRecheck-time.Minute {
		t.Fatalf("job %+v", j)
	}
	if f.nextWait() < noRepoRecheck-time.Minute {
		t.Fatalf("next wait %s", f.nextWait())
	}
	vol, _ := sessionDirs(data, id)
	if _, err := os.Stat(filepath.Join(vol, "note")); err != nil {
		t.Fatalf("volume: %v", err)
	}
	if got := tempLayers(t, data); len(got) != 0 {
		t.Fatalf("packed %v", got)
	}

	// A commit's ensure wakes the loop once, but the job is not due yet.
	f.ensure(id)
	if !woken(f) {
		t.Fatal("ensure did not wake the loop")
	}
	next := f.jobs[id].next
	f.pass()
	f.pass()
	if woken(f) {
		t.Fatal("a pass woke the loop")
	}
	if !f.jobs[id].next.Equal(next) {
		t.Fatalf("next moved from %s to %s", next, f.jobs[id].next)
	}
	if n := strings.Count(logs.String(), "state_out held"); n != 1 {
		t.Fatalf("hold logged %d times:\n%s", n, logs.String())
	}
	if strings.Contains(logs.String(), "state_out push") {
		t.Fatalf("counted as a push failure:\n%s", logs.String())
	}
}

// A push that fails reuses the packed layer on the next attempt. After it lands the
// layer, the volume and the job are gone (E09-D100).
func TestFinisherReusesLayerAcrossFailedPushes(t *testing.T) {
	const id = "b2c3d4"
	p, logs, data := finisherPlane(t, id)
	reg := newMemRegistry()
	t.Cleanup(reg.Close)
	client, err := artifact.Open(artifact.Registry{PlainHTTPHosts: []string{reg.addr()}}, data)
	if err != nil {
		t.Fatal(err)
	}
	p.art = client
	p.dataRepo = reg.addr() + "/state"
	f := readyFinisher(p)

	reg.setDown(true)
	f.pass()
	first := f.jobs[id].layer
	if first == "" {
		t.Fatalf("no layer kept:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "state_out push") {
		t.Fatalf("no push failure logged:\n%s", logs.String())
	}
	if wait := time.Until(f.jobs[id].next); wait > pushBackoff.Max {
		t.Fatalf("temporary failure waits %s", wait)
	}
	f.jobs[id].next = time.Time{}
	f.pass()
	if got := f.jobs[id].layer; got != first {
		t.Fatalf("packed again: %s then %s", first, got)
	}
	if got := tempLayers(t, data); len(got) != 1 || got[0] != first {
		t.Fatalf("layers on disk %v, want [%s]", got, first)
	}

	reg.setDown(false)
	f.jobs[id].next = time.Time{}
	f.pass()
	if _, ok := f.jobs[id]; ok {
		t.Fatalf("job still there:\n%s", logs.String())
	}
	if _, ok := f.finished[id]; !ok {
		t.Fatal("not finished")
	}
	if got := tempLayers(t, data); len(got) != 0 {
		t.Fatalf("layers left %v", got)
	}
	vol, _ := sessionDirs(data, id)
	if _, err := os.Stat(vol); !os.IsNotExist(err) {
		t.Fatalf("volume: %v", err)
	}
	snap := p.saved[id].GetStateOut()
	if snap.GetDigest() == "" || snap.GetRepository() != p.dataRepo {
		t.Fatalf("state_out %+v", snap)
	}
	if p.w.(*putRecorder).count() != 1 {
		t.Fatalf("puts %d", p.w.(*putRecorder).count())
	}
}

// A refusal waits on the slow ladder; the kept layer is removed when the loop stops.
func TestFinisherRefusedPushBacksOffAndStopCleans(t *testing.T) {
	const id = "c3d4e5"
	p, logs, data := finisherPlane(t, id)
	reg := newMemRegistry()
	reg.user, reg.pass = "u", "p"
	t.Cleanup(reg.Close)
	client, err := artifact.Open(artifact.Registry{PlainHTTPHosts: []string{reg.addr()}}, data)
	if err != nil {
		t.Fatal(err)
	}
	p.art = client
	p.dataRepo = reg.addr() + "/state"
	ctx, cancel := context.WithCancel(context.Background())
	f := newFinisher(ctx, p)
	f.ready = true
	p.finish = f

	go f.loop()
	f.kick()
	deadline := time.Now().Add(10 * time.Second)
	for {
		f.mu.Lock()
		j := f.jobs[id]
		done := j != nil && j.fails > 0
		var wait time.Duration
		if done {
			wait = time.Until(j.next)
		}
		f.mu.Unlock()
		if done {
			if wait < refusedBackoff.Min-5*time.Second {
				t.Fatalf("refused push waits %s:\n%s", wait, logs.String())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no attempt:\n%s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := tempLayers(t, data); len(got) != 1 {
		t.Fatalf("layers %v", got)
	}
	cancel()
	<-f.done
	if got := tempLayers(t, data); len(got) != 0 {
		t.Fatalf("layers left after stop %v", got)
	}
}

func TestRefused(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{&artifact.Error{Reason: artifact.ReasonUnauthorized, Permanent: true}, true},
		{&artifact.Error{Reason: artifact.ReasonUnavailable, Permanent: false}, false},
		{fmt.Errorf("%w: repository is required", artifact.ErrRequest), true},
		{errors.New("disk full"), false},
	} {
		if got := refused(c.err); got != c.want {
			t.Errorf("refused(%v) = %v", c.err, got)
		}
	}
}
