package shim

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
)

// view is the worker as the C3 server sees it: what the spawn and the reaper know, and the little of what the
// worker said on C4 that Attached reports (PROTOCOL §4.2). Nothing is forwarded yet (E1-11, E1-12).
type view struct {
	w *Worker

	mu         sync.Mutex
	hello      *workerv1.Hello
	stage      string
	lastBeatNs int64
}

func newView(w *Worker) *view { return &view{w: w} }

// Signal sends sig to the worker's process group.
func (v *view) Signal(sig syscall.Signal) error { return v.w.Signal(sig) }

// Attached reports the worker. Its state is EXITED once reaped, RUNNING once it said Hello, STARTING before.
func (v *view) Attached() *shimv1.Attached {
	a := &shimv1.Attached{Pid: int32(v.w.Pid), Pgid: int32(v.w.Pgid), StartTime: v.w.StartTime}
	v.mu.Lock()
	a.Hello, a.Stage, a.LastBeatAtNs = proto.CloneOf(v.hello), v.stage, v.lastBeatNs
	v.mu.Unlock()
	select {
	case <-v.w.Done():
		a.State = shimv1.WorkerState_WORKER_STATE_EXITED
	default:
		if a.Hello != nil {
			a.State = shimv1.WorkerState_WORKER_STATE_RUNNING
		} else {
			a.State = shimv1.WorkerState_WORKER_STATE_STARTING
		}
	}
	return a
}

// follow reads the worker's messages until it closes its end, so the worker never blocks on a full socket, and keeps
// what Attached reports.
func (v *view) follow(log zerolog.Logger, conn io.Reader) {
	r := newFrameReader(conn)
	for {
		m := &workerv1.WorkerUp{}
		if err := r.Read(m); err != nil {
			if !errors.Is(err, io.EOF) {
				log.Warn().Err(err).Msg("worker channel")
			}
			log.Info().Msg("worker channel closed")
			return
		}
		v.mu.Lock()
		switch m := m.GetMsg().(type) {
		case *workerv1.WorkerUp_Hello:
			if v.hello == nil { // the first one counts; a worker says Hello once
				v.hello = m.Hello
			}
		case *workerv1.WorkerUp_Stage:
			v.stage = m.Stage.GetName()
		case *workerv1.WorkerUp_Beat:
			v.lastBeatNs = time.Now().UnixNano()
		}
		v.mu.Unlock()
		log.Debug().Str("msg", fmt.Sprintf("%T", m.GetMsg())).Msg("from worker")
	}
}
