package shim

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"

	workerv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/worker/v1"
)

// Run is the shim process (cmd/mftiko-shim): it becomes a session leader and subreaper, reads its Launch from
// stdin, spawns the worker and reaps (PROTOCOL §4.6). It returns the process's exit code.
//
// For now it serves no agent (C3, E1-09), forwards nothing (E1-11, E1-12), writes no exit file (E1-13) and sends
// the worker's output to /dev/null (E1-14). It reads and logs what the worker sends, so that the worker never
// blocks on a full socket, and it stays until SIGTERM or SIGINT, reaping grandchildren after the worker is gone.
func Run(log zerolog.Logger) int {
	// stderr is the agent's to connect, and may be a pipe whose reader is gone: a log line must then fail, not kill
	// the shim. A handled signal, unlike an ignored one, goes back to its default in the worker.
	signal.Notify(make(chan os.Signal, 1), syscall.SIGPIPE)
	if err := Become(); err != nil {
		log.Error().Err(err).Msg("shim setup failed")
		return 1
	}
	noLaunch := time.AfterFunc(launchTimeout, func() {
		log.Error().Dur("after", launchTimeout).Msg("no launch on stdin")
		os.Exit(1)
	})
	l, err := ReadLaunch(os.Stdin)
	noLaunch.Stop()
	if err != nil {
		log.Error().Err(err).Msg("no launch")
		return 1
	}
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		log.Error().Err(err).Msg("open /dev/null")
		return 1
	}
	if err := unix.Dup3(int(null.Fd()), 0, 0); err != nil { // nothing more comes on stdin: do not hold the agent's pipe
		log.Error().Err(err).Msg("replace stdin")
		return 1
	}
	log = log.With().Str("plane", l.GetPlane()).Str("instance", l.GetAgentInstance()).
		Str("worker", l.GetWorker().GetKey()).Uint32("incarnation", l.GetWorker().GetIncarnation()).Logger()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	// The reaper must not block, and a log line can: it hands over, and drops what does not fit.
	reaped := make(chan Reaped, 64)
	w, err := Spawn(l, null, null, func(r Reaped) {
		select {
		case reaped <- r:
		default:
		}
	})
	if err != nil {
		log.Error().Err(err).Msg("spawn failed")
		return 1
	}
	log.Info().Int("pid", w.Pid).Int("pgid", w.Pgid).Uint64("start_time", w.StartTime).Msg("worker started")
	if w.StartTime == 0 {
		log.Warn().Int("pid", w.Pid).Msg("no start time in /proc: the worker cannot be told from a later process with its pid")
	}
	go func() {
		for r := range reaped {
			log.Info().Int("pid", r.Pid).Int("code", r.Status.ExitStatus()).Int("signal", signalOf(r.Status)).Msg("reaped")
		}
	}()

	go drain(log, w.Conn)
	done := w.Done()
	for {
		select {
		case <-done:
			st := w.Status()
			log.Info().Int("pid", w.Pid).Int("code", st.ExitStatus()).Int("signal", signalOf(st)).Msg("worker exited")
			done = nil // seen; grandchildren are still reaped
		case s := <-stop:
			log.Info().Str("signal", s.String()).Msg("shim stopping")
			return 0
		}
	}
}

// launchTimeout bounds the wait for the Launch: an agent that started the shim and then wrote nothing is gone.
const launchTimeout = 10 * time.Second

// drain reads the worker's messages until it closes its end.
func drain(log zerolog.Logger, conn io.Reader) {
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
		log.Debug().Str("msg", fmt.Sprintf("%T", m.GetMsg())).Msg("from worker")
	}
}

func signalOf(st syscall.WaitStatus) int {
	if st.Signaled() {
		return int(st.Signal())
	}
	return 0
}
