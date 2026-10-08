package shim

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

// Run is the shim process (cmd/mftiko-shim): it becomes a session leader and subreaper, reads its Launch from
// stdin, opens its C3 socket, spawns the worker, serves agents and reaps (PROTOCOL §4.6). It returns the process's
// exit code.
//
// Of C3 it does Attach, fencing and Signal (E1-09), and the lifecycle (E1-11): Init, Start and Relay go to the
// worker, Stop is timed here, Release ends the shim once the worker has been reaped. Ack is answered unimplemented
// and nothing is buffered for the agent yet (E1-12); there is no exit file (E1-13). The worker's stdout and stderr go
// to rotated files under ${WORK_DIR}/log/{plane} (E1-14).
//
// SIGTERM or SIGINT stops the worker as a Stop would, with shutdownGrace and shutdownKillAfter, and the shim ends
// once the worker has been reaped; one that comes after the worker is gone ends the shim at once. Either way it
// removes its socket, and once the worker is reaped it writes out the last of its output first.
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

	ln, err := listen(l) // before the spawn: a worker no agent can reach must not start
	if err != nil {
		log.Error().Err(err).Msg("no socket")
		return 1
	}
	defer ln.Close() // removes the socket
	log.Info().Str("socket", ln.Addr().String()).Msg("listening")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	out, err := openOutput(log, l, outputHeader(l, time.Now())) // before the spawn: its output must have a place
	if err != nil {
		log.Error().Err(err).Msg("no output files")
		return 1
	}

	// The reaper must not block, and a log line can: it hands over, and drops what does not fit.
	reaped := make(chan Reaped, 64)
	w, err := Spawn(l, out.Stdout, out.Stderr, func(r Reaped) {
		select {
		case reaped <- r:
		default:
		}
	})
	out.spawned()
	if err != nil {
		log.Error().Err(err).Msg("spawn failed")
		return 1
	}
	defer func() {
		select {
		case <-w.Done(): // the worker's last words reach the file before the shim goes
			if !out.wait(outputDrain) {
				log.Warn().Dur("after", outputDrain).Msg("worker output still open: something the worker started holds it")
			}
		default: // still running: it keeps writing until it sees the shim gone, and nothing will read it
		}
	}()
	log.Info().Int("pid", w.Pid).Int("pgid", w.Pgid).Uint64("start_time", w.StartTime).Msg("worker started")
	if w.StartTime == 0 {
		log.Warn().Int("pid", w.Pid).Msg("no start time in /proc: the worker cannot be told from a later process with its pid")
	}
	go func() {
		for r := range reaped {
			log.Info().Int("pid", r.Pid).Int("code", r.Status.ExitStatus()).Int("signal", signalOf(r.Status)).Msg("reaped")
		}
	}()

	v := newView(log, w)
	go v.follow(w.Conn)
	srv := newServer(log, ln, l, v, os.Geteuid())
	srv.start()
	done, stopping := w.Done(), false
	for {
		select {
		case <-done:
			st := w.Status()
			log.Info().Int("pid", w.Pid).Int("code", st.ExitStatus()).Int("signal", signalOf(st)).Msg("worker exited")
			done = nil // seen; grandchildren are still reaped
			if stopping {
				log.Info().Msg("shim stopping: the worker is gone")
				srv.close()
				return 0
			}
		case s := <-stop:
			if done == nil {
				log.Info().Str("signal", s.String()).Msg("shim stopping")
				srv.close()
				return 0
			}
			// Not before the worker: it gets a Stop, runs its OnStop, and the shim ends when it is reaped (§4.3).
			stopping = true
			if first, _ := v.Stop("shim: "+s.String(), shutdownGrace, shutdownKillAfter); first {
				log.Info().Str("signal", s.String()).Msg("shim stopping: stopping the worker first")
			} else {
				log.Info().Str("signal", s.String()).Msg("shim stopping: the worker is already stopping")
			}
		case <-srv.Released():
			log.Info().Msg("shim released")
			srv.close()
			return 0
		}
	}
}

// What the shim's own SIGTERM gives the worker (PROTOCOL §4.3): the grace before SIGTERM, then before SIGKILL. A
// host shutting down waits for the shim; these keep that wait bounded, and give OnStop the larger part of it.
const (
	shutdownGrace     = 10 * time.Second
	shutdownKillAfter = 5 * time.Second
)

// outputDrain bounds how long an ending shim waits for the worker's output to be written out.
const outputDrain = 2 * time.Second

// launchTimeout bounds the wait for the Launch: an agent that started the shim and then wrote nothing is gone.
const launchTimeout = 10 * time.Second

func signalOf(st syscall.WaitStatus) int {
	if st.Signaled() {
		return int(st.Signal())
	}
	return 0
}
