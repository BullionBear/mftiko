package shim

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
)

// The worker's output (PROTOCOL §4.5 S4): the shim holds its stdout and stderr, and writes each to a file of its
// own under ${WORK_DIR}/log/{plane}/, rotated by size.
const (
	// DefaultOutputMaxBytes is the size a file is rotated at when the Launch sets none.
	DefaultOutputMaxBytes = 64 << 20
	// DefaultOutputKeep is how many files of a stream are kept, the current one included, when the Launch sets none.
	DefaultOutputKeep = 5
	// MinOutputMaxBytes is the smallest max_bytes a Launch may set: smaller files would rotate on every few lines.
	MinOutputMaxBytes = 64 << 10
	// MaxOutputKeep is the largest keep a Launch may set.
	MaxOutputKeep = 100
)

// LogDir is where a plane's shims write their workers' output: ${WORK_DIR}/log/{plane} (PROTOCOL §4.5 S4).
func LogDir(workDir, plane string) string { return filepath.Join(workDir, "log", plane) }

const (
	// LogDirMode is ${WORK_DIR}/log/{plane} after chmod (D1015, D1021).
	LogDirMode os.FileMode = 0o750
	// LogFileMode is a newly created stdout, stderr, or shim log (D1015, D1021).
	LogFileMode os.FileMode = 0o640
)

// EnsureLogDir creates ${WORK_DIR}/log/{plane} if needed and chmods it to 0750
// (D1021, D1022). A setgid bit already on the directory is kept, so new files
// inherit the group set on ${WORK_DIR}/log (D1015).
func EnsureLogDir(workDir, plane string) error {
	dir := LogDir(workDir, plane)
	if err := os.MkdirAll(dir, LogDirMode); err != nil {
		return err
	}
	return chmodLogDir(dir)
}

// chmodLogDir sets 0750 and keeps setgid when the directory already has it.
// Chmod of 0750 alone would clear the bit the parent ${WORK_DIR}/log passed down.
func chmodLogDir(dir string) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	mode := LogDirMode
	if fi.Mode()&os.ModeSetgid != 0 {
		mode |= os.ModeSetgid
	}
	return os.Chmod(dir, mode)
}

// OpenLogFile appends to path. A new file is created at 0640, then chmodded so
// the process umask does not apply (D1021). An existing file keeps its mode (D1022).
func OpenLogFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_EXCL, LogFileMode)
	if err == nil {
		if cerr := f.Chmod(LogFileMode); cerr != nil {
			f.Close()
			os.Remove(path)
			return nil, cerr
		}
		return f, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND, LogFileMode)
}

// OutputPath is the file of one stream of a worker, "stdout" or "stderr": ${WORK_DIR}/log/{plane}/{h}.{stream}. Its
// rotated files add .1, .2 and so on, .1 the newest.
func OutputPath(workDir, plane, workerKey, stream string) string {
	return filepath.Join(LogDir(workDir, plane), workerHash(workerKey)+"."+stream)
}

// outputLimits are the Launch's limits, with the defaults filled in.
func outputLimits(l *shimv1.Launch) (maxBytes int64, keep int) {
	maxBytes, keep = int64(l.GetOutput().GetMaxBytes()), int(l.GetOutput().GetKeep())
	if maxBytes == 0 {
		maxBytes = DefaultOutputMaxBytes
	}
	if keep == 0 {
		keep = DefaultOutputKeep
	}
	return maxBytes, keep
}

// checkOutput is the part of step 3 of PROTOCOL §4.6 about Launch.output: zero takes the default, anything else
// must be in range.
func checkOutput(o *shimv1.Output) error {
	if n := o.GetMaxBytes(); n != 0 && n < MinOutputMaxBytes {
		return fmt.Errorf("%w: output max_bytes %d is less than %d", ErrBadLaunch, n, MinOutputMaxBytes)
	}
	if n := o.GetKeep(); n > MaxOutputKeep {
		return fmt.Errorf("%w: output keep %d is more than %d", ErrBadLaunch, n, MaxOutputKeep)
	}
	return nil
}

// rotator is one stream's files: path, and path.1 to path.(keep-1) before it. A write never makes a file larger than
// maxBytes: what does not fit goes to a new file, and the oldest falls off. It is used by one goroutine.
type rotator struct {
	path     string
	maxBytes int64
	keep     int

	f    *os.File
	size int64
}

// openRotator opens path for appending, after whatever an earlier incarnation of the worker left there.
func openRotator(path string, maxBytes int64, keep int) (*rotator, error) {
	r := &rotator{path: path, maxBytes: maxBytes, keep: keep}
	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rotator) open() error {
	f, err := OpenLogFile(r.path)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	r.f, r.size = f, fi.Size()
	return nil
}

// Write writes p whole, rotating as often as it takes. It returns len(p) or an error.
func (r *rotator) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		if r.size >= r.maxBytes {
			if err := r.rotate(); err != nil {
				return n, err
			}
		}
		chunk := p[:min(int64(len(p)), r.maxBytes-r.size)]
		w, err := r.f.Write(chunk)
		n += w
		r.size += int64(w)
		if err != nil {
			return n, err
		}
		p = p[len(chunk):]
	}
	return n, nil
}

// rotate shifts path.i to path.(i+1), drops what would be past keep, and starts path afresh. With keep 1 there is
// only path, and it starts over empty.
func (r *rotator) rotate() error {
	if err := r.f.Close(); err != nil {
		return err
	}
	r.f = nil
	numbered := func(i int) string { return r.path + "." + strconv.Itoa(i) }
	if r.keep == 1 {
		if err := os.Remove(r.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return r.open()
	}
	if err := os.Remove(numbered(r.keep - 1)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for i := r.keep - 2; i >= 1; i-- {
		if err := os.Rename(numbered(i), numbered(i+1)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Rename(r.path, numbered(1)); err != nil {
		return err
	}
	return r.open()
}

func (r *rotator) Close() error {
	if r.f == nil {
		return nil
	}
	return r.f.Close()
}

// outputQueue is how many reads of a stream may wait for the disk; outputChunk is the most one read takes. Together
// they are what the shim holds for a worker whose disk is slow, 2 MiB a stream, before it drops.
const (
	outputQueue = 64
	outputChunk = 32 << 10
)

// stream copies one pipe of the worker to its rotator without ever making the worker wait: one goroutine reads the
// pipe as fast as the worker writes, another writes to the file. When the file falls behind by more than the queue,
// reads are dropped; the next chunk that gets through carries how much went missing just before it, and the file
// gets a note at exactly that point. A write that fails (a full disk) is counted the same way.
type stream struct {
	log  zerolog.Logger
	name string
	dst  io.WriteCloser
	q    chan chunk
	done chan struct{}
}

// chunk is one read of the pipe, after lost bytes that never made it to the file.
type chunk struct {
	lost int64
	b    []byte
}

// copyStream starts copying src to dst. dst is written by one goroutine only, and closed when src ends and what was
// queued is written.
func copyStream(log zerolog.Logger, name string, src io.Reader, dst io.WriteCloser) *stream {
	s := &stream{log: log.With().Str("stream", name).Logger(), name: name, dst: dst,
		q: make(chan chunk, outputQueue), done: make(chan struct{})}
	go s.read(src)
	go s.write()
	return s
}

// Done is closed once the pipe has ended and everything kept has been written and the file closed.
func (s *stream) Done() <-chan struct{} { return s.done }

func (s *stream) read(src io.Reader) {
	defer close(s.q)
	var lost int64
	for {
		buf := make([]byte, outputChunk)
		n, err := src.Read(buf)
		if n > 0 {
			select {
			case s.q <- chunk{lost: lost, b: buf[:n]}:
				lost = 0
			default:
				if lost == 0 {
					s.log.Warn().Msg("worker output: the file is behind, dropping")
				}
				lost += int64(n)
			}
		}
		if err != nil { // EOF once the worker, and whatever it left holding the pipe, are gone
			if lost > 0 {
				s.q <- chunk{lost: lost} // the pipe is done: waiting here holds up no one
			}
			return
		}
	}
}

func (s *stream) write() {
	defer close(s.done)
	defer s.dst.Close()
	var carry int64 // lost to failed writes, to note before the next one that works
	for c := range s.q {
		lost := carry + c.lost
		out := c.b
		if lost > 0 {
			out = append(lostNote(lost, s.name), c.b...)
		}
		if _, err := s.dst.Write(out); err != nil {
			if carry == 0 { // a full disk: say so once, keep draining the pipe
				s.log.Error().Err(err).Msg("worker output: write failed; dropping until it works again")
			}
			carry = lost + int64(len(c.b)) // the worker's bytes, not the note's
			continue
		}
		if carry > 0 {
			s.log.Info().Msg("worker output: writing again")
		}
		carry = 0
	}
	if carry > 0 { // the last writes failed: one more try, so the file says what it is missing
		s.dst.Write(lostNote(carry, s.name)) //nolint:errcheck // nothing left to do if this fails too
	}
}

// lostNote is the line that marks where n bytes of a stream went missing.
func lostNote(n int64, stream string) []byte {
	return fmt.Appendf(nil, "\n[mftiko-shim] %d bytes of %s lost here: the disk fell behind or failed\n", n, stream)
}

// workerOutput is the worker's stdout and stderr: the pipes' write ends for the spawn, and the copies to the files.
type workerOutput struct {
	Stdout, Stderr *os.File // the worker's ends; the shim closes its copies once the worker has them
	streams        []*stream
}

// openOutput creates ${WORK_DIR}/log/{plane} if missing, opens the worker's two files for appending, writes a header
// for this incarnation in each, and starts copying a pipe into each. The worker's ends go to Spawn.
func openOutput(log zerolog.Logger, l *shimv1.Launch, header string) (*workerOutput, error) {
	if err := EnsureLogDir(l.GetWorkDir(), l.GetPlane()); err != nil {
		return nil, fmt.Errorf("shim: log dir: %w", err)
	}
	maxBytes, keep := outputLimits(l)
	o := &workerOutput{}
	var opened []io.Closer
	fail := func(err error) (*workerOutput, error) {
		for _, c := range opened {
			c.Close()
		}
		return nil, err
	}
	for _, name := range []string{"stdout", "stderr"} {
		path := OutputPath(l.GetWorkDir(), l.GetPlane(), l.GetWorker().GetKey(), name)
		rot, err := openRotator(path, maxBytes, keep)
		if err != nil {
			return fail(fmt.Errorf("shim: open %s: %w", path, err))
		}
		opened = append(opened, rot)
		if _, err := rot.Write([]byte(header)); err != nil {
			return fail(fmt.Errorf("shim: write %s: %w", path, err))
		}
		pr, pw, err := os.Pipe()
		if err != nil {
			return fail(fmt.Errorf("shim: pipe: %w", err))
		}
		opened = append(opened, pr, pw)
		if name == "stdout" {
			o.Stdout = pw
		} else {
			o.Stderr = pw
		}
		o.streams = append(o.streams, copyStream(log, name, pr, rot))
	}
	return o, nil
}

// spawned closes the shim's copies of the worker's ends: from then on a pipe ends when the worker, and whatever it
// left holding it, are gone.
func (o *workerOutput) spawned() {
	o.Stdout.Close()
	o.Stderr.Close()
}

// wait waits up to d for both streams to end and be written out. A grandchild may hold a pipe past the worker; the
// shim does not wait for it beyond d.
func (o *workerOutput) wait(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	for _, s := range o.streams {
		select {
		case <-s.Done():
		case <-t.C:
			return false
		}
	}
	return true
}

// outputHeader is the line each incarnation starts its files with.
func outputHeader(l *shimv1.Launch, now time.Time) string {
	return fmt.Sprintf("[mftiko-shim] %s worker %s incarnation %d\n", now.UTC().Format(time.RFC3339Nano),
		l.GetWorker().GetKey(), l.GetWorker().GetIncarnation())
}
