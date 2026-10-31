// Package evlog writes the strategy event log (DECISIONS E20-D006, E20-D007, PROTOCOL §12).
//
// Append copies one frame into a preallocated ring and returns. It does not wait for the disk: a
// frame the ring cannot take is dropped and counted. A goroutine moves the ring to files under
// VolumeDir()/eventlog, starts a new file when the current one reaches its size, and deletes the
// oldest files so the directory stays within the keep limit. Close appends the end frame and waits
// until everything before it is in a file.
//
// A nil *Log is the event log turned off. Callers check for nil before building a frame, so a
// session without the log allocates and writes nothing for it.
package evlog

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/BullionBear/mftiko/sdk/eventlog"
)

const (
	// DefaultKeep is the keep limit when the spec's keep_mb is 0.
	DefaultKeep = 256 << 20
	// DefaultRing is the ring between the dispatcher and the disk.
	DefaultRing = 8 << 20
	// DefaultFlush is how long a frame may wait in the ring when the ring is quiet.
	DefaultFlush = 100 * time.Millisecond

	minFile    = 64 << 10
	maxFile    = 64 << 20
	retryAfter = time.Second
	closeWait  = 5 * time.Second
)

// File is where the frames of one file go. Create returns one.
type File interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// Config opens a Log. Header carries the session, the incarnation and the SDK release; Log sets N
// and CreatedNs of each file. Zero values select the defaults. Create nil creates the file with
// os.OpenFile and O_EXCL. Now nil is time.Now. Log nil discards the writer's own failure lines.
type Config struct {
	Dir        string
	Header     eventlog.Header
	KeepBytes  int64
	RingBytes  int
	FlushEvery time.Duration
	Create     func(path string) (File, error)
	Now        func() time.Time
	Log        func(string)
}

// Log is one session's event log writer. Append and Stats are safe for concurrent use.
type Log struct {
	mu      sync.Mutex
	moved   *sync.Cond
	buf     []byte
	head    uint64 // bytes before head are in a file or were dropped
	tail    uint64 // the next frame starts here
	seq     uint64
	dropped uint64
	written uint64
	closed  bool
	exited  bool
	kicked  bool
	kickAt  uint64

	kick chan struct{}
	stop chan struct{}
	done chan struct{}
	once sync.Once

	mem  bool
	recs []eventlog.Record

	// The rest belongs to the goroutine that writes.
	dir        string
	header     eventlog.Header
	every      time.Duration
	keep       int64
	fileMax    int64
	create     func(string) (File, error)
	now        func() time.Time
	log        func(string)
	f          File
	fileN      uint32
	fileSize   int64
	fileFrames uint64
	retryAt    time.Time
	failing    bool
}

// Open starts the writer. It does not touch the disk until the first frame is due, and it does
// not fail: a directory or file it cannot write counts the frames as dropped, logs once, and
// tries again a second later.
func Open(cfg Config) *Log {
	ring := cfg.RingBytes
	if ring <= 0 {
		ring = DefaultRing
	}
	keep := cfg.KeepBytes
	if keep <= 0 {
		keep = DefaultKeep
	}
	l := &Log{
		buf:     make([]byte, ring),
		kickAt:  uint64(ring / 4),
		kick:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		dir:     cfg.Dir,
		header:  cfg.Header,
		every:   cfg.FlushEvery,
		keep:    keep,
		fileMax: min(max(keep/4, minFile), maxFile),
		create:  cfg.Create,
		now:     cfg.Now,
		log:     cfg.Log,
	}
	l.moved = sync.NewCond(&l.mu)
	if l.every <= 0 {
		l.every = DefaultFlush
	}
	if l.create == nil {
		l.create = createFile
	}
	if l.now == nil {
		l.now = time.Now
	}
	if l.log == nil {
		l.log = func(string) {}
	}
	go l.run()
	return l
}

// Memory is a Log that keeps its frames in memory and writes no file. A replay records the
// outputs it compares into one.
func Memory() *Log {
	l := &Log{mem: true}
	l.moved = sync.NewCond(&l.mu)
	return l
}

// Records is a copy of the frames a Memory log holds.
func (l *Log) Records() []eventlog.Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]eventlog.Record(nil), l.recs...)
}

// Append adds one frame whose body is a followed by b, and returns its rec_seq. The number is
// taken even when the frame is dropped, so a reader sees the gap.
func (l *Log) Append(kind eventlog.Kind, flags uint16, recvNs, tNs int64, ref uint64, a, b []byte) uint64 {
	l.mu.Lock()
	seq, kick := l.appendLocked(kind, flags, recvNs, tNs, ref, a, b)
	l.mu.Unlock()
	if kick {
		select {
		case l.kick <- struct{}{}:
		default:
		}
	}
	return seq
}

func (l *Log) appendLocked(kind eventlog.Kind, flags uint16, recvNs, tNs int64, ref uint64, a, b []byte) (uint64, bool) {
	l.seq++
	seq := l.seq
	if l.mem {
		body := make([]byte, 0, len(a)+len(b))
		body = append(append(body, a...), b...)
		l.recs = append(l.recs, eventlog.Record{
			Kind: kind, Flags: flags, Seq: seq, RecvNs: recvNs, TNs: tNs, Ref: ref, Body: body,
		})
		return seq, false
	}
	n := uint64(eventlog.FrameHead + len(a) + len(b))
	if l.closed || n-4 > eventlog.MaxFrame || n > uint64(len(l.buf))-(l.tail-l.head) {
		l.dropped++
		return seq, false
	}
	var head [eventlog.FrameHead]byte
	eventlog.PutFrameHead(head[:], kind, flags, seq, recvNs, tNs, ref, len(a)+len(b))
	pos := l.put(l.tail, head[:])
	pos = l.put(pos, a)
	l.tail = l.put(pos, b)
	if !l.kicked && l.tail-l.head >= l.kickAt {
		l.kicked = true
		return seq, true
	}
	return seq, false
}

func (l *Log) put(pos uint64, p []byte) uint64 {
	size := uint64(len(l.buf))
	for len(p) > 0 {
		c := copy(l.buf[pos%size:], p)
		p = p[c:]
		pos += uint64(c)
	}
	return pos
}

// Stats is how many frames are in files and how many were dropped: the ring was full, the
// file could not be written, or the log was already closed (E20-D007). Both accumulate.
func (l *Log) Stats() (records, dropped uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.mem {
		return uint64(len(l.recs)), 0
	}
	return l.written, l.dropped
}

// Flush waits until every frame appended before it is in a file or counted as dropped.
func (l *Log) Flush() {
	if l.mem {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	target := l.tail
	for l.head < target && !l.exited {
		select {
		case l.kick <- struct{}{}:
		default:
		}
		l.moved.Wait()
	}
}

// Close appends the end frame and waits until the ring is in files, at most five seconds. A frame
// appended after Close is dropped. A second Close does nothing.
func (l *Log) Close() {
	if l == nil || l.mem {
		return
	}
	l.once.Do(func() {
		var body [8]byte
		l.mu.Lock()
		l.appendLocked(eventlog.KindEnd, 0, 0, l.now().UnixNano(), 0, eventlog.AppendEnd(body[:0], l.dropped), nil)
		l.closed = true
		l.mu.Unlock()
		close(l.stop)
		select {
		case <-l.done:
		case <-time.After(closeWait):
			l.log("event log: close did not finish writing")
		}
	})
}

func (l *Log) run() {
	defer func() {
		l.mu.Lock()
		l.exited = true
		l.moved.Broadcast()
		l.mu.Unlock()
		close(l.done)
	}()
	tick := time.NewTicker(l.every)
	defer tick.Stop()
	for {
		select {
		case <-l.kick:
		case <-tick.C:
		case <-l.stop:
			l.drain()
			l.closeFile(true)
			return
		}
		l.drain()
	}
}

func (l *Log) drain() {
	l.mu.Lock()
	h, t := l.head, l.tail
	l.kicked = false
	l.mu.Unlock()
	if h == t {
		return
	}
	written, lost := l.writeOut(h, t)
	l.mu.Lock()
	l.head = t
	l.written += written
	l.dropped += lost
	l.moved.Broadcast()
	l.mu.Unlock()
}

// writeOut writes the frames in [h, t) and starts a new file before a frame that would take
// the current one past its size. A frame larger than that size gets a file of its own.
func (l *Log) writeOut(h, t uint64) (written, lost uint64) {
	pos := h
	for pos < t {
		if l.f != nil && l.fileFrames > 0 && l.fileSize+l.frameLen(pos) > l.fileMax {
			l.closeFile(false)
		}
		if !l.openFile() {
			return written, lost + l.frames(pos, t)
		}
		end := pos
		var k uint64
		size := l.fileSize
		for end < t {
			n := l.frameLen(end)
			if (l.fileFrames > 0 || k > 0) && size+n > l.fileMax {
				break
			}
			size += n
			end += uint64(n)
			k++
		}
		if err := l.writeRange(pos, end); err != nil {
			lost += k
			l.fail(err)
		} else {
			written += k
			l.fileSize = size
			l.fileFrames += k
			l.failing = false
		}
		pos = end
	}
	return written, lost
}

// frameLen is the whole frame at pos, length field included.
func (l *Log) frameLen(pos uint64) int64 {
	var b [4]byte
	size := uint64(len(l.buf))
	for i := range b {
		b[i] = l.buf[(pos+uint64(i))%size]
	}
	return 4 + int64(uint32(b[0])|uint32(b[1])<<8|uint32(b[2])<<16|uint32(b[3])<<24)
}

func (l *Log) frames(pos, t uint64) uint64 {
	var k uint64
	for pos < t {
		pos += uint64(l.frameLen(pos))
		k++
	}
	return k
}

func (l *Log) writeRange(from, to uint64) error {
	size := uint64(len(l.buf))
	off := from % size
	n := to - from
	if off+n <= size {
		_, err := l.f.Write(l.buf[off : off+n])
		return err
	}
	if _, err := l.f.Write(l.buf[off:]); err != nil {
		return err
	}
	_, err := l.f.Write(l.buf[:off+n-size])
	return err
}

func (l *Log) openFile() bool {
	if l.f != nil {
		return true
	}
	now := l.now()
	if now.Before(l.retryAt) {
		return false
	}
	if l.dir == "" {
		l.fail(errors.New("no volume directory"))
		return false
	}
	if err := os.MkdirAll(l.dir, 0o750); err != nil {
		l.fail(err)
		return false
	}
	l.retain()
	n := l.nextN()
	f, err := l.create(filepath.Join(l.dir, eventlog.Name(l.header.Incarnation, n)))
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			l.fileN = n
		}
		l.fail(err)
		return false
	}
	h := l.header
	h.N = n
	h.CreatedNs = now.UnixNano()
	hb := eventlog.AppendHeader(nil, h)
	if _, err := f.Write(hb); err != nil {
		_ = f.Close()
		l.fileN = n
		l.fail(err)
		return false
	}
	l.f = f
	l.fileN = n
	l.fileSize = int64(len(hb))
	l.fileFrames = 0
	return true
}

// nextN is one past the highest file of this incarnation, so a worker that comes back with the
// same incarnation does not overwrite what is there.
func (l *Log) nextN() uint32 {
	if l.fileN > 0 {
		return l.fileN + 1
	}
	fs, _ := eventlog.Files(l.dir)
	var n uint32
	for _, f := range fs {
		if f.Incarnation == l.header.Incarnation && f.N > n {
			n = f.N
		}
	}
	return n + 1
}

// retain deletes the oldest files, of any incarnation, until the files left and one more full
// file fit in the keep limit. It runs before a file is created, so the file being written is
// never one of them.
func (l *Log) retain() {
	fs, err := eventlog.Files(l.dir)
	if err != nil {
		return
	}
	sizes := make([]int64, len(fs))
	var total int64
	for i, f := range fs {
		if st, err := os.Stat(f.Path); err == nil {
			sizes[i] = st.Size()
			total += sizes[i]
		}
	}
	for i := 0; i < len(fs) && total+l.fileMax > l.keep; i++ {
		if err := os.Remove(fs[i].Path); err == nil {
			total -= sizes[i]
		}
	}
}

func (l *Log) closeFile(final bool) {
	if l.f == nil {
		return
	}
	if final {
		_ = l.f.Sync()
	}
	_ = l.f.Close()
	l.f = nil
}

func (l *Log) fail(err error) {
	if l.f != nil {
		_ = l.f.Close()
		l.f = nil
	}
	l.retryAt = l.now().Add(retryAfter)
	if !l.failing {
		l.failing = true
		l.log("event log: " + err.Error())
	}
}

func createFile(path string) (File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
}

// Outcome classifies a request error for order_reply, read_reply and resolve (PROTOCOL §12.4).
// text is the error's message; every outcome but OK carries it.
func Outcome(err error) (flags uint16, text string) {
	switch {
	case err == nil:
		return eventlog.OK, ""
	case errors.Is(err, nats.ErrNoResponders):
		flags = eventlog.NoResponders
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		flags = eventlog.Timeout
	case errors.Is(err, context.Canceled):
		flags = eventlog.Canceled
	default:
		flags = eventlog.Failed
	}
	return flags, err.Error()
}

// Err is the error a replay hands back for an outcome: the recorded message, and errors.Is
// matches the sentinel the recorded error matched.
func Err(flags uint16, body []byte) error {
	text := string(body)
	switch flags {
	case eventlog.OK:
		return nil
	case eventlog.NoResponders:
		return Wrap(text, nats.ErrNoResponders)
	case eventlog.Timeout:
		if strings.HasSuffix(text, nats.ErrTimeout.Error()) {
			return Wrap(text, nats.ErrTimeout)
		}
		return Wrap(text, context.DeadlineExceeded)
	case eventlog.Canceled:
		return Wrap(text, context.Canceled)
	}
	return Wrap(text, nil)
}

// Wrap is an error with the recorded text that errors.Is matches to is. It is is itself when the
// text is is's own message or empty.
func Wrap(text string, is error) error {
	if is != nil && (text == "" || text == is.Error()) {
		return is
	}
	return &recorded{text: text, is: is}
}

type recorded struct {
	text string
	is   error
}

func (e *recorded) Error() string { return e.text }
func (e *recorded) Unwrap() error { return e.is }
