//go:build unix

package logread

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// pollInterval is how often follow, sitting at the end of the live file,
// checks that path again (E14-D018). It is not configurable.
const pollInterval = 250 * time.Millisecond

// pollHook, when set, runs at the start of a follow wait. Tests use it to see
// that the reader is blocked on the end of the file. It is not a setting.
var pollHook func()

// Origin chooses where a stream starts.
type Origin int

const (
	// OriginStart reads from the oldest byte still in the rotation.
	OriginStart Origin = iota
	// OriginTail reads the last Tail lines. Tail 0 starts at the end.
	OriginTail
	// OriginCursor resumes at Cursor.
	OriginCursor
)

// Options is one read of one stream. Path is the live file, with no .N suffix.
// Limit, when set, shares its cap and its token bucket with every other read
// that uses it.
type Options struct {
	Path   string
	Origin Origin
	Tail   int
	Cursor Cursor
	Follow bool
	Limit  *Limiter
}

// Record is one chunk of a stream. A gap record is the gap frame (E14-D017):
// Data is empty and there is no lost-byte count. Cursor is the resume position
// after this record. On a gap it is where bytes continue, or empty when no
// file exists yet.
type Record struct {
	Data   []byte
	Gap    bool
	Cursor Cursor
}

// Stream reads one rotated log. Next is called from one goroutine. Cancel ctx
// to unblock Next, then Close.
type Stream struct {
	ctx    context.Context
	path   string
	origin Origin
	tail   int
	want   Cursor
	follow bool
	lim    *Limiter
	hold   bool

	f      *os.File
	dev    uint64
	ino    uint64
	size   int64
	off    int64
	prefix []byte
	buf    []byte

	gapArmed      bool
	cursorApplied bool
	sawFile       bool
	closed        bool
	spins         int
}

// Open starts a read. A missing live file is an empty stream, not an error:
// without Follow, Next returns EOF; with Follow, Next waits until a file appears.
func Open(ctx context.Context, opt Options) (*Stream, error) {
	if ctx == nil {
		return nil, errors.New("logread: nil context")
	}
	if opt.Path == "" {
		return nil, errors.New("logread: empty path")
	}
	switch opt.Origin {
	case OriginStart, OriginTail, OriginCursor:
	default:
		return nil, ErrOrigin
	}
	if opt.Origin == OriginTail && opt.Tail < 0 {
		return nil, ErrOrigin
	}
	if opt.Origin == OriginCursor {
		if _, err := decode(opt.Cursor); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := &Stream{
		ctx:    ctx,
		path:   opt.Path,
		origin: opt.Origin,
		tail:   opt.Tail,
		want:   opt.Cursor,
		follow: opt.Follow,
		lim:    opt.Limit,
	}
	if opt.Limit != nil && opt.Limit.max > 0 {
		if err := opt.Limit.acquire(); err != nil {
			return nil, err
		}
		s.hold = true
	}
	if err := s.catchUp(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Next returns the next record. It returns io.EOF when the stream has been
// read to the live end and Follow is false. A cancelled ctx returns ctx.Err().
func (s *Stream) Next() (Record, error) {
	if s.closed {
		return Record{}, errClosed
	}
	for {
		if err := s.ctx.Err(); err != nil {
			return Record{}, err
		}
		if s.gapArmed {
			s.gapArmed = false
			return Record{Gap: true, Cursor: s.cursor()}, nil
		}
		if s.f == nil {
			if err := s.catchUp(); err != nil {
				return Record{}, err
			}
			if s.gapArmed {
				continue
			}
			if s.f == nil {
				if !s.follow {
					return Record{}, io.EOF
				}
				if err := s.poll(); err != nil {
					return Record{}, err
				}
				continue
			}
		}
		if err := s.fstat(); err != nil {
			return Record{}, err
		}
		if err := s.fill(); err != nil {
			return Record{}, err
		}
		fileDone := s.off+int64(len(s.buf)) >= s.size
		if len(s.buf) > 0 {
			chunk, rest := cut(s.buf, maxChunk, fileDone || len(s.buf) >= maxChunk)
			if len(chunk) > 0 {
				if err := s.pace(len(chunk)); err != nil {
					return Record{}, err
				}
				out := make([]byte, len(chunk))
				copy(out, chunk)
				s.note(chunk)
				copy(s.buf, rest)
				s.buf = s.buf[:len(rest)]
				s.spins = 0
				return Record{Data: out, Cursor: s.cursor()}, nil
			}
		}
		if s.off+int64(len(s.buf)) < s.size || len(s.buf) > 0 {
			s.spins++
			if s.spins > 8 {
				return Record{}, errors.New("logread: short read")
			}
			continue
		}
		err := s.afterEOF()
		if err == nil {
			s.spins = 0
			continue
		}
		if errors.Is(err, errWait) {
			if err := s.fstat(); err != nil {
				return Record{}, err
			}
			if s.size > s.off {
				continue
			}
			if err := s.poll(); err != nil {
				return Record{}, err
			}
			continue
		}
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		if errors.Is(err, errRetry) {
			s.spins++
			if s.spins > 8 && s.follow {
				s.spins = 0
				if err := s.poll(); err != nil {
					return Record{}, err
				}
				continue
			}
			if s.spins > 8 {
				return Record{}, fmt.Errorf("logread: %s changed while reading", s.path)
			}
			continue
		}
		return Record{}, err
	}
}

// Close releases the file and, if this stream took a concurrency slot, the slot.
// It is safe to call more than once. Do not call it concurrently with Next.
func (s *Stream) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	err := s.closeFile()
	if s.hold {
		s.lim.release()
		s.hold = false
	}
	return err
}

func (s *Stream) catchUp() error {
	var err error
	for range 8 {
		err = s.catchOnce()
		if !errors.Is(err, errRetry) {
			return err
		}
	}
	return fmt.Errorf("logread: %s changed while opening", s.path)
}

func (s *Stream) catchOnce() error {
	if s.f != nil {
		return nil
	}
	if s.origin == OriginTail && !s.sawFile {
		return s.openTail()
	}
	if s.origin == OriginCursor && !s.cursorApplied {
		err := s.openCursor()
		if errors.Is(err, errRetry) {
			return err
		}
		s.cursorApplied = true
		return err
	}
	return s.openStart()
}

func (s *Stream) openStart() error {
	ents, err := listFiles(s.path)
	if err != nil {
		return err
	}
	if len(ents) == 0 {
		return nil
	}
	return s.switchTo(ents[0], 0)
}

func (s *Stream) openTail() error {
	ents, err := listFiles(s.path)
	if err != nil {
		return err
	}
	if len(ents) == 0 {
		return nil
	}
	idx, off, err := findTail(ents, s.tail)
	if err != nil {
		return err
	}
	return s.switchTo(ents[idx], off)
}

func (s *Stream) openCursor() error {
	c, err := decode(s.want)
	if err != nil {
		return err
	}
	ents, err := listFiles(s.path)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.dev != c.dev || e.ino != c.ino || e.size < c.off {
			continue
		}
		h, err := hashFile(e.path, c.off)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return errRetry
			}
			return err
		}
		if h != c.hash {
			continue
		}
		return s.switchTo(e, c.off)
	}
	if len(ents) == 0 {
		s.gapArmed = true
		return nil
	}
	if err := s.switchTo(ents[0], 0); err != nil {
		return err
	}
	s.gapArmed = true
	s.sawFile = true
	return nil
}

// afterEOF runs once the current descriptor has been fully emitted. A normal
// rotation still has this inode under a .N name: reading continues at the next
// newer file and does not gap. keep=1 removes the inode and leaves only the
// new live file: that is tail -F, and it does not gap either. Numbered files
// with our inode already gone mean a generation fell off the rotation, which
// is a gap, and bytes resume at the oldest survivor (E14-D017).
func (s *Stream) afterEOF() error {
	ents, err := listFiles(s.path)
	if err != nil {
		return err
	}
	idx, err := s.find(ents)
	if err != nil {
		return err
	}
	if idx >= 0 {
		if ents[idx].size > s.off {
			s.size = ents[idx].size
			return nil
		}
		if idx+1 < len(ents) {
			return s.switchTo(ents[idx+1], 0)
		}
		if !s.follow {
			return io.EOF
		}
		return errWait
	}
	if len(ents) == 0 {
		if !s.follow {
			return io.EOF
		}
		return errWait
	}
	if hasNumbered(s.path, ents) {
		if err := s.switchTo(ents[0], 0); err != nil {
			return err
		}
		s.gapArmed = true
		return nil
	}
	return s.switchTo(ents[len(ents)-1], 0)
}

func (s *Stream) find(ents []fileEnt) (int, error) {
	for i, e := range ents {
		if e.dev != s.dev || e.ino != s.ino {
			continue
		}
		if e.size < s.off {
			return -1, nil
		}
		h, err := hashFile(e.path, s.off)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return -1, errRetry
			}
			return -1, err
		}
		if h != fnv64(s.prefix) {
			return -1, nil
		}
		return i, nil
	}
	return -1, nil
}

func (s *Stream) switchTo(e fileEnt, off int64) error {
	f, err := openNoFollow(e.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errRetry
		}
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	dev, ino, err := fileID(fi)
	if err != nil || dev != e.dev || ino != e.ino || fi.Size() < off {
		f.Close()
		return errRetry
	}
	var prefix []byte
	if off > 0 {
		n := off
		if n > prefixN {
			n = prefixN
		}
		prefix = make([]byte, n)
		if _, err := io.ReadFull(io.NewSectionReader(f, off-n, n), prefix); err != nil {
			f.Close()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return errRetry
			}
			return err
		}
	}
	if err := s.closeFile(); err != nil {
		f.Close()
		return err
	}
	s.f = f
	s.dev = dev
	s.ino = ino
	s.size = fi.Size()
	s.off = off
	s.prefix = prefix
	s.buf = nil
	s.sawFile = true
	return nil
}

func (s *Stream) fstat() error {
	fi, err := s.f.Stat()
	if err != nil {
		return err
	}
	dev, ino, err := fileID(fi)
	if err != nil {
		return err
	}
	s.dev, s.ino = dev, ino
	s.size = fi.Size()
	return nil
}

func (s *Stream) fill() error {
	room := maxChunk - len(s.buf)
	if room <= 0 {
		return nil
	}
	pos := s.off + int64(len(s.buf))
	if pos >= s.size {
		return nil
	}
	if int64(room) > s.size-pos {
		room = int(s.size - pos)
	}
	tmp := make([]byte, room)
	n, err := s.f.ReadAt(tmp, pos)
	if n > 0 {
		s.buf = append(s.buf, tmp[:n]...)
	}
	if n == 0 && err == nil {
		return io.ErrUnexpectedEOF
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if errors.Is(err, io.EOF) && pos+int64(n) < s.size {
		s.size = pos + int64(n)
	}
	return nil
}

func (s *Stream) note(chunk []byte) {
	if len(chunk) >= prefixN {
		s.prefix = append([]byte(nil), chunk[len(chunk)-prefixN:]...)
	} else {
		merged := make([]byte, 0, prefixN)
		merged = append(merged, s.prefix...)
		merged = append(merged, chunk...)
		if len(merged) > prefixN {
			merged = append([]byte(nil), merged[len(merged)-prefixN:]...)
		}
		s.prefix = merged
	}
	s.off += int64(len(chunk))
}

func (s *Stream) cursor() Cursor {
	if s.f == nil {
		return ""
	}
	return encode(s.dev, s.ino, s.off, fnv64(s.prefix))
}

func (s *Stream) pace(n int) error {
	if s.lim == nil {
		return nil
	}
	return s.lim.WaitN(s.ctx, n)
}

func (s *Stream) poll() error {
	if pollHook != nil {
		pollHook()
	}
	timer := time.NewTimer(pollInterval)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Stream) closeFile() error {
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
