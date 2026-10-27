//go:build unix

package logread

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// Snapshot is a fixed view of every rotation file, oldest first (E14-03).
// The descriptors are opened before Export returns, and each file is read only
// up to the size it had then, so a later append is not included and removing a
// name does not hide bytes already opened.
type Snapshot struct {
	ctx    context.Context
	files  []snapFile
	size   int64
	i      int
	off    int64
	lim    *Limiter
	hold   bool
	closed bool
}

type snapFile struct {
	f    *os.File
	size int64
}

// Export opens every rotation file of path. The returned snapshot's Size is
// the sum of those sizes and does not change. Close releases the descriptors
// and any concurrency slot.
func Export(ctx context.Context, path string, lim *Limiter) (*Snapshot, error) {
	if ctx == nil {
		return nil, errors.New("logread: nil context")
	}
	if path == "" {
		return nil, errors.New("logread: empty path")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := &Snapshot{ctx: ctx, lim: lim}
	if lim != nil && lim.max > 0 {
		if err := lim.acquire(); err != nil {
			return nil, err
		}
		s.hold = true
	}
	ents, err := listFiles(path)
	if err != nil {
		s.Close()
		return nil, err
	}
	for _, e := range ents {
		f, err := openNoFollow(e.path)
		if err != nil {
			s.Close()
			return nil, err
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			s.Close()
			return nil, err
		}
		dev, ino, err := fileID(fi)
		if err != nil || dev != e.dev || ino != e.ino {
			f.Close()
			s.Close()
			return nil, fmt.Errorf("logread: %s changed while opening", e.path)
		}
		sz := fi.Size()
		s.files = append(s.files, snapFile{f: f, size: sz})
		s.size += sz
	}
	return s, nil
}

// Size is the total number of bytes Read will produce.
func (s *Snapshot) Size() int64 { return s.size }

// Read copies the snapshot, oldest file first. It stops at each file's size
// from Export, and at io.EOF when the snapshot is exhausted.
func (s *Snapshot) Read(p []byte) (int, error) {
	if s.closed {
		return 0, errClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	for s.i < len(s.files) {
		sf := &s.files[s.i]
		if s.off >= sf.size {
			s.i++
			s.off = 0
			continue
		}
		want := len(p)
		remain := sf.size - s.off
		if int64(want) > remain {
			want = int(remain)
		}
		if err := s.lim.WaitN(s.ctx, want); err != nil {
			return 0, err
		}
		n, err := sf.f.ReadAt(p[:want], s.off)
		s.off += int64(n)
		if n > 0 {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
		return 0, io.ErrUnexpectedEOF
	}
	return 0, io.EOF
}

// Close closes every descriptor and releases a concurrency slot, if this
// snapshot took one. It is safe to call more than once.
func (s *Snapshot) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	var err error
	for i := range s.files {
		if s.files[i].f == nil {
			continue
		}
		if cerr := s.files[i].f.Close(); err == nil {
			err = cerr
		}
		s.files[i].f = nil
	}
	if s.hold {
		s.lim.release()
		s.hold = false
	}
	return err
}
