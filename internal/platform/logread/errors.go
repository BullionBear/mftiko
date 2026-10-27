//go:build unix

package logread

import "errors"

// ErrCursor means the token is not a cursor this package wrote. A cursor that
// decodes but does not match a file is a gap, not ErrCursor (E14-D016, E14-D017).
var ErrCursor = errors.New("logread: bad cursor")

// ErrOrigin means Origin is not one of the three, or Tail is negative.
var ErrOrigin = errors.New("logread: bad origin")

// ErrBusy means the limiter's concurrent-read cap is already taken.
// Open and Export return it immediately; they do not queue.
var ErrBusy = errors.New("logread: too many readers")

// ErrPause means Pause asked a follow read to stop. It is not an I/O failure.
// Next and Prime return it instead of waiting out the poll (E14-D024).
var ErrPause = errors.New("logread: pause")

// errClosed is Next or Read after Close.
var errClosed = errors.New("logread: closed")

// errWait means follow has caught up to the live file and should poll.
var errWait = errors.New("logread: wait")
