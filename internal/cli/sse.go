package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// sseIdle is three server heartbeats (E10-D005 is 15s; E11-D030). Any byte,
// including a comment, resets it. Silence this long is a disconnect.
const sseIdle = 45 * time.Second

const (
	sseBackoffMin = time.Second
	sseBackoffMax = 30 * time.Second
)

// errSSEIdle is a read or header wait that saw no bytes for sseIdle.
var errSSEIdle = errors.New("sse idle")

// sseIdleC, when non-nil, is an extra idle signal. Production leaves it nil.
// Tests close it instead of waiting 45s.
var sseIdleC chan struct{}

// sseNow is the clock for --wait deadlines. Tests replace it.
var sseNow = time.Now

// sseRetryWait sleeps before a reconnect. Tests replace it so a dropped
// stream does not wait on the wall clock. A canceled parent context ends
// the wait and that attempt is not sent.
var sseRetryWait = func(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// nextBackoff is the reconnect delay (E11-D030). The first failure waits 1s.
// After that the delay doubles, and it does not grow past 30s.
func nextBackoff(prev time.Duration) time.Duration {
	if prev <= 0 {
		return sseBackoffMin
	}
	next := prev * 2
	if next > sseBackoffMax {
		return sseBackoffMax
	}
	return next
}

// sseEvent is one dispatched server-sent event. Resume is the Last-Event-ID
// to send on the next connection, after this event has been applied. ID is
// set only when this event carried an id field. An empty id field clears
// Resume; a missing id field leaves it alone.
type sseEvent struct {
	Event  string
	ID     string
	HasID  bool
	Data   string
	Resume string
}

// sseParser is the standard event-stream state machine (E11-D029). It reads
// lines, not a third-party decoder.
type sseParser struct {
	resume string
	event  string
	id     string
	hasID  bool
	data   []string
}

func (p *sseParser) line(line string) (sseEvent, bool) {
	line = strings.TrimSuffix(line, "\r")
	if line == "" {
		return p.dispatch()
	}
	if strings.HasPrefix(line, ":") {
		return sseEvent{}, false
	}
	field, value, ok := strings.Cut(line, ":")
	if !ok {
		value = ""
	} else if strings.HasPrefix(value, " ") {
		value = value[1:]
	}
	switch field {
	case "event":
		p.event = value
	case "data":
		p.data = append(p.data, value)
	case "id":
		if strings.ContainsRune(value, 0) {
			break
		}
		p.id = value
		p.hasID = true
		p.resume = value
	}
	return sseEvent{}, false
}

func (p *sseParser) dispatch() (sseEvent, bool) {
	data := strings.Join(p.data, "\n")
	ev := sseEvent{Event: p.event, Data: data, Resume: p.resume}
	if p.hasID {
		ev.HasID = true
		ev.ID = p.id
	}
	empty := p.event == "" && data == ""
	p.event = ""
	p.id = ""
	p.hasID = false
	p.data = nil
	if empty {
		return sseEvent{}, false
	}
	return ev, true
}

// readSSE parses r until it ends, ctx is canceled, or the idle timer fires.
// got is true when any byte arrived, including a comment heartbeat. resume is
// updated when an id field is seen, including an id field on a comment-only
// block that does not dispatch. An incomplete event at EOF is discarded.
// The caller closes r to unblock the read.
func readSSE(ctx context.Context, r io.Reader, resume *string, fn func(sseEvent) error) (got bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type chunk struct {
		b   []byte
		err error
	}
	ch := make(chan chunk, 1)
	go func() {
		buf := make([]byte, 32<<10)
		for {
			n, rerr := r.Read(buf)
			c := chunk{err: rerr}
			if n > 0 {
				c.b = append([]byte(nil), buf[:n]...)
			}
			select {
			case ch <- c:
			case <-ctx.Done():
				return
			}
			if rerr != nil {
				return
			}
		}
	}()

	p := sseParser{resume: *resume}
	pending := make([]byte, 0, 4096)
	idle := time.NewTimer(sseIdle)
	defer idle.Stop()
	for {
		var poke <-chan struct{}
		if sseIdleC != nil {
			poke = sseIdleC
		}
		select {
		case <-ctx.Done():
			return got, ctx.Err()
		case <-idle.C:
			return got, errSSEIdle
		case <-poke:
			return got, errSSEIdle
		case c := <-ch:
			if len(c.b) > 0 {
				got = true
				resetTimer(idle, sseIdle)
				pending = append(pending, c.b...)
				if len(pending) > maxBody {
					return got, errors.New("sse event is too large")
				}
				for {
					i := bytes.IndexByte(pending, '\n')
					if i < 0 {
						break
					}
					line := pending[:i]
					pending = pending[i+1:]
					ev, ok := p.line(string(line))
					*resume = p.resume
					if !ok {
						continue
					}
					if err := fn(ev); err != nil {
						return got, err
					}
				}
			}
			if c.err != nil {
				*resume = p.resume
				if c.err == io.EOF {
					return got, io.EOF
				}
				return got, c.err
			}
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// streamBody is one GET /events response. Close cancels the request context
// and closes the body. The request context is not the parent's: canceling it
// on idle must not cancel the command, and closing it after headers must not
// happen before the caller is done reading.
type streamBody struct {
	resp   *http.Response
	cancel context.CancelFunc
	once   sync.Once
}

func (s *streamBody) Close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		if s.resp != nil && s.resp.Body != nil {
			_ = s.resp.Body.Close()
		}
		if s.cancel != nil {
			s.cancel()
		}
	})
}

// openEvents calls GET /events. Last-Event-ID is sent only when lastID is
// not empty. The header wait uses the same 45s idle as the body (E11-D030).
func (a *API) openEvents(ctx context.Context, sessionID, lastID string) (*streamBody, error) {
	return a.openStream(ctx, func(ctx context.Context) (*http.Response, error) {
		return a.doEvents(ctx, sessionID, lastID)
	})
}

// openStream waits up to sseIdle for response headers, then returns the body.
// The HTTP client is the caller's. Canceling the header wait does not cancel
// the command context.
func (a *API) openStream(ctx context.Context, do func(context.Context) (*http.Response, error)) (*streamBody, error) {
	reqCtx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	idleHit := false
	stopped := false
	arrived := make(chan struct{})
	go func() {
		timer := time.NewTimer(sseIdle)
		defer timer.Stop()
		var poke <-chan struct{}
		if sseIdleC != nil {
			poke = sseIdleC
		}
		select {
		case <-arrived:
			return
		case <-timer.C:
			mu.Lock()
			if !stopped {
				idleHit = true
				cancel()
			}
			mu.Unlock()
		case <-poke:
			mu.Lock()
			if !stopped {
				idleHit = true
				cancel()
			}
			mu.Unlock()
		case <-ctx.Done():
			cancel()
		}
	}()
	resp, err := do(reqCtx)
	mu.Lock()
	stopped = true
	wasIdle := idleHit
	mu.Unlock()
	close(arrived)
	if err != nil || wasIdle {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		cancel()
		if wasIdle {
			return nil, errSSEIdle
		}
		return nil, err
	}
	return &streamBody{resp: resp, cancel: cancel}, nil
}

func (a *API) doEvents(ctx context.Context, sessionID, lastID string) (*http.Response, error) {
	params := &apiv1.StreamEventsParams{
		Topics: []apiv1.EventTopic{apiv1.Sessions},
	}
	if sessionID != "" {
		id := sessionID
		params.SessionId = &id
	}
	if lastID != "" {
		id := lastID
		params.LastEventID = &id
	}
	req, err := apiv1.NewStreamEventsRequest(a.base, params)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	if err := a.edit(ctx, req); err != nil {
		return nil, err
	}
	return a.stream.Do(req)
}
