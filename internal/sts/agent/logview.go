package agent

import (
	"context"
	"maps"
	"sync"
)

// logQuiescer waits until no log request still holds id's files open.
// releaseHost calls it before deleting shim logs (E10-D020).
type logQuiescer interface {
	quiesce(ctx context.Context, id string) error
}

// logView is the spec and phase slice the log handlers read. Handlers do not
// take plane.mu. The reconcile loop publishes into it while holding plane.mu,
// and must not call back into the plane from here.
type logView struct {
	mu      sync.Mutex
	ready   bool
	readyCh chan struct{}
	specs   map[string]struct{}
	term    map[string]bool
	gone    map[string]chan struct{}
}

func newLogView() *logView {
	return &logView{
		readyCh: make(chan struct{}),
		specs:   map[string]struct{}{},
		term:    map[string]bool{},
		gone:    map[string]chan struct{}{},
	}
}

// setSpecs is one reconcile's spec set. The first call marks the view ready.
// An id that disappeared closes its watch. A spec that comes back is not
// terminal until setPhase says so, so a reused session id does not inherit
// the previous final (E10-D007).
func (v *logView) setSpecs(specs map[string]struct{}) {
	if v == nil {
		return
	}
	if specs == nil {
		specs = map[string]struct{}{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for id, ch := range v.gone {
		if _, ok := specs[id]; ok {
			continue
		}
		close(ch)
		delete(v.gone, id)
	}
	for id := range v.term {
		if _, ok := specs[id]; !ok {
			delete(v.term, id)
		}
	}
	v.specs = maps.Clone(specs)
	if !v.ready {
		v.ready = true
		close(v.readyCh)
	}
}

// setPhase records whether id's local phase is terminal. Terminal is not
// absorbing: a later non-terminal phase, such as RESTARTING, clears it
// (E14-D024).
func (v *logView) setPhase(id string, terminal bool) {
	if v == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if terminal {
		v.term[id] = true
		return
	}
	delete(v.term, id)
}

// present reports whether id's spec is in the latest view. ready is false
// until the first spec list.
func (v *logView) present(id string) (ok, ready bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.ready {
		return false, false
	}
	_, ok = v.specs[id]
	return ok, true
}

func (v *logView) terminal(id string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.term[id]
}

// waitReady blocks until the first spec list or ctx ends.
func (v *logView) waitReady(ctx context.Context) error {
	select {
	case <-v.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// watch is closed when id's spec leaves the view. Before the first list, or
// while the spec is present, the same channel is shared. Once the spec is
// already gone, the caller gets a fresh closed channel: a reused session id
// must not observe the previous close (E10-D007).
func (v *logView) watch(id string) <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.ready {
		if _, ok := v.specs[id]; !ok {
			ch := make(chan struct{})
			close(ch)
			return ch
		}
	}
	ch, ok := v.gone[id]
	if !ok {
		ch = make(chan struct{})
		v.gone[id] = ch
	}
	return ch
}
