package agent

import (
	"sync"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/agent/runner"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// sources is the spec, assignment, and status mirrors as one runner.Source.
// The runner's staleness, Synced, and Notify follow all three: status is how a
// restarted agent sees a terminal session (D945), and the spec's mod_revision
// is observed_generation (D947). The prefixes do not overlap.
type sources struct {
	spec, demand, status runner.Source

	once   sync.Once
	synced chan struct{}
	notify chan struct{}
}

func newSources(spec, demand, status runner.Source) *sources {
	return &sources{
		spec: spec, demand: demand, status: status,
		synced: make(chan struct{}), notify: make(chan struct{}, 1),
	}
}

// run fans the three mirrors' notifications in, until done. It closes Synced
// once every mirror has completed its first list.
func (s *sources) run(done <-chan struct{}) {
	go func() {
		for _, src := range []runner.Source{s.spec, s.demand, s.status} {
			select {
			case <-src.Synced():
			case <-done:
				return
			}
		}
		s.once.Do(func() { close(s.synced) })
	}()
	for {
		select {
		case <-done:
			return
		case <-s.spec.Notify():
		case <-s.demand.Notify():
		case <-s.status.Notify():
		}
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
}

func (s *sources) Synced() <-chan struct{} { return s.synced }
func (s *sources) Notify() <-chan struct{} { return s.notify }

// Snapshot is all three prefixes. Rev is the newest.
func (s *sources) Snapshot() state.View {
	parts := []state.View{s.spec.Snapshot(), s.demand.Snapshot(), s.status.Snapshot()}
	n := 0
	for _, p := range parts {
		n += len(p.Entries)
	}
	out := state.View{Entries: make(map[string]state.Entry, n)}
	for _, p := range parts {
		if p.Rev > out.Rev {
			out.Rev = p.Rev
		}
		for k, v := range p.Entries {
			out.Entries[k] = v
		}
	}
	return out
}

// LastContact is the oldest of the three. Zero, from a mirror that has not
// listed yet, makes the whole copy stale.
func (s *sources) LastContact() time.Time {
	t := s.spec.LastContact()
	for _, src := range []runner.Source{s.demand, s.status} {
		if c := src.LastContact(); c.Before(t) {
			t = c
		}
	}
	return t
}
