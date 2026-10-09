package runner

import (
	"context"
	"fmt"
	"maps"
	"slices"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// The phases of WorkerStatus.
const (
	PhaseStarting = "STARTING"
	PhaseRunning  = "RUNNING"
	PhaseStopping = "STOPPING"
	PhaseBackoff  = "BACKOFF"
	PhaseExited   = "EXITED"
	PhaseLost     = "LOST"
)

// Lister reads the agent's own worker/ prefix: a state.Reader of its region.
type Lister interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

// writer is the term's fenced writes.
type writer interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) error
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error
}

// statuses writes the WorkerStatus of each worker, only when it changes (PROTOCOL §3.2 W7.2): it remembers what
// each key holds, from the list at startup and its own writes since.
type statuses struct {
	w     writer
	plane state.Plane
	inst  string
	held  map[string]string // etcd key -> the value it holds
}

func newStatuses(w writer, p state.Plane, inst string) *statuses {
	return &statuses{w: w, plane: p, inst: inst, held: map[string]string{}}
}

// load lists what the worker/ prefix holds now (W7.3).
func (s *statuses) load(ctx context.Context, r Lister) error {
	resp, err := r.Get(ctx, state.WorkerStatusPrefix(s.plane, s.inst), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("agent: list worker statuses: %w", err)
	}
	clear(s.held)
	for _, kv := range resp.Kvs {
		s.held[string(kv.Key)] = string(kv.Value)
	}
	return nil
}

func (s *statuses) key(k agent.WorkerKey) string {
	return state.WorkerStatusKey(s.plane, s.inst, state.WorkerHash(string(k)))
}

// sync makes etcd hold want, one key at a time, writing only what differs: a key it holds that want lacks is
// deleted unless keep says to leave it. The first failure stops it; what was not written is tried at the next sync.
func (s *statuses) sync(ctx context.Context, want map[string]string, keep func(key string) bool) error {
	for _, k := range slices.Sorted(maps.Keys(want)) {
		v := want[k]
		if cur, ok := s.held[k]; ok && cur == v {
			continue
		}
		if err := s.w.Put(ctx, k, v); err != nil {
			return fmt.Errorf("agent: write %s: %w", k, err)
		}
		s.held[k] = v
	}
	for _, k := range slices.Sorted(maps.Keys(s.held)) {
		if _, ok := want[k]; ok || keep(k) {
			continue
		}
		if err := s.w.Delete(ctx, k); err != nil {
			return fmt.Errorf("agent: delete %s: %w", k, err)
		}
		delete(s.held, k)
	}
	return nil
}

// statusOf is the WorkerStatus of an observed worker. planned and gaveUp say what an ended one waits for: a
// restart (BACKOFF) or nothing (EXITED).
func statusOf(o agent.Observed, planned, gaveUp bool, restarts uint32) *statev1.WorkerStatus {
	st := &statev1.WorkerStatus{
		Worker:      &commonv1.WorkerRef{Key: string(o.Key), Incarnation: o.Incarnation},
		Stage:       o.Stage,
		Release:     o.Release,
		ShimRelease: o.ShimRelease,
		Restarts:    restarts,
		LastExit:    o.Exit,
	}
	if !o.StartedAt.IsZero() {
		st.StartedAtNs = o.StartedAt.UnixNano()
	}
	switch o.State {
	case agent.StateInitializing, agent.StateInitialized, agent.StateStarting:
		st.Phase = PhaseStarting
	case agent.StateRunning:
		st.Phase = PhaseRunning
	case agent.StateStopping:
		st.Phase = PhaseStopping
	case agent.StateExited:
		st.Phase = PhaseExited
		if planned && !gaveUp {
			st.Phase = PhaseBackoff
		}
	case agent.StateLost:
		st.Phase = PhaseLost
	}
	return st
}

func encode(m proto.Message) string {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("agent: encode %T: %v", m, err)) // not reached: the messages are built here
	}
	return string(b)
}
