package api

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// agentMemory is the last AgentRecord this process saw for each agent key (D1096).
// Entries are never removed: after the election key disappears the response still
// carries that copy and marks it last known. A process that never saw the key
// reports the agent absent.
type agentMemory struct {
	mu sync.Mutex
	m  map[string]*statev1.AgentRecord
}

func (a *agentMemory) note(key string, rec *statev1.AgentRecord) {
	if a == nil || rec == nil || key == "" {
		return
	}
	cloned, ok := proto.Clone(rec).(*statev1.AgentRecord)
	if !ok || cloned == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.m == nil {
		a.m = map[string]*statev1.AgentRecord{}
	}
	a.m[key] = cloned
}

func (a *agentMemory) last(key string) *statev1.AgentRecord {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	rec := a.m[key]
	if rec == nil {
		return nil
	}
	cloned, ok := proto.Clone(rec).(*statev1.AgentRecord)
	if !ok {
		return nil
	}
	return cloned
}

func (s *Server) rememberAgents(views map[string]state.View) {
	for _, view := range views {
		for key, e := range view.Entries {
			s.noteAgentValue(key, e.Value)
		}
	}
}

func (s *Server) noteAgentValue(key string, val []byte) {
	plane, inst, err := state.Owner(key)
	if err != nil || key != state.AgentKey(plane, inst) {
		return
	}
	rec := &statev1.AgentRecord{}
	if proto.Unmarshal(val, rec) != nil {
		return
	}
	s.agents.note(key, rec)
}

// agentOf is the AgentRecord readers return. available is false when the
// instance's region is not in this response: the object is omitted, and memory
// is not consulted. A live key is the current value. A missing key with memory
// is that copy, marked last known. Neither is an absent agent.
func (s *Server) agentOf(view state.View, available bool, plane state.Plane, inst string) *apiv1.AgentStatus {
	if !available {
		return nil
	}
	key := state.AgentKey(plane, inst)
	if e, ok := view.Entries[key]; ok {
		rec := &statev1.AgentRecord{}
		if proto.Unmarshal(e.Value, rec) != nil {
			return nil
		}
		s.agents.note(key, rec)
		return agentStatus(rec, false)
	}
	if rec := s.agents.last(key); rec != nil {
		return agentStatus(rec, true)
	}
	return nil
}

// agentMissing reports that the provider's region is available and its agent
// key is not. An unknown instance or an unavailable region is not "missing":
// the caller keeps the ack it has (D1036).
func (s *Server) agentMissing(views map[string]state.View, plane state.Plane, inst string) bool {
	region, err := s.cfg.Topology.RegionOf(inst)
	if err != nil {
		return false
	}
	view, ok := views[region]
	if !ok {
		return false
	}
	_, present := view.Entries[state.AgentKey(plane, inst)]
	return !present
}

func agentStatus(rec *statev1.AgentRecord, last bool) *apiv1.AgentStatus {
	if rec == nil {
		return nil
	}
	out := &apiv1.AgentStatus{
		Epoch:     rec.GetEpoch(),
		Release:   rec.GetRelease(),
		StartedAt: time.Unix(0, rec.GetStartedAtNs()).UTC(),
	}
	if rec.GetHost() != "" {
		host := rec.GetHost()
		out.Host = &host
	}
	if rec.GetWorkDir() != "" {
		dir := rec.GetWorkDir()
		out.WorkDir = &dir
	}
	if last {
		known := true
		out.LastKnown = &known
	}
	return out
}
