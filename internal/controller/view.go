package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
)

// View is one reading of the regions the controller watches (E13-D020).
//
// Regions do not share a moment: logic must not treat a key in one region and
// a key in another as one commit. A region with Synced false has not finished
// its first list; it is stale, and nothing is written there (E13-D018). A stale
// region is not subtracted from (E13-D006): no demand is deleted because of it,
// and no session is ended because of an ack it shows. Undecodable lists keys
// whose values did not decode; logic does not write or delete those keys
// (E13-D021).
//
// Assemble fills every map, so a view it built has no nil maps.
type View struct {
	Regions  map[string]Region
	Sessions map[string]Session // SessionRef(sts instance, session id)
	Demands  map[string]Demand  // etcd key of an MD or TD demand entry
	Acks     map[string]Ack     // etcd key; only acks whose demand decoded
	Workers  map[string]Worker  // etcd key of an STS session worker
	Agents   map[string]Agent   // instance name
	// Retired is the set of instance names whose /retired/{inst} key is present
	// in that instance's home region (E12-D034). Nil and empty mean no instance
	// is retired: derivation and lifecycle then behave as they do without the key.
	Retired     map[string]struct{}
	Undecodable []string // sorted
}

// Region is one cluster's reading (E13-D020). Rev is how far every instance
// prefix in the region is known: the oldest mirror's revision, since the
// mirrors share one cluster's revision space. Rev is 0 when any mirror has
// not finished its first list, because a number from the others would not
// cover the prefixes still missing.
type Region struct {
	Rev    int64
	Stale  bool
	Synced bool
}

// Session is what one STS instance shows for one session id.
type Session struct {
	ID       string
	Instance string
	Region   string
	Spec     *sessionv1.SessionSpec
	SpecRev  int64 // mod revision of the spec key; 0 when Spec is nil. Generation is this (W3).
	Demand   *statev1.SessionDemand
	Status   *statev1.SessionStatus
	Worker   *Worker // the session worker, when its WorkerStatus is present
}

// Demand is one MD or TD demand entry.
type Demand struct {
	Key      string
	Region   string
	Instance string
	Plane    string // md or td
	Session  string
	Entry    string // feed/{feed key}, select/{name}, or the api id
	Value    *statev1.DemandEntry
}

// Ack is a provider's EntryStatus for a demand that is in the view.
// LastKnown is set when that provider's AgentRecord is absent: the value is
// the last one it wrote (PROTOCOL §3.5, W7).
type Ack struct {
	Key       string
	Region    string
	Instance  string
	Plane     string
	Session   string
	Entry     string
	Value     *statev1.EntryStatus
	LastKnown bool
}

// Worker is one STS session worker's WorkerStatus. LastKnown is set when that
// STS instance's AgentRecord is absent (W7). Session is empty when the hash
// matches no session on the instance; the worker is still in the view.
type Worker struct {
	Key       string
	Region    string
	Instance  string
	Hash      string
	Session   string
	Status    *statev1.WorkerStatus
	LastKnown bool
}

// Agent is an instance's AgentRecord. The key is absent when the record is
// not here.
type Agent struct {
	Instance string
	Region   string
	Plane    string
	Record   *statev1.AgentRecord
}

// SessionRef is the Sessions key: the STS instance and the session id.
func SessionRef(instance, id string) string { return instance + "/" + id }

// Snapshot is one instance mirror after classification. The daemon fills it;
// Assemble does not decode.
type Snapshot struct {
	Plane    string
	Instance string
	Region   string
	Rev      int64
	Synced   bool
	// LastContact is the mirror's. Zero before the first list.
	LastContact time.Time

	Specs          []SpecIn
	Statuses       []StatusIn
	SessionDemands []SessionDemandIn
	Workers        []WorkerIn
	Demands        []DemandIn
	Acks           []AckIn
	Agent          *statev1.AgentRecord // nil when the key is absent or did not decode
	Undecodable    []string
}

// SpecIn is one decoded session spec.
type SpecIn struct {
	Session string
	Rev     int64
	Value   *sessionv1.SessionSpec
}

// StatusIn is one decoded session status.
type StatusIn struct {
	Session string
	Value   *statev1.SessionStatus
}

// SessionDemandIn is one decoded STS assignment.
type SessionDemandIn struct {
	Session string
	Value   *statev1.SessionDemand
}

// WorkerIn is one decoded STS WorkerStatus. Hash is the key's last segment.
type WorkerIn struct {
	Key   string
	Hash  string
	Value *statev1.WorkerStatus
}

// DemandIn is one decoded MD or TD demand entry.
type DemandIn struct {
	Key     string
	Session string
	Entry   string
	Value   *statev1.DemandEntry
}

// AckIn is one decoded ack. DemandKey is the demand entry it answers.
type AckIn struct {
	Key       string
	DemandKey string
	Session   string
	Entry     string
	Value     *statev1.EntryStatus
}

// Assemble builds the global view. staleAfter is the LastContact threshold:
// a synced mirror is stale when it has no contact yet, or the contact is
// older than staleAfter (超過門檻, same as the agent's Demand.Stale). A region
// is stale when any of its mirrors is. now is the clock the threshold is
// compared with.
func Assemble(now time.Time, staleAfter time.Duration, regions []string, snaps []Snapshot) View {
	v := View{
		Regions:  map[string]Region{},
		Sessions: map[string]Session{},
		Demands:  map[string]Demand{},
		Acks:     map[string]Ack{},
		Workers:  map[string]Worker{},
		Agents:   map[string]Agent{},
		Retired:  map[string]struct{}{},
	}
	v.Regions = assembleRegions(now, staleAfter, regions, snaps)

	byInst := map[string]map[string]*Session{} // instance -> session id -> session
	for _, s := range snaps {
		byID := byInst[s.Instance]
		if byID == nil {
			byID = map[string]*Session{}
			byInst[s.Instance] = byID
		}
		touch := func(id string) *Session {
			sess := byID[id]
			if sess == nil {
				sess = &Session{ID: id, Instance: s.Instance, Region: s.Region}
				byID[id] = sess
			}
			return sess
		}
		for _, spec := range s.Specs {
			sess := touch(spec.Session)
			sess.Spec, sess.SpecRev = spec.Value, spec.Rev
		}
		for _, st := range s.Statuses {
			touch(st.Session).Status = st.Value
		}
		for _, d := range s.SessionDemands {
			touch(d.Session).Demand = d.Value
		}
		if s.Agent != nil {
			v.Agents[s.Instance] = Agent{
				Instance: s.Instance, Region: s.Region, Plane: s.Plane, Record: s.Agent,
			}
		}
		for _, d := range s.Demands {
			v.Demands[d.Key] = Demand{
				Key: d.Key, Region: s.Region, Instance: s.Instance, Plane: s.Plane,
				Session: d.Session, Entry: d.Entry, Value: d.Value,
			}
		}
	}
	for _, byID := range byInst {
		for id, sess := range byID {
			v.Sessions[SessionRef(sess.Instance, id)] = *sess
		}
	}

	for _, s := range snaps {
		known := s.Agent != nil
		byHash := map[string]string{} // worker hash -> session id, on this instance
		for id := range byInst[s.Instance] {
			byHash[workerHash(id)] = id
		}
		for _, in := range s.Workers {
			w := Worker{
				Key: in.Key, Region: s.Region, Instance: s.Instance, Hash: in.Hash,
				Status: in.Value, LastKnown: !known,
			}
			if id, ok := byHash[in.Hash]; ok {
				w.Session = id
			}
			v.Workers[w.Key] = w
			if w.Session == "" {
				continue
			}
			ref := SessionRef(s.Instance, w.Session)
			sess := v.Sessions[ref]
			sess.Worker = workerPtr(w)
			v.Sessions[ref] = sess
		}
		for _, a := range s.Acks {
			if _, ok := v.Demands[a.DemandKey]; !ok {
				continue // an ack with no demand item stays out of the view
			}
			v.Acks[a.Key] = Ack{
				Key: a.Key, Region: s.Region, Instance: s.Instance, Plane: s.Plane,
				Session: a.Session, Entry: a.Entry, Value: a.Value, LastKnown: !known,
			}
		}
	}

	bad := map[string]struct{}{}
	for _, s := range snaps {
		for _, k := range s.Undecodable {
			bad[k] = struct{}{}
		}
	}
	v.Undecodable = make([]string, 0, len(bad))
	for k := range bad {
		v.Undecodable = append(v.Undecodable, k)
	}
	slices.Sort(v.Undecodable)
	return v
}

// workerPtr copies w onto the heap. The session holds that copy; the Workers
// map holds its own, and neither is mutated after Assemble returns.
func workerPtr(w Worker) *Worker { return &w }

func assembleRegions(now time.Time, staleAfter time.Duration, regions []string, snaps []Snapshot) map[string]Region {
	type acc struct {
		n      int
		synced int
		rev    int64
		revSet bool
		stale  bool
	}
	ag := map[string]acc{}
	for _, s := range snaps {
		a := ag[s.Region]
		a.n++
		if s.Synced {
			a.synced++
			if !a.revSet || s.Rev < a.rev {
				a.rev, a.revSet = s.Rev, true
			}
		}
		if !s.Synced || s.LastContact.IsZero() || now.Sub(s.LastContact) > staleAfter {
			a.stale = true
		}
		ag[s.Region] = a
	}
	names := map[string]struct{}{}
	for _, name := range regions {
		names[name] = struct{}{}
	}
	for name := range ag {
		names[name] = struct{}{}
	}
	out := make(map[string]Region, len(names))
	for name := range names {
		a := ag[name]
		synced := a.n == 0 || a.synced == a.n
		rev := int64(0)
		if synced && a.n > 0 {
			rev = a.rev
		}
		out[name] = Region{Rev: rev, Synced: synced, Stale: !synced || a.stale}
	}
	return out
}

// workerHash is h for the session worker, hex(sha256("sts/"+id))[:16]
// (PROTOCOL §2, E13-D009). It matches state.WorkerHash; controller does not
// import state (E13-D001).
func workerHash(sessionID string) string {
	sum := sha256.Sum256([]byte("sts/" + sessionID))
	return hex.EncodeToString(sum[:8])
}
