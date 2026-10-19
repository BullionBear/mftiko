package agent

import (
	"maps"
	"slices"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// demandView is the assignment fields the plan reads.
type demandView struct {
	Desired   sessionv1.DesiredState
	Start     bool
	EndReason string
	IssuedAt  int64
	Bindings  []*statev1.Binding
}

// statusView is the phase and reason already published, in memory or in etcd.
type statusView struct {
	Phase  statev1.Phase
	Reason string
}

// sess is one session as of this commit: the three mirrors, the status this
// process published, and the assignment remembered after its key disappeared.
type sess struct {
	spec          *Spec
	parsed        *sessionv1.SessionSpec
	specErr       error
	specPresent   bool
	demand        *demandView
	demandErr     error
	demandPresent bool
	mirrored      *statusView
	local         *statusView
	remembered    *demandView
	observed      *agent.Observed
}

func (s *sess) status() *statusView {
	if s.local != nil {
		return s.local
	}
	return s.mirrored
}

func (s *sess) terminal() bool {
	st := s.status()
	return st != nil && (st.Phase == statev1.Phase_PHASE_DONE || st.Phase == statev1.Phase_PHASE_FAILED)
}

// demandNow is the assignment to act on. current is false when the key is gone
// and the value is the one remembered from the last time it was present (N4).
func (s *sess) demandNow() (demandView, bool, bool) {
	if s.demandPresent {
		if s.demand == nil {
			return demandView{}, true, false
		}
		return *s.demand, true, true
	}
	if s.remembered != nil {
		return *s.remembered, false, true
	}
	return demandView{}, false, false
}

// kind is what this round does with one session.
type kind int

const (
	kindNone kind = iota
	kindPlan
	kindConflict
	kindEnded
	kindFailed // the session ends FAILED: artifact rejection, or volume_over_limit (D958, D974, D976)
)

// intent is a session whose status this round may publish.
type intent struct {
	Kind       kind
	Start      bool
	EndReason  string
	FailReason string // kindFailed: an artifact reason (D928, D954)
	Rev        int64
	SpecHash   string
}

type specErr struct {
	ID  string
	Err error
}

// decision is one round with no I/O.
type decision struct {
	Plans   map[agent.WorkerKey]agent.Plan[Spec, Live]
	Intents map[string]intent
	Missing []string // assignment, no spec key
	Bad     []specErr
	Changed []string // in the plan, and the spec encoding changed
	Hashes  map[string]string
}

type anchor struct {
	id     string
	issued int64
	parsed *sessionv1.SessionSpec
}

type candidate struct {
	id     string
	issued int64
	start  bool
	spec   Spec
	parsed *sessionv1.SessionSpec
}

// buildViews joins the three prefixes. local overrides the mirrored status so a
// terminal write counts before the mirror has it (D945). remembered is the last
// assignment this process saw, used once the key is gone. Spec bytes are copied;
// the mirror's buffer is not modified.
func buildViews(inst string, specV, demandV, statusV state.View, local map[string]statusView, remembered map[string]demandView, observed map[agent.WorkerKey]agent.Observed) map[string]*sess {
	ids := map[string]struct{}{}
	take := func(prefix string, entries map[string]state.Entry) {
		for key := range entries {
			if id, ok := idFrom(prefix, key); ok {
				ids[id] = struct{}{}
			}
		}
	}
	take(state.SessionSpecPrefix(inst), specV.Entries)
	take(state.DemandPrefix(state.PlaneSTS, inst), demandV.Entries)
	take(state.SessionStatusPrefix(inst), statusV.Entries)
	for id := range remembered {
		ids[id] = struct{}{}
	}
	for key := range observed {
		if id, ok := idFrom("sts/", string(key)); ok {
			ids[id] = struct{}{}
		}
	}
	out := make(map[string]*sess, len(ids))
	for id := range ids {
		s := &sess{}
		if e, ok := specV.Entries[state.SessionSpecKey(inst, id)]; ok {
			s.specPresent = true
			raw := append([]byte(nil), e.Value...)
			msg, err := parseSpec(raw)
			if err != nil {
				s.specErr = err
			} else {
				s.spec = &Spec{Raw: raw, Rev: e.ModRevision}
				s.parsed = msg
			}
		}
		if e, ok := demandV.Entries[state.SessionDemandKey(inst, id)]; ok {
			s.demandPresent = true
			msg, err := parseDemand(e.Value)
			if err != nil {
				s.demandErr = err
			} else {
				s.demand = &demandView{
					Desired: msg.GetDesired(), Start: msg.GetStart(),
					EndReason: msg.GetEndReason(), IssuedAt: msg.GetIssuedAtNs(),
					Bindings: cloneDemandBindings(msg.GetBindings()),
				}
			}
		}
		if e, ok := statusV.Entries[state.SessionStatusKey(inst, id)]; ok {
			if msg, err := parseStatus(e.Value); err == nil {
				s.mirrored = &statusView{Phase: msg.GetPhase(), Reason: msg.GetReason()}
			}
		}
		if st, ok := local[id]; ok {
			s.local = &st
		}
		if d, ok := remembered[id]; ok && !s.demandPresent {
			s.remembered = &d
		}
		if o, ok := observed[WorkerKey(id)]; ok {
			s.observed = &o
		}
		out[id] = s
	}
	return out
}

// decide is the plan: which sessions spawn, which end, which are algo_conflict.
// A terminal session is left alone (D945). A session with no usable spec is not
// planned and gets no status (D944). Only a desired of RUNNING enters the plan,
// and only when no earlier non-terminal session on this instance overlaps it
// (D916, D917). An assignment that is ENDED or gone is not a conflict; it still
// occupies its algos until it is terminal.
func decide(sessions map[string]*sess, prevHash map[string]string) decision {
	out := decision{
		Plans:   map[agent.WorkerKey]agent.Plan[Spec, Live]{},
		Intents: map[string]intent{},
		Hashes:  maps.Clone(prevHash),
	}
	if out.Hashes == nil {
		out.Hashes = map[string]string{}
	}
	var anchors []anchor
	var candidates []candidate
	for _, id := range sortedIDs(sessions) {
		s := sessions[id]
		if s.terminal() {
			continue
		}
		if s.spec == nil {
			if !complainMissing(s) {
				continue
			}
			if s.specErr != nil {
				out.Bad = append(out.Bad, specErr{ID: id, Err: s.specErr})
			} else {
				out.Missing = append(out.Missing, id)
			}
			continue
		}
		// The key is present but will not decode: no plan and no status (D944's
		// sibling for the assignment). The remembered value is not a stand-in.
		if s.demandPresent && s.demand == nil {
			continue
		}
		d, current, ok := s.demandNow()
		if !ok {
			if s.observed != nil || s.status() != nil {
				out.Intents[id] = intent{Kind: kindEnded, Rev: s.spec.Rev, SpecHash: s.spec.Hash()}
			}
			continue
		}
		item := intent{Rev: s.spec.Rev, SpecHash: s.spec.Hash()}
		if !current {
			item.Kind = kindEnded
			item.EndReason = d.EndReason
			out.Intents[id] = item
			anchors = append(anchors, anchor{id: id, issued: d.IssuedAt, parsed: s.parsed})
			continue
		}
		switch d.Desired {
		case sessionv1.DesiredState_DESIRED_STATE_RUNNING:
			anchors = append(anchors, anchor{id: id, issued: d.IssuedAt, parsed: s.parsed})
			candidates = append(candidates, candidate{
				id: id, issued: d.IssuedAt, start: d.Start, spec: *s.spec, parsed: s.parsed,
			})
		case sessionv1.DesiredState_DESIRED_STATE_ENDED:
			item.Kind = kindEnded
			item.EndReason = d.EndReason
			out.Intents[id] = item
			anchors = append(anchors, anchor{id: id, issued: d.IssuedAt, parsed: s.parsed})
		default:
			// UNSPECIFIED is not RUNNING and not ENDED: no spawn, no status.
			// It is still a non-terminal session, so it occupies its algos.
			anchors = append(anchors, anchor{id: id, issued: d.IssuedAt, parsed: s.parsed})
		}
	}
	for _, c := range candidates {
		if blocked(c, anchors) {
			out.Intents[c.id] = intent{Kind: kindConflict, Rev: c.spec.Rev, SpecHash: c.spec.Hash()}
			continue
		}
		out.Plans[WorkerKey(c.id)] = agent.Plan[Spec, Live]{Spawn: c.spec, Start: c.start}
		out.Intents[c.id] = intent{Kind: kindPlan, Start: c.start, Rev: c.spec.Rev, SpecHash: c.spec.Hash()}
		if old, ok := prevHash[c.id]; ok && old != c.spec.Hash() {
			out.Changed = append(out.Changed, c.id)
		}
		out.Hashes[c.id] = c.spec.Hash()
	}
	return out
}

// complainMissing reports whether a session that has no usable spec should be
// logged. A spec with no assignment, no status, and no worker is left untouched
// (D916). Anything else was assigned, or still has a worker, and is waiting.
func complainMissing(s *sess) bool {
	if s.demandPresent || s.remembered != nil || s.observed != nil {
		return true
	}
	return s.status() != nil
}

func blocked(c candidate, anchors []anchor) bool {
	for _, a := range anchors {
		if a.id == c.id || !overlaps(c.parsed, a.parsed) {
			continue
		}
		if earlier(a.id, a.issued, c.id, c.issued) {
			return true
		}
	}
	return false
}

// earlier reports whether a was assigned before b. Equal issued_at_ns breaks
// toward the smaller session id (D917).
func earlier(idA string, atA int64, idB string, atB int64) bool {
	if atA != atB {
		return atA < atB
	}
	return idA < idB
}

// overlaps reports whether two specs share an api_id and at least one algo_id.
func overlaps(a, b *sessionv1.SessionSpec) bool {
	if a == nil || b == nil {
		return false
	}
	for _, aa := range a.GetAccounts() {
		for _, bb := range b.GetAccounts() {
			if aa.GetApiId() == bb.GetApiId() && intersect(aa.GetAlgoIds(), bb.GetAlgoIds()) {
				return true
			}
		}
	}
	return false
}

func intersect(a, b []uint32) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	set := make(map[uint32]struct{}, len(a))
	for _, x := range a {
		set[x] = struct{}{}
	}
	for _, y := range b {
		if _, ok := set[y]; ok {
			return true
		}
	}
	return false
}

func sortedIDs(sessions map[string]*sess) []string {
	return slices.Sorted(maps.Keys(sessions))
}

// remember keeps the last decoded assignment of every session whose key is
// present, and the previous memory of the ones whose key is gone.
func remember(sessions map[string]*sess, prev map[string]demandView) map[string]demandView {
	out := maps.Clone(prev)
	if out == nil {
		out = map[string]demandView{}
	}
	for id, s := range sessions {
		if s.demand != nil {
			out[id] = *s.demand
		}
	}
	return out
}
