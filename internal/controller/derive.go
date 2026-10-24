package controller

import (
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
)

// Plane labels in the deployment JSON (E13-D017, E13-D027). They match state.Plane.
// This package does not import state (E13-D001).
const (
	planeMD = "md"
	planeTD = "td"
)

// endInvalid is SessionDemand.end_reason for a spec N1 will not run (E13-D004).
const endInvalid = "invalid_spec"

// keyRoot is state.Root. Key helpers below match state.SessionDemandKey,
// MDDemandKey and TDDemandKey.
const keyRoot = "/mftiko/v1"

// Placement is one instance in the deployment: the plane label and the region
// the instance's keys live in (E13-D007, E13-D017).
type Placement struct {
	Plane  string
	Region string
}

// Write is one key to put. Derivation does not delete; removal is N4 and N5.
type Write struct {
	Key   string
	Value proto.Message
}

// Drift is a session whose spec revision has left the revision the assignment
// was derived from (E13-D022). The stored demands are kept.
type Drift struct {
	Instance string
	Session  string
	SpecRev  int64 // spec key mod revision
	Have     int64 // SessionDemand.spec_revision
}

// Plan is what one reconcile should put, and which sessions were left as stored.
type Plan struct {
	Writes []Write
	Drifts []Drift
}

// Derive plans the puts that bring etcd into line with N1 (E09-D001, E13-D002–E13-D004,
// E13-D008, E13-D022–E13-D027).
//
// A non-terminal RUNNING spec yields one SessionDemand plus one demand entry
// per feed, selector and account. start is false and binding statuses are
// empty (E13-D024); the gate fills those. An already-ENDED spec writes nothing
// (E13-D026). A spec whose revision differs from the assignment's spec_revision
// is left as stored (E13-D022). Comparison ignores SessionDemand.updated_at_ns
// and DemandEntry.spec_revision (E13-D023).
//
// now is the wall clock. The first assignment records it in issued_at_ns and
// later writes copy that value (E13-D002). A put that differs also records it in
// updated_at_ns; an equal value is not put, so the clock alone never writes.
// An assignment already ENDED is left as stored (E13-D028).
//
// A region that is not synced, or is stale, is not written (E13-D006, E13-D018).
// A session whose own region is in that state is skipped entirely, so a stale
// spec is not pushed into a region that is still fresh. An undecodable
// assignment is not rewritten and its session is not extended (E13-D021).
func Derive(v View, inst map[string]Placement, now time.Time) Plan {
	if len(v.Sessions) == 0 {
		return Plan{}
	}
	b := builder{
		v: v, inst: inst, now: now.UnixNano(),
		bad: make(map[string]struct{}, len(v.Undecodable)),
	}
	for _, k := range v.Undecodable {
		b.bad[k] = struct{}{}
	}
	for _, ref := range slices.Sorted(maps.Keys(v.Sessions)) {
		b.session(v.Sessions[ref])
	}
	slices.SortFunc(b.plan.Writes, func(a, b Write) int { return strings.Compare(a.Key, b.Key) })
	slices.SortFunc(b.plan.Drifts, func(a, b Drift) int {
		if c := strings.Compare(a.Instance, b.Instance); c != 0 {
			return c
		}
		return strings.Compare(a.Session, b.Session)
	})
	return b.plan
}

type builder struct {
	v    View
	inst map[string]Placement
	now  int64
	bad  map[string]struct{}
	plan Plan
}

func (b *builder) session(sess Session) {
	if sess.Spec == nil || terminal(sess.Status) || !b.open(sess.Region) {
		return
	}
	if _, undecodable := b.bad[sessionDemandKey(sess.Instance, sess.ID)]; undecodable {
		return
	}
	// Ending is irreversible. An assignment already ENDED is not derived
	// back to RUNNING, so ack recovery and a spec that runs again do not
	// revive it (E13-D028).
	if sess.Demand.GetDesired() == sessionv1.DesiredState_DESIRED_STATE_ENDED {
		return
	}
	// ENDED is not derived. No assignment yet means write nothing; an
	// assignment that exists is ended by the gate, not here (E13-D026).
	if sess.Spec.GetDesiredState() == sessionv1.DesiredState_DESIRED_STATE_ENDED {
		return
	}
	if sess.Spec.GetDesiredState() != sessionv1.DesiredState_DESIRED_STATE_RUNNING {
		return
	}
	// The revision moved, so this is not a desired_state-only edit. Keep the
	// stored demands and do not end the session as invalid_spec (E13-D022).
	if sess.Demand != nil && sess.Demand.GetSpecRevision() != sess.SpecRev {
		b.plan.Drifts = append(b.plan.Drifts, Drift{
			Instance: sess.Instance, Session: sess.ID,
			SpecRev: sess.SpecRev, Have: sess.Demand.GetSpecRevision(),
		})
		return
	}
	if specInvalid(sess.Spec, b.inst) {
		b.putSession(sess, &statev1.SessionDemand{
			Desired:      sessionv1.DesiredState_DESIRED_STATE_ENDED,
			EndReason:    endInvalid,
			SpecRevision: sess.SpecRev,
		})
		return
	}
	b.running(sess)
}

func (b *builder) running(sess Session) {
	spec := sess.Spec
	var bindings []*statev1.Binding
	for _, f := range spec.GetFeeds() {
		b.putEntry(mdDemandKey(f.GetMdInstance(), sess.ID, "feed/"+f.GetFeed()), b.inst[f.GetMdInstance()].Region, &statev1.DemandEntry{
			Target:       &statev1.DemandEntry_Feed{Feed: f.GetFeed()},
			SpecRevision: sess.SpecRev,
		})
		bindings = append(bindings, &statev1.Binding{
			Target:   &statev1.Binding_Feed{Feed: f.GetFeed()},
			Instance: f.GetMdInstance(),
		})
	}
	for _, s := range spec.GetSelectors() {
		b.putEntry(mdDemandKey(s.GetMdInstance(), sess.ID, "select/"+s.GetName()), b.inst[s.GetMdInstance()].Region, &statev1.DemandEntry{
			Target:       &statev1.DemandEntry_Selector{Selector: proto.Clone(s).(*sessionv1.SelectorDecl)},
			SpecRevision: sess.SpecRev,
		})
		bindings = append(bindings, &statev1.Binding{
			Target:   &statev1.Binding_Selector{Selector: s.GetName()},
			Instance: s.GetMdInstance(),
		})
	}
	for _, a := range spec.GetAccounts() {
		b.putEntry(tdDemandKey(a.GetTdInstance(), sess.ID, a.GetApiId()), b.inst[a.GetTdInstance()].Region, &statev1.DemandEntry{
			Target:       &statev1.DemandEntry_ApiId{ApiId: a.GetApiId()},
			SpecRevision: sess.SpecRev,
			AlgoIds:      slices.Clone(a.GetAlgoIds()),
		})
		bindings = append(bindings, &statev1.Binding{
			Target:   &statev1.Binding_ApiId{ApiId: a.GetApiId()},
			Instance: a.GetTdInstance(),
		})
	}
	b.putSession(sess, &statev1.SessionDemand{
		Desired:      sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		SpecRevision: sess.SpecRev,
		Bindings:     bindings,
	})
}

// putSession copies issued_at_ns off an assignment that already has one, and
// skips the put when the value already matches (E13-D002, E13-D008).
func (b *builder) putSession(sess Session, derived *statev1.SessionDemand) {
	have := sess.Demand
	if have != nil && have.GetIssuedAtNs() != 0 {
		derived.IssuedAtNs = have.GetIssuedAtNs()
	} else {
		derived.IssuedAtNs = b.now
	}
	if have != nil && sameSession(derived, have) {
		return
	}
	derived.UpdatedAtNs = b.now
	b.put(sessionDemandKey(sess.Instance, sess.ID), sess.Region, derived)
}

func (b *builder) putEntry(key, region string, derived *statev1.DemandEntry) {
	if have := b.v.Demands[key].Value; have != nil && sameEntry(derived, have) {
		return
	}
	b.put(key, region, derived)
}

func (b *builder) put(key, region string, msg proto.Message) {
	if _, no := b.bad[key]; no || !b.open(region) {
		return
	}
	b.plan.Writes = append(b.plan.Writes, Write{Key: key, Value: proto.Clone(msg)})
}

func (b *builder) open(region string) bool {
	r, ok := b.v.Regions[region]
	return ok && r.Synced && !r.Stale
}

func terminal(st *statev1.SessionStatus) bool {
	if st == nil {
		return false
	}
	return st.GetPhase() == statev1.Phase_PHASE_DONE || st.GetPhase() == statev1.Phase_PHASE_FAILED
}

// specInvalid reports a spec N1 refuses (E13-D004, E13-D027). A duplicate feed is the
// same feed key, a duplicate selector is the same name, and a duplicate
// account is the same api_id. An instance is usable when its plane label is
// md for a market-data decl and td for an account.
func specInvalid(spec *sessionv1.SessionSpec, inst map[string]Placement) bool {
	feeds := map[string]struct{}{}
	for _, f := range spec.GetFeeds() {
		if f == nil || !first(feeds, f.GetFeed()) || inst[f.GetMdInstance()].Plane != planeMD {
			return true
		}
	}
	names := map[string]struct{}{}
	for _, s := range spec.GetSelectors() {
		if s == nil || !first(names, s.GetName()) || inst[s.GetMdInstance()].Plane != planeMD {
			return true
		}
	}
	accts := map[int64]struct{}{}
	for _, a := range spec.GetAccounts() {
		if a == nil {
			return true
		}
		if _, dup := accts[a.GetApiId()]; dup || inst[a.GetTdInstance()].Plane != planeTD {
			return true
		}
		accts[a.GetApiId()] = struct{}{}
	}
	return false
}

func first(seen map[string]struct{}, key string) bool {
	if _, ok := seen[key]; ok {
		return false
	}
	seen[key] = struct{}{}
	return true
}

func sameSession(derived, have *statev1.SessionDemand) bool {
	a := proto.Clone(derived).(*statev1.SessionDemand)
	b := proto.Clone(have).(*statev1.SessionDemand)
	a.UpdatedAtNs, b.UpdatedAtNs = 0, 0
	return proto.Equal(a, b)
}

func sameEntry(derived, have *statev1.DemandEntry) bool {
	a := proto.Clone(derived).(*statev1.DemandEntry)
	b := proto.Clone(have).(*statev1.DemandEntry)
	a.SpecRevision, b.SpecRevision = 0, 0
	return proto.Equal(a, b)
}

func sessionDemandKey(inst, id string) string {
	return keyRoot + "/sts/" + inst + "/demand/" + id
}

func mdDemandKey(inst, session, entry string) string {
	return keyRoot + "/md/" + inst + "/demand/" + session + "/" + entry
}

func tdDemandKey(inst, session string, apiID int64) string {
	return keyRoot + "/td/" + inst + "/demand/" + session + "/" + strconv.FormatInt(apiID, 10)
}
