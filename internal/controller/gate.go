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

// End reasons the gate writes on SessionDemand (N3). invalid_spec is N1's.
const (
	endBySpec      = "ended_by_spec"
	endDepsFailed  = "deps_failed"
	endDepsTimeout = "deps_timeout"
)

// defaultDepsTimeout is the deadline used when deps_timeout_s is 0 (D1302).
const defaultDepsTimeout = 60 * time.Second

// Gate fills binding statuses and start, and ends sessions (N2, N3,
// D1328–D1332). plan is what Derive decided this round. A SessionDemand put
// is replaced when statuses, start or the end change, and dropped when it
// would only wipe a value the gate already wrote. Demand-entry puts and
// drifts pass through.
//
// next is the earliest deps deadline still ahead, or the zero time when no
// session is waiting on dependencies (D1331). The daemon waits on it so a
// timeout does not sit until the full resync.
//
// An assignment that is already ENDED is left untouched, bindings included
// (D1328, D1330). A session region that is not synced, or is stale, is not
// written (D1306, D1318); its deadline is still reported while it is waiting.
// An ack from a stale region is copied into bindings and does not release
// start or end the session (D1332). Silence leaves the status unset. A missing
// provider AgentRecord is not a nack: the ack's value is used as it stands.
func Gate(v View, plan Plan, now time.Time) (Plan, time.Time) {
	g := newGate(v, plan, now)
	for _, ref := range slices.Sorted(maps.Keys(v.Sessions)) {
		g.session(v.Sessions[ref])
	}
	slices.SortFunc(g.out, func(a, b Write) int { return strings.Compare(a.Key, b.Key) })
	return Plan{Writes: g.out, Drifts: plan.Drifts}, g.next
}

type gate struct {
	v        View
	now      time.Time
	byDemand map[string]Ack
	bad      map[string]struct{}
	planned  map[string]*statev1.SessionDemand
	out      []Write
	next     time.Time
}

func newGate(v View, plan Plan, now time.Time) *gate {
	g := &gate{
		v: v, now: now,
		byDemand: indexAcks(v),
		bad:      make(map[string]struct{}, len(v.Undecodable)),
		planned:  map[string]*statev1.SessionDemand{},
	}
	for _, k := range v.Undecodable {
		g.bad[k] = struct{}{}
	}
	for _, w := range plan.Writes {
		d, ok := w.Value.(*statev1.SessionDemand)
		if !ok {
			g.out = append(g.out, w)
			continue
		}
		g.planned[w.Key] = d
	}
	return g
}

func (g *gate) session(sess Session) {
	key := sessionDemandKey(sess.Instance, sess.ID)
	if terminal(sess.Status) {
		return
	}
	if _, no := g.bad[key]; no {
		return
	}
	// Already ended: do not revive, and do not copy later acks (D1328, D1330).
	if sess.Demand.GetDesired() == sessionv1.DesiredState_DESIRED_STATE_ENDED {
		return
	}
	planned := g.planned[key]
	base := planned
	if base == nil {
		base = sess.Demand
	}
	if base == nil {
		return // nothing assigned yet; an ENDED spec writes nothing (D1326)
	}
	// N1 ended it this round (invalid_spec). Keep that reason.
	if base.GetDesired() == sessionv1.DesiredState_DESIRED_STATE_ENDED {
		g.out = append(g.out, Write{Key: key, Value: planned})
		return
	}
	if !g.open(sess.Region) {
		if !specEnded(sess) {
			g.arm(sess, base)
		}
		return
	}

	next := proto.Clone(base).(*statev1.SessionDemand)
	nack, allReady := g.fill(sess, next)
	// Derive's put carries an empty start bit. A start already granted stays
	// granted even when that put is the base and a later ack is not READY.
	if sess.Demand.GetStart() {
		next.Start = true
	}
	reason := ""
	switch {
	case specEnded(sess):
		reason = endBySpec
	case nack:
		reason = endDepsFailed
	case !next.GetStart() && !allReady && g.expired(sess, next):
		reason = endDepsTimeout
	}
	if reason == "" && allReady {
		next.Start = true
	}
	if reason != "" {
		next.Desired = sessionv1.DesiredState_DESIRED_STATE_ENDED
		next.EndReason = reason
	} else if !next.GetStart() {
		g.arm(sess, next)
	}

	stored := sess.Demand
	if stored != nil && sameSession(next, stored) {
		return
	}
	if planned != nil && sameSession(next, planned) {
		g.out = append(g.out, Write{Key: key, Value: planned})
		return
	}
	next.UpdatedAtNs = g.now.UnixNano()
	g.out = append(g.out, Write{Key: key, Value: next})
}

// fill copies each binding's ack. allReady is true when every binding has a
// fresh READY, which includes a session with no bindings. nack is a fresh
// REJECTED or DEAD. Stale acks are copied and count as neither (D1332).
func (g *gate) fill(sess Session, next *statev1.SessionDemand) (nack, allReady bool) {
	allReady = true
	for _, b := range next.GetBindings() {
		if b == nil {
			allReady = false
			continue
		}
		dk, ok := bindingDemandKey(sess.ID, b)
		ack, has := g.byDemand[dk]
		if !ok || !has || ack.Value == nil {
			b.Status = nil
			allReady = false
			continue
		}
		b.Status = proto.Clone(ack.Value).(*statev1.EntryStatus)
		fresh := g.open(ack.Region)
		switch ack.Value.GetState() {
		case statev1.EntryState_ENTRY_STATE_REJECTED, statev1.EntryState_ENTRY_STATE_DEAD:
			if fresh {
				nack = true
			}
			allReady = false
		case statev1.EntryState_ENTRY_STATE_READY:
			if !fresh {
				allReady = false
			}
		default:
			allReady = false
		}
	}
	return nack, allReady
}

// specEnded is a desired_state of ENDED, or a spec deleted before the session
// is terminal (D1303). The caller has already skipped terminal sessions.
func specEnded(sess Session) bool {
	if sess.Spec == nil {
		return true
	}
	return sess.Spec.GetDesiredState() == sessionv1.DesiredState_DESIRED_STATE_ENDED
}

func (g *gate) expired(sess Session, d *statev1.SessionDemand) bool {
	if d.GetStart() || d.GetIssuedAtNs() == 0 {
		return false
	}
	return !g.now.Before(depsDeadline(d.GetIssuedAtNs(), depsTimeout(sess)))
}

// arm records the earliest future deps deadline of a session that has not started.
func (g *gate) arm(sess Session, d *statev1.SessionDemand) {
	if d.GetStart() || d.GetIssuedAtNs() == 0 {
		return
	}
	if d.GetDesired() == sessionv1.DesiredState_DESIRED_STATE_ENDED {
		return
	}
	dl := depsDeadline(d.GetIssuedAtNs(), depsTimeout(sess))
	if !g.now.Before(dl) {
		return
	}
	if g.next.IsZero() || dl.Before(g.next) {
		g.next = dl
	}
}

func (g *gate) open(region string) bool {
	r, ok := g.v.Regions[region]
	return ok && r.Synced && !r.Stale
}

func depsTimeout(sess Session) uint32 {
	if sess.Spec == nil {
		return 0
	}
	return sess.Spec.GetDepsTimeoutS()
}

func depsDeadline(issuedNs int64, timeoutS uint32) time.Time {
	d := time.Duration(timeoutS) * time.Second
	if d == 0 {
		d = defaultDepsTimeout
	}
	return time.Unix(0, issuedNs).Add(d)
}

func indexAcks(v View) map[string]Ack {
	out := make(map[string]Ack, len(v.Acks))
	for _, k := range slices.Sorted(maps.Keys(v.Acks)) {
		a := v.Acks[k]
		if dk := ackDemandKey(a); dk != "" {
			out[dk] = a
		}
	}
	return out
}

func ackDemandKey(a Ack) string {
	if a.Plane == planeTD {
		id, err := strconv.ParseInt(a.Entry, 10, 64)
		if err != nil {
			return ""
		}
		return tdDemandKey(a.Instance, a.Session, id)
	}
	return mdDemandKey(a.Instance, a.Session, a.Entry)
}

func bindingDemandKey(session string, b *statev1.Binding) (string, bool) {
	switch t := b.GetTarget().(type) {
	case *statev1.Binding_Feed:
		if t.Feed == "" {
			return "", false
		}
		return mdDemandKey(b.GetInstance(), session, "feed/"+t.Feed), true
	case *statev1.Binding_Selector:
		if t.Selector == "" {
			return "", false
		}
		return mdDemandKey(b.GetInstance(), session, "select/"+t.Selector), true
	case *statev1.Binding_ApiId:
		return tdDemandKey(b.GetInstance(), session, t.ApiId), true
	default:
		return "", false
	}
}
