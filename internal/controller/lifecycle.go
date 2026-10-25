package controller

import (
	"maps"
	"slices"
	"strings"
)

// planeSTS is the deployment label of an STS instance (E13-D017). planeMD and
// planeTD are declared beside Derive.
const planeSTS = "sts"

// Delete is one key to remove. Lifecycle names only demand entries and
// SessionDemand (E13-D035): spec, SessionStatus, ack and WorkerStatus belong to
// other writers (W1).
type Delete struct {
	Key string
}

// Lifecycle plans the deletes of one reconcile (N4, N5; E13-D006, E13-D009, E13-D010,
// E13-D021, E13-D033–E13-D035).
//
// full is the election round or a full resync. A demand whose session is on
// no STS instance is an orphan and is removed only on those rounds (E13-D010,
// E13-D034). A session that has reached a terminal status is removed on every
// round once its worker is gone: seeing the status is level-triggered, and
// the resync repeats the same check.
//
// N4 removes the MD and TD demands first. SessionDemand is named only on a
// later round, once the view no longer contains those demands, so the order
// holds across regions (E13-D033). Any provider region that is not synced, or is
// stale, holds SessionDemand back even when this round can see no demands
// there. A stale region is never subtracted from (E13-D006): its own demands
// stay, and a session whose STS region is stale is left entirely, including
// demands that live in some other region.
//
// The worker is gone when its WorkerStatus key is absent (E13-D009). The key is
// deleted only after the worker is confirmed gone, and it is not on a lease,
// so a fresh region reporting it absent is the current state. An absent
// AgentRecord does not stand in for that: a WorkerStatus still in the view is
// the last value, and N4 keeps waiting. The exception is a retired STS
// instance (E12-D034): /retired/{inst} in the view already satisfies both the
// terminal status and the missing worker, and N4 then deletes in the same
// order. An undecodable worker key is not treated as absent (E13-D021), and
// that still holds when the instance is not retired.
//
// A session counts as present when any STS instance has it in the view, as a
// spec, a status or a SessionDemand. The demand key carries only the session
// id (E13-D034), so every instance is checked. A spec that is already visible
// while its SessionDemand is not yet, which happens because the two regions
// are not one commit (E13-D025), is still that session: deleting the demand
// would fight the derive that is about to write it. An undecodable spec,
// status or SessionDemand for the id is not read as "no session" (E13-D021).
func Lifecycle(v View, inst map[string]Placement, full bool) []Delete {
	b := newLife(v, inst)
	if full && b.stsFresh() {
		b.orphans()
	}
	for _, id := range slices.Sorted(maps.Keys(b.groups)) {
		b.ended(b.groups[id])
	}
	return b.list()
}

type life struct {
	v               View
	bad             map[string]struct{}
	groups          map[string][]Session // session id -> every STS instance that has it
	byDemand        map[string][]Demand  // session id -> MD and TD demands
	stsRegions      map[string]struct{}
	providerRegions map[string]struct{}
	drop            map[string]struct{}
}

func newLife(v View, inst map[string]Placement) *life {
	b := &life{
		v:               v,
		bad:             make(map[string]struct{}, len(v.Undecodable)),
		groups:          map[string][]Session{},
		byDemand:        map[string][]Demand{},
		stsRegions:      map[string]struct{}{},
		providerRegions: map[string]struct{}{},
		drop:            map[string]struct{}{},
	}
	for _, key := range v.Undecodable {
		b.bad[key] = struct{}{}
	}
	for _, sess := range v.Sessions {
		b.groups[sess.ID] = append(b.groups[sess.ID], sess)
	}
	for _, d := range v.Demands {
		b.byDemand[d.Session] = append(b.byDemand[d.Session], d)
	}
	for _, p := range inst {
		switch p.Plane {
		case planeSTS:
			if p.Region != "" {
				b.stsRegions[p.Region] = struct{}{}
			}
		case planeMD, planeTD:
			if p.Region != "" {
				b.providerRegions[p.Region] = struct{}{}
			}
		}
	}
	return b
}

// orphans removes demands whose session id is on no STS instance (E13-D034).
// The caller has already required every STS region to be fresh.
func (b *life) orphans() {
	for _, d := range b.v.Demands {
		if _, owned := b.groups[d.Session]; owned || b.hiddenSession(d.Session) {
			continue
		}
		b.remove(d.Key, d.Region)
	}
}

// ended removes one session id. Every instance that has it must be ready:
// the demand key does not say which instance it belongs to, so one instance
// that is still live, or whose region is stale, keeps the demands.
func (b *life) ended(group []Session) {
	for _, sess := range group {
		if !b.ready(sess) {
			return
		}
	}
	id := group[0].ID
	for _, d := range b.byDemand[id] {
		b.remove(d.Key, d.Region)
	}
	// The demands just named are still in this view. SessionDemand waits
	// until a later view shows them gone, and until every provider region
	// can be seen (E13-D033).
	if b.demandsLeft(id) || !b.providersFresh() {
		return
	}
	for _, sess := range group {
		if sess.Demand == nil {
			continue
		}
		b.remove(sessionDemandKey(sess.Instance, sess.ID), sess.Region)
	}
}

func (b *life) ready(sess Session) bool {
	if _, retired := b.v.Retired[sess.Instance]; retired {
		// The retirement key stands in for a terminal status and a gone worker
		// (E12-D034). A stale or unsynced home region still waits (W9).
		return b.open(sess.Region)
	}
	return terminal(sess.Status) && b.open(sess.Region) && b.workerGone(sess)
}

// workerGone reports that this session's WorkerStatus is confirmed absent.
func (b *life) workerGone(sess Session) bool {
	if _, undecodable := b.bad[sessionWorkerKey(sess.Instance, sess.ID)]; undecodable || sess.Worker != nil {
		return false
	}
	want := workerHash(sess.ID)
	for _, w := range b.v.Workers {
		if w.Instance == sess.Instance && w.Hash == want {
			return false
		}
	}
	return true
}

// demandsLeft reports a demand of this session that the view still shows, or
// an undecodable key that may be one (E13-D021, E13-D033).
func (b *life) demandsLeft(id string) bool {
	if len(b.byDemand[id]) > 0 {
		return true
	}
	for _, key := range b.v.Undecodable {
		if got, ok := providerDemandSession(key); ok && got == id {
			return true
		}
	}
	return false
}

// hiddenSession reports an undecodable STS record that may be this session,
// so the id cannot be declared an orphan (E13-D021).
func (b *life) hiddenSession(id string) bool {
	for _, key := range b.v.Undecodable {
		if got, ok := stsRecordSession(key); ok && got == id {
			return true
		}
	}
	return false
}

func (b *life) remove(key, region string) {
	if key == "" || !b.open(region) {
		return
	}
	if _, undecodable := b.bad[key]; undecodable {
		return
	}
	b.drop[key] = struct{}{}
}

func (b *life) open(region string) bool {
	r, ok := b.v.Regions[region]
	return ok && r.Synced && !r.Stale
}

func (b *life) stsFresh() bool {
	for region := range b.stsRegions {
		if !b.open(region) {
			return false
		}
	}
	return true
}

func (b *life) providersFresh() bool {
	for region := range b.providerRegions {
		if !b.open(region) {
			return false
		}
	}
	return true
}

func (b *life) list() []Delete {
	if len(b.drop) == 0 {
		return nil
	}
	out := make([]Delete, 0, len(b.drop))
	for key := range b.drop {
		out = append(out, Delete{Key: key})
	}
	slices.SortFunc(out, func(a, b Delete) int { return strings.Compare(a.Key, b.Key) })
	return out
}

func sessionWorkerKey(inst, id string) string {
	return keyRoot + "/sts/" + inst + "/worker/" + workerHash(id)
}

// splitKey parses /mftiko/v1/{plane}/{inst}/{kind}/{tail}.
func splitKey(key string) (plane, kind, tail string, ok bool) {
	rest, found := strings.CutPrefix(key, keyRoot+"/")
	if !found {
		return "", "", "", false
	}
	parts := strings.SplitN(rest, "/", 4)
	if len(parts) < 4 || parts[0] == "" || parts[1] == "" || parts[2] == "" || parts[3] == "" {
		return "", "", "", false
	}
	return parts[0], parts[2], parts[3], true
}

// providerDemandSession is the session id of an MD or TD demand key.
func providerDemandSession(key string) (string, bool) {
	plane, kind, tail, ok := splitKey(key)
	if !ok || kind != "demand" || (plane != planeMD && plane != planeTD) {
		return "", false
	}
	session, _, found := strings.Cut(tail, "/")
	if !found || session == "" {
		return "", false
	}
	return session, true
}

// stsRecordSession is the session id of an STS spec, status or SessionDemand key.
func stsRecordSession(key string) (string, bool) {
	plane, kind, tail, ok := splitKey(key)
	if !ok || plane != planeSTS || (kind != "spec" && kind != "status" && kind != "demand") {
		return "", false
	}
	if strings.Contains(tail, "/") {
		return "", false
	}
	return tail, true
}
