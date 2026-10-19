package daemon

import (
	"time"

	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// apply derives N1, fills bindings and ends sessions (N2, N3), then deletes
// what N4 and N5 remove. full is the election round or a resync tick (D1310).
// It returns the earliest deps deadline still ahead, or the zero time when
// none is (D1331). A region's puts are posted before its deletes, and its
// queue runs them in that order: Derive and Gate skip a session whose status
// is already terminal, and Lifecycle does not remove a session that is still
// running, so one round does not put a key it deletes. Each region's keys run
// on that region's queue, at the same time as the others, and the round does
// not wait for them, so a region that cannot be written does not hold the
// rest (D1306, D1325, D1337).
func (c *Controller) apply(v controller.View, full bool) time.Time {
	now := c.intake.now()
	plan := controller.Derive(v, c.intake.planes, now)
	// A round can run again before the mirror lists the assignment just
	// written. Copy the issued_at_ns this term already chose, so the retry
	// does not start deps_timeout over (D1302). Gate then reads that value.
	c.keepIssued(v, plan.Writes)
	c.intake.noteDrifts(plan.Drifts)
	gated, next := controller.Gate(v, plan, c.intake.planes, now)
	c.putAll(v, gated.Writes)
	c.deleteAll(v, controller.Lifecycle(v, c.intake.planes, full))
	return next
}

func (c *Controller) keepIssued(v controller.View, writes []controller.Write) {
	for _, w := range writes {
		d, ok := w.Value.(*statev1.SessionDemand)
		if !ok {
			continue
		}
		if storedIssued(v, w.Key) == 0 {
			if prev := c.intake.issued[w.Key]; prev != 0 {
				d.IssuedAtNs = prev
			}
		}
		if d.GetIssuedAtNs() != 0 {
			c.intake.issued[w.Key] = d.GetIssuedAtNs()
		}
	}
}

// keepUpdated copies updated_at_ns from the SessionDemand this term last
// posted for the key when the two differ in nothing else. A round that runs
// before the mirror lists that write derives it again with a new clock; with
// the old value the bytes match and the queue drops the repeat (D1308, D1323,
// D1337).
func (c *Controller) keepUpdated(w controller.Write) {
	d, ok := w.Value.(*statev1.SessionDemand)
	if !ok {
		return
	}
	if last := c.intake.posted[w.Key]; last != nil && sameButUpdated(d, last) {
		d.UpdatedAtNs = last.GetUpdatedAtNs()
	}
	c.intake.posted[w.Key] = proto.Clone(d).(*statev1.SessionDemand)
}

func sameButUpdated(a, b *statev1.SessionDemand) bool {
	a = proto.Clone(a).(*statev1.SessionDemand)
	b = proto.Clone(b).(*statev1.SessionDemand)
	a.UpdatedAtNs, b.UpdatedAtNs = 0, 0
	return proto.Equal(a, b)
}

func storedIssued(v controller.View, key string) int64 {
	for _, sess := range v.Sessions {
		if state.SessionDemandKey(sess.Instance, sess.ID) != key || sess.Demand == nil {
			continue
		}
		return sess.Demand.GetIssuedAtNs()
	}
	return 0
}

// putAll posts the round's puts on their regions' queues. It does not wait,
// so a region whose write hangs holds neither this loop nor the other regions
// (D1306, D1337). The queue logs a failed put. ErrUnsynced is a region that
// has not finished its first list; it is not fenced (D1318). v tells the
// queue how far the round has read each region.
func (c *Controller) putAll(v controller.View, writes []controller.Write) {
	for _, w := range writes {
		c.keepUpdated(w)
		c.writes.post(c.ctx, w.Key, controller.Encode(w.Value), false, c.seen(v, w.Key))
	}
}

// deleteAll posts removals the same way putAll posts puts (D1306, D1335).
func (c *Controller) deleteAll(v controller.View, keys []controller.Delete) {
	for _, d := range keys {
		c.writes.post(c.ctx, d.Key, "", true, c.seen(v, d.Key))
	}
}

// seen is the revision v has read key's region up to, or 0.
func (c *Controller) seen(v controller.View, key string) int64 {
	region, err := c.cl.Topology().RegionOfKey(key)
	if err != nil {
		return 0
	}
	return v.Regions[region].Rev
}
