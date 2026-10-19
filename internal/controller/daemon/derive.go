package daemon

import (
	"time"

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
