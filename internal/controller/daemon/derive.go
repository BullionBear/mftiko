package daemon

import (
	"sync"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// apply derives N1 and deletes what N4 and N5 remove. full is the election
// round or a resync tick (D1310). Puts of a region finish before its deletes:
// Derive does not put a terminal session, and Lifecycle does not remove a
// session that is still running, so one round does not put a key it deletes.
// A later gate has to keep that split. Each region's keys run on that
// region's queue, at the same time as the others, so a region that cannot be
// written does not hold the rest (D1306, D1325).
func (c *Controller) apply(v controller.View, full bool) {
	plan := controller.Derive(v, c.intake.planes, c.intake.now())
	// A round can run again before the mirror lists the assignment just
	// written. Copy the issued_at_ns this term already chose, so the retry
	// does not start deps_timeout over (D1302).
	c.keepIssued(v, plan.Writes)
	c.intake.noteDrifts(plan.Drifts)
	c.putAll(plan.Writes)
	c.deleteAll(controller.Lifecycle(v, c.intake.planes, full))
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

func (c *Controller) putAll(writes []controller.Write) {
	if len(writes) == 0 {
		return
	}
	topo := c.cl.Topology()
	groups := map[string][]controller.Write{}
	var order []string
	for _, w := range writes {
		region, err := topo.RegionOfKey(w.Key)
		if err != nil {
			c.log.Warn().Err(err).Str("key", w.Key).Msg("derive")
			continue
		}
		if _, ok := groups[region]; !ok {
			order = append(order, region)
		}
		groups[region] = append(groups[region], w)
	}
	var wg sync.WaitGroup
	for _, region := range order {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, w := range groups[region] {
				if c.ctx.Err() != nil || c.term.Err() != nil {
					return
				}
				// The queue logs a failed put. ErrUnsynced is a region that
				// has not finished its first list; it is not fenced (D1318).
				_ = c.writes.submit(c.ctx, w.Key, controller.Encode(w.Value), false)
			}
		}()
	}
	wg.Wait()
}

// deleteAll removes keys the same way putAll writes them (D1306, D1335).
func (c *Controller) deleteAll(keys []controller.Delete) {
	if len(keys) == 0 {
		return
	}
	topo := c.cl.Topology()
	groups := map[string][]string{}
	var order []string
	for _, d := range keys {
		region, err := topo.RegionOfKey(d.Key)
		if err != nil {
			c.log.Warn().Err(err).Str("key", d.Key).Msg("lifecycle")
			continue
		}
		if _, ok := groups[region]; !ok {
			order = append(order, region)
		}
		groups[region] = append(groups[region], d.Key)
	}
	var wg sync.WaitGroup
	for _, region := range order {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, key := range groups[region] {
				if c.ctx.Err() != nil || c.term.Err() != nil {
					return
				}
				// The queue logs a failed delete. A region that has not
				// finished its first list is not fenced (D1318).
				_ = c.writes.submit(c.ctx, key, "", true)
			}
		}()
	}
	wg.Wait()
}
