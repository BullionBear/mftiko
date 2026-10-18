package daemon

import (
	"sync"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/controller"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// apply derives N1 from one view and puts the diff (D1308, D1325). Each
// region's keys run on that region's queue, at the same time as the others,
// so a region that cannot be written does not hold the rest (D1306).
func (c *Controller) apply(v controller.View) {
	plan := controller.Derive(v, c.intake.planes, c.intake.now())
	// A round can run again before the mirror lists the assignment just
	// written. Copy the issued_at_ns this term already chose, so the retry
	// does not start deps_timeout over (D1302).
	c.keepIssued(v, plan.Writes)
	c.intake.noteDrifts(plan.Drifts)
	c.putAll(plan.Writes)
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
