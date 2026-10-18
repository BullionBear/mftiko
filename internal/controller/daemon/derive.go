package daemon

import (
	"sync"

	"github.com/BullionBear/mftiko/internal/controller"
)

// apply derives N1 from one view and puts the diff (D1308, D1325). Each
// region's keys run on that region's queue, at the same time as the others,
// so a region that cannot be written does not hold the rest (D1306).
func (c *Controller) apply(v controller.View) {
	plan := controller.Derive(v, c.intake.planes, c.intake.now())
	c.intake.noteDrifts(plan.Drifts)
	c.putAll(plan.Writes)
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
