package runner

import (
	"context"
	"fmt"

	"github.com/BullionBear/mftiko/internal/platform/state"
)

// Connect is how an agent joins the control plane: clients for the clusters it may use, which is its own region's
// only (E00-D005, state.AgentRegions), and the term it wins there (E1-05). On error nothing is held. The caller closes
// the term, then the clusters.
func Connect(ctx context.Context, topo state.Topology, p state.Plane, inst string, dial state.DialFunc,
	opt state.TermOptions) (*state.Clusters, *state.Term, error) {
	regions, err := state.AgentRegions(topo, p, inst)
	if err != nil {
		return nil, nil, err
	}
	if p == state.PlaneCtl {
		return nil, nil, fmt.Errorf("agent: %s is the controller, not an agent", inst)
	}
	cl, err := state.NewClusters(topo, regions, dial)
	if err != nil {
		return nil, nil, err
	}
	// Before campaign, so a retired name never takes an epoch or writes an AgentRecord (E12-D036).
	// Permission denied on the marker is "not retired": the role table has not been reapplied yet.
	retired, err := state.InstanceRetired(ctx, cl, inst)
	if err != nil {
		cl.Close()
		return nil, nil, err
	}
	if retired {
		cl.Close()
		return nil, nil, fmt.Errorf("%w: %s", state.ErrRetired, inst)
	}
	term, err := state.Campaign(ctx, cl, p, inst, opt)
	if err != nil {
		cl.Close()
		return nil, nil, err
	}
	return cl, term, nil
}

// Home is the region of the term's instance: the only one an agent reads and writes.
func Home(cl *state.Clusters, inst string) (string, error) {
	return cl.Topology().RegionOf(inst)
}
