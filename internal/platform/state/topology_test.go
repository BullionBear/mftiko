package state

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	if err := twoRegions().Validate(); err != nil {
		t.Fatalf("good topology: %v", err)
	}
	ep := []string{"http://127.0.0.1:1"}
	bad := map[string]Topology{
		"no regions":         {Instances: map[string]string{}},
		"region without eps": {Regions: map[string]Region{"tw": {}}},
		"empty endpoint":     {Regions: map[string]Region{"tw": {Endpoints: []string{""}}}},
		"empty region name":  {Regions: map[string]Region{"": {Endpoints: ep}}},
		"undefined region": {
			Regions:   map[string]Region{"tw": {Endpoints: ep}},
			Instances: map[string]string{"md-jp-1": "jp"},
		},
		"uppercase instance": {
			Regions:   map[string]Region{"tw": {Endpoints: ep}},
			Instances: map[string]string{"MD-tw-1": "tw"},
		},
		"instance starts with a digit": {
			Regions:   map[string]Region{"tw": {Endpoints: ep}},
			Instances: map[string]string{"1md": "tw"},
		},
		"instance with a slash": {
			Regions:   map[string]Region{"tw": {Endpoints: ep}},
			Instances: map[string]string{"md/tw": "tw"},
		},
		"empty instance": {
			Regions:   map[string]Region{"tw": {Endpoints: ep}},
			Instances: map[string]string{"": "tw"},
		},
		"instance too long": {
			Regions:   map[string]Region{"tw": {Endpoints: ep}},
			Instances: map[string]string{strings.Repeat("a", 65): "tw"},
		},
	}
	for name, topo := range bad {
		if err := topo.Validate(); !errors.Is(err, ErrBadTopology) {
			t.Errorf("%s: Validate = %v, want ErrBadTopology", name, err)
		}
	}
}

func TestRegionOf(t *testing.T) {
	topo := twoRegions()
	if r, err := topo.RegionOf("md-jp-1"); err != nil || r != "jp" {
		t.Fatalf("RegionOf(md-jp-1) = %s, %v", r, err)
	}
	if _, err := topo.RegionOf("md-kr-1"); !errors.Is(err, ErrUnknownInstance) {
		t.Fatalf("unknown instance: %v", err)
	}
	if _, err := topo.RegionOfKey(AgentKey(PlaneMD, "md-kr-1")); !errors.Is(err, ErrUnknownInstance) {
		t.Fatalf("key of unknown instance: %v", err)
	}
	if _, err := topo.RegionOfKey("/elsewhere"); !errors.Is(err, ErrBadKey) {
		t.Fatalf("bad key: %v", err)
	}
	// The region comes from the map, never from the name.
	topo.Instances["md-jp-2"] = "tw"
	if r, _ := topo.RegionOfKey(AgentKey(PlaneMD, "md-jp-2")); r != "tw" {
		t.Fatalf("md-jp-2 mapped to tw routes to %s", r)
	}
}

func TestAgentRegions(t *testing.T) {
	topo := twoRegions()
	for _, c := range []struct {
		p    Plane
		inst string
		want []string
	}{
		{PlaneMD, "md-jp-1", []string{"jp"}},
		{PlaneTD, "td-tw-1", []string{"tw"}},
		{PlaneSTS, "sts-tw-1", []string{"tw"}}, // the controller writes its demands; it no longer writes elsewhere
		{PlaneCtl, "ctl-tw-1", nil},
	} {
		got, err := AgentRegions(topo, c.p, c.inst)
		if err != nil || !slices.Equal(got, c.want) || (c.want == nil) != (got == nil) {
			t.Errorf("AgentRegions(%s, %s) = %v, %v; want %v", c.p, c.inst, got, err, c.want)
		}
	}
	if _, err := AgentRegions(topo, "xx", "md-jp-1"); !errors.Is(err, ErrUnknownPlane) {
		t.Errorf("unknown plane: %v", err)
	}
	if _, err := AgentRegions(topo, PlaneMD, "md-kr-1"); !errors.Is(err, ErrUnknownInstance) {
		t.Errorf("unknown instance: %v", err)
	}
}
