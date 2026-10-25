package state

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
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
		{PlaneCtl, ControllerInstance, nil},
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

func TestRegionTimeouts(t *testing.T) {
	dial, req, stale, err := (Region{}).Timeouts()
	if err != nil || dial != 0 || req != 0 || stale != 0 {
		t.Fatalf("omitted: %s %s %s %v", dial, req, stale, err)
	}
	for _, raw := range []string{"0s", "0"} {
		zero := Region{DialTimeout: raw, RequestTimeout: raw, StaleAfter: raw}
		dial, req, stale, err = zero.Timeouts()
		if err != nil || dial != 0 || req != 0 || stale != 0 {
			t.Fatalf("zero %q: %s %s %s %v", raw, dial, req, stale, err)
		}
	}
	set := Region{DialTimeout: "8s", RequestTimeout: "2s", StaleAfter: "20s"}
	dial, req, stale, err = set.Timeouts()
	if err != nil || dial != 8*time.Second || req != 2*time.Second || stale != 20*time.Second {
		t.Fatalf("set: %s %s %s %v", dial, req, stale, err)
	}

	ep := []string{"http://127.0.0.1:1"}
	bad := map[string]Region{
		"not a duration": {Endpoints: ep, DialTimeout: "nope"},
		"negative dial":  {Endpoints: ep, DialTimeout: "-1s"},
		"negative stale": {Endpoints: ep, StaleAfter: "-1ms"},
	}
	for name, region := range bad {
		topo := Topology{
			Regions:   map[string]Region{"tw": region},
			Instances: map[string]string{ControllerInstance: "tw"},
		}
		if err := topo.Validate(); !errors.Is(err, ErrBadTopology) {
			t.Errorf("%s: %v", name, err)
		}
	}

	const body = `{
		"regions": {"tw": {"endpoints": ["http://127.0.0.1:1"], "dial_timeout": "5s", "request_timeout": "2s", "stale_after": "15s"}},
		"instances": {"controller": "tw"},
		"planes": {"md-tw-1": "md"}
	}`
	var topo Topology
	if err := json.Unmarshal([]byte(body), &topo); err != nil {
		t.Fatal(err)
	}
	got := topo.Regions["tw"]
	if len(got.Endpoints) != 1 || got.Endpoints[0] != "http://127.0.0.1:1" ||
		got.DialTimeout != "5s" || got.RequestTimeout != "2s" || got.StaleAfter != "15s" {
		t.Fatalf("%+v", got)
	}
	if err := topo.Validate(); err != nil {
		t.Fatal(err)
	}
}
