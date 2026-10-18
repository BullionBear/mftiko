package etcdtest

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestStartRegions(t *testing.T) {
	servers := StartRegions(t, "tw", "jp", "kr")
	if len(servers) != 3 {
		t.Fatalf("%d servers", len(servers))
	}
	seen := map[string]bool{}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for _, region := range []string{"tw", "jp", "kr"} {
		srv := servers[region]
		if srv == nil {
			t.Fatalf("region %s has no server", region)
		}
		ep := srv.Endpoints()[0]
		if seen[ep] {
			t.Fatalf("region %s reused endpoint %s", region, ep)
		}
		seen[ep] = true
		if _, err := srv.Client(t).Get(ctx, "/health"); err != nil {
			t.Fatalf("%s: %v", region, err)
		}
	}
}

func TestStartRegionsRejects(t *testing.T) {
	for name, regions := range map[string][]string{
		"no regions": nil,
		"empty name": {"tw", ""},
		"duplicate":  {"tw", "jp", "tw"},
	} {
		t.Run(name, func(t *testing.T) {
			r := &recorder{TB: t}
			if servers := StartRegions(r, regions...); servers != nil {
				t.Fatal("started servers")
			}
			if len(r.failures) != 1 || !strings.Contains(r.failures[0], "etcdtest:") {
				t.Fatalf("failures %q", r.failures)
			}
		})
	}
}
