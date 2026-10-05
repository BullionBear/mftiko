package state

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

// twoRegions is the topology the routing tests share: a TW STS that uses MD and TD in both regions.
func twoRegions() Topology {
	return Topology{
		Regions: map[string]Region{
			"tw": {Endpoints: []string{"http://127.0.0.1:1"}},
			"jp": {Endpoints: []string{"http://127.0.0.1:2"}},
		},
		Instances: map[string]string{
			"sts-tw-1": "tw", "md-tw-1": "tw", "td-tw-1": "tw",
			"md-jp-1": "jp", "td-jp-1": "jp",
		},
	}
}

func TestKeys(t *testing.T) {
	const r = Root
	cases := []struct {
		got, want string
		plane     Plane
		inst      string
		region    string // in twoRegions
	}{
		{SessionSpecKey("sts-tw-1", "a1b2c3"), r + "/sts/sts-tw-1/spec/a1b2c3", PlaneSTS, "sts-tw-1", "tw"},
		{SessionStatusKey("sts-tw-1", "a1b2c3"), r + "/sts/sts-tw-1/status/a1b2c3", PlaneSTS, "sts-tw-1", "tw"},
		{AccountKey("td-jp-1", 42), r + "/td/td-jp-1/account/42", PlaneTD, "td-jp-1", "jp"},
		// A TW STS asking for JP market data writes into JP's cluster; JP's MD acks there (D17 rule 1).
		{
			MDLeaseKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT"),
			r + "/md/md-jp-1/lease/sts-tw-1/a1b2c3/feed/trade.BinanceUM_Perp_BTCUSDT", PlaneMD, "md-jp-1", "jp",
		},
		{
			MDLeaseKey("md-tw-1", "sts-tw-1", "a1b2c3", "select/top10"),
			r + "/md/md-tw-1/lease/sts-tw-1/a1b2c3/select/top10", PlaneMD, "md-tw-1", "tw",
		},
		{
			TDLeaseKey("td-jp-1", "sts-tw-1", "a1b2c3", 42),
			r + "/td/td-jp-1/lease/sts-tw-1/a1b2c3/42", PlaneTD, "td-jp-1", "jp",
		},
		{
			MDAckKey("md-jp-1", "sts-tw-1", "a1b2c3", "feed/trade.BinanceUM_Perp_BTCUSDT"),
			r + "/md/md-jp-1/ack/sts-tw-1/a1b2c3/feed/trade.BinanceUM_Perp_BTCUSDT", PlaneMD, "md-jp-1", "jp",
		},
		{
			TDAckKey("td-jp-1", "sts-tw-1", "a1b2c3", 42),
			r + "/td/td-jp-1/ack/sts-tw-1/a1b2c3/42", PlaneTD, "td-jp-1", "jp",
		},
		{ElectionPrefix(PlaneMD, "md-jp-1"), r + "/md/md-jp-1/election", PlaneMD, "md-jp-1", "jp"},
		{EpochKey(PlaneSTS, "sts-tw-1"), r + "/sts/sts-tw-1/epoch", PlaneSTS, "sts-tw-1", "tw"},
		{WriterKey(PlaneTD, "td-tw-1"), r + "/td/td-tw-1/writer", PlaneTD, "td-tw-1", "tw"},
		{AgentKey(PlaneMD, "md-tw-1"), r + "/md/md-tw-1/agent", PlaneMD, "md-tw-1", "tw"},
		{WorkerStatusKey(PlaneTD, "td-jp-1", "0123456789abcdef"), r + "/td/td-jp-1/worker/0123456789abcdef", PlaneTD, "td-jp-1", "jp"},
		{IncarnationKey(PlaneSTS, "sts-tw-1", "0123456789abcdef"), r + "/sts/sts-tw-1/incarnation/0123456789abcdef", PlaneSTS, "sts-tw-1", "tw"},
		{SelectorKey("md-jp-1", "s1"), r + "/md/md-jp-1/selector/s1", PlaneMD, "md-jp-1", "jp"},
		{PlacementKey("md-tw-1", "a1"), r + "/md/md-tw-1/placement/a1", PlaneMD, "md-tw-1", "tw"},

		{SessionSpecPrefix("sts-tw-1"), r + "/sts/sts-tw-1/spec/", PlaneSTS, "sts-tw-1", "tw"},
		{SessionStatusPrefix("sts-tw-1"), r + "/sts/sts-tw-1/status/", PlaneSTS, "sts-tw-1", "tw"},
		{AccountPrefix("td-tw-1"), r + "/td/td-tw-1/account/", PlaneTD, "td-tw-1", "tw"},
		{MDLeasePrefix("md-jp-1"), r + "/md/md-jp-1/lease/", PlaneMD, "md-jp-1", "jp"},
		{TDLeasePrefix("td-jp-1"), r + "/td/td-jp-1/lease/", PlaneTD, "td-jp-1", "jp"},
		{MDAckPrefix("md-jp-1"), r + "/md/md-jp-1/ack/", PlaneMD, "md-jp-1", "jp"},
		{TDAckPrefix("td-tw-1"), r + "/td/td-tw-1/ack/", PlaneTD, "td-tw-1", "tw"},
		{IncarnationPrefix(PlaneMD, "md-jp-1"), r + "/md/md-jp-1/incarnation/", PlaneMD, "md-jp-1", "jp"},
		{SelectorPrefix("md-jp-1"), r + "/md/md-jp-1/selector/", PlaneMD, "md-jp-1", "jp"},
		{PlacementPrefix("md-tw-1"), r + "/md/md-tw-1/placement/", PlaneMD, "md-tw-1", "tw"},
		{WorkerStatusPrefix(PlaneSTS, "sts-tw-1"), r + "/sts/sts-tw-1/worker/", PlaneSTS, "sts-tw-1", "tw"},
	}
	topo := twoRegions()
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("key %q, want %q", c.got, c.want)
			continue
		}
		p, inst, err := Owner(c.got)
		if err != nil || p != c.plane || inst != c.inst {
			t.Errorf("Owner(%q) = %s, %s, %v; want %s, %s", c.got, p, inst, err, c.plane, c.inst)
		}
		region, err := topo.RegionOfKey(c.got)
		if err != nil || region != c.region {
			t.Errorf("RegionOfKey(%q) = %s, %v; want %s", c.got, region, err, c.region)
		}
	}
}

func TestOwnerRejects(t *testing.T) {
	for _, key := range []string{
		"",
		"/other/v1/md/md-tw-1/agent",
		"/mftiko/v10/md/md-tw-1/agent", // Root is a path prefix, not a string prefix
		"mftiko/v1/md/md-tw-1/agent",
		Root,
		Root + "/",
		Root + "/md",
		Root + "/md/md-tw-1", // too few segments: no key class
		Root + "/xx/md-tw-1/agent",
		Root + "//md-tw-1/agent",
		Root + "/md//agent",
		Root + "/md/MD-TW-1/agent",
		Root + "/md/1md/agent",
		Root + "/md/" + strings.Repeat("a", 65) + "/agent",
	} {
		if p, inst, err := Owner(key); !errors.Is(err, ErrBadKey) {
			t.Errorf("Owner(%q) = %s, %s, %v; want ErrBadKey", key, p, inst, err)
		}
	}
	if _, inst, err := Owner(Root + "/md/" + strings.Repeat("a", 64) + "/agent"); err != nil || len(inst) != 64 {
		t.Errorf("64-character instance name: %q, %v", inst, err)
	}
}

func TestWorkerHash(t *testing.T) {
	sum := sha256.Sum256([]byte("td/42"))
	want := hex.EncodeToString(sum[:])[:16]
	if got := WorkerHash("td/42"); got != want || len(got) != 16 {
		t.Fatalf("WorkerHash(td/42) = %q, want %q", got, want)
	}
	if WorkerHash("td/42") == WorkerHash("td/43") {
		t.Fatal("different worker keys, same hash")
	}
}
