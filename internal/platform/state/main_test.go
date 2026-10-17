package state

import (
	"os"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// shared is the etcd the tests of this package take in turn: singleRegion and proxied. A test that restarts or
// rebuilds etcd, runs at a non-default token or session TTL, or needs more than one region (twoServers, D1316)
// starts its own.
var shared etcdtest.Shared

func TestMain(m *testing.M) {
	code := m.Run()
	shared.Close()
	os.Exit(code)
}
