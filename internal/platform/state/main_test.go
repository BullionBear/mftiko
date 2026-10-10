package state

import (
	"os"
	"testing"

	"github.com/BullionBear/mftiko/internal/platform/state/etcdtest"
)

// shared is the etcd the tests of this package take in turn: singleRegion, proxied and the tw of twoServers. A test
// that restarts or rebuilds etcd, or runs at a non-default token or session TTL, starts one of its own.
var shared etcdtest.Shared

func TestMain(m *testing.M) {
	code := m.Run()
	shared.Close()
	os.Exit(code)
}
