package symtest

import (
	"testing"

	"github.com/BullionBear/mftiko/internal/sym"
)

// The table holds on the logic layer itself, so a failure in a protocol's run is the protocol's.
func TestDirect(t *testing.T) {
	Run(t, func(_ *testing.T, api sym.API) Client { return Direct(api) })
}
