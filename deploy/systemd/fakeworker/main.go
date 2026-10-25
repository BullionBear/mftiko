// Command fakeworker is the worker binary the systemd adoption test packs as
// md.conn, td.account, and a strategy (E12-D021). It is not a host binary and
// it is not part of the release tarball.
//
// A manifest must not set MFTIKO_* (CheckLaunchFields), so this process sets
// MFTIKO_FAKE_WORKER itself and then speaks C4. The sdk import keeps the
// contracts marker PackStrategy reads out of a strategy binary.
package main

import (
	"os"

	"github.com/BullionBear/mftiko/internal/platform/shim/fakeworker"
	_ "github.com/BullionBear/mftiko/sdk"
)

func main() {
	if os.Getenv(fakeworker.EnvWorker) != "1" {
		if err := os.Setenv(fakeworker.EnvWorker, "1"); err != nil {
			os.Exit(1)
		}
	}
	fakeworker.MaybeMain()
	os.Exit(1)
}
