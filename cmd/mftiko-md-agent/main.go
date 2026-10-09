// Command mftiko-md-agent is the market-data-plane agent. It derives atoms from leases, places them on connections, and spawns conn workers (DECISIONS D901, D401, D403).
//
// Shape only: not implemented yet.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-md-agent")
	log.Error().Msg("not implemented")
	os.Exit(1)
}
