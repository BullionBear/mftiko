// Command mftiko-sts-agent is the strategy-plane agent. It reconciles session specs, writes leases to MD and TD, signs each session's NATS credentials, and spawns session workers through shims (DECISIONS D001, D101, D901, D906).
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
	log := logger.New("mftiko-sts-agent")
	log.Error().Msg("not implemented")
	os.Exit(1)
}
