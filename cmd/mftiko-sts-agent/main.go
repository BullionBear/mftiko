// Command mftiko-sts-agent is the strategy-plane agent. It runs the sessions the controller assigns to its
// instance (/sts/{inst}/demand/): it pulls the strategy artifact, signs each session's NATS credentials, spawns
// the session worker through a shim, sends Init and, once the assignment allows it, Start, and writes
// SessionStatus. It writes no demands and reads no acks (DECISIONS D001, D101, D906, D907, D916).
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
