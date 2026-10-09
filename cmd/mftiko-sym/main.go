// Command mftiko-sym is the symbol service (sym), following mftik's symbol model (DECISIONS D201).
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
	log := logger.New("mftiko-sym")
	log.Error().Msg("not implemented")
	os.Exit(1)
}
