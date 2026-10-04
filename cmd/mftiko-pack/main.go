// Command mftiko-pack builds a strategy artifact: an OCI image carrying the mftiko manifest (DECISIONS D18, PROTOCOL §8).
//
// Shape only: not implemented yet.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BullionBear/mftiko/internal/buildinfo"
	"github.com/BullionBear/mftiko/internal/logger"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	log := logger.New("mftiko-pack")
	log.Error().Msg("not implemented")
	os.Exit(1)
}
