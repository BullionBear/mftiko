// Command mftiko-md-fetch is the stateless md.fetch service. It forwards public REST reads such as klines (DECISIONS D21).
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
	log := logger.New("mftiko-md-fetch")
	log.Error().Msg("not implemented")
	os.Exit(1)
}
