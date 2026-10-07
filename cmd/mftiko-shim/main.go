// Command mftiko-shim is the per-worker shim: the sole parent of one worker and the point where a new agent adopts it after a rotation (DECISIONS D3-D7, PROTOCOL §4).
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
	log := logger.New("mftiko-shim")
	log.Error().Msg("not implemented")
	os.Exit(1)
}
