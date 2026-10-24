// Command mftiko-shim is the per-worker shim: the sole parent of one worker and the point where a new agent adopts it after a rotation (DECISIONS E01-D002–E01-D005, E00-D002, PROTOCOL §4).
//
// The agent execs it and writes one Launch frame on its stdin (PROTOCOL §4.6).
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/logger"
	"github.com/BullionBear/mftiko/internal/platform/shim"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	os.Exit(shim.Run(logger.New("mftiko-shim")))
}
