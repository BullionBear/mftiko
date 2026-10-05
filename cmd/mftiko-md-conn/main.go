// Command mftiko-md-conn is the MD conn worker: one public websocket per (venue, route, n). It folds books, keeps tapes and serves md.view (DECISIONS D21, D26).
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
	log := logger.New("mftiko-md-conn")
	log.Error().Msg("not implemented")
	os.Exit(1)
}
