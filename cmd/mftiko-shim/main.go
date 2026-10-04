// Command mftiko-shim is the per-worker shim: the sole parent of one worker and the point where a new agent adopts it after a rotation (DECISIONS D3-D7, PROTOCOL §4).
//
// Shape only: not implemented yet.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BullionBear/mftiko/internal/buildinfo"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.Version)
		return
	}
	fmt.Fprintln(os.Stderr, "mftiko-shim: not implemented")
	os.Exit(1)
}
