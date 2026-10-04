// Command mftiko-pack builds a strategy artifact: an OCI image carrying the mftiko manifest (DECISIONS D18, PROTOCOL §8).
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
	fmt.Fprintln(os.Stderr, "mftiko-pack: not implemented")
	os.Exit(1)
}
