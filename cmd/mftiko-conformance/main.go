// Command mftiko-conformance runs the SDK conformance suite against a strategy artifact: a fake shim on fd 3, a real nats-server, and fake MD and TD peers (DECISIONS D19, PROTOCOL §9.8).
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
	fmt.Fprintln(os.Stderr, "mftiko-conformance: not implemented")
	os.Exit(1)
}
