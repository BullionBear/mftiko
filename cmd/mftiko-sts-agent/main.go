// Command mftiko-sts-agent is the strategy-plane agent. It reconciles session specs, writes leases to MD and TD, signs each session's NATS credentials, and spawns session workers through shims (DECISIONS D1, D2, D9, D30).
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
	fmt.Fprintln(os.Stderr, "mftiko-sts-agent: not implemented")
	os.Exit(1)
}
