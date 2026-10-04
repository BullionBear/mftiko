// Command mftiko-md-agent is the market-data-plane agent. It derives atoms from leases, places them on connections, and spawns conn workers (DECISIONS D9, D12, D26).
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
	fmt.Fprintln(os.Stderr, "mftiko-md-agent: not implemented")
	os.Exit(1)
}
