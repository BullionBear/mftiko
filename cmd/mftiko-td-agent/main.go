// Command mftiko-td-agent is the execution-plane agent. It starts account workers by refcount, runs the backfill scheduler, and drains history into Postgres (DECISIONS D9, D20).
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
	fmt.Fprintln(os.Stderr, "mftiko-td-agent: not implemented")
	os.Exit(1)
}
