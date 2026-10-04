// Command mftiko-td-account is the TD account worker for one api_id: private websocket, HTTP pool, OMS, ledger and recon (DECISIONS D9, D27).
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
	fmt.Fprintln(os.Stderr, "mftiko-td-account: not implemented")
	os.Exit(1)
}
