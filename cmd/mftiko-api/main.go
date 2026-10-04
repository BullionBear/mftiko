// Command mftiko-api is the API server. It writes declarations (sessions, accounts) to etcd and serves the web console; it never calls an agent directly (DECISIONS D13, D14).
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
	fmt.Fprintln(os.Stderr, "mftiko-api: not implemented")
	os.Exit(1)
}
