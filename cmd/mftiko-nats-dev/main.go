// Command mftiko-nats-dev generates the dev NATS operator, accounts, and user credentials,
// or checks a server that is already using them (D412–D417).
//
// Generate writes into -out, which make dev-up sets to deploy/dev/data/nats. That directory
// is gitignored. Keys already present are reused. -check does not generate and does not
// write session credentials; it signs one ephemeral caller in memory.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/platform/natsdev"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	out := flag.String("out", "deploy/dev/data/nats", "directory for generated dev NATS files")
	mdInst := flag.String("md-inst", natsdev.DefaultInst, "MD instance token")
	fetchInst := flag.String("fetch-inst", natsdev.DefaultInst, "md.fetch instance token")
	check := flag.Bool("check", false, "check a running server instead of generating")
	url := flag.String("url", "nats://127.0.0.1:4222", "server URL for -check")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	if *check {
		if err := natsdev.Check(*out, *url); err != nil {
			fmt.Fprintln(os.Stderr, "mftiko-nats-dev:", err)
			os.Exit(1)
		}
		fmt.Println("ok")
		return
	}
	if err := natsdev.Generate(*out, natsdev.Options{
		MDInsts:    []string{*mdInst},
		FetchInsts: []string{*fetchInst},
	}); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-nats-dev:", err)
		os.Exit(1)
	}
	lay, err := natsdev.Load(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-nats-dev:", err)
		os.Exit(1)
	}
	fmt.Print(lay.Manifest.Summary())
}
