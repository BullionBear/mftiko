// Command mftiko-conformance runs the SDK conformance suite against a worker
// command: a fake shim on fd 3, an embedded nats-server, and fake MD and TD
// peers (E08-D028–E08-D039, PROTOCOL §9.8).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/internal/sts/conformance"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	jsonPath := flag.String("json", "", "also write the JSON report to this path")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: mftiko-conformance [-json PATH] -- <worker argv>")
		os.Exit(2)
	}
	rep, err := conformance.Run(context.Background(), conformance.Config{Command: conformance.Command(args)})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	raw, err := rep.JSON()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	raw = append(raw, '\n')
	if _, err := os.Stdout.Write(raw); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *jsonPath != "" {
		if err := os.WriteFile(*jsonPath, raw, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	fmt.Fprint(os.Stderr, rep.Table())
	if rep.Failed() {
		os.Exit(1)
	}
}
