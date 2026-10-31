// Command mftiko-eventlog reads the strategy event log a session writes under VolumeDir()/eventlog
// when its spec turns the log on (DECISIONS E20-D002, E20-D009, PROTOCOL §12).
//
//	mftiko-eventlog dump [-incarnation N] PATH...   print the log as JSON lines on stdout
//
// PATH is an eventlog directory, printed oldest file first, or one .mel file. -incarnation limits
// a directory to one worker incarnation. Each file is a header line, then one line per frame.
// A replay needs the strategy's own code, so it is not here: a test of the strategy calls
// sdk.Replay on the directory.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/sdk/eventlog/dump"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mftiko-eventlog:", err)
		os.Exit(1)
	}
}

const usage = "usage: mftiko-eventlog dump [-incarnation N] PATH..., or -version"

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	if args[0] == "-version" || args[0] == "--version" {
		_, err := fmt.Fprintln(stdout, buildinfo.String())
		return err
	}
	if args[0] != "dump" {
		return errors.New(usage)
	}
	fs := flag.NewFlagSet("dump", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	inc := fs.Uint("incarnation", 0, "print only this incarnation of a directory; 0 prints all")
	if err := fs.Parse(args[1:]); err != nil {
		return fmt.Errorf("%w\n%s", err, usage)
	}
	if fs.NArg() == 0 {
		return errors.New(usage)
	}
	if *inc > 1<<32-1 {
		return fmt.Errorf("incarnation %d is not a uint32", *inc)
	}
	for _, path := range fs.Args() {
		if err := dump.Path(stdout, path, uint32(*inc)); err != nil {
			return err
		}
	}
	return nil
}
