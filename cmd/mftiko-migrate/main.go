// Command mftiko-migrate applies the PostgreSQL migrations embedded from migrations/ (E12-D027–E12-D032).
//
//	mftiko-migrate [-database URL] up
//	mftiko-migrate [-database URL] status
//	mftiko-migrate -version
//
// -database defaults to $DATABASE_URL. up applies each pending file in its own transaction and
// records it in schema_migrations. status lists applied and pending files and does not lock.
// There is no down: a deployment does not downgrade the schema (E12-D024, E12-D028).
// It is a deploy step; agents never migrate on startup.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/migrations"
)

func main() {
	code := exitCode(run(os.Args[1:], os.Stdout))
	os.Exit(code)
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	fmt.Fprintln(os.Stderr, "mftiko-migrate:", err)
	var usage usageError
	if errors.As(err, &usage) {
		return 2
	}
	return 1
}

type usageError struct{ err error }

func (e usageError) Error() string { return e.err.Error() }
func (e usageError) Unwrap() error { return e.err }

const usageText = "usage: mftiko-migrate [-database URL] up|status"

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("mftiko-migrate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	version := fs.Bool("version", false, "print the release and exit")
	database := fs.String("database", os.Getenv("DATABASE_URL"), "PostgreSQL URL (default $DATABASE_URL)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return usageError{errors.New(usageText)}
		}
		return usageError{err}
	}
	if *version {
		_, err := fmt.Fprintln(stdout, buildinfo.String())
		return err
	}
	rest := fs.Args()
	if len(rest) != 1 {
		return usageError{errors.New(usageText)}
	}
	if strings.TrimSpace(*database) == "" {
		return usageError{errors.New("database URL is empty (set -database or DATABASE_URL)")}
	}
	ctx := context.Background()
	switch rest[0] {
	case "up":
		res, err := migrations.Up(ctx, *database)
		for _, a := range res.Applied {
			if _, werr := fmt.Fprintf(stdout, "applied %s\n", a); werr != nil && err == nil {
				err = werr
			}
		}
		if err != nil {
			return err
		}
		if len(res.Applied) == 0 {
			_, err = fmt.Fprintln(stdout, "up to date")
		}
		return err
	case "status":
		lines, err := migrations.Status(ctx, *database)
		for _, line := range lines {
			if _, werr := fmt.Fprintln(stdout, line); werr != nil && err == nil {
				err = werr
			}
		}
		return err
	default:
		return usageError{fmt.Errorf("unknown command %q", rest[0])}
	}
}
