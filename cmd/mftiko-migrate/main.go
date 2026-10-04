// Command mftiko-migrate applies the PostgreSQL migrations embedded from migrations/ (up, down N,
// status). It is a deploy step; agents never migrate on startup.
//
// Shape only: not implemented yet.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BullionBear/mftiko/internal/buildinfo"
	"github.com/BullionBear/mftiko/internal/logger"
	"github.com/BullionBear/mftiko/migrations"
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	database := flag.String("database", os.Getenv("DATABASE_URL"), "PostgreSQL URL (default $DATABASE_URL)")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	_, _ = database, migrations.FS
	log := logger.New("mftiko-migrate")
	log.Error().Msg("not implemented")
	os.Exit(1)
}
