// Command mftiko is the API client (E11-D001).
//
//	mftiko login
//	mftiko whoami
//
// It speaks only to the API. mftiko-pack and mftiko-seal are separate programs.
package main

import (
	"context"
	"os"

	"github.com/BullionBear/mftiko/internal/cli"
)

func main() {
	home, err := os.UserHomeDir()
	if err != nil {
		home = ""
	}
	os.Exit(cli.Run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv, home))
}
