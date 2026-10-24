// Command mftiko is the API client (E11-D001).
//
//	mftiko login
//	mftiko whoami
//	mftiko strategy list
//	mftiko strategy versions <id>
//	mftiko strategy show <id> <ref>
//	mftiko instance list
//	mftiko instance show <name>
//	mftiko account-kind list
//	mftiko account list|show|create|update|delete
//	mftiko session create -f <file> [--wait <phase|terminal>]
//	mftiko session list|show|end|delete|watch
//	mftiko session end <id> [--wait]
//	mftiko record list|show
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
