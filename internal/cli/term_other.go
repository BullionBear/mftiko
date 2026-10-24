//go:build !linux

package cli

import (
	"errors"
	"io"
	"os"
)

func stdinIsTerminal(io.Reader) bool { return false }

func withoutEcho(*os.File) (func(), error) {
	return func() {}, errors.New("not a terminal")
}
