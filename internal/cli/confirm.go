package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrNotConfirmed is a declined destructive command. ErrYesRequired is the same
// command off a terminal without --yes. Both are usage errors (exit 1, E11-D009).
var (
	ErrNotConfirmed = errors.New("not confirmed")
	ErrYesRequired  = errors.New("not a terminal; pass --yes")
)

// Confirm is the shared check for a destructive command (E11-D009).
// E11-04 and E11-06 call it.
//
// On a terminal it writes prompt and " [y/N] " to out, then reads one line from
// in. y and Y continue. An empty line and any other answer, including "yes",
// refuse. --yes does not skip the question on a terminal.
//
// Off a terminal, yes must be true. stdin is not read.
func Confirm(in io.Reader, out io.Writer, tty, yes bool, prompt string) error {
	if !tty {
		if yes {
			return nil
		}
		return ErrYesRequired
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		prompt = "confirm?"
	}
	if _, err := fmt.Fprintf(out, "%s [y/N] ", prompt); err != nil {
		return err
	}
	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	switch strings.TrimSpace(line) {
	case "y", "Y":
		return nil
	default:
		return ErrNotConfirmed
	}
}
