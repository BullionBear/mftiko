package cli

import (
	"io"
	"os"
)

// stdoutIsTerminal reports whether a writer is a terminal. Tests replace it.
// A bytes.Buffer is not a terminal, so watch tests opt in.
var stdoutIsTerminal = fileIsTerminal

func fileIsTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	return stdinIsTerminal(f)
}
