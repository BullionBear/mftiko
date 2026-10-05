package sdk

import (
	"fmt"
	"os"
)

// Run hosts s as a session worker: it speaks C4 on fd 3, connects to NATS with the session's
// credentials, and dispatches hooks (PROTOCOL §5, §9). It does not return.
func Run(s Strategy) {
	_ = s
	fmt.Fprintln(os.Stderr, "mftiko sdk: not implemented")
	os.Exit(10) // INIT_FAILED (PROTOCOL §5.3)
}
