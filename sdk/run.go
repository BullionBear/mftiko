package sdk

import (
	"fmt"
	"os"
)

// Run hosts s as a session worker: it speaks C4 on fd 3, connects to NATS with the session's
// credentials, and dispatches hooks (PROTOCOL §5, §9). It does not return.
//
// SIGTERM belongs to Run, not to c4.Serve. Run takes it with signal.NotifyContext and passes that
// context to Serve, as mftiko-md-conn and mftiko-td-account do. Cancelling it is a Stop. Serve does
// not watch signals (DECISIONS D805).
func Run(s Strategy) {
	_ = s
	fmt.Fprintln(os.Stderr, "mftiko sdk: not implemented")
	os.Exit(10) // INIT_FAILED (PROTOCOL §5.3)
}
