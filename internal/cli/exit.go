package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// Process exit codes (E11-D007). 0 is success, 1 is a usage or configuration
// error, 2 is a network error. API problem codes start at 10.
const (
	ExitOK                  = 0
	ExitUsage               = 1
	ExitNetwork             = 2
	ExitValidationFailed    = 10
	ExitUnauthenticated     = 11
	ExitNotFound            = 12
	ExitConflict            = 13
	ExitNotTerminal         = 14
	ExitReferenced          = 15
	ExitAlgoOverlap         = 16
	ExitRegionUnavailable   = 17
	ExitStorageUnavailable  = 18
	ExitRegistryUnavailable = 19
	ExitInternal            = 20
)

// ExitCode maps an API problem code to a process exit code (E11-D007).
// ok is false for a code the table does not name. invalid_artifact is one of
// those: the decision does not assign it, and this function does not invent a number.
func ExitCode(code apiv1.ErrorCode) (int, bool) {
	switch code {
	case apiv1.ErrorCodeValidationFailed:
		return ExitValidationFailed, true
	case apiv1.ErrorCodeUnauthenticated:
		return ExitUnauthenticated, true
	case apiv1.ErrorCodeNotFound:
		return ExitNotFound, true
	case apiv1.ErrorCodeConflict:
		return ExitConflict, true
	case apiv1.ErrorCodeNotTerminal:
		return ExitNotTerminal, true
	case apiv1.ErrorCodeReferenced:
		return ExitReferenced, true
	case apiv1.ErrorCodeAlgoOverlap:
		return ExitAlgoOverlap, true
	case apiv1.ErrorCodeRegionUnavailable:
		return ExitRegionUnavailable, true
	case apiv1.ErrorCodeStorageUnavailable:
		return ExitStorageUnavailable, true
	case apiv1.ErrorCodeRegistryUnavailable:
		return ExitRegistryUnavailable, true
	case apiv1.ErrorCodeInternal:
		return ExitInternal, true
	default:
		return 0, false
	}
}

// reportProblem writes a problem+json body to stderr and returns the exit code.
// The name and the number are on the same line (E11-D007). Each errors[].path
// is its own line, and X-Request-Id is printed when the response carried one
// (E11-D008). The exit code comes from the body's code, not the HTTP status.
// A code outside E11-D007 is printed without a number and returns ExitUsage.
func reportProblem(w io.Writer, raw []byte, requestID string, sec secrets) int {
	var p apiv1.Problem
	if err := json.Unmarshal(raw, &p); err != nil || p.Code == "" {
		fmt.Fprintf(w, "mftiko: %s\n", sec.redact("response is not a problem"))
		return ExitUsage
	}
	code, ok := ExitCode(p.Code)
	var b strings.Builder
	if ok {
		fmt.Fprintf(&b, "mftiko: %s (%d): %s\n", p.Code, code, p.Title)
	} else {
		fmt.Fprintf(&b, "mftiko: %s: %s\n", p.Code, p.Title)
	}
	if p.Detail != nil && *p.Detail != "" {
		fmt.Fprintln(&b, *p.Detail)
	}
	if p.Errors != nil {
		for _, e := range *p.Errors {
			if e.Reason != "" {
				fmt.Fprintf(&b, "%s: %s\n", e.Path, e.Reason)
			} else if e.Path != "" {
				fmt.Fprintln(&b, e.Path)
			}
		}
	}
	if requestID != "" {
		fmt.Fprintf(&b, "X-Request-Id: %s\n", requestID)
	}
	if !ok {
		// Not an assignment. The line names the gap so a later decision can
		// number the code without this package having picked one.
		fmt.Fprintf(&b, "mftiko: API code %s has no exit code in E11-D007\n", p.Code)
		code = ExitUsage
	}
	fmt.Fprint(w, sec.redact(b.String()))
	return code
}
