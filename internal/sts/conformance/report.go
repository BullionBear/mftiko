package conformance

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	statusPass = "pass"
	statusFail = "fail"
	statusSkip = "skip"
)

// Check is one row of the report (D831, D838).
type Check struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// Report is the JSON document the tool writes to stdout.
type Report struct {
	Checks []Check `json:"checks"`
}

// Failed is true when any check failed. Skip is not a failure (D838).
func (r Report) Failed() bool {
	for _, c := range r.Checks {
		if c.Status == statusFail {
			return true
		}
	}
	return false
}

// JSON is the indented report.
func (r Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// Table is the human-readable form written to stderr.
func (r Report) Table() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-16s  %-6s  %s\n", "id", "status", "detail")
	for _, c := range r.Checks {
		fmt.Fprintf(&b, "%-16s  %-6s  %s\n", c.ID, c.Status, c.Detail)
	}
	return b.String()
}
