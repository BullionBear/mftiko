//go:build !linux

package shim

import (
	"errors"

	"github.com/rs/zerolog"
)

// Run needs Linux: setsid, PR_SET_CHILD_SUBREAPER and Pdeathsig (PROTOCOL §4.5 S1, S2).
func Run(log zerolog.Logger) int {
	log.Error().Err(errors.New("shim: needs linux")).Msg("unsupported")
	return 1
}
