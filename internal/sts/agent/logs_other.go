//go:build !unix

package agent

import "context"

// startLogs is a no-op off unix: the log reader is unix-only. A configured
// endpoint is logged and left unpublished so the term still runs.
func (p *plane) startLogs(_ context.Context, _ <-chan struct{}, cfg LogConfig) (func() string, <-chan struct{}, func()) {
	if cfg.Listen != "" && cfg.TokenSHA256 != "" {
		p.log.Warn().Msg("log endpoint is not supported on this platform")
	}
	return nil, nil, func() {}
}
