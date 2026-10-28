// Package logger builds the zerolog logger every mftiko process uses.
//
// Logs go to stderr. For workers the shim captures stderr into rotated files (PROTOCOL §4.5 S4), so
// the default format is JSON, one object per line. Every line carries the component and the build
// identity the caller passes. This package does not import internal: the platform passes buildinfo,
// and a strategy passes its own release (DECISIONS E15-D018, E15-D020).
package logger

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Environment variables read by New.
const (
	// EnvLevel is trace, debug, info, warn or error. Default info.
	EnvLevel = "MFTIKO_LOG_LEVEL"
	// EnvFormat is json (default) or console, a human-readable form for local development.
	EnvFormat = "MFTIKO_LOG_FORMAT"
)

func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
}

// New returns the logger for one process, writing to stderr. component is the binary name, for
// example "mftiko-md-agent", or "strategy" for a session worker. version and commit are that
// binary's own identity; empty strings are still written.
func New(component, version, commit string) zerolog.Logger {
	return newLogger(component, version, commit, writer(os.Getenv(EnvFormat), os.Stderr))
}

// NewWithWriter is New with an explicit writer and no format handling; tests use it.
// The level still comes from EnvLevel.
func NewWithWriter(component, version, commit string, w io.Writer) zerolog.Logger {
	return newLogger(component, version, commit, w)
}

func newLogger(component, version, commit string, w io.Writer) zerolog.Logger {
	return zerolog.New(w).
		Level(level(os.Getenv(EnvLevel))).
		With().
		Timestamp().
		Str("component", component).
		Str("version", version).
		Str("commit", commit).
		Logger()
}

func level(s string) zerolog.Level {
	lvl, err := zerolog.ParseLevel(strings.ToLower(strings.TrimSpace(s)))
	if err != nil || lvl == zerolog.NoLevel {
		return zerolog.InfoLevel
	}
	return lvl
}

func writer(format string, out io.Writer) io.Writer {
	if strings.EqualFold(strings.TrimSpace(format), "console") {
		return zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	}
	return out
}
