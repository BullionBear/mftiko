//go:build linux

package release_test

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

var measureMu sync.Mutex

func measureSelf() {
	extra := ""
	if self, err := os.Executable(); err == nil {
		if st, err := os.Stat(self); err == nil {
			extra += fmt.Sprintf("\tbytes=%d", st.Size())
		}
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "-race" || s.Key == "CGO_ENABLED" {
				extra += fmt.Sprintf("\t%s=%s", strings.TrimPrefix(s.Key, "-"), s.Value)
			}
		}
	}
	measureLog("-", "setup", "test_binary", 0, extra)
}

func measureLog(test, kind, what string, ms int64, extra string) {
	path := os.Getenv("MFTIKO_MEASURE")
	if path == "" {
		return
	}
	if test == "" {
		test = "-"
	}
	what = strings.ReplaceAll(what, "\t", " ")
	what = strings.ReplaceAll(what, "\n", " ")
	line := fmt.Sprintf("MEASURE\ttest=%s\tkind=%s\twhat=%s\tms=%d%s\n", test, kind, what, ms, extra)
	measureMu.Lock()
	defer measureMu.Unlock()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	_, _ = f.WriteString(line)
	_ = f.Close()
}

type pollClock struct {
	t     *testing.T
	kind  string
	what  string
	start time.Time
	every time.Duration
	n     int
}

func beginPoll(t *testing.T, kind, what string, every time.Duration) *pollClock {
	t.Helper()
	return &pollClock{t: t, kind: kind, what: what, start: time.Now(), every: every}
}

func (p *pollClock) slept() {
	if p != nil {
		p.n++
	}
}

func (p *pollClock) done() {
	if p == nil || p.t == nil {
		return
	}
	sleepMs := int64(0)
	if p.every > 0 {
		sleepMs = (time.Duration(p.n) * p.every).Milliseconds()
	}
	measureLog(p.t.Name(), p.kind, p.what, time.Since(p.start).Milliseconds(),
		fmt.Sprintf("\tsleep_ms=%d\tsleeps=%d", sleepMs, p.n))
}
