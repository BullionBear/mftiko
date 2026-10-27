package agent

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Temporary timing log for the sts/agent and deploy study. Lines are written
// only when MFTIKO_MEASURE names a file. This is not a product setting.

var measureMu sync.Mutex

func measureLog(test, kind, what string, ms int64, extra string) {
	path := os.Getenv("MFTIKO_MEASURE")
	if path == "" {
		return
	}
	if test == "" {
		test = os.Getenv("MFTIKO_MEASURE_TEST")
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

func measureProc(role string) {
	ms := int64(-1)
	if start, ok := procWallStart(); ok {
		ms = time.Since(start).Milliseconds()
	}
	var n int64
	if self, err := os.Executable(); err == nil {
		if st, err := os.Stat(self); err == nil {
			n = st.Size()
		}
	}
	measureLog("", "proc", role, ms, fmt.Sprintf("\tbytes=%d", n))
}

func procWallStart() (time.Time, bool) {
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return time.Time{}, false
	}
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 || i+2 >= len(b) {
		return time.Time{}, false
	}
	fields := strings.Fields(string(b[i+2:]))
	const idx = 22 - 3
	if len(fields) <= idx {
		return time.Time{}, false
	}
	ticks, err := strconv.ParseInt(fields[idx], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	const hz = 100 // Linux user clock ticks, the unit of /proc stat field 22
	stat, err := os.ReadFile("/proc/stat")
	if err != nil {
		return time.Time{}, false
	}
	var btime int64
	found := false
	for _, line := range strings.Split(string(stat), "\n") {
		if !strings.HasPrefix(line, "btime ") {
			continue
		}
		btime, err = strconv.ParseInt(strings.TrimPrefix(line, "btime "), 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		found = true
		break
	}
	if !found {
		return time.Time{}, false
	}
	return time.Unix(btime, 0).Add(time.Duration(ticks) * time.Second / time.Duration(hz)), true
}
