package shim

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
)

// procStat is what /proc/<pid>/stat says of a process: its state (field 3) and its start time (field 22, in clock
// ticks after boot).
func procStat(pid int) (state byte, startTime uint64, err error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, err
	}
	// The command name is field 2, in parentheses, and may hold spaces or parentheses itself: count from the last ")".
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return 0, 0, fmt.Errorf("shim: /proc/%d/stat: no command name", pid)
	}
	fields := bytes.Fields(b[i+1:]) // field 3 onwards
	const idx = 22 - 3
	if len(fields) <= idx || len(fields[0]) != 1 {
		return 0, 0, fmt.Errorf("shim: /proc/%d/stat: %d fields", pid, len(fields)+2)
	}
	startTime, err = strconv.ParseUint(string(fields[idx]), 10, 64)
	return fields[0][0], startTime, err
}

// procStartTime is when the process started, in clock ticks after boot.
func procStartTime(pid int) (uint64, error) {
	_, st, err := procStat(pid)
	return st, err
}

// Running reports whether the process that had pid and startTime (Attached.pid and start_time) is still running.
// A pid that now belongs to a process started at another time was reused: the one asked about is gone. So is a
// zombie: it has ended, and only waits to be reaped. An error is a /proc that could not be read for another
// reason; the caller cannot tell, and must not take the process for gone.
func Running(pid int, startTime uint64) (bool, error) {
	if pid <= 0 {
		return false, fmt.Errorf("shim: pid %d", pid)
	}
	state, st, err := procStat(pid)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, err
	case st != startTime:
		return false, nil
	default:
		return state != 'Z' && state != 'X', nil
	}
}
