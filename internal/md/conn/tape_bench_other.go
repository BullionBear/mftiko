//go:build !linux

package conn

import "time"

func threadCPU() time.Duration { return 0 }
