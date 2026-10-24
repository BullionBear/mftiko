//go:build !linux

package artifact

import "errors"

// RunningDigests reports image digests of processes running from ${DATA} (E12-D014).
// Off Linux there is no /proc to read, so GC is told to delete nothing.
func RunningDigests(string) ([]string, error) {
	return nil, errors.New("artifact: running digests are read from /proc")
}
