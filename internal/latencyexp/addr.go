package latencyexp

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// CheckAddr reports whether addr is host:port on a loopback, link-local, or
// private network. A wildcard (0.0.0.0, ::, or an empty host) is rejected:
// the exporter listens only on a private network (E17-D006).
func CheckAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("-metrics-addr must be a private host:port, got %q", addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return fmt.Errorf("-metrics-addr port must be 0–65535, got %q", addr)
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(zoneFree(host))
	if ip == nil {
		return fmt.Errorf("-metrics-addr host must be a private IP, got %q", addr)
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
		return nil
	}
	return fmt.Errorf("-metrics-addr %q is not a private network address", addr)
}

// zoneFree drops an IPv6 zone so ParseIP can see the address.
func zoneFree(host string) string {
	if i := strings.LastIndex(host, "%"); i >= 0 {
		return host[:i]
	}
	return host
}
