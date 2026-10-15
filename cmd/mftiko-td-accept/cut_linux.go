package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// accountComm is mftiko-td-account as /proc/<pid>/comm spells it: the first 15 bytes.
const accountComm = "mftiko-td-accou"

type sock struct {
	fd     int
	remote string // ip:port
}

// conns lists the account workers on this host and their established TCP sockets. It must run in
// the workers' PID and network namespace (in deploy/accept, the runner container).
func conns() error {
	flag.Parse()
	pids, err := accountPIDs()
	if err != nil {
		return err
	}
	for _, pid := range pids {
		fmt.Printf("%s pid=%d sockets=%v\n", stamp(), pid, sockets(pid))
	}
	return nil
}

// cut shuts down the worker's sockets to host from outside: pidfd_getfd, then shutdown(SHUT_RDWR)
// on the copy. The worker is not signalled; it sees the connection end, as when the venue closes
// it. With host the private stream's (stream.binancefuture.com on the BinanceUM testnet), only that
// connection goes: REST and the trade websocket are on other addresses. Where they share an address
// (Bybit's /v5/private and /v5/trade), -fd picks one of the sockets conns lists. Needs Linux 5.6 and
// ptrace access to the worker (same user, or CAP_SYS_PTRACE).
func cut() error {
	pid := flag.Int("pid", 0, "account worker pid; 0 when there is exactly one")
	host := flag.String("host", "", "cut only the sockets to this host's addresses")
	only := flag.Int("fd", -1, "cut only this one of the worker's sockets to host")
	flag.Parse()
	if *host == "" {
		return errors.New("-host is required")
	}
	if *pid == 0 {
		pids, err := accountPIDs()
		if err != nil {
			return err
		}
		if len(pids) != 1 {
			return fmt.Errorf("%d account workers here; name one with -pid", len(pids))
		}
		*pid = pids[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupHost(ctx, *host)
	if err != nil {
		return err
	}
	var target []sock
	for _, s := range sockets(*pid) {
		ip, _, _ := net.SplitHostPort(s.remote)
		if slices.Contains(addrs, ip) && (*only < 0 || s.fd == *only) {
			target = append(target, s)
		}
	}
	if len(target) == 0 {
		return fmt.Errorf("pid %d has no established socket to %s %v", *pid, *host, addrs)
	}
	pfd, err := unix.PidfdOpen(*pid, 0)
	if err != nil {
		return fmt.Errorf("pidfd_open %d: %w", *pid, err)
	}
	defer unix.Close(pfd)
	for _, s := range target {
		fd, err := unix.PidfdGetfd(pfd, s.fd, 0)
		if err != nil {
			return fmt.Errorf("pidfd_getfd %d/%d: %w", *pid, s.fd, err)
		}
		err = unix.Shutdown(fd, unix.SHUT_RDWR)
		unix.Close(fd)
		if err != nil {
			return fmt.Errorf("shutdown %d/%d: %w", *pid, s.fd, err)
		}
		fmt.Printf("%s cut pid=%d fd=%d remote=%s (%s)\n", stamp(), *pid, s.fd, s.remote, *host)
	}
	return nil
}

func accountPIDs() ([]int, error) {
	dirs, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var out []int
	for _, d := range dirs {
		pid, err := strconv.Atoi(d.Name())
		if err != nil {
			continue
		}
		comm, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
		if err == nil && strings.TrimSpace(string(comm)) == accountComm {
			out = append(out, pid)
		}
	}
	return out, nil
}

// sockets is the pid's established IPv4 TCP sockets.
func sockets(pid int) []sock {
	inodes := map[string]int{}
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return nil
	}
	for _, fd := range fds {
		link, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, fd.Name()))
		if err != nil {
			continue
		}
		if ino, ok := strings.CutPrefix(link, "socket:["); ok {
			n, _ := strconv.Atoi(fd.Name())
			inodes[strings.TrimSuffix(ino, "]")] = n
		}
	}
	f, err := os.Open(fmt.Sprintf("/proc/%d/net/tcp", pid))
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []sock
	sc := bufio.NewScanner(f)
	sc.Scan() // header
	for sc.Scan() {
		// sl local rem st tx:rx tr:when retrnsmt uid timeout inode
		col := strings.Fields(sc.Text())
		if len(col) < 10 || col[3] != "01" {
			continue
		}
		if fd, ok := inodes[col[9]]; ok {
			out = append(out, sock{fd: fd, remote: remoteAddr(col[2])})
		}
	}
	_ = sc.Err() // a short read lists fewer sockets; cut then says there are none
	return out
}

// remoteAddr spells an IPv4 /proc/net/tcp address.
func remoteAddr(hex string) string {
	ip, port, _ := strings.Cut(hex, ":")
	p, _ := strconv.ParseInt(port, 16, 32)
	v, _ := strconv.ParseUint(ip, 16, 32)
	return fmt.Sprintf("%d.%d.%d.%d:%d", v&0xff, v>>8&0xff, v>>16&0xff, v>>24, p)
}
