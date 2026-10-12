package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

const connComm = "mftiko-md-conn"

var ports = flag.String("ports", "443,8443,9443", "remote TCP ports that count as venue sockets")

// worker is one running conn worker as seen from /proc.
type worker struct {
	pid     int
	key     string // from the shim's log name; empty when no placement has that hash
	hash    string
	sockets []sock
}

type sock struct {
	fd     int
	remote string
}

// conns lists the conn workers on this host. It must run in the workers' PID and network
// namespace (in the deploy/accept runner, that is the runner container).
func conns() error {
	flag.Parse()
	ws, err := workers()
	if err != nil {
		return err
	}
	for _, w := range ws {
		fmt.Printf("%s pid=%d key=%q hash=%s venue sockets=%v\n", stamp(), w.pid, w.key, w.hash, w.sockets)
	}
	return nil
}

// cut shuts down a conn worker's venue sockets from outside: pidfd_getfd, then
// shutdown(SHUT_RDWR) on the copy. The worker is not signalled; it sees its connection end, as
// when the venue closes it, and redials with the next conn_epoch (D409). Needs Linux 5.6 and
// ptrace access to the worker (same user, or CAP_SYS_PTRACE).
func cut() error {
	key := flag.String("conn", "", "conn worker key, e.g. md/BinanceUM/public/0")
	pid := flag.Int("pid", 0, "conn worker pid, instead of -conn")
	flag.Parse()
	if (*key == "") == (*pid == 0) {
		return errors.New("one of -conn and -pid")
	}
	ws, err := workers()
	if err != nil {
		return err
	}
	var target *worker
	for i, w := range ws {
		if (*key != "" && w.key == *key) || w.pid == *pid {
			target = &ws[i]
		}
	}
	if target == nil {
		return fmt.Errorf("no conn worker %q (pid %d) here", *key, *pid)
	}
	if len(target.sockets) == 0 {
		return fmt.Errorf("pid %d has no established socket on ports %s", target.pid, *ports)
	}
	pfd, err := unix.PidfdOpen(target.pid, 0)
	if err != nil {
		return fmt.Errorf("pidfd_open %d: %w", target.pid, err)
	}
	defer unix.Close(pfd)
	for _, s := range target.sockets {
		fd, err := unix.PidfdGetfd(pfd, s.fd, 0)
		if err != nil {
			return fmt.Errorf("pidfd_getfd %d/%d: %w", target.pid, s.fd, err)
		}
		err = unix.Shutdown(fd, unix.SHUT_RDWR)
		unix.Close(fd)
		if err != nil {
			return fmt.Errorf("shutdown %d/%d: %w", target.pid, s.fd, err)
		}
		fmt.Printf("%s cut pid=%d key=%q fd=%d remote=%s\n", stamp(), target.pid, target.key, s.fd, s.remote)
	}
	return nil
}

func workers() ([]worker, error) {
	keys := placementKeys()
	want, err := portSet(*ports)
	if err != nil {
		return nil, err
	}
	dirs, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var out []worker
	for _, d := range dirs {
		pid, err := strconv.Atoi(d.Name())
		if err != nil {
			continue
		}
		comm, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
		if err != nil || strings.TrimSpace(string(comm)) != connComm {
			continue
		}
		w := worker{pid: pid, hash: shimHash(parent(pid))}
		w.key = keys[w.hash]
		w.sockets = venueSockets(pid, want)
		out = append(out, w)
	}
	return out, nil
}

// placementKeys maps WorkerHash(conn) to conn for the instance's placements. Without etcd the
// map is empty and workers show only their hash.
func placementKeys() map[string]string {
	out := map[string]string{}
	c, err := root()
	if err != nil {
		return out
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.Get(ctx, state.PlacementPrefix(*instance), clientv3.WithPrefix())
	if err != nil {
		return out
	}
	for _, kv := range resp.Kvs {
		p := &statev1.Placement{}
		if proto.Unmarshal(kv.Value, p) == nil && p.GetConn() != "" {
			out[state.WorkerHash(p.GetConn())] = p.GetConn()
		}
	}
	return out
}

func parent(pid int) int {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	// pid (comm) state ppid ...: comm may hold spaces, so start after the last ')'.
	f := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+1:]))
	if len(f) < 2 {
		return 0
	}
	ppid, _ := strconv.Atoi(f[1])
	return ppid
}

// shimHash is the worker hash in the name of the log the shim keeps open
// (fleet: <WorkerHash(key)>.shim.log).
func shimHash(shim int) string {
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", shim))
	if err != nil {
		return ""
	}
	for _, fd := range fds {
		link, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", shim, fd.Name()))
		if err != nil {
			continue
		}
		if h, ok := strings.CutSuffix(filepath.Base(link), ".shim.log"); ok {
			return h
		}
	}
	return ""
}

// venueSockets is the pid's established TCP sockets whose remote port is in want.
func venueSockets(pid int, want map[int]bool) []sock {
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
	var out []sock
	for _, table := range []string{"tcp", "tcp6"} {
		f, err := os.Open(fmt.Sprintf("/proc/%d/net/%s", pid, table))
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Scan() // header
		for sc.Scan() {
			// sl local rem st tx:rx tr:when retrnsmt uid timeout inode
			col := strings.Fields(sc.Text())
			if len(col) < 10 || col[3] != "01" {
				continue
			}
			fd, ok := inodes[col[9]]
			if !ok {
				continue
			}
			_, portHex, _ := strings.Cut(col[2], ":")
			port, err := strconv.ParseInt(portHex, 16, 32)
			if err != nil || !want[int(port)] {
				continue
			}
			out = append(out, sock{fd: fd, remote: remoteAddr(col[2])})
		}
		_ = sc.Err() // a short read lists fewer sockets; cut then says there are none
		f.Close()
	}
	return out
}

// remoteAddr spells an IPv4 /proc/net/tcp address; IPv6 stays hex.
func remoteAddr(hex string) string {
	ip, port, _ := strings.Cut(hex, ":")
	p, _ := strconv.ParseInt(port, 16, 32)
	if len(ip) != 8 {
		return fmt.Sprintf("[%s]:%d", ip, p)
	}
	v, _ := strconv.ParseUint(ip, 16, 32)
	return fmt.Sprintf("%d.%d.%d.%d:%d", v&0xff, v>>8&0xff, v>>16&0xff, v>>24, p)
}

func portSet(s string) (map[int]bool, error) {
	out := map[int]bool{}
	for f := range strings.SplitSeq(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			return nil, fmt.Errorf("-ports %q", s)
		}
		out[n] = true
	}
	return out, nil
}
