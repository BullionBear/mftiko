package shim

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	commonv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/common/v1"
	shimv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/shim/v1"
	"github.com/BullionBear/mftiko/contract/wire/frame"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

func TestCheckLaunch(t *testing.T) {
	ok := func() *shimv1.Launch {
		return &shimv1.Launch{Path: "/bin/true", Dir: "/", Env: []string{"MFTIKO_PLANE=td", "EMPTY="}}
	}
	if err := CheckLaunch(ok()); err != nil {
		t.Fatalf("a good launch: %v", err)
	}
	if err := CheckLaunch(&shimv1.Launch{Dir: "/opt/rootfs/", Path: "/opt/rootfs/bin/w"}); err != nil {
		t.Fatalf("a dir with a trailing slash: %v", err)
	}
	for name, mut := range map[string]func(*shimv1.Launch){
		"no path":        func(l *shimv1.Launch) { l.Path = "" },
		"relative path":  func(l *shimv1.Launch) { l.Path = "bin/true" },
		"no dir":         func(l *shimv1.Launch) { l.Dir = "" },
		"relative dir":   func(l *shimv1.Launch) { l.Dir = "tmp" },
		"outside dir":    func(l *shimv1.Launch) { l.Dir = "/opt/rootfs" },
		"sibling prefix": func(l *shimv1.Launch) { l.Dir, l.Path = "/opt/root", "/opt/rootfs/bin/w" },
		"unclean path":   func(l *shimv1.Launch) { l.Dir, l.Path = "/opt/rootfs", "/opt/rootfs/../etc/w" },
		"path is dir":    func(l *shimv1.Launch) { l.Dir, l.Path = "/opt/rootfs", "/opt/rootfs" },
		"no =":           func(l *shimv1.Launch) { l.Env = append(l.Env, "MFTIKO_PLANE") },
		"empty key":      func(l *shimv1.Launch) { l.Env = append(l.Env, "=x") },
		"ctl fd":         func(l *shimv1.Launch) { l.Env = append(l.Env, EnvCtlFD+"=4") },
		"twice":          func(l *shimv1.Launch) { l.Env = append(l.Env, "MFTIKO_PLANE=md") },
	} {
		l := ok()
		mut(l)
		if err := CheckLaunch(l); !errors.Is(err, ErrBadLaunch) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// ReadLaunch takes one frame and not a byte more: whatever follows stays in the stream.
func TestReadLaunch(t *testing.T) {
	want := &shimv1.Launch{Plane: "td", AgentInstance: "td-tw-1", Path: "/bin/true", Dir: "/",
		Worker: &commonv1.WorkerRef{Key: "td/42", Incarnation: 3}, WorkDir: "/var/lib/mftiko"}
	var buf bytes.Buffer
	if err := frame.NewWriter(&buf).Write(want); err != nil {
		t.Fatal(err)
	}
	buf.WriteString("rest")
	got, err := ReadLaunch(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetWorker().GetIncarnation() != 3 || got.GetAgentInstance() != "td-tw-1" {
		t.Fatalf("read %v", got)
	}
	if buf.String() != "rest" {
		t.Fatalf("left %q in the stream", buf.String())
	}

	if _, err := ReadLaunch(strings.NewReader("")); err == nil {
		t.Fatal("no frame read as a launch")
	}
	for name, bad := range map[string]*shimv1.Launch{
		"exec fields": {Path: "true", Dir: "/"},
		"identity":    {Path: "/bin/true", Dir: "/"},
	} {
		buf.Reset()
		if err := frame.NewWriter(&buf).Write(bad); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadLaunch(&buf); !errors.Is(err, ErrBadLaunch) {
			t.Fatalf("a launch with bad %s: %v", name, err)
		}
	}
}

// The agent's own fields: names, a worker key, and a work dir that leaves room for the socket path.
func TestCheckIdentity(t *testing.T) {
	ok := func() *shimv1.Launch {
		return &shimv1.Launch{Plane: "td", AgentInstance: "td-tw-1", Worker: &commonv1.WorkerRef{Key: "td/42"},
			WorkDir: "/var/lib/mftiko"}
	}
	if err := checkIdentity(ok()); err != nil {
		t.Fatalf("a good launch: %v", err)
	}
	// The longest work dir that fits, the socket path under it exactly 107 bytes. SocketPath("/", ...) is what
	// follows the work dir.
	longest := "/" + strings.Repeat("w", maxSocketPath-len(SocketPath("/", "td", "td/42"))-1)
	if n := len(SocketPath(longest, "td", "td/42")); n != maxSocketPath {
		t.Fatalf("test setup: socket path of %d bytes", n)
	}
	if l := ok(); func() error { l.WorkDir = longest; return checkIdentity(l) }() != nil {
		t.Fatalf("a work dir of %d bytes refused", len(longest))
	}
	for name, mut := range map[string]func(*shimv1.Launch){
		"no plane":          func(l *shimv1.Launch) { l.Plane = "" },
		"plane with slash":  func(l *shimv1.Launch) { l.Plane = "td/x" },
		"plane dot-dot":     func(l *shimv1.Launch) { l.Plane = ".." },
		"no instance":       func(l *shimv1.Launch) { l.AgentInstance = "" },
		"bad instance":      func(l *shimv1.Launch) { l.AgentInstance = "TD-1" },
		"no worker":         func(l *shimv1.Launch) { l.Worker = nil },
		"no worker key":     func(l *shimv1.Launch) { l.Worker.Key = "" },
		"no work dir":       func(l *shimv1.Launch) { l.WorkDir = "" },
		"relative work dir": func(l *shimv1.Launch) { l.WorkDir = "var/lib/mftiko" },
		"unclean work dir":  func(l *shimv1.Launch) { l.WorkDir = "/var/lib/mftiko/" },
		"socket too long":   func(l *shimv1.Launch) { l.WorkDir = longest + "w" },
	} {
		l := ok()
		mut(l)
		if err := checkIdentity(l); !errors.Is(err, ErrBadLaunch) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The socket is ${WORK_DIR}/run/{plane}/{h}.sock, with the h etcd keys use (PROTOCOL §2, §4.1).
func TestSocketPath(t *testing.T) {
	for _, key := range []string{"td/42", "sts/0a1b2c", "md/BinanceUM/default/1", ""} {
		want := "/var/lib/mftiko/run/td/" + state.WorkerHash(key) + ".sock"
		if got := SocketPath("/var/lib/mftiko", "td", key); got != want {
			t.Errorf("SocketPath(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestWorkerEnv(t *testing.T) {
	l := &shimv1.Launch{Env: []string{"A=1"}}
	got := strings.Join(workerEnv(l), " ")
	if got != "A=1 MFTIKO_CTL_FD=3" {
		t.Fatalf("env %q", got)
	}
	if len(l.Env) != 1 {
		t.Fatal("workerEnv changed the launch")
	}
}
