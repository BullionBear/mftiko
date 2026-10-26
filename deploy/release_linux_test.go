//go:build linux

package release_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BullionBear/mftiko/internal/platform/pgtest"
)

// Acceptance for bin/mftiko-release (E12-D022–E12-D026, E12-D032). Versions
// are the literal tags A–E. switch runs the real mftiko-migrate against a
// pgtest database (user mftiko, trust, no password).

var migrateBinary []byte

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mftiko-migrate")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)
	bin := filepath.Join(dir, "mftiko-migrate")
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "runtime.Caller")
		os.Exit(1)
	}
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/mftiko-migrate")
	cmd.Dir = filepath.Dir(filepath.Dir(file))
	cmd.Env = append(withoutEnv(os.Environ(), "CGO_ENABLED", "GOFLAGS"), "CGO_ENABLED=0")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
	migrateBinary, err = os.ReadFile(bin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

const systemctlScript = `#!/bin/bash
printf '%s\n' "$*" >>"$MFTIKO_TEST_LOG"
if [[ "$1" == "is-enabled" && "$2" == "td-agent.service" ]]; then
  exit 1
fi
if [[ "$1" == "restart" && -n "${MFTIKO_TEST_RESTART_FAIL:-}" ]]; then
  exit 1
fi
exit 0
`

func TestReleaseSwitchRollback(t *testing.T) {
	blobs := map[string][]byte{
		"A": releaseTar(t, "A", nil, true),
		"B": releaseTar(t, "B", nil, false),
	}
	w := newWorld(t, serve(t, blobs))
	w.withDB()

	w.fetchInstall("A")
	assertReadOnly(t, filepath.Join(w.prefix, "releases", "A"))
	if _, err := os.Lstat(filepath.Join(w.systemd, ".gitkeep")); !os.IsNotExist(err) {
		t.Fatalf(".gitkeep copied into systemd dir: %v", err)
	}
	w.mustList("A\n")

	w.fetchInstall("B")
	w.mustList("B\nA\n")

	if out := w.must("switch", "A"); !strings.Contains(out, "applied 0001_history\n") {
		t.Fatalf("switch A stdout %q", out)
	}
	appliedAt := w.migrationTimes(6)
	w.mustCurrent("A")
	w.mustMarker("A")
	w.mustList("B\nA current\n")
	if !pathExists(filepath.Join(w.systemd, "legacy.service")) {
		t.Fatal("legacy.service was not installed")
	}
	if !pathExists(filepath.Join(w.systemd, "etcd.service")) || !pathExists(filepath.Join(w.systemd, "decoy.service")) {
		t.Fatal("units that are not restarted were not installed")
	}

	if out := w.must("switch", "B"); !strings.Contains(out, "up to date\n") {
		t.Fatalf("switch B stdout %q", out)
	}
	w.migrationTimes(6, appliedAt...)
	w.mustCurrent("B")
	w.mustMarker("B")
	w.mustList("B current\nA previous\n")
	legacy, err := os.ReadFile(filepath.Join(w.systemd, "legacy.service"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(legacy), "Marker=A\n") {
		t.Fatalf("switch removed a unit the new release does not ship:\n%s", legacy)
	}

	w.must("rollback")
	w.mustCurrent("A")
	w.mustMarker("A")
	w.mustList("B previous\nA current\n")

	w.must("rollback")
	w.mustCurrent("B")
	w.mustMarker("B")
	w.mustList("B current\nA previous\n")

	one := restartLog()
	want := one + one + one + one
	if got := w.logText(); got != want {
		t.Fatalf("operation log:\n%s\nwant:\n%s", got, want)
	}

	w.must("fetch", "A")
	_, errb, code := w.run("install")
	if code == 0 || !strings.Contains(errb, "release exists: A") {
		t.Fatalf("second install exit %d, stderr %s", code, errb)
	}
	w.mustCurrent("B")

	if err := os.WriteFile(filepath.Join(w.prefix, "previous"), []byte("B\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := w.must("rollback")
	if out != "already B\n" {
		t.Fatalf("rollback stdout %q", out)
	}
	w.mustCurrent("B")
	if got := w.logText(); got != want {
		t.Fatal("rollback of the current release invoked systemctl")
	}

	relA := filepath.Join(w.prefix, "releases", "A")
	if err := exec.Command("chmod", "-R", "u+w", relA).Run(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(relA); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(w.prefix, "previous"), []byte("A\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, errb, code = w.run("rollback")
	if code == 0 || !strings.Contains(errb, "previous release is gone: A") {
		t.Fatalf("rollback of a missing release exit %d, stderr %s", code, errb)
	}
	w.mustCurrent("B")
	if got := w.logText(); got != want {
		t.Fatal("rejected rollback invoked systemctl")
	}
}

func TestReleaseGCKeepsRunning(t *testing.T) {
	hold := sleepBytes(t)
	blobs := map[string][]byte{
		"A": releaseTar(t, "A", hold, false),
		"B": releaseTar(t, "B", nil, false),
		"C": releaseTar(t, "C", nil, false),
		"D": releaseTar(t, "D", nil, false),
	}
	w := newWorld(t, serve(t, blobs))
	w.withDB()
	for _, tag := range []string{"A", "B", "C", "D"} {
		w.fetchInstall(tag)
	}

	bin := filepath.Join(w.prefix, "releases", "A", "bin", "hold")
	cmd := exec.Command(bin, "infinity")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	rel, err := filepath.EvalSymlinks(filepath.Join(w.prefix, "releases", "A"))
	if err != nil {
		t.Fatal(err)
	}
	waitExe(t, cmd.Process.Pid, func(exe string, ok bool) bool {
		return ok && strings.HasPrefix(exe, rel+"/")
	})

	out := w.must("switch", "D")
	if strings.Contains(out, "gc: deleted") {
		t.Fatalf("gc deleted a release while A is in use:\n%s", out)
	}
	for _, tag := range []string{"A", "B", "C", "D"} {
		if !pathExists(filepath.Join(w.prefix, "releases", tag)) {
			t.Fatalf("release %s missing after switch", tag)
		}
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	waitExe(t, cmd.Process.Pid, func(exe string, ok bool) bool {
		return !ok || !strings.HasPrefix(exe, rel+"/")
	})

	out = w.must("gc")
	if out != "gc: deleted A\n" {
		t.Fatalf("gc stdout %q", out)
	}
	if pathExists(filepath.Join(w.prefix, "releases", "A")) {
		t.Fatal("gc left A after the process exited")
	}
	for _, tag := range []string{"B", "C", "D"} {
		if !pathExists(filepath.Join(w.prefix, "releases", tag)) {
			t.Fatalf("gc deleted %s", tag)
		}
	}
}

func TestReleaseGCKeepsCurrent(t *testing.T) {
	blobs := map[string][]byte{}
	for _, tag := range []string{"A", "B", "C", "D", "E"} {
		blobs[tag] = releaseTar(t, tag, nil, false)
	}
	w := newWorld(t, serve(t, blobs))
	w.withDB()
	w.fetchInstall("A")
	w.must("switch", "A")
	for _, tag := range []string{"B", "C", "D", "E"} {
		w.fetchInstall(tag)
	}
	out := w.must("gc")
	if out != "gc: deleted B\n" {
		t.Fatalf("gc stdout %q", out)
	}
	for _, tag := range []string{"A", "C", "D", "E"} {
		if !pathExists(filepath.Join(w.prefix, "releases", tag)) {
			t.Fatalf("gc deleted %s", tag)
		}
	}
	if pathExists(filepath.Join(w.prefix, "releases", "B")) {
		t.Fatal("gc left B")
	}
	w.mustCurrent("A")
}

func TestReleaseMigrateFailureLeavesCurrent(t *testing.T) {
	blobs := map[string][]byte{
		"A": releaseTar(t, "A", nil, false),
		"B": releaseTar(t, "B", nil, false),
	}
	w := newWorld(t, serve(t, blobs))
	w.withDB()
	w.fetchInstall("A")
	w.must("switch", "A")
	w.fetchInstall("B")
	out := w.must("switch", "B")
	if !strings.Contains(out, "up to date\n") {
		t.Fatalf("second switch stdout %q", out)
	}
	w.mustCurrent("B")
	before := w.logText()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, w.database)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tag, err := conn.Exec(ctx, `UPDATE schema_migrations SET sha256 = $1 WHERE version = 1`, strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("updated %d rows", tag.RowsAffected())
	}

	_, errb, code := w.run("switch", "A")
	if code == 0 || !strings.Contains(errb, "mftiko-migrate up failed") || !strings.Contains(errb, "changed") {
		t.Fatalf("switch exit %d, stderr %s", code, errb)
	}
	w.mustCurrent("B")
	w.mustMarker("B")
	if extra := strings.TrimPrefix(w.logText(), before); extra != "" {
		t.Fatalf("log after failed migrate:\n%s", extra)
	}
}

func TestReleaseRestartFailureSkipsGC(t *testing.T) {
	blobs := map[string][]byte{}
	for _, tag := range []string{"A", "B", "C", "D", "E"} {
		blobs[tag] = releaseTar(t, tag, nil, false)
	}
	w := newWorld(t, serve(t, blobs))
	w.withDB()
	w.extra = []string{"MFTIKO_TEST_RESTART_FAIL=1"}
	for _, tag := range []string{"A", "B", "C", "D", "E"} {
		w.fetchInstall(tag)
	}
	out, errb, code := w.run("switch", "E")
	if code == 0 || !strings.Contains(errb, "restart failed: sym.service") {
		t.Fatalf("switch exit %d, stderr %s", code, errb)
	}
	if strings.Contains(out, "gc: deleted") || strings.Contains(w.logText(), "is-enabled mdfetch.service") {
		t.Fatalf("restart failure continued past the first unit\nstdout: %s\nlog: %s", out, w.logText())
	}
	w.mustCurrent("E")
	for _, tag := range []string{"A", "B", "C", "D", "E"} {
		if !pathExists(filepath.Join(w.prefix, "releases", tag)) {
			t.Fatalf("release %s missing; gc ran after a failed restart", tag)
		}
	}
}

func TestReleaseFetchRejects(t *testing.T) {
	blob := releaseTar(t, "A", nil, false)
	w := newWorld(t, serveSums(t, map[string][]byte{"A": blob}, map[string]bool{"A": true}))
	_, errb, code := w.run("fetch", "A")
	if code == 0 || !strings.Contains(errb, "checksum failed: A") {
		t.Fatalf("fetch exit %d, stderr %s", code, errb)
	}
	if pathExists(filepath.Join(w.prefix, "incoming", "A")) || pathExists(filepath.Join(w.prefix, "incoming", "last")) {
		t.Fatal("failed fetch left a download")
	}

	w.base = "http://127.0.0.1:1"
	for _, tag := range []string{"../x", "A/B", "A..B"} {
		_, errb, code = w.run("fetch", tag)
		if code == 0 || !strings.Contains(errb, "invalid tag: "+tag) {
			t.Fatalf("fetch %s exit %d, stderr %s", tag, code, errb)
		}
	}

	fresh := newWorld(t, "http://127.0.0.1:1")
	out := fresh.must("list")
	if out != "" {
		t.Fatalf("list %q", out)
	}
	_, errb, code = fresh.run("install")
	if code == 0 || !strings.Contains(errb, "nothing fetched") {
		t.Fatalf("install exit %d, stderr %s", code, errb)
	}
}

func TestReleaseUnsafeMember(t *testing.T) {
	for _, name := range []string{"../escaped", "/tmp/escaped", "bin/../../escaped"} {
		t.Run(name, func(t *testing.T) {
			blob := tarGz(t, []member{{name: name, body: []byte("nope\n"), mode: 0o644}})
			w := newWorld(t, serve(t, map[string][]byte{"A": blob}))
			w.must("fetch", "A")
			_, errb, code := w.run("install")
			if code == 0 || !strings.Contains(errb, "unsafe path: A") {
				t.Fatalf("install exit %d, stderr %s", code, errb)
			}
			if pathExists(filepath.Join(w.prefix, "releases", "A")) {
				t.Fatal("unsafe tarball was installed")
			}
		})
	}
}

func TestReleaseSymlinkEscape(t *testing.T) {
	// Enough ".." to climb out of the temp directory and land on /etc/passwd.
	// A shorter relative link can dangle inside /tmp, which is a different refusal.
	rel := strings.Repeat("../", 40) + "etc/passwd"
	for _, target := range []string{"/etc/passwd", rel} {
		t.Run(target, func(t *testing.T) {
			blob := tarGz(t, []member{
				dir("bin"), dir("web"), dir("systemd"),
				{name: "bin/escape", link: target, mode: 0o777},
			})
			w := newWorld(t, serve(t, map[string][]byte{"A": blob}))
			w.must("fetch", "A")
			_, errb, code := w.run("install")
			if code == 0 || !strings.Contains(errb, "tarball escapes: A") {
				t.Fatalf("install exit %d, stderr %s", code, errb)
			}
			if pathExists(filepath.Join(w.prefix, "releases", "A")) {
				t.Fatal("escaping symlink was installed")
			}
		})
	}
}

func TestReleaseRequiresRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root")
	}
	cmd := exec.Command("bash", releaseScript(t), "list")
	cmd.Env = withoutEnv(os.Environ(), envKeys...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil || !strings.Contains(stderr.String(), "run as root") {
		t.Fatalf("err %v, stderr %s", err, stderr.String())
	}
}

type world struct {
	t        *testing.T
	prefix   string
	systemd  string
	logPath  string
	ctl      string
	base     string
	database string
	extra    []string
}

func newWorld(t *testing.T, base string) *world {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = exec.Command("chmod", "-R", "u+w", root).Run()
	})
	w := &world{
		t:       t,
		prefix:  filepath.Join(root, "opt"),
		systemd: filepath.Join(root, "systemd"),
		logPath: filepath.Join(root, "log"),
		ctl:     filepath.Join(root, "systemctl"),
		base:    base,
	}
	if err := os.WriteFile(w.ctl, []byte(systemctlScript), 0o755); err != nil {
		t.Fatal(err)
	}
	return w
}

var envKeys = []string{
	"MFTIKO_PREFIX",
	"MFTIKO_SYSTEMD_DIR",
	"MFTIKO_SYSTEMCTL",
	"MFTIKO_PROC",
	"MFTIKO_RELEASE_BASE",
	"DATABASE_URL",
	"MFTIKO_TEST_LOG",
	"MFTIKO_TEST_RESTART_FAIL",
}

func (w *world) environ() []string {
	env := withoutEnv(os.Environ(), envKeys...)
	env = append(env,
		"MFTIKO_PREFIX="+w.prefix,
		"MFTIKO_SYSTEMD_DIR="+w.systemd,
		"MFTIKO_SYSTEMCTL="+w.ctl,
		"MFTIKO_PROC=/proc",
		"MFTIKO_RELEASE_BASE="+w.base,
		"DATABASE_URL="+w.database,
		"MFTIKO_TEST_LOG="+w.logPath,
	)
	return append(env, w.extra...)
}

func (w *world) run(args ...string) (string, string, int) {
	w.t.Helper()
	cmd := exec.Command("bash", append([]string{releaseScript(w.t)}, args...)...)
	cmd.Env = w.environ()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return stdout.String(), stderr.String(), 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return stdout.String(), stderr.String(), ee.ExitCode()
	}
	w.t.Fatal(err)
	return "", "", -1
}

func (w *world) must(args ...string) string {
	w.t.Helper()
	out, errb, code := w.run(args...)
	if code != 0 {
		w.t.Fatalf("mftiko-release %s: exit %d\n%s\n%s", strings.Join(args, " "), code, out, errb)
	}
	return out
}

func (w *world) fetchInstall(tag string) {
	w.t.Helper()
	w.must("fetch", tag)
	w.must("install")
}

func (w *world) mustList(want string) {
	w.t.Helper()
	if got := w.must("list"); got != want {
		w.t.Fatalf("list %q, want %q", got, want)
	}
}

func (w *world) mustCurrent(tag string) {
	w.t.Helper()
	got, err := os.Readlink(filepath.Join(w.prefix, "current"))
	if err != nil {
		w.t.Fatal(err)
	}
	if got != "releases/"+tag {
		w.t.Fatalf("current -> %s, want releases/%s", got, tag)
	}
}

func (w *world) mustMarker(tag string) {
	w.t.Helper()
	b, err := os.ReadFile(filepath.Join(w.systemd, "sym.service"))
	if err != nil {
		w.t.Fatal(err)
	}
	if !strings.Contains(string(b), "Marker="+tag+"\n") {
		w.t.Fatalf("sym.service:\n%s", b)
	}
}

func (w *world) logText() string {
	w.t.Helper()
	b, err := os.ReadFile(w.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		w.t.Fatal(err)
	}
	return string(b)
}

func (w *world) withDB() {
	w.t.Helper()
	srv := pgtest.Start(w.t)
	w.database = srv.DSN()
}

func (w *world) migrationTimes(n int, prev ...time.Time) []time.Time {
	w.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, w.database)
	if err != nil {
		w.t.Fatal(err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT version, applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		w.t.Fatal(err)
	}
	defer rows.Close()
	var got []time.Time
	var vers []int
	for rows.Next() {
		var v int
		var at time.Time
		if err := rows.Scan(&v, &at); err != nil {
			w.t.Fatal(err)
		}
		vers = append(vers, v)
		got = append(got, at)
	}
	if err := rows.Err(); err != nil {
		w.t.Fatal(err)
	}
	if len(vers) != n {
		w.t.Fatalf("schema_migrations %d rows, want %d (%v)", len(vers), n, vers)
	}
	for i, v := range vers {
		if v != i+1 {
			w.t.Fatalf("schema_migrations versions %v", vers)
		}
	}
	if len(prev) > 0 {
		if len(prev) != len(got) {
			w.t.Fatalf("applied_at %d, previous %d", len(got), len(prev))
		}
		for i := range got {
			if !got[i].Equal(prev[i]) {
				w.t.Fatalf("applied_at changed: %v -> %v", prev, got)
			}
		}
	}
	return got
}

func restartLog() string {
	units := []struct {
		name    string
		restart bool
	}{
		{"sym.service", true},
		{"mdfetch.service", true},
		{"api.service", true},
		{"controller.service", true},
		{"md-agent.service", true},
		{"td-agent.service", false},
		{"sts-agent.service", true},
	}
	var b strings.Builder
	b.WriteString("daemon-reload\n")
	for _, u := range units {
		fmt.Fprintf(&b, "is-enabled %s\n", u.name)
		if u.restart {
			fmt.Fprintf(&b, "restart %s\n", u.name)
		}
	}
	return b.String()
}

func serve(t *testing.T, blobs map[string][]byte) string {
	t.Helper()
	return serveSums(t, blobs, nil)
}

func serveSums(t *testing.T, blobs map[string][]byte, bad map[string]bool) string {
	t.Helper()
	files := map[string][]byte{}
	for tag, blob := range blobs {
		name := assetName(tag)
		sum := sha256.Sum256(blob)
		hexSum := hex.EncodeToString(sum[:])
		if bad[tag] {
			hexSum = strings.Repeat("b", 64)
		}
		decoy := strings.Repeat("a", 64) + "  " + fmt.Sprintf("mftiko_%s_linux_amd64.tar.gz", tag) + "\n"
		real := hexSum + "  " + name + "\n"
		files["/"+tag+"/"+name] = blob
		files["/"+tag+"/SHA256SUMS"] = []byte(decoy + real)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

type member struct {
	name string
	body []byte
	mode int64
	link string
}

func dir(name string) member {
	return member{name: name, mode: 0o755}
}

func releaseTar(t *testing.T, tag string, hold []byte, legacy bool) []byte {
	t.Helper()
	if len(migrateBinary) == 0 {
		t.Fatal("mftiko-migrate was not built")
	}
	bins := []string{
		"mftiko-sym",
		"mftiko-mdfetch",
		"mftiko-api",
		"mftiko-controller",
		"mftiko-md-agent",
		"mftiko-td-agent",
		"mftiko-sts-agent",
	}
	members := []member{
		dir("bin"), dir("web"), dir("systemd"),
		{name: "bin/mftiko-migrate", body: migrateBinary, mode: 0o755},
		{name: "web/index.html", body: []byte("ok\n"), mode: 0o644},
		{name: "systemd/.gitkeep", body: []byte("keep\n"), mode: 0o644},
	}
	if hold != nil {
		members = append(members, member{name: "bin/hold", body: hold, mode: 0o755})
	}
	for _, bin := range bins {
		unit := bin
		if bin == "mftiko-mdfetch" {
			unit = "mdfetch"
		} else {
			unit = strings.TrimPrefix(bin, "mftiko-")
		}
		body := "[Service]\nExecStart=/opt/mftiko/current/bin/" + bin + "\nMarker=" + tag + "\n"
		members = append(members, member{name: "systemd/" + unit + ".service", body: []byte(body), mode: 0o644})
	}
	members = append(members,
		member{name: "systemd/etcd.service", body: []byte("ExecStart=/usr/bin/etcd\nMarker=" + tag + "\n"), mode: 0o644},
		member{name: "systemd/decoy.service", body: []byte("# mentions mftiko-api\nExecStart=/usr/bin/etcd\n"), mode: 0o644},
	)
	if legacy {
		members = append(members, member{
			name: "systemd/legacy.service",
			body: []byte("ExecStart=/usr/bin/true\nMarker=" + tag + "\n"),
			mode: 0o644,
		})
	}
	return tarGz(t, members)
}

func tarGz(t *testing.T, members []member) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, m := range members {
		hdr := &tar.Header{Name: m.name, Mode: m.mode, ModTime: time.Unix(0, 0)}
		var body []byte
		switch {
		case m.link != "":
			hdr.Typeflag = tar.TypeSymlink
			hdr.Linkname = m.link
		case m.body == nil && !strings.Contains(filepath.Base(m.name), "."):
			hdr.Typeflag = tar.TypeDir
			if !strings.HasSuffix(hdr.Name, "/") {
				hdr.Name += "/"
			}
		default:
			hdr.Typeflag = tar.TypeReg
			hdr.Size = int64(len(m.body))
			body = m.body
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if body != nil {
			if _, err := tw.Write(body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func assetName(tag string) string {
	return "mftiko-server_" + tag + "_linux_amd64.tar.gz"
}

func sleepBytes(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("/bin/sleep")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func releaseScript(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	return filepath.Join(filepath.Dir(file), "mftiko-release")
}

func withoutEnv(env []string, keys ...string) []string {
	drop := map[string]bool{}
	for _, k := range keys {
		drop[k] = true
	}
	var out []string
	for _, e := range env {
		k, _, ok := strings.Cut(e, "=")
		if ok && drop[k] {
			continue
		}
		out = append(out, e)
	}
	return out
}

func pathExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// assertReadOnly checks an installed release: nothing in it is writable, and every directory and file is
// readable, every directory enterable, by other users. The services run as their own users, not root
// (E12-D005); a release left 0700 by its unpack directory starts none of them (#385).
func assertReadOnly(t *testing.T, root string) {
	t.Helper()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode()&0o222 != 0 {
			t.Errorf("%s mode %o is writable", path, info.Mode().Perm())
		}
		want := os.FileMode(0o004)
		if info.IsDir() {
			want = 0o005
		}
		if info.Mode().Type()&os.ModeSymlink == 0 && info.Mode().Perm()&want != want {
			t.Errorf("%s mode %o: other users cannot read it", path, info.Mode().Perm())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func waitExe(t *testing.T, pid int, want func(exe string, ok bool) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last string
	var ok bool
	for {
		last, ok = procExe(pid)
		if want(last, ok) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d exe %q present %v", pid, last, ok)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func procExe(pid int) (string, bool) {
	p, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return "", false
	}
	return strings.TrimSuffix(p, " (deleted)"), true
}
