//go:build linux

package adopt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	sessionv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/session/v1"
	statev1 "github.com/BullionBear/mftiko/contract/gen/mftiko/state/v1"
	"github.com/BullionBear/mftiko/internal/platform/agent/latest"
	"github.com/BullionBear/mftiko/internal/platform/artifact"
	"github.com/BullionBear/mftiko/internal/platform/sealbox"
	"github.com/BullionBear/mftiko/internal/platform/state"
)

// TestAdopt is the E12-D021 acceptance: three agent units under real systemd,
// with etcd, NATS, and the registry from deploy/accept. It skips unless
// MFTIKO_SYSTEMD_ADOPT=1. With that set, a machine whose PID 1 is not systemd
// fails the test instead of skipping.
//
// Workers are the fakeworker binary packed as OCI artifacts. The units still
// start with -registry. nkey seeds and the TD seal key are generated here and
// are not written into the failure output.
func TestAdopt(t *testing.T) {
	if os.Getenv("MFTIKO_SYSTEMD_ADOPT") != "1" {
		t.Skip("set MFTIKO_SYSTEMD_ADOPT=1 to run the systemd adoption test")
	}
	comm, err := os.ReadFile("/proc/1/comm")
	if err != nil || strings.TrimSpace(string(comm)) != "systemd" {
		t.Fatalf("MFTIKO_SYSTEMD_ADOPT=1 but PID 1 is not systemd (%q, %v)", strings.TrimSpace(string(comm)), err)
	}
	root := repoRoot(t)
	refuseHost(t)

	natsDir := filepath.Join(root, "deploy", "accept", "data", "nats")
	_, statErr := os.Stat(natsDir)
	createdNats := os.IsNotExist(statErr)
	t.Cleanup(func() {
		composeDown(t, root)
		if createdNats {
			if err := os.RemoveAll(natsDir); err != nil {
				t.Logf("teardown nats dir: %v", err)
			}
		}
	})

	phase("generate dev NATS config")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	_, err = combined(ctx, root, "go", "run", "./cmd/mftiko-nats-dev", "-out", "deploy/accept/data/nats")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	binDir := t.TempDir()
	phase("build binaries and start etcd, nats, registry")
	errc := make(chan error, 2)
	go func() { errc <- buildBins(root, binDir) }()
	go func() { errc <- composeUp(root) }()
	var fails []string
	for range 2 {
		if err := <-errc; err != nil {
			fails = append(fails, err.Error())
		}
	}
	if len(fails) != 0 {
		t.Fatal(strings.Join(fails, "\n"))
	}
	if !poll(2*time.Minute, func() bool {
		return httpUp("http://127.0.0.1:5000/v2/") && httpUp("http://127.0.0.1:2379/health")
	}) {
		t.Fatal("registry or etcd did not answer on 127.0.0.1")
	}

	phase("pack and push fake workers")
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	raw, err := os.ReadFile(filepath.Join(binDir, "fakeworker"))
	if err != nil {
		t.Fatal(err)
	}
	mdImg, err := artifact.PackWorker(raw, artifact.KindMDConn, "fake", nil)
	if err != nil {
		t.Fatal(err)
	}
	tdImg, err := artifact.PackWorker(raw, artifact.KindTDAccount, "fake", nil)
	if err != nil {
		t.Fatal(err)
	}
	stImg, err := artifact.PackStrategy(raw, "fake", nil)
	if err != nil {
		t.Fatal(err)
	}
	pushImage(t, mdImg, "127.0.0.1:5000/"+latest.RepoMDConn)
	pushImage(t, tdImg, "127.0.0.1:5000/"+latest.RepoTDAccount)
	pushImage(t, stImg, "127.0.0.1:5000/strategies/fake")

	phase("enable etcd auth and write demand")
	cli := enableAuth(t)
	defer cli.Close()
	sealPath := writeDemand(t, cli, stImg.Digest)

	phase("install units, users, and env")
	t.Cleanup(func() { teardownHost(t) })
	installBins(t, binDir)
	installHost(t, root, sealPath)

	phase("start agents")
	for _, a := range agents {
		startCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		_, err := combined(startCtx, "", rootCmd("systemctl", "start", "--no-block", a.unit)...)
		cancel()
		if err != nil {
			dumpUnit(t, a.unit, "")
			t.Fatal(err)
		}
	}
	var before []kids
	var reportsBefore []int
	for _, a := range agents {
		k := waitKids(t, a, 2*time.Minute)
		before = append(before, k)
		// fleet.Adopt logs "shims adopted" only when run/{plane} already exists.
		// The first start creates that directory later, inside the shim, so this
		// count is a baseline and is usually zero. Restart, stop then start, and
		// SIGKILL are what must append a report with adopted>=1.
		reportsBefore = append(reportsBefore, len(adoptReports(t, a.unit)))
	}

	for i, a := range agents {
		phase("restart " + a.unit)
		before[i] = restart(t, a, before[i], reportsBefore[i])
		reportsBefore[i] = len(adoptReports(t, a.unit))
		assertOthers(t, i, before)

		phase("stop then start " + a.unit)
		before[i] = stopStart(t, a, before[i], reportsBefore[i])
		reportsBefore[i] = len(adoptReports(t, a.unit))
		assertOthers(t, i, before)

		phase("SIGKILL MainPID " + a.unit)
		before[i] = sigkill(t, a, before[i], reportsBefore[i])
		reportsBefore[i] = len(adoptReports(t, a.unit))
		assertOthers(t, i, before)
	}
}

type agent struct {
	unit   string
	user   string
	inst   string
	name   string // /etc/mftiko and StateDirectory leaf
	suffix string // unpacked worker exe
}

var agents = []agent{
	{unit: "mftiko-md-agent.service", user: "mftiko-md", inst: "md1", name: "md-agent", suffix: "/bin/worker"},
	{unit: "mftiko-td-agent.service", user: "mftiko-td", inst: "td1", name: "td-agent", suffix: "/bin/worker"},
	{unit: "mftiko-sts-agent.service", user: "mftiko-sts", inst: "sts1", name: "sts-agent", suffix: "/bin/strategy"},
}

const (
	shimExe   = "/opt/mftiko/current/bin/mftiko-shim"
	etcdURL   = "http://127.0.0.1:2379"
	agentPass = "test-token-a"
	rootPass  = "accept"
	sessionID = "a1b2c3"
	mdFeed    = "mark_price.BinanceUM_Perp_BTCUSDT"
)

func restart(t *testing.T, a agent, prev kids, prevReports int) kids {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	out, err := combined(ctx, "", rootCmd("systemctl", "restart", a.unit)...)
	cancel()
	if err != nil {
		dumpUnit(t, a.unit, prev.cgroup)
		if strings.Contains(strings.ToLower(out), "cgroup") {
			t.Fatalf("systemd refused to start %s because the cgroup is not empty (E12-D016). Not adding Delegate=.\n%v\n%s", a.unit, err, out)
		}
		t.Fatal(err)
	}
	return waitAdopted(t, a, prev, prevReports, 45*time.Second)
}

func stopStart(t *testing.T, a agent, prev kids, prevReports int) kids {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	_, err := combined(ctx, "", rootCmd("systemctl", "stop", a.unit)...)
	cancel()
	if err != nil {
		dumpUnit(t, a.unit, prev.cgroup)
		t.Fatal(err)
	}
	if st := show(t, a.unit, "ActiveState"); st != "inactive" {
		dumpUnit(t, a.unit, prev.cgroup)
		t.Fatalf("%s ActiveState after stop is %q, want inactive", a.unit, st)
	}
	procPath := cgroupProcs(prev.cgroup)
	if !fileExists(procPath) {
		dumpUnit(t, a.unit, prev.cgroup)
		t.Fatalf("shim left the cgroup or died: %s disappeared after stop. E12-D016 expected the shim to stay; not adding Delegate=", procPath)
	}
	pids := readProcs(t, procPath)
	if !contains(pids, prev.shim) || !contains(pids, prev.worker) || !alive(prev.shim) || !alive(prev.worker) {
		dumpUnit(t, a.unit, prev.cgroup)
		t.Fatalf("shim left the cgroup or died: shim %d alive=%v worker %d alive=%v procs %v path %s. E12-D016 expected both to stay; not adding Delegate=",
			prev.shim, alive(prev.shim), prev.worker, alive(prev.worker), pids, procPath)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 40*time.Second)
	out, err := combined(ctx, "", rootCmd("systemctl", "start", "--no-block", a.unit)...)
	cancel()
	if err != nil {
		dumpUnit(t, a.unit, prev.cgroup)
		if strings.Contains(strings.ToLower(out), "cgroup") {
			t.Fatalf("systemd refused to start %s because the cgroup is not empty (E12-D016). Not adding Delegate=.\n%v\n%s", a.unit, err, out)
		}
		t.Fatal(err)
	}
	return waitAdopted(t, a, prev, prevReports, 45*time.Second)
}

func sigkill(t *testing.T, a agent, prev kids, prevReports int) kids {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := combined(ctx, "", rootCmd("kill", "-KILL", strconv.Itoa(prev.main))...)
	cancel()
	if err != nil {
		dumpUnit(t, a.unit, prev.cgroup)
		t.Fatal(err)
	}
	return waitAdopted(t, a, prev, prevReports, 60*time.Second)
}

func waitAdopted(t *testing.T, a agent, prev kids, prevReports int, d time.Duration) kids {
	t.Helper()
	var last snap
	var reps []report
	ok := poll(d, func() bool {
		last = inspect(t, a)
		reps = adoptReports(t, a.unit)
		if len(reps) <= prevReports {
			return false
		}
		r := reps[prevReports]
		if r.Adopted < 1 || r.Unreachable != 0 {
			dumpUnit(t, a.unit, prev.cgroup)
			t.Fatalf("%s first new adoption report is adopted=%d unreachable=%d, want adopted>=1 unreachable=0", a.unit, r.Adopted, r.Unreachable)
		}
		return last.main != 0 && last.main != prev.main && last.state == "active" &&
			last.shim == prev.shim && last.worker == prev.worker &&
			last.shimExe == shimExe && strings.HasSuffix(last.workerExe, a.suffix)
	})
	if !ok {
		dumpUnit(t, a.unit, prev.cgroup)
		t.Fatalf("%s did not adopt: main %d->%d state %s shim %d->%d (%s) worker %d->%d (%s) reports %d->%d",
			a.unit, prev.main, last.main, last.state, prev.shim, last.shim, last.shimExe, prev.worker, last.worker, last.workerExe, prevReports, len(reps))
	}
	if !alive(prev.shim) || !alive(prev.worker) {
		dumpUnit(t, a.unit, prev.cgroup)
		t.Fatalf("shim left the cgroup or died: %s shim %d worker %d after adopt. Not adding Delegate=", a.unit, prev.shim, prev.worker)
	}
	return last.kids
}

func assertOthers(t *testing.T, skip int, prev []kids) {
	t.Helper()
	for i, a := range agents {
		if i == skip {
			continue
		}
		got := inspect(t, a)
		if got.shim != prev[i].shim || got.worker != prev[i].worker || got.main != prev[i].main || got.state != "active" {
			dumpUnit(t, a.unit, prev[i].cgroup)
			t.Fatalf("while another agent was restarted, %s changed: main %d->%d shim %d->%d worker %d->%d state %s",
				a.unit, prev[i].main, got.main, prev[i].shim, got.shim, prev[i].worker, got.worker, got.state)
		}
	}
}

type kids struct {
	main, shim, worker int
	cgroup             string
}

type snap struct {
	kids
	state     string
	shimExe   string
	workerExe string
	detail    string
}

func waitKids(t *testing.T, a agent, d time.Duration) kids {
	t.Helper()
	var last snap
	ok := poll(d, func() bool {
		last = inspect(t, a)
		return last.state == "active" && last.main != 0 && last.shim != 0 && last.worker != 0 &&
			last.shimExe == shimExe && strings.HasSuffix(last.workerExe, a.suffix)
	})
	if !ok {
		dumpUnit(t, a.unit, last.cgroup)
		t.Fatalf("%s did not reach one shim and one worker: %s", a.unit, last.detail)
	}
	return last.kids
}

func inspect(t *testing.T, a agent) snap {
	t.Helper()
	var s snap
	vals := showMulti(t, a.unit, "ActiveState", "MainPID", "ControlGroup")
	if len(vals) >= 1 {
		s.state = vals[0]
	}
	if len(vals) >= 2 {
		s.main, _ = strconv.Atoi(vals[1])
	}
	if len(vals) >= 3 {
		s.cgroup = vals[2]
	}
	if s.cgroup != "" && !strings.HasPrefix(s.cgroup, "/") && strings.Contains(s.cgroup, "/") {
		s.cgroup = "/" + s.cgroup
	}
	if s.cgroup == "" || !fileExists(cgroupProcs(s.cgroup)) {
		s.detail = fmt.Sprintf("state=%s main=%d cgroup=%q", s.state, s.main, s.cgroup)
		return s
	}
	var extra []string
	for _, pid := range readProcs(t, cgroupProcs(s.cgroup)) {
		if pid == s.main {
			continue
		}
		exe := procExe(pid)
		switch {
		case exe == shimExe && s.shim == 0:
			s.shim, s.shimExe = pid, exe
		case strings.HasSuffix(exe, a.suffix) && s.worker == 0:
			s.worker, s.workerExe = pid, exe
		default:
			extra = append(extra, fmt.Sprintf("%d:%s", pid, exe))
		}
	}
	if len(extra) != 0 || s.shim == 0 || s.worker == 0 {
		s.shim, s.worker = 0, 0
	}
	s.detail = fmt.Sprintf("state=%s main=%d shim=%d worker=%d extra=%v", s.state, s.main, s.shim, s.worker, extra)
	return s
}

type report struct {
	Message     string `json:"message"`
	Adopted     int    `json:"adopted"`
	Unreachable int    `json:"unreachable"`
}

func adoptReports(t *testing.T, unit string) []report {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := combined(ctx, "", rootCmd("journalctl", "-u", unit, "--no-pager", "-o", "cat")...)
	if err != nil {
		t.Fatalf("journalctl %s: %v\n%s", unit, err, out)
	}
	var reps []report
	for _, line := range strings.Split(out, "\n") {
		i := strings.IndexByte(line, '{')
		if i < 0 || !strings.Contains(line, "shims adopted") {
			continue
		}
		var r report
		dec := json.NewDecoder(strings.NewReader(line[i:]))
		if err := dec.Decode(&r); err != nil || r.Message != "shims adopted" {
			continue
		}
		reps = append(reps, r)
	}
	return reps
}

func show(t *testing.T, unit, prop string) string {
	t.Helper()
	vals := showMulti(t, unit, prop)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func showMulti(t *testing.T, unit string, props ...string) []string {
	t.Helper()
	args := []string{"systemctl", "show"}
	for _, p := range props {
		args = append(args, "-p", p)
	}
	args = append(args, unit)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := combined(ctx, "", rootCmd(args...)...)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	return parseShow(out, props)
}

// parseShow reads `systemctl show` lines of the form Name=value and returns the
// requested properties in the caller's order. systemd 255 does not emit --value
// lines in -p order: ActiveState, MainPID, ControlGroup came back as the PID,
// the cgroup path, then "active", and the test then read cgroup "/active".
func parseShow(out string, props []string) []string {
	got := map[string]string{}
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		got[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	vals := make([]string, len(props))
	for i, p := range props {
		vals[i] = got[p]
	}
	return vals
}

func cgroupProcs(cgroup string) string {
	return "/sys/fs/cgroup" + cgroup + "/cgroup.procs"
}

func readProcs(t *testing.T, path string) []int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := combined(ctx, "", rootCmd("cat", path)...)
	if err != nil {
		return nil
	}
	var pids []int
	for _, f := range strings.Fields(out) {
		n, err := strconv.Atoi(f)
		if err == nil {
			pids = append(pids, n)
		}
	}
	return pids
}

func procExe(pid int) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := combined(ctx, "", rootCmd("readlink", "-f", "/proc/"+strconv.Itoa(pid)+"/exe")...)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := combined(ctx, "", rootCmd("kill", "-0", strconv.Itoa(pid))...)
	return err == nil
}

func fileExists(path string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := combined(ctx, "", rootCmd("test", "-e", path)...)
	return err == nil
}

func contains(pids []int, pid int) bool {
	for _, p := range pids {
		if p == pid {
			return true
		}
	}
	return false
}

func enableAuth(t *testing.T) *clientv3.Client {
	t.Helper()
	plain, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdURL}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	_, err = plain.UserAdd(ctx, "root", rootPass)
	if err == nil {
		_, err = plain.UserGrantRole(ctx, "root", "root")
	}
	if err == nil {
		_, err = plain.AuthEnable(ctx)
	}
	cancel()
	plain.Close()
	if err != nil {
		t.Fatal(err)
	}
	root, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcdURL}, DialTimeout: 5 * time.Second,
		Username: "root", Password: rootPass,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	topo := testTopo()
	tables, err := state.AuthTables(topo, map[string]state.Plane{
		"md1": state.PlaneMD, "td1": state.PlaneTD, "sts1": state.PlaneSTS,
	})
	if err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err := state.EnsureAuth(ctx, root, tables["home"], func(string) string { return agentPass }); err != nil {
		root.Close()
		t.Fatal(err)
	}
	return root
}

func testTopo() state.Topology {
	return state.Topology{
		Regions: map[string]state.Region{
			"home": {Endpoints: []string{etcdURL}},
		},
		Instances: map[string]string{
			"md1": "home", "td1": "home", "sts1": "home", state.ControllerInstance: "home",
		},
	}
}

func writeDemand(t *testing.T, cli *clientv3.Client, strategyDigest string) string {
	t.Helper()
	put := func(key string, m proto.Message) {
		t.Helper()
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := cli.Put(ctx, key, string(b)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	put(state.MDDemandKey("md1", sessionID, "feed/"+mdFeed), &statev1.DemandEntry{
		Target: &statev1.DemandEntry_Feed{Feed: mdFeed},
	})

	seal, err := sealbox.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealbox.SealCredentials(seal.Public(), 1, "BinanceUM", sealbox.Credentials{
		KeyType: "hmac", Key: agentPass, Secret: agentPass,
	})
	if err != nil {
		t.Fatal(err)
	}
	sealPath := filepath.Join(t.TempDir(), "seal.key")
	if err := sealbox.WriteKeyFile(sealPath, seal); err != nil {
		t.Fatal(err)
	}

	put(state.AccountKey("td1", 1), &statev1.Account{
		Name: "a", Kind: "BinanceUM", SealedCredentials: sealed, Enabled: true,
	})
	put(state.TDDemandKey("td1", sessionID, 1), &statev1.DemandEntry{
		Target: &statev1.DemandEntry_ApiId{ApiId: 1},
	})
	put(state.SessionSpecKey("sts1", sessionID), &sessionv1.SessionSpec{
		Name: "s", DesiredState: sessionv1.DesiredState_DESIRED_STATE_RUNNING,
		Strategy: &sessionv1.StrategyRef{Name: "fake", Code: strategyDigest, Repository: "strategies/fake"},
	})
	put(state.SessionDemandKey("sts1", sessionID), &statev1.SessionDemand{
		Desired: sessionv1.DesiredState_DESIRED_STATE_RUNNING, Start: true, IssuedAtNs: time.Now().UnixNano(),
	})
	return sealPath
}

func installBins(t *testing.T, binDir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	must(t, ctx, "", rootCmd("install", "-d", "-m", "0755", "/opt/mftiko/current/bin")...)
	for _, name := range []string{"mftiko-md-agent", "mftiko-td-agent", "mftiko-sts-agent", "mftiko-shim"} {
		must(t, ctx, "", rootCmd("install", "-m", "0755", filepath.Join(binDir, name), "/opt/mftiko/current/bin/"+name)...)
	}
}

func installHost(t *testing.T, repo, sealPath string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, a := range agents {
		must(t, ctx, "", rootCmd("useradd", "--system", "--no-create-home", "--shell", "/usr/sbin/nologin", "--user-group", a.user)...)
	}
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	seedPath := filepath.Join(tmp, "nats-signing.nk")
	if err := os.WriteFile(seedPath, seed, 0o600); err != nil {
		t.Fatal(err)
	}
	credsPath := filepath.Join(tmp, "nats.creds")
	if err := os.WriteFile(credsPath, []byte(agentPass+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	topoPath := filepath.Join(tmp, "topology.json")
	raw, err := json.MarshalIndent(testTopo(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(topoPath, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}

	must(t, ctx, "", rootCmd("install", "-d", "-m", "0755", "-o", "root", "-g", "root", "/etc/mftiko")...)
	for _, a := range agents {
		dir := "/etc/mftiko/" + a.name
		must(t, ctx, "", rootCmd("install", "-d", "-m", "0750", "-o", "root", "-g", a.user, dir)...)
		must(t, ctx, "", rootCmd("install", "-m", "0640", "-o", "root", "-g", a.user, topoPath, dir+"/topology.json")...)
		envPath := filepath.Join(tmp, a.name+".env")
		if err := os.WriteFile(envPath, []byte(envText(a, pub)), 0o600); err != nil {
			t.Fatal(err)
		}
		must(t, ctx, "", rootCmd("install", "-m", "0600", "-o", "root", "-g", "root", envPath, dir+"/env")...)
	}
	must(t, ctx, "", rootCmd("install", "-m", "0600", "-o", "mftiko-md", "-g", "mftiko-md", credsPath, "/etc/mftiko/md-agent/nats.creds")...)
	must(t, ctx, "", rootCmd("install", "-m", "0600", "-o", "mftiko-td", "-g", "mftiko-td", seedPath, "/etc/mftiko/td-agent/nats-signing.nk")...)
	must(t, ctx, "", rootCmd("install", "-m", "0600", "-o", "mftiko-sts", "-g", "mftiko-sts", seedPath, "/etc/mftiko/sts-agent/nats-signing.nk")...)
	must(t, ctx, "", rootCmd("install", "-m", "0600", "-o", "mftiko-td", "-g", "mftiko-td", sealPath, "/etc/mftiko/td-agent/seal.key")...)

	for _, a := range agents {
		src := filepath.Join(repo, "deploy", "systemd", a.unit)
		must(t, ctx, "", rootCmd("install", "-m", "0644", src, "/etc/systemd/system/"+a.unit)...)
	}
	must(t, ctx, "", rootCmd("systemctl", "daemon-reload")...)
}

func envText(a agent, accountPub string) string {
	lines := []string{
		"MFTIKO_ETCD_PASSWORD=" + agentPass,
		"MFTIKO_INSTANCE=" + a.inst,
		"MFTIKO_REGISTRY=127.0.0.1:5000",
		"MFTIKO_PLAIN_HTTP=true",
		"NATS_URL=nats://127.0.0.1:4222",
	}
	switch a.name {
	case "md-agent":
		lines = append(lines, "MFTIKO_NATS_CREDS=/etc/mftiko/md-agent/nats.creds")
	case "td-agent", "sts-agent":
		lines = append(lines, "MFTIKO_NATS_ACCOUNT="+accountPub)
	}
	return strings.Join(lines, "\n") + "\n"
}

func buildBins(root, binDir string) error {
	bins := []struct{ pkg, name string }{
		{"./cmd/mftiko-md-agent", "mftiko-md-agent"},
		{"./cmd/mftiko-td-agent", "mftiko-td-agent"},
		{"./cmd/mftiko-sts-agent", "mftiko-sts-agent"},
		{"./cmd/mftiko-shim", "mftiko-shim"},
		{"./deploy/systemd/fakeworker", "fakeworker"},
	}
	for _, b := range bins {
		phase("go build " + b.name)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		cmd := exec.CommandContext(ctx, "go", "build", "-o", filepath.Join(binDir, b.name), b.pkg)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			return fmt.Errorf("go build %s: %w\n%s", b.name, err, out)
		}
	}
	return nil
}

func composeUp(root string) error {
	phase("docker compose up")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	_, err := combined(ctx, root, "docker", "compose",
		"-f", "deploy/accept/compose.yaml",
		"-f", "deploy/systemd/accept-ports.yaml",
		"-p", "mftiko-systemd-adopt",
		"up", "-d", "--wait", "etcd", "nats", "registry")
	return err
}

func composeDown(t *testing.T, root string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	out, err := combined(ctx, root, "docker", "compose",
		"-f", "deploy/accept/compose.yaml",
		"-f", "deploy/systemd/accept-ports.yaml",
		"-p", "mftiko-systemd-adopt",
		"down", "-v", "--remove-orphans")
	if err != nil {
		t.Logf("teardown compose: %v\n%s", err, out)
	}
}

func pushImage(t *testing.T, img artifact.Image, repository string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := artifact.Push(ctx, img, artifact.Destination{
		Repository: repository, Tag: latest.Tag, PlainHTTP: true,
	}); err != nil {
		t.Fatalf("push %s: %v", repository, err)
	}
}

func teardownHost(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, a := range agents {
		logRun(t, ctx, rootCmd("systemctl", "stop", a.unit)...)
		logRun(t, ctx, rootCmd("systemctl", "kill", a.unit)...)
		if out, err := combined(ctx, "", "ps", "-u", a.user, "-o", "pid="); err == nil {
			for _, pid := range strings.Fields(out) {
				logRun(t, ctx, rootCmd("kill", "-KILL", pid)...)
			}
		}
		logRun(t, ctx, rootCmd("systemctl", "reset-failed", a.unit)...)
		logRun(t, ctx, rootCmd("rm", "-f", "/etc/systemd/system/"+a.unit)...)
	}
	logRun(t, ctx, rootCmd("systemctl", "daemon-reload")...)
	for _, a := range agents {
		logRun(t, ctx, rootCmd("userdel", a.user)...)
		logRun(t, ctx, rootCmd("rm", "-rf", "/var/lib/mftiko/"+a.name)...)
	}
	logRun(t, ctx, rootCmd("rmdir", "/var/lib/mftiko")...)
	logRun(t, ctx, rootCmd("rm", "-rf", "/opt/mftiko/current")...)
	logRun(t, ctx, rootCmd("rmdir", "/opt/mftiko")...)
	logRun(t, ctx, rootCmd("rm", "-rf", "/etc/mftiko")...)
}

func dumpUnit(t *testing.T, unit, cgroup string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	status, _ := combined(ctx, "", rootCmd("systemctl", "status", "--no-pager", "-l", unit)...)
	journal, _ := combined(ctx, "", rootCmd("journalctl", "-u", unit, "--no-pager", "-n", "80", "-o", "cat")...)
	procs := ""
	if cgroup != "" {
		procs, _ = combined(ctx, "", rootCmd("cat", cgroupProcs(cgroup))...)
	}
	t.Logf("systemctl status %s:\n%s\njournal:\n%s\ncgroup procs:\n%s", unit, status, journal, procs)
}

func refuseHost(t *testing.T) {
	t.Helper()
	for _, p := range []string{"/opt/mftiko", "/etc/mftiko", "/var/lib/mftiko"} {
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("%s already exists; refusing to clobber this host", p)
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	for _, a := range agents {
		p := "/etc/systemd/system/" + a.unit
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("%s already exists; refusing to clobber this host", p)
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err := exec.Command("id", a.user).Run(); err == nil {
			t.Fatalf("user %s already exists; refusing to clobber this host", a.user)
		}
	}
}

func rootCmd(args ...string) []string {
	if os.Geteuid() == 0 {
		return args
	}
	out := make([]string, 0, len(args)+2)
	out = append(out, "sudo", "-n")
	return append(out, args...)
}

func must(t *testing.T, ctx context.Context, dir string, args ...string) string {
	t.Helper()
	out, err := combined(ctx, dir, args...)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	return out
}

func logRun(t *testing.T, ctx context.Context, args ...string) {
	t.Helper()
	out, err := combined(ctx, "", args...)
	if err != nil {
		t.Logf("teardown %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func combined(ctx context.Context, dir string, args ...string) (string, error) {
	if len(args) == 0 {
		return "", errors.New("empty command")
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(), "SYSTEMD_COLORS=0", "LC_ALL=C")
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return string(out), fmt.Errorf("%s: %w", strings.Join(args, " "), ctx.Err())
		}
		return string(out), fmt.Errorf("%s: %w\n%s", strings.Join(args, " "), err, out)
	}
	return string(out), nil
}

func httpUp(url string) bool {
	c := &http.Client{Timeout: time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode < 500
}

func poll(d time.Duration, ok func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if ok() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func phase(msg string) {
	fmt.Fprintf(os.Stderr, "adopt: %s\n", msg)
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(filepath.Dir(filepath.Dir(file)))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestParseShow(t *testing.T) {
	// systemd 255 printed these three in an order that is not the -p order.
	out := "MainPID=6416\nControlGroup=/system.slice/mftiko-md-agent.service\nActiveState=active\n"
	got := parseShow(out, []string{"ActiveState", "MainPID", "ControlGroup"})
	want := []string{"active", "6416", "/system.slice/mftiko-md-agent.service"}
	if len(got) != len(want) {
		t.Fatalf("len %d", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("prop %d: got %q want %q", i, got[i], want[i])
		}
	}
	empty := parseShow("ActiveState=inactive\nMainPID=0\nControlGroup=\n", []string{"ActiveState", "MainPID", "ControlGroup"})
	if empty[0] != "inactive" || empty[1] != "0" || empty[2] != "" {
		t.Fatalf("empty cgroup: %#v", empty)
	}
}

// TestUnitKeys locks the unit files to E12-D016–E12-D020, and the etcd snapshot unit and timer to E12-D054. It does
// not need systemd.
func TestUnitKeys(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	matches, err := filepath.Glob(filepath.Join(dir, "*.service"))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]section{}
	var names []string
	for _, path := range matches {
		name := filepath.Base(path)
		names = append(names, name)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		secs, err := parseUnit(string(b))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got[name] = secs
	}
	want := []string{
		"etcd.service",
		"mftiko-api.service",
		"mftiko-controller.service",
		"mftiko-md-agent.service",
		"mftiko-sts-agent.service",
		"mftiko-sym.service",
		"mftiko-td-agent.service",
		"nats.service",
	}
	// Oneshot units started by a timer: not long-running, so the restart policy below is not theirs.
	oneshot := []string{"mftiko-etcd-backup.service"}
	if strings.Join(sorted(names), ",") != strings.Join(sorted(append(slices.Clone(want), oneshot...)), ",") {
		t.Fatalf("units %v, want %v and %v", sorted(names), want, oneshot)
	}
	agents := map[string]string{
		"mftiko-md-agent.service":  "mftiko/md-agent",
		"mftiko-td-agent.service":  "mftiko/td-agent",
		"mftiko-sts-agent.service": "mftiko/sts-agent",
	}
	forbidden := []string{"PrivateTmp", "ProtectSystem", "RuntimeDirectory", "LoadCredential", "ExecStop", "Delegate", "LimitNOFILE", "Type"}
	for _, name := range want {
		secs := got[name]
		if v, ok := lookup(secs, "[Unit]", "StartLimitIntervalSec"); !ok || v != "0" {
			t.Fatalf("%s StartLimitIntervalSec in [Unit] = %q, present %v", name, v, ok)
		}
		if inSection(secs, "[Service]", "StartLimitIntervalSec") {
			t.Fatalf("%s sets StartLimitIntervalSec in [Service]", name)
		}
		if v, ok := lookup(secs, "[Service]", "Restart"); !ok || v != "always" {
			t.Fatalf("%s Restart=%q", name, v)
		}
		if v, ok := lookup(secs, "[Service]", "RestartSec"); !ok || v != "1s" {
			t.Fatalf("%s RestartSec=%q", name, v)
		}
		for _, key := range forbidden {
			if hasKey(secs, key) {
				t.Fatalf("%s sets %s", name, key)
			}
		}
		if dir, ok := agents[name]; ok {
			for _, key := range []struct{ k, v string }{
				{"KillMode", "process"},
				{"OOMPolicy", "continue"},
				{"TimeoutStopSec", "30s"},
				{"KillSignal", "SIGTERM"},
				{"StateDirectory", dir},
			} {
				if v, ok := lookup(secs, "[Service]", key.k); !ok || v != key.v {
					t.Fatalf("%s %s=%q, want %s", name, key.k, v, key.v)
				}
			}
			exec := fields(lookupMust(t, secs, name, "ExecStart"))
			if !containsStr(exec, "-registry") {
				t.Fatalf("%s ExecStart has no -registry: %v", name, exec)
			}
			for _, bad := range []string{"-conn", "-account"} {
				if containsStr(exec, bad) {
					t.Fatalf("%s ExecStart contains %s", name, bad)
				}
			}
			continue
		}
		for _, key := range []string{"KillMode", "TimeoutStopSec", "OOMPolicy"} {
			if hasKey(secs, key) {
				t.Fatalf("%s sets %s", name, key)
			}
		}
	}
	etcd := got["etcd.service"]
	if v, _ := lookup(etcd, "[Service]", "User"); v != "etcd" {
		t.Fatalf("etcd User=%q", v)
	}
	if v, _ := lookup(etcd, "[Service]", "ExecStart"); v != "/usr/bin/etcd" {
		t.Fatalf("etcd ExecStart=%q", v)
	}
	if v, _ := lookup(etcd, "[Service]", "StateDirectory"); v != "etcd" {
		t.Fatalf("etcd StateDirectory=%q", v)
	}
	if v, _ := lookup(etcd, "[Service]", "Environment"); !strings.Contains(v, "ETCD_DATA_DIR=/var/lib/etcd") {
		t.Fatalf("etcd Environment=%q", v)
	}
	nats := got["nats.service"]
	if v, _ := lookup(nats, "[Service]", "User"); v != "nats" {
		t.Fatalf("nats User=%q", v)
	}
	if v, _ := lookup(nats, "[Service]", "ExecStart"); !strings.Contains(v, "/usr/bin/nats-server") {
		t.Fatalf("nats ExecStart=%q", v)
	}
	for _, name := range []string{"mftiko-controller.service", "mftiko-api.service", "mftiko-sym.service", "nats.service"} {
		if _, ok := lookup(got[name], "[Service]", "StateDirectory"); ok {
			t.Fatalf("%s sets StateDirectory", name)
		}
	}

	backup := got["mftiko-etcd-backup.service"]
	if v, _ := lookup(backup, "[Service]", "Type"); v != "oneshot" {
		t.Fatalf("mftiko-etcd-backup Type=%q", v)
	}
	for _, key := range []string{"Restart", "User"} { // root reads /etc/mftiko/etcd (E12-D050)
		if hasKey(backup, key) {
			t.Fatalf("mftiko-etcd-backup sets %s", key)
		}
	}
	exec := fields(lookupMust(t, backup, "mftiko-etcd-backup.service", "ExecStart"))
	for _, want := range []string{"/opt/mftiko/current/bin/mftiko-etcd-backup", "/etc/mftiko/etcd", "/var/backups/mftiko/etcd"} {
		if !containsStr(exec, want) {
			t.Fatalf("mftiko-etcd-backup ExecStart has no %s: %v", want, exec)
		}
	}
	b, err := os.ReadFile(filepath.Join(dir, "mftiko-etcd-backup.timer"))
	if err != nil {
		t.Fatal(err)
	}
	timer, err := parseUnit(string(b))
	if err != nil {
		t.Fatalf("mftiko-etcd-backup.timer: %v", err)
	}
	for _, kv := range []struct{ sec, k, v string }{
		{"[Timer]", "OnCalendar", "hourly"}, {"[Timer]", "Persistent", "true"}, {"[Install]", "WantedBy", "timers.target"},
	} {
		if v, _ := lookup(timer, kv.sec, kv.k); v != kv.v {
			t.Fatalf("mftiko-etcd-backup.timer %s=%q, want %s", kv.k, v, kv.v)
		}
	}
}

func lookupMust(t *testing.T, secs []section, name, key string) string {
	t.Helper()
	v, ok := lookup(secs, "[Service]", key)
	if !ok {
		t.Fatalf("%s has no %s", name, key)
	}
	return v
}

func fields(s string) []string { return strings.Fields(s) }

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

type pair struct{ k, v string }

type section struct {
	name  string
	pairs []pair
}

func parseUnit(text string) ([]section, error) {
	var secs []section
	cur := -1
	cont := false
	var ck string
	var cv strings.Builder
	flush := func() {
		if cur < 0 || ck == "" {
			ck = ""
			cv.Reset()
			cont = false
			return
		}
		secs[cur].pairs = append(secs[cur].pairs, pair{ck, strings.TrimSpace(cv.String())})
		ck = ""
		cv.Reset()
		cont = false
	}
	for i, line := range strings.Split(text, "\n") {
		if cont {
			body, more := stripCont(line)
			cv.WriteByte(' ')
			cv.WriteString(strings.TrimSpace(body))
			if !more {
				flush()
			}
			continue
		}
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "#") || strings.HasPrefix(trim, ";") {
			continue
		}
		if strings.HasPrefix(trim, "[") && strings.HasSuffix(trim, "]") {
			flush()
			secs = append(secs, section{name: trim})
			cur = len(secs) - 1
			continue
		}
		if cur < 0 {
			return nil, fmt.Errorf("line %d: directive outside a section", i+1)
		}
		body, more := stripCont(line)
		key, val, ok := strings.Cut(strings.TrimSpace(body), "=")
		if !ok {
			return nil, fmt.Errorf("line %d: not a directive", i+1)
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if more {
			ck = key
			cv.WriteString(val)
			cont = true
			continue
		}
		secs[cur].pairs = append(secs[cur].pairs, pair{key, val})
	}
	if cont {
		return nil, errors.New("unterminated continuation")
	}
	return secs, nil
}

func stripCont(line string) (string, bool) {
	s := strings.TrimRight(line, " \t\r")
	if strings.HasSuffix(s, `\`) {
		return s[:len(s)-1], true
	}
	return s, false
}

func lookup(secs []section, name, key string) (string, bool) {
	for _, s := range secs {
		if s.name != name {
			continue
		}
		for i := len(s.pairs) - 1; i >= 0; i-- {
			if s.pairs[i].k == key {
				return s.pairs[i].v, true
			}
		}
	}
	return "", false
}

func inSection(secs []section, name, key string) bool {
	_, ok := lookup(secs, name, key)
	return ok
}

func hasKey(secs []section, key string) bool {
	for _, s := range secs {
		for _, p := range s.pairs {
			if p.k == key {
				return true
			}
		}
	}
	return false
}
