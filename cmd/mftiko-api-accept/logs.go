package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// chatterRepository is examples/strategies/chatter, which accept.sh e2e-up pushes next to hello.
const chatterRepository = "registry:5000/strategies/chatter"

// gapLine is what the CLI prints on stderr for a gap (E14-D032).
const gapLine = "mftiko: some output was rotated out"

// logSteps is E14-08 (#443): session logs through the CLI alone. sts1 rotates at 64 KiB and keeps 3
// files (accept.sh), so chatter's output rotates within seconds.
//
//  1. read: a fixed amount of chatter output. session logs from the start, --tail and export-logs
//     match the files on sts1's host, byte for byte, oldest rotated file first.
//  2. restart: session logs -f while sts1 is restarted. Slow output, so nothing rotates out: the
//     lines it prints are consecutive, none missing and none twice.
//  3. gap: the same with fast output. Before the restart -f keeps up across rotations; across the
//     restart the output it was reading has rotated out, so it prints the gap line and goes on with
//     later lines, still none twice.
//  4. token: sts1's log endpoint refuses no token and a wrong one; its command line has only the
//     token's SHA-256 (E14-D001, E14-D013).
func (c *cli) logSteps(logAddr, tokenFile string) error {
	id, err := c.chatter("accept-cli-logs-read", `{"lines":5000,"bytes":100,"stderr_every":10}`)
	if err != nil {
		return err
	}
	if err := c.readSteps(id); err != nil {
		return err
	}
	if err := c.tokenSteps(id, logAddr, tokenFile); err != nil {
		return err
	}
	if err := c.endAndDelete(id); err != nil {
		return err
	}

	// 50 lines of 100 bytes a second: a 64 KiB file lasts 13 s, three of them well over the restart.
	id, err = c.chatter("accept-cli-logs-restart", `{"rate":50,"bytes":100}`)
	if err != nil {
		return err
	}
	seqs, stderr, err := c.followAcrossRestart(id, 100)
	if err != nil {
		return err
	}
	if strings.Contains(stderr, gapLine) {
		return fmt.Errorf("slow output: -f printed %q", gapLine)
	}
	if err := consecutive(seqs); err != nil {
		return fmt.Errorf("slow output across the restart: %w", err)
	}
	say("session logs -f across a restart of %s: lines %d to %d, none missing, none twice", stsInst, seqs[0], seqs[len(seqs)-1])
	if err := c.endAndDelete(id); err != nil {
		return err
	}

	// 5000 lines of 100 bytes a second: three 64 KiB files are under 0.4 s of output.
	id, err = c.chatter("accept-cli-logs-gap", `{"rate":5000,"bytes":100}`)
	if err != nil {
		return err
	}
	seqs, stderr, err = c.followAcrossRestart(id, 10000)
	if err != nil {
		return err
	}
	if !strings.Contains(stderr, gapLine) {
		return fmt.Errorf("fast output across the restart: no %q on stderr", gapLine)
	}
	jumps, err := increasing(seqs)
	if err != nil {
		return fmt.Errorf("fast output: %w", err)
	}
	if len(jumps) == 0 {
		return errors.New("fast output: a gap was printed but the line numbers never jump")
	}
	if first := jumps[0]; first.from-seqs[0] < 3*64<<10/100 {
		return fmt.Errorf("fast output: the first jump is after line %d, before three rotations", first.from)
	}
	say("session logs -f with fast output: consecutive to line %d across rotations, then %q, then from line %d; none twice",
		jumps[0].from, gapLine, jumps[0].to)
	return c.endAndDelete(id)
}

// chatter creates a chatter session on sts1 and waits until it runs.
func (c *cli) chatter(name, params string) (string, error) {
	file, err := c.sessionFile(name, fmt.Sprintf("name: %s\nsts_instance: %s\nstrategy:\n  repository: %s\n  ref: %s\nparams: %s\nrestart:\n  mode: never\n",
		name, stsInst, chatterRepository, strategyTag, params))
	if err != nil {
		return "", err
	}
	var s apiv1.Session
	if err := c.okJSON(&s, "session", "create", "-f", file, "--wait", "running", "--wait-timeout", "2m"); err != nil {
		return "", err
	}
	say("session %s: chatter %s on %s", s.Id, params, stsInst)
	return s.Id, nil
}

func (c *cli) endAndDelete(id string) error {
	if _, err := c.ok("session", "end", id, "--wait", "--yes"); err != nil {
		return err
	}
	if _, err := c.ok("session", "delete", id, "--yes"); err != nil {
		return err
	}
	return nil
}

// readSteps waits until chatter has written all 5000 lines, then compares what the CLI reads with the
// files on this host.
func (c *cli) readSteps(id string) error {
	s, err := c.show(id)
	if err != nil {
		return err
	}
	if s.Logs == nil {
		return errors.New("session show has no log location")
	}
	var out, errb []byte
	if err := until(c.f.ctx, time.Minute, "chatter's 5000 lines on disk", func() error {
		if out, err = hostLog(s.Logs.Stdout); err != nil {
			return err
		}
		if !bytes.Contains(out, []byte("\nchatter 5000 5000 ")) {
			return errors.New("not yet")
		}
		errb, err = hostLog(s.Logs.Stderr)
		return err
	}); err != nil {
		return err
	}
	rotated, err := filepath.Glob(s.Logs.Stdout + ".*")
	if err != nil {
		return err
	}
	if len(rotated) == 0 {
		return fmt.Errorf("%s did not rotate: is sts1 running with -log-max-bytes 65536?", s.Logs.Stdout)
	}
	say("on disk: stdout %d bytes in %d files, stderr %d bytes", len(out), len(rotated)+1, len(errb))

	for _, st := range []struct {
		name string
		want []byte
	}{{"stdout", out}, {"stderr", errb}} {
		// The worker's stdout goes to the CLI's stdout and its stderr to the CLI's stderr (E14-D032).
		o, err := c.ok("session", "logs", id, "--stream", st.name)
		if err != nil {
			return err
		}
		got := o.stdout
		if st.name == "stderr" {
			got = o.stderr
		}
		if got != string(st.want) {
			return fmt.Errorf("session logs --stream %s: %d bytes, the files on disk have %d", st.name, len(got), len(st.want))
		}
	}
	say("session logs --stream stdout and stderr: the same bytes as the files on disk")

	o, err := c.ok("session", "logs", id, "--stream", "stdout", "--tail", "100")
	if err != nil {
		return err
	}
	if want := lastLines(out, 100); o.stdout != want {
		return fmt.Errorf("session logs --tail 100: %q..., want the last 100 lines on disk %q...", head(o.stdout), head(want))
	}
	say("session logs --tail 100: the last 100 lines on disk")

	tgz := filepath.Join(c.dir, "logs-"+id+".tar.gz")
	if _, err := c.ok("session", "export-logs", id, "-o", tgz); err != nil {
		return err
	}
	if err := checkExport(tgz, out, errb); err != nil {
		return err
	}
	if _, err := c.fails(1, "file exists", "session", "export-logs", id, "-o", tgz); err != nil {
		return err
	}
	say("session export-logs: stdout.log and stderr.log are the files on disk, manifest.json has their size and sha256; an existing -o is refused")
	return nil
}

// tokenSteps asks sts1's log endpoint directly, not through the API.
func (c *cli) tokenSteps(id, logAddr, tokenFile string) error {
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(raw))
	c.secrets = append(c.secrets, token)
	url := "http://" + logAddr + "/v1/sessions/" + id + "/logs/stdout?tail=1"
	for _, tc := range []struct {
		name, auth string
		want       int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"a wrong token", "Bearer " + strings.Repeat("x", len(token)), http.StatusUnauthorized},
		{"the token", "Bearer " + token, http.StatusOK},
	} {
		req, err := http.NewRequestWithContext(c.f.ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		if tc.auth != "" {
			req.Header.Set("Authorization", tc.auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			return fmt.Errorf("%s with %s: HTTP %d, want %d", logAddr, tc.name, resp.StatusCode, tc.want)
		}
	}
	p, err := stsProc(stsInst)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(token))
	cmdline := strings.Join(p.argv, " ")
	if strings.Contains(cmdline, token) || !strings.Contains(cmdline, hex.EncodeToString(sum[:])) {
		return fmt.Errorf("%s's command line must have the token's SHA-256 and not the token", stsInst)
	}
	say("%s: no token and a wrong token are 401, the token is 200; %s has only its SHA-256", logAddr, stsInst)
	return nil
}

// followAcrossRestart runs session logs -f on id, restarts sts1 once it has n lines, waits for n more
// after the new agent is elected, ends the session and returns the chatter line numbers -f printed
// and its stderr. -f must end by itself on the session's end (E14-D024).
func (c *cli) followAcrossRestart(id string, n int) ([]uint64, string, error) {
	f, err := c.start("session", "logs", id, "-f", "--tail", "1", "--stream", "stdout")
	if err != nil {
		return nil, "", err
	}
	defer f.kill()
	if err := until(c.f.ctx, 2*time.Minute, fmt.Sprintf("%d lines from session logs -f", n), func() error {
		if got := len(chatterSeqs(f.out.String())); got < n {
			return fmt.Errorf("%d lines", got)
		}
		return f.exited()
	}); err != nil {
		return nil, "", err
	}
	var inst apiv1.InstanceDetail
	if err := c.okJSON(&inst, "instance", "show", stsInst); err != nil {
		return nil, "", err
	}
	if inst.Agent == nil {
		return nil, "", fmt.Errorf("instance show %s: no agent", stsInst)
	}
	epoch := inst.Agent.Epoch
	if err := restartSTS(stsInst); err != nil {
		return nil, "", err
	}
	if err := until(c.f.ctx, 2*time.Minute, stsInst+" elected again", func() error {
		// A fresh value each time: an omitted last_known must not keep an earlier true.
		inst = apiv1.InstanceDetail{}
		if err := c.okJSON(&inst, "instance", "show", stsInst); err != nil {
			return stop(err)
		}
		if inst.Agent == nil || (inst.Agent.LastKnown != nil && *inst.Agent.LastKnown) || inst.Agent.Epoch <= epoch {
			return fmt.Errorf("agent %+v", inst.Agent)
		}
		return nil
	}); err != nil {
		return nil, "", err
	}
	say("%s restarted: epoch %d -> %d", stsInst, epoch, inst.Agent.Epoch)
	before := len(chatterSeqs(f.out.String()))
	if err := until(c.f.ctx, 3*time.Minute, fmt.Sprintf("%d more lines after the restart", n), func() error {
		if got := len(chatterSeqs(f.out.String())) - before; got < n {
			return fmt.Errorf("%d lines; stderr %q", got, f.err.String())
		}
		return f.exited()
	}); err != nil {
		return nil, "", err
	}
	if _, err := c.ok("session", "end", id, "--wait", "--yes"); err != nil {
		return nil, "", err
	}
	select {
	case <-f.done:
	case <-time.After(time.Minute):
		return nil, "", errors.New("session logs -f did not end after the session ended")
	}
	if f.code != 0 {
		return nil, "", fmt.Errorf("session logs -f: exit %d, stderr %q", f.code, f.err.String())
	}
	if err := c.secretFree(f.out.String() + f.err.String()); err != nil {
		return nil, "", err
	}
	seqs := chatterSeqs(f.out.String())
	if len(seqs) == 0 {
		return nil, "", errors.New("session logs -f printed no chatter line")
	}
	return seqs, f.err.String(), nil
}

// bgRun is a CLI run in the background.
type bgRun struct {
	cmd      *exec.Cmd
	out, err *syncBuf
	done     chan struct{}
	code     int
}

func (c *cli) start(args ...string) (*bgRun, error) {
	cmd := exec.CommandContext(c.f.ctx, c.bin, args...)
	cmd.Env = []string{"HOME=" + c.home, "MFTIKO_CONTEXT=accept", "PATH=" + os.Getenv("PATH")}
	r := &bgRun{cmd: cmd, out: &syncBuf{}, err: &syncBuf{}, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = r.out, r.err
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		err := cmd.Wait()
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			r.code = exit.ExitCode()
		} else if err != nil {
			r.code = -1
		}
		close(r.done)
	}()
	return r, nil
}

// exited is an error that stops an until when the run has ended.
func (r *bgRun) exited() error {
	select {
	case <-r.done:
		return stop(fmt.Errorf("exit %d, stderr %q", r.code, r.err.String()))
	default:
		return nil
	}
}

func (r *bgRun) kill() {
	select {
	case <-r.done:
	default:
		_ = r.cmd.Process.Kill()
		<-r.done
	}
}

func (c *cli) secretFree(s string) error {
	for _, sec := range c.secrets {
		if sec != "" && strings.Contains(s, sec) {
			return errors.New("the CLI printed a token or a credential")
		}
	}
	return nil
}

// syncBuf is a bytes.Buffer a child's output and the flow can share.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// stsAgent is a running mftiko-sts-agent: its pid, command line and environment.
type stsAgent struct {
	pid       int
	argv, env []string
}

// stsProc finds inst's agent in /proc. The flow runs in the runner, next to it (accept.sh).
func stsProc(inst string) (stsAgent, error) {
	dirs, err := os.ReadDir("/proc")
	if err != nil {
		return stsAgent{}, err
	}
	for _, d := range dirs {
		pid, err := strconv.Atoi(d.Name())
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", d.Name(), "cmdline"))
		if err != nil {
			continue
		}
		argv := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
		if len(argv) < 3 || argv[0] != "/mftiko/mftiko-sts-agent" || argv[1] != "-instance" || argv[2] != inst {
			continue
		}
		env, err := os.ReadFile(filepath.Join("/proc", d.Name(), "environ"))
		if err != nil {
			return stsAgent{}, err
		}
		return stsAgent{pid: pid, argv: argv, env: strings.Split(strings.TrimRight(string(env), "\x00"), "\x00")}, nil
	}
	return stsAgent{}, fmt.Errorf("no mftiko-sts-agent -instance %s running", inst)
}

// restartSTS stops inst's agent with SIGTERM, as systemctl restart does, and starts it again with
// the same command line and environment. Its shims and workers keep running (E12-D016). The new
// agent's log is /work/e2e/<inst>-restart-<UTC>.log.
func restartSTS(inst string) error {
	p, err := stsProc(inst)
	if err != nil {
		return err
	}
	if err := syscall.Kill(p.pid, syscall.SIGTERM); err != nil {
		return err
	}
	if err := until(context.Background(), 30*time.Second, inst+" agent stopped", func() error {
		if _, err := stsProc(inst); err == nil {
			return errors.New("still running")
		}
		return nil
	}); err != nil {
		return err
	}
	logPath := filepath.Join(*stateDir, inst+"-restart-"+time.Now().UTC().Format("20060102T150405Z")+".log")
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(p.argv[0], p.argv[1:]...)
	cmd.Env = p.env
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // outlives this flow
	if err := cmd.Start(); err != nil {
		return err
	}
	say("%s agent %d stopped; started again as %d, log %s", inst, p.pid, cmd.Process.Pid, logPath)
	return cmd.Process.Release()
}

// hostLog is one stream of a worker on this host: its rotated files, oldest first, then the live
// one (E10-D021), concatenated.
func hostLog(path string) ([]byte, error) {
	rotated, err := filepath.Glob(path + ".*")
	if err != nil {
		return nil, err
	}
	nums := []int{}
	for _, r := range rotated {
		if n, err := strconv.Atoi(strings.TrimPrefix(r, path+".")); err == nil && n > 0 {
			nums = append(nums, n)
		}
	}
	slices.Sort(nums)
	slices.Reverse(nums)
	var out []byte
	for _, n := range nums {
		b, err := os.ReadFile(path + "." + strconv.Itoa(n))
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return append(out, b...), nil
}

// lastLines is the last n lines of b, a last line without a newline counted (E14-D012).
func lastLines(b []byte, n int) string {
	s := string(b)
	end := len(s)
	if strings.HasSuffix(s, "\n") {
		end--
	}
	i := end
	for range n {
		j := strings.LastIndexByte(s[:i], '\n')
		if j < 0 {
			return s
		}
		i = j
	}
	return s[i+1:]
}

func head(s string) string {
	if len(s) > 60 {
		return s[:60]
	}
	return s
}

// checkExport reads an export (E14-D014): stdout.log, stderr.log, then manifest.json.
func checkExport(path string, stdout, stderr []byte) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	tr := tar.NewReader(zr)
	var names []string
	files := map[string][]byte{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			return err
		}
		names = append(names, h.Name)
		files[h.Name] = b
	}
	if !slices.Equal(names, []string{"stdout.log", "stderr.log", "manifest.json"}) {
		return fmt.Errorf("export has %v", names)
	}
	var m map[string]struct {
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
	}
	if err := json.Unmarshal(files["manifest.json"], &m); err != nil {
		return err
	}
	for _, st := range []struct {
		name string
		want []byte
	}{{"stdout", stdout}, {"stderr", stderr}} {
		got := files[st.name+".log"]
		if !bytes.Equal(got, st.want) {
			return fmt.Errorf("export %s.log: %d bytes, the files on disk have %d", st.name, len(got), len(st.want))
		}
		sum := sha256.Sum256(got)
		if m[st.name].Size != int64(len(got)) || m[st.name].SHA256 != hex.EncodeToString(sum[:]) {
			return fmt.Errorf("manifest.json %s is %+v, the file is %d bytes", st.name, m[st.name], len(got))
		}
	}
	return nil
}

// chatterSeqs is the number of each "chatter <seq> <seq> " line in s, in order. A line whose two
// numbers differ was cut at a rotation and joined to another cut line across a gap; it is skipped.
func chatterSeqs(s string) []uint64 {
	var out []uint64
	for _, l := range strings.Split(s, "\n") {
		f := strings.Fields(l)
		if len(f) < 3 || f[0] != "chatter" || f[1] != f[2] {
			continue
		}
		if n, err := strconv.ParseUint(f[1], 10, 64); err == nil {
			out = append(out, n)
		}
	}
	return out
}

func consecutive(seqs []uint64) error {
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return fmt.Errorf("line %d follows line %d", seqs[i], seqs[i-1])
		}
	}
	return nil
}

type jump struct{ from, to uint64 }

// increasing reports the places seqs skips ahead, and fails when a number repeats or goes back.
func increasing(seqs []uint64) ([]jump, error) {
	var jumps []jump
	for i := 1; i < len(seqs); i++ {
		switch {
		case seqs[i] <= seqs[i-1]:
			return nil, fmt.Errorf("line %d after line %d: printed twice or out of order", seqs[i], seqs[i-1])
		case seqs[i] > seqs[i-1]+1:
			jumps = append(jumps, jump{seqs[i-1], seqs[i]})
		}
	}
	return jumps, nil
}
