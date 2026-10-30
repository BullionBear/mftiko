// Command mftiko-alert-smoke is make dev-alert-smoke (DECISIONS E16-D032–E16-D035, E16-04). It runs
// against the stack make dev-up starts and checks the strategy log alerts end to end, through the one
// Alertmanager and the webhook receiver that writes deploy/dev/data/webhook/received.jsonl:
//
//   - R1, R2 and R5 reach the receiver once each, with the labels and the logs annotation that
//     deploy/dev/loki/mftiko.rules.yml gives them, and without the text of the line (E16-D010). Each
//     logs annotation leads back to its session: the smoke runs the mftiko session logs command, or
//     the Loki query, that it names.
//   - With sts1 stopped, MftikoAgentDown{instance="sts1"} reaches the receiver, and Alertmanager
//     reports the three strategy alerts as inhibited by it (E16-D030).
//
// The lines come from examples/strategies/conform, which writes no warn line of its own:
//
//   - R1: init_sleep_ms is not a number. OnInit returns the error, and the SDK logs "init failed" at
//     error with the logger that ctx.Log() returns.
//   - R5: OnStop sleeps 150ms, and the SDK logs "hook exceeded 100ms".
//   - R2: no example strategy panics outside a hook, so the smoke appends a simulated panic line to a
//     finished session's stderr inside sts1.
//
// It refuses to start while sts1 runs a worker, because stopping sts1 stops that worker too. sts1 is
// started again however the smoke ends. The smoke's sessions are deleted when it passes and kept when
// it fails.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

const (
	stsInstance = "sts1"
	region      = "dev"
	// pushRepo is the registry as the host sees it; sessionRepo is the same registry as the API and
	// sts1 see it (docs/operations.md, dev).
	pushRepo    = "127.0.0.1:5000/strategies/conform"
	sessionRepo = "registry:5000/strategies/conform"
	packTag     = "alert-smoke"
	r2Line      = "panic: alert-smoke: a simulated R2 line, not a real panic"
	// settleWait covers the dev Alertmanager's group_interval of 5m: a resolved notification goes out
	// at the group's next interval.
	settleWait = 6 * time.Minute
	pollEvery  = 2 * time.Second
)

type config struct {
	api, alertmanager, loki string
	received, token         string
	cli, pack, strategy     string
	compose                 string
	wait                    time.Duration
}

func main() {
	var c config
	version := flag.Bool("version", false, "print the release and exit")
	flag.StringVar(&c.api, "api", "http://127.0.0.1:8080", "the dev API")
	flag.StringVar(&c.alertmanager, "alertmanager", "http://127.0.0.1:9093", "the dev Alertmanager")
	flag.StringVar(&c.loki, "loki", "http://127.0.0.1:3100", "the dev Loki")
	flag.StringVar(&c.received, "received", "deploy/dev/data/webhook/received.jsonl", "the file the dev webhook receiver writes")
	flag.StringVar(&c.token, "token", "deploy/dev/data/api/token", "the dev API token")
	flag.StringVar(&c.cli, "mftiko", "bin/mftiko", "the mftiko CLI, from make build")
	flag.StringVar(&c.pack, "pack", "bin/mftiko-pack", "mftiko-pack, from make build")
	flag.StringVar(&c.strategy, "strategy", "bin/linux/examples/conform", "conform built for sts1, from make dev-up")
	flag.StringVar(&c.compose, "compose", "deploy/dev/compose.yaml", "the dev compose file")
	flag.DurationVar(&c.wait, "wait", 3*time.Minute, "how long each step waits")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, c); err != nil {
		fmt.Fprintln(os.Stderr, "alert-smoke: FAIL:", err)
		os.Exit(1)
	}
}

func say(format string, args ...any) {
	fmt.Printf("alert-smoke: "+format+"\n", args...)
}

func run(ctx context.Context, c config) (err error) {
	for _, need := range []struct{ path, from string }{{c.cli, "make build"}, {c.pack, "make build"}, {c.strategy, "make dev-up"}} {
		if _, err := os.Stat(need.path); err != nil {
			return fmt.Errorf("%w; run %s", err, need.from)
		}
	}
	cli, cleanup, err := newCLI(c.cli, c.api, c.token)
	if err != nil {
		return err
	}
	defer cleanup()
	dc := compose{file: c.compose}

	inst, err := cli.instance(ctx)
	if err != nil {
		return err
	}
	if inst.Agent == nil {
		return errors.New("sts1 has no agent; run make dev-up")
	}
	if busy := inst.busy(); len(busy) > 0 {
		return fmt.Errorf("sts1 runs %s. The smoke stops sts1, which stops those workers too; end them first (mftiko session end <id>)", strings.Join(busy, ", "))
	}
	if err := settle(ctx, c); err != nil {
		return err
	}
	from := fileSize(c.received)

	digest, err := pack(ctx, c.pack, c.strategy)
	if err != nil {
		return err
	}
	say("pushed conform to %s@%s", pushRepo, digest)

	var made []session
	defer func() {
		if err == nil || len(made) == 0 {
			return
		}
		ids := make([]string, len(made))
		for i, s := range made {
			ids[i] = s.id
		}
		fmt.Fprintf(os.Stderr, "alert-smoke: kept sessions %s; delete them with mftiko --yes session delete <id>\n", strings.Join(ids, " "))
	}()
	steps := []struct {
		rule, name, params string
		exit               int
	}{
		{"R1", "alert-smoke-r1", `{"init_sleep_ms":"not-a-number"}`, exitInitFailed},
		{"R5", "alert-smoke-r5", `{"on_stop_sleep_ms":150,"self_exit":true}`, 0},
		{"R2", "alert-smoke-r2", `{"self_exit":true}`, 0},
	}
	for _, st := range steps {
		s, err := cli.create(ctx, st.name, st.params, digest, st.exit, c.wait)
		if err != nil {
			return err
		}
		s.rule = st.rule
		made = append(made, s)
		say("%s: session %s, %s", st.rule, s.id, s.stderr)
	}
	r1, r5, r2 := made[0], made[1], made[2]
	if err := dc.run(ctx, "exec", "-T", stsInstance, "sh", "-c", `printf '%s\n' "$2" >>"$1"`, "sh", r2.stderr, r2Line); err != nil {
		return err
	}
	say("R2: appended %q to %s", r2Line, r2.stderr)

	logAlerts := []want{logWant(r1), logWant(r2), logWant(r5)}
	if err := waitReceived(ctx, c.received, from, logAlerts, c.wait); err != nil {
		return err
	}
	if err := cli.followLogs(ctx, logAlerts[0], r1, map[string]string{"level": "error", "message": "init failed"}); err != nil {
		return err
	}
	if err := cli.followLogs(ctx, logAlerts[2], r5, map[string]string{"message": "hook exceeded 100ms"}); err != nil {
		return err
	}
	if err := followQuery(ctx, c.loki, logAlerts[1], r2); err != nil {
		return err
	}

	if err := dc.run(ctx, "stop", stsInstance); err != nil {
		return err
	}
	say("stopped sts1")
	started := false
	defer func() {
		if started {
			return
		}
		bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if serr := dc.run(bg, "start", stsInstance); serr != nil {
			fmt.Fprintln(os.Stderr, "alert-smoke:", serr)
			return
		}
		fmt.Fprintln(os.Stderr, "alert-smoke: started sts1 again. Alertmanager resolves MftikoAgentDown{instance=\"sts1\"} at its next group_interval (5m); the next run waits for that")
	}()
	if err := waitInhibited(ctx, c.alertmanager, logAlerts, c.wait); err != nil {
		return err
	}
	if err := waitReceived(ctx, c.received, from, []want{agentDownWant("firing")}, c.wait); err != nil {
		return err
	}

	if err := dc.run(ctx, "start", stsInstance); err != nil {
		return err
	}
	started = true
	if err := poll(ctx, c.wait, func() (bool, error) {
		inst, err := cli.instance(ctx)
		return err == nil && inst.Agent != nil, nil
	}); err != nil {
		return fmt.Errorf("sts1's agent did not come back: %w", err)
	}
	say("started sts1; its agent is back")

	for _, s := range made {
		if _, stderr, code, err := cli.run(ctx, "", "--yes", "session", "delete", s.id); err != nil || code != 0 {
			return fmt.Errorf("session delete %s: exit %d %v: %s", s.id, code, err, stderr)
		}
	}
	made = nil
	say("deleted the sessions")
	say("ok: R1, R2 and R5 once each, and MftikoAgentDown once, all through the one Alertmanager; the three were inhibited while sts1 was stopped")
	say("Alertmanager resolves MftikoAgentDown{instance=\"sts1\"} at its next group_interval (5m)")
	return nil
}

// settle waits until Alertmanager has no MftikoAgentDown{instance="sts1"} and the receiver's last
// notification of it, if any, is resolved. Otherwise the one this run causes is the same alert to
// Alertmanager, which holds it back until repeat_interval (4h).
func settle(ctx context.Context, c config) error {
	err := poll(ctx, c.wait, func() (bool, error) {
		alerts, err := getAlerts(ctx, c.alertmanager, `alertname="MftikoAgentDown"`, `instance="`+stsInstance+`"`)
		return len(alerts) == 0, err
	})
	if err != nil {
		return fmt.Errorf("Alertmanager still has MftikoAgentDown{instance=\"sts1\"}: %w", err)
	}
	group := agentDownWant("").group
	ns, err := readReceived(c.received, 0)
	if err != nil {
		return err
	}
	if lastStatus(ns, group) != "firing" {
		return nil
	}
	say("waiting up to %s for the resolved MftikoAgentDown{instance=\"sts1\"} of an earlier stop", settleWait)
	err = poll(ctx, settleWait, func() (bool, error) {
		ns, err := readReceived(c.received, 0)
		return lastStatus(ns, group) == "resolved", err
	})
	if err != nil {
		return fmt.Errorf("the receiver's last MftikoAgentDown{instance=\"sts1\"} is still firing: %w", err)
	}
	return nil
}

// poll calls f every pollEvery until it returns true or an error, or until limit passes.
func poll(ctx context.Context, limit time.Duration, f func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	t := time.NewTicker(pollEvery)
	defer t.Stop()
	for {
		done, err := f()
		if err != nil || done {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
