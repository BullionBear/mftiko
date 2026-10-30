package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// exitInitFailed is what mftiko session create --wait exits with when the session ends init_failed.
const exitInitFailed = 31

// cli runs the mftiko CLI with a HOME of its own, whose one context is the dev API.
type cli struct {
	bin string
	env []string
}

func newCLI(bin, api, tokenFile string) (cli, func(), error) {
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return cli{}, nil, fmt.Errorf("%w; run make dev-up", err)
	}
	home, err := os.MkdirTemp("", "mftiko-alert-smoke-")
	if err != nil {
		return cli{}, nil, err
	}
	cleanup := func() { _ = os.RemoveAll(home) }
	dir := filepath.Join(home, ".config", "mftiko")
	conf := fmt.Sprintf("contexts:\n  dev:\n    url: %s/api/v1\n    insecure_http: true\n", api)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		cleanup()
		return cli{}, nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(conf), 0o600); err != nil {
		cleanup()
		return cli{}, nil, err
	}
	env := []string{"HOME=" + home, "MFTIKO_CONTEXT=dev", "MFTIKO_TOKEN=" + strings.TrimSpace(string(token)), "PATH=" + os.Getenv("PATH")}
	return cli{bin: bin, env: env}, cleanup, nil
}

// run returns stdout, stderr and the exit code. A nonzero exit is not an error; failing to run is.
func (c cli) run(ctx context.Context, stdin string, args ...string) (stdout, stderr string, code int, err error) {
	cmd := exec.CommandContext(ctx, c.bin, args...)
	cmd.Env = c.env
	cmd.Stdin = strings.NewReader(stdin)
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err = cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return so.String(), strings.TrimSpace(se.String()), exit.ExitCode(), nil
	}
	return so.String(), strings.TrimSpace(se.String()), 0, err
}

// session is one session the smoke made, for one rule.
type session struct {
	rule   string
	id     string
	stderr string // the path on sts1, which is the alert's filename (E16-D004)
}

// create makes a session of conform with params, waits until it is terminal, and checks the exit
// code of mftiko session create --wait.
func (c cli) create(ctx context.Context, name, params, digest string, exit int, limit time.Duration) (session, error) {
	spec, err := json.Marshal(map[string]any{
		"name":         name,
		"sts_instance": stsInstance,
		"strategy":     map[string]string{"repository": sessionRepo, "ref": digest},
		"params":       json.RawMessage(params),
		"restart":      map[string]string{"mode": "never"},
	})
	if err != nil {
		return session{}, err
	}
	out, stderr, code, err := c.run(ctx, string(spec), "-o", "json", "session", "create", "-f", "-", "--wait", "terminal", "--wait-timeout", limit.String())
	if err != nil {
		return session{}, err
	}
	if code != exit {
		return session{}, fmt.Errorf("session create %s: exit %d, want %d: %s", name, code, exit, stderr)
	}
	var got struct {
		ID   string `json:"id"`
		Logs struct {
			Stderr string `json:"stderr"`
		} `json:"logs"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil || got.ID == "" || got.Logs.Stderr == "" {
		return session{}, fmt.Errorf("session create %s: no id and stderr path in %q", name, out)
	}
	return session{id: got.ID, stderr: got.Logs.Stderr}, nil
}

// instanceView is the part of mftiko instance show sts1 the smoke reads. Agent is null while sts1's
// agent is not there.
type instanceView struct {
	Agent    *json.RawMessage `json:"agent"`
	Sessions []struct {
		SessionID string `json:"session_id"`
		Phase     string `json:"phase"`
	} `json:"sessions"`
}

func (c cli) instance(ctx context.Context) (instanceView, error) {
	var v instanceView
	out, stderr, code, err := c.run(ctx, "", "-o", "json", "instance", "show", stsInstance)
	if err != nil {
		return v, err
	}
	if code != 0 {
		return v, fmt.Errorf("instance show %s: exit %d: %s", stsInstance, code, stderr)
	}
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		return v, fmt.Errorf("instance show %s: %w", stsInstance, err)
	}
	return v, nil
}

// busy lists the sessions that have, or are about to have, a worker on sts1.
func (v instanceView) busy() []string {
	var ids []string
	for _, s := range v.Sessions {
		switch s.Phase {
		case "starting", "running", "stopping", "restarting":
			ids = append(ids, s.SessionID+" ("+s.Phase+")")
		}
	}
	return ids
}

// followLogs runs the logs annotation of w, a mftiko session logs command, and checks that it prints
// the line of s that has fields. The CLI writes the session's stderr to its own stderr.
func (c cli) followLogs(ctx context.Context, w want, s session, fields map[string]string) error {
	args, ok := strings.CutPrefix(w.logs, "mftiko ")
	if !ok {
		return fmt.Errorf("%s: logs annotation %q is not a mftiko command", w.what, w.logs)
	}
	out, stderr, code, err := c.run(ctx, "", strings.Fields(args)...)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("%s: %s: exit %d: %s", w.what, w.logs, code, stderr)
	}
	want := map[string]string{"session_id": s.id}
	for k, v := range fields {
		want[k] = v
	}
	if !hasLine(out+"\n"+stderr, want) {
		return fmt.Errorf("%s: %s prints no line with %v", w.what, w.logs, want)
	}
	say("%s: %s prints the line", s.rule, w.logs)
	return nil
}

// hasLine reports whether out has a JSON line whose string fields include fields.
func hasLine(out string, fields map[string]string) bool {
	for line := range strings.SplitSeq(out, "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		match := true
		for k, v := range fields {
			if s, _ := m[k].(string); s != v {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// pack pushes conform to the dev registry and returns its digest.
func pack(ctx context.Context, bin, strategy string) (string, error) {
	cmd := exec.CommandContext(ctx, bin, "-bin", strategy, "-name", "conform", "-repo", pushRepo, "-tag", packTag, "-plain-http")
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("mftiko-pack: %w: %s", err, strings.TrimSpace(se.String()))
	}
	digest := strings.TrimSpace(so.String())
	if !strings.HasPrefix(digest, "sha256:") || strings.ContainsAny(digest, " \n") {
		return "", fmt.Errorf("mftiko-pack printed %q, want one digest", so.String())
	}
	return digest, nil
}

// compose runs docker compose on the dev file, with DEV_UID and DEV_GID set as the Makefile sets them.
type compose struct{ file string }

func (c compose) run(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", c.file}, args...)...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("DEV_UID=%d", os.Getuid()), fmt.Sprintf("DEV_GID=%d", os.Getgid()))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return nil
}
