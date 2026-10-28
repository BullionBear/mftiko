package sdk

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/pkg/logger"
)

func TestContextLogFields(t *testing.T) {
	var buf bytes.Buffer
	prev := base
	base = logger.NewWithWriter("strategy", "v1", "abc", &buf).Level(zerolog.InfoLevel)
	t.Cleanup(func() { base = prev })

	c := &sessCtx{p: &plane{sessionID: "c0ffee"}}
	log := c.Log()
	log.Info().Msg("hook")

	line := decodeLog(t, &buf)
	for _, key := range []string{"time", "level", "component", "version", "commit", "session_id", "message"} {
		if _, ok := line[key]; !ok {
			t.Errorf("missing %q in %v", key, line)
		}
	}
	if line["component"] != "strategy" || line["version"] != "v1" || line["commit"] != "abc" ||
		line["session_id"] != "c0ffee" || line["message"] != "hook" {
		t.Fatalf("line %v", line)
	}

	buf.Reset()
	pkg := Log()
	pkg.Info().Msg("pkg")
	line = decodeLog(t, &buf)
	if _, ok := line["session_id"]; ok {
		t.Fatalf("package log has session_id: %v", line)
	}
	if line["component"] != "strategy" || line["message"] != "pkg" {
		t.Fatalf("line %v", line)
	}
}

func TestNoteUsesLogger(t *testing.T) {
	var buf bytes.Buffer
	prev := base
	base = logger.NewWithWriter("strategy", "v1", "abc", &buf).Level(zerolog.InfoLevel)
	t.Cleanup(func() { base = prev })

	var got string
	hooked := &plane{host: host{log: func(s string) { got = s }}, sessionID: "c0ffee"}
	hooked.note("command bindings: 1")
	if got != "command bindings: 1" || buf.Len() != 0 {
		t.Fatalf("hook note %q buf %q", got, buf.String())
	}

	p := &plane{sessionID: "c0ffee"}
	p.note("command bindings: 1")
	if strings.Contains(buf.String(), "mftiko sdk:") {
		t.Fatalf("plain prefix still used: %s", buf.String())
	}
	line := decodeLog(t, &buf)
	if line["message"] != "command bindings: 1" || line["session_id"] != "c0ffee" || line["component"] != "strategy" {
		t.Fatalf("line %v", line)
	}
}

func TestVersionCommit(t *testing.T) {
	version, commit := versionCommit(&debug.BuildInfo{
		Main:     debug.Module{Version: "v1.2.3"},
		Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}},
	})
	if version != "v1.2.3" || commit != "abc" {
		t.Fatalf("version %q commit %q", version, commit)
	}
	if got := chosenRelease(version, commit); got != "v1.2.3" {
		t.Fatalf("release %q", got)
	}

	version, commit = versionCommit(&debug.BuildInfo{
		Main:     debug.Module{Version: "(devel)"},
		Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}},
	})
	if version != "" || commit != "abc" {
		t.Fatalf("devel version %q commit %q", version, commit)
	}
	if got := chosenRelease(version, commit); got != "abc" {
		t.Fatalf("release %q", got)
	}

	version, commit = versionCommit(&debug.BuildInfo{Main: debug.Module{Version: ""}})
	if version != "" || commit != "" || chosenRelease(version, commit) != "" {
		t.Fatalf("empty version %q commit %q", version, commit)
	}
}

func TestVersionCommitNil(t *testing.T) {
	version, commit := versionCommit(nil)
	if version != "" || commit != "" {
		t.Fatalf("version %q commit %q", version, commit)
	}
}

func chosenRelease(version, commit string) string {
	if version != "" {
		return version
	}
	return commit
}

func TestLogLevelFromEnv(t *testing.T) {
	if os.Getenv("MFTIKO_LOG_PROBE") == "1" {
		log := Log()
		log.Debug().Msg("dbg")
		log.Info().Msg("inf")
		log.Warn().Msg("warn")
		return
	}

	warnOut := runLogProbe(t, "warn", "json")
	if strings.Contains(warnOut, `"message":"dbg"`) || strings.Contains(warnOut, `"message":"inf"`) {
		t.Fatalf("warn level kept a lower line: %s", warnOut)
	}
	if !strings.Contains(warnOut, `"message":"warn"`) || !strings.Contains(warnOut, `"component":"strategy"`) {
		t.Fatalf("warn line missing: %s", warnOut)
	}

	debugOut := runLogProbe(t, "debug", "json")
	if !strings.Contains(debugOut, `"message":"dbg"`) || !strings.Contains(debugOut, `"message":"inf"`) {
		t.Fatalf("debug level dropped a line: %s", debugOut)
	}

	consoleOut := runLogProbe(t, "info", "console")
	if strings.Contains(consoleOut, `"message":"inf"`) || strings.HasPrefix(strings.TrimSpace(consoleOut), "{") {
		t.Fatalf("console format was JSON: %s", consoleOut)
	}
	if !strings.Contains(consoleOut, "inf") {
		t.Fatalf("console format dropped the info line: %s", consoleOut)
	}
}

func runLogProbe(t *testing.T, level, format string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLogLevelFromEnv$")
	cmd.Env = append(os.Environ(),
		"MFTIKO_LOG_PROBE=1",
		"MFTIKO_LOG_LEVEL="+level,
		"MFTIKO_LOG_FORMAT="+format,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = io.Discard
	if err := cmd.Run(); err != nil {
		t.Fatalf("probe level %s format %s: %v\n%s", level, format, err, stderr.String())
	}
	return stderr.String()
}

func decodeLog(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &line); err != nil {
		t.Fatalf("not one JSON object: %v: %q", err, buf.String())
	}
	return line
}
