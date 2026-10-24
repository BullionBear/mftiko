package cli

import (
	"context"
	"encoding/json"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestAPIVersionMatchesContract: APIVersion is the contract's info.version (E11-D036).
func TestAPIVersionMatchesContract(t *testing.T) {
	raw, err := os.ReadFile("../../contract/openapi/api/v1.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Info struct {
			Version string `yaml:"version"`
		} `yaml:"info"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Info.Version != APIVersion {
		t.Fatalf("v1.yaml info.version %q, APIVersion %q", doc.Info.Version, APIVersion)
	}
}

func TestBuildFrom(t *testing.T) {
	installed := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Main:     debug.Module{Path: "github.com/BullionBear/mftiko", Version: "v0.2.0"},
			Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "12c62e6bc976fbb76fc5561ef95f1dde50fb1687"}},
		}, true
	}
	devel := func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, true
	}
	none := func() (*debug.BuildInfo, bool) { return nil, false }
	for _, c := range []struct {
		name                  string
		version, commit, time string
		read                  func() (*debug.BuildInfo, bool)
		want                  build
	}{
		{"stamped", "v0.3.0", "abc1234", "2026-10-24T00:00:00Z", installed,
			build{"v0.3.0", "abc1234", "2026-10-24T00:00:00Z", APIVersion}},
		{"go install", "dev", "unknown", "unknown", installed,
			build{"v0.2.0", "12c62e6", "unknown", APIVersion}},
		{"devel", "dev", "unknown", "unknown", devel,
			build{"dev", "unknown", "unknown", APIVersion}},
		{"no build info", "dev", "unknown", "unknown", none,
			build{"dev", "unknown", "unknown", APIVersion}},
	} {
		if got := buildFrom(c.version, c.commit, c.time, c.read); got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestVersionCommand(t *testing.T) {
	home := t.TempDir() // no config file: version reads none
	want := versionText(cliBuild())
	if !strings.HasSuffix(want, "\napi.v1 "+APIVersion+"\n") || !strings.HasPrefix(want, "mftiko ") {
		t.Fatalf("version text %q", want)
	}
	for _, args := range [][]string{{"version"}, {"-version"}, {"--version"}} {
		got := runAt(context.Background(), home, nil, strings.NewReader(""), args...)
		if got.code != ExitOK || got.out != want {
			t.Fatalf("%v: %+v", args, got)
		}
	}
	got := runAt(context.Background(), home, nil, strings.NewReader(""), "-o", "json", "version")
	var b build
	if got.code != ExitOK || json.Unmarshal([]byte(got.out), &b) != nil || b != cliBuild() {
		t.Fatalf("json: %+v", got)
	}
	if got := runAt(context.Background(), home, nil, strings.NewReader(""), "version", "x"); got.code != ExitUsage {
		t.Fatalf("argument: %+v", got)
	}
	if got := runAt(context.Background(), home, nil, strings.NewReader(""), "help", "version"); got.code != ExitOK || !strings.Contains(got.out, "usage: mftiko version") {
		t.Fatalf("help: %+v", got)
	}
}
