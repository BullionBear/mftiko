package cli

import (
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

// APIVersion is the info.version of contract/openapi/api/v1.yaml this CLI was
// built against (E11-D036, E10-D116). TestAPIVersionMatchesContract keeps the
// two equal.
const APIVersion = "1.0.1"

// build is the CLI's identity. The Makefile and the release stamp buildinfo
// with -ldflags. A go install build is not stamped, so the module version and
// the VCS revision from debug.ReadBuildInfo stand in (E11-D036).
type build struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Built   string `json:"built"`
	API     string `json:"api_version"`
}

func cliBuild() build {
	return buildFrom(buildinfo.Version, buildinfo.CommitHash, buildinfo.BuildTime, debug.ReadBuildInfo)
}

func buildFrom(version, commit, built string, read func() (*debug.BuildInfo, bool)) build {
	b := build{Version: version, Commit: commit, Built: built, API: APIVersion}
	if version != "dev" {
		return b
	}
	info, ok := read()
	if !ok {
		return b
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		b.Version = v
	}
	if commit == "unknown" {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 7 {
				b.Commit = s.Value[:7]
			}
		}
	}
	return b
}

// versionText is what mftiko version and -version print.
func versionText(b build) string {
	return fmt.Sprintf("mftiko %s (commit %s, built %s)\napi.v1 %s\n", b.Version, b.Commit, b.Built, b.API)
}

func runVersion(a *App) int {
	if len(a.Args) != 0 {
		return a.exit("version takes no arguments")
	}
	if code := a.rejectPageFlags(); code != 0 {
		return code
	}
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	b := cliBuild()
	if !a.JSON {
		fmt.Fprint(a.Stdout, versionText(b))
		return ExitOK
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return a.exit(err.Error())
	}
	fmt.Fprintf(a.Stdout, "%s\n", raw)
	return ExitOK
}
