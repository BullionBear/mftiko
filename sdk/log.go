package sdk

import (
	"runtime/debug"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/pkg/logger"
)

// strategyVersion and strategyCommit are this binary's own release, split into the two fields
// release() chooses between (E15-D020). They are not the platform's buildinfo.
var (
	strategyVersion, strategyCommit = releaseParts()
	base                            = logger.New("strategy", strategyVersion, strategyCommit)
)

// Log is the package-level logger (E15-D019). Use it outside a hook. It carries component=strategy
// and this binary's version and commit, and does not carry session_id. Inside a hook, use Context.Log.
// Logs go to stderr. The level and format come from MFTIKO_LOG_LEVEL and MFTIKO_LOG_FORMAT.
//
// zerolog's level methods have pointer receivers, so keep the returned value:
//
//	log := sdk.Log()
//	log.Info().Msg("outside a hook")
func Log() zerolog.Logger { return base }

func (c *sessCtx) Log() zerolog.Logger {
	return Log().With().Str("session_id", c.p.sessionID).Logger()
}

func (p *plane) logger() zerolog.Logger {
	log := Log()
	if p.sessionID != "" {
		log = log.With().Str("session_id", p.sessionID).Logger()
	}
	return log
}

// release is the module version, or the VCS revision, from the build info.
// The session binary does not import the platform's buildinfo package.
func release() string {
	if strategyVersion != "" {
		return strategyVersion
	}
	return strategyCommit
}

// releaseParts splits the build info the way release() does: version is the module version
// unless it is empty or "(devel)", and commit is vcs.revision when the build recorded one.
func releaseParts() (version, commit string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", ""
	}
	return versionCommit(info)
}

func versionCommit(info *debug.BuildInfo) (version, commit string) {
	if info == nil {
		return "", ""
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && s.Value != "" {
			commit = s.Value
			break
		}
	}
	return version, commit
}
