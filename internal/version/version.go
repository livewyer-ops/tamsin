package version

import (
	"fmt"
	"runtime/debug"
	"strings"
)

var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

func String() string {
	commit := SourceCommit()
	if commit == "unknown" {
		return Version
	}
	if date := BuildDate(); date != "" {
		return fmt.Sprintf("%s (%s, %s)", Version, commit, date)
	}
	return fmt.Sprintf("%s (%s)", Version, commit)
}

// SourceCommit returns source/build provenance rather than claiming to be a
// digest of the executable. Release builds inject Commit; ordinary `go build`
// binaries fall back to Go's VCS stamping so development results are still
// distinguishable whenever the toolchain recorded a revision.
func SourceCommit() string {
	if value := strings.TrimSpace(Commit); value != "" && value != "unknown" {
		return value
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		revision := ""
		modified := false
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = strings.TrimSpace(setting.Value)
			case "vcs.modified":
				modified = setting.Value == "true"
			}
		}
		if revision != "" {
			if modified {
				return revision + "+dirty"
			}
			return revision
		}
	}
	return "unknown"
}

// BuildDate returns an available build timestamp. It is optional result
// provenance because some reproducible and test builds deliberately omit it.
func BuildDate() string {
	if value := strings.TrimSpace(Date); value != "" && value != "unknown" {
		return value
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.time" && strings.TrimSpace(setting.Value) != "" {
				return setting.Value
			}
		}
	}
	return ""
}
