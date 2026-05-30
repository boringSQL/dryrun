package buildinfo

import (
	"regexp"
	"runtime/debug"
)

// set via ldflags: -X github.com/boringsql/dryrun/internal/buildinfo.Version=v0.9.0
var Version string

// pseudo-version forms v0.0.0-<ts>-<sha> and vX.Y.Z-0.<ts>-<sha>
var pseudoVersion = regexp.MustCompile(`-(0\.)?\d{14}-[0-9a-f]{12}(\+incompatible)?$`)

func Get() string {
	if Version != "" {
		return Version
	}
	// go install records the module version; pseudo-versions and "(devel)" are dev builds
	if info, ok := debug.ReadBuildInfo(); ok {
		v := info.Main.Version
		if v != "" && v != "(devel)" && !pseudoVersion.MatchString(v) {
			return v
		}
	}
	return "dev"
}
