package audit

import (
	"github.com/boringsql/dryrun/pkg/lint"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// RunRulesAnnotated also runs the stats-dependent rules (bloat) when planner sizing is present.
func RunRulesAnnotated(a *snapshot.AnnotatedSchema, config *Config) []lint.Finding {
	return runAllRules(a, config)
}
