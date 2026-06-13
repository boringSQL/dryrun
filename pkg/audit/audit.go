package audit

import (
	"github.com/boringsql/dryrun/pkg/lint"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

func RunRules(snap *snapshot.SchemaSnapshot, config *Config) []lint.Finding {
	return runAllRules(snap, config)
}
