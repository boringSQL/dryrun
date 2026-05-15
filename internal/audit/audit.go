package audit

import (
	"github.com/boringsql/dryrun/internal/lint"
	"github.com/boringsql/dryrun/internal/schema"
)

func RunRules(snap *schema.SchemaSnapshot, config *Config) []lint.Finding {
	return runAllRules(snap, config)
}
