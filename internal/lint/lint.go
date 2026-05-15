package lint

import (
	"fmt"

	"github.com/boringsql/dryrun/internal/schema"
)

// CLI entry: run rules and wrap in a report
func LintSchema(snap *schema.SchemaSnapshot, config *Config) Report {
	configSource := "default (boringsql)"
	if len(config.DisabledRules) > 0 {
		configSource = fmt.Sprintf("custom (%d rules disabled)", len(config.DisabledRules))
	}
	return NewReport(RunRules(snap, config), len(snap.Tables), configSource)
}

func RunRules(snap *schema.SchemaSnapshot, config *Config) []Finding {
	return suppressOverlapping(runAllRules(snap, config))
}

// Drop lower-severity duplicates when multiple rules hit same table+column
func suppressOverlapping(findings []Finding) []Finding {
	type key struct {
		table, column, category string
	}
	// highest severity per location+category
	best := make(map[key]Severity)
	for _, f := range findings {
		col := ""
		if f.Column != nil {
			col = *f.Column
		}
		cat := f.Rule
		if i := len(f.Rule); i > 0 {
			for j, c := range f.Rule {
				if c == '/' {
					cat = f.Rule[:j]
					break
				}
			}
		}
		table := ""
		if len(f.Tables) > 0 {
			table = f.Tables[0]
		}
		k := key{table, col, cat}
		if existing, ok := best[k]; !ok || severityRank(f.Severity) > severityRank(existing) {
			best[k] = f.Severity
		}
	}

	// keep only top-severity for each group
	var result []Finding
	seen := make(map[string]bool)
	for _, f := range findings {
		col := ""
		if f.Column != nil {
			col = *f.Column
		}
		cat := f.Rule
		for j, c := range f.Rule {
			if c == '/' {
				cat = f.Rule[:j]
				break
			}
		}
		table := ""
		if len(f.Tables) > 0 {
			table = f.Tables[0]
		}
		k := key{table, col, cat}
		dedup := table + "|" + col + "|" + f.Rule
		if !seen[dedup] {
			seen[dedup] = true
			if best[k] == f.Severity {
				result = append(result, f)
			}
		}
	}
	return result
}

func severityRank(s Severity) int {
	switch s {
	case SeverityError:
		return 3
	case SeverityWarning:
		return 2
	case SeverityInfo:
		return 1
	default:
		return 0
	}
}
