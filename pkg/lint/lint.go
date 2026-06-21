package lint

import (
	"fmt"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// CLI entry: run rules and wrap in a report
func LintSchema(snap *snapshot.SchemaSnapshot, config *Config) Report {
	configSource := "default (boringsql)"
	if len(config.DisabledRules) > 0 {
		configSource = fmt.Sprintf("custom (%d rules disabled)", len(config.DisabledRules))
	}
	findings := GateMinSeverity(RunRules(snap, config), config.MinSeverity)
	return NewReport(findings, len(snap.Tables), configSource)
}

func GateMinSeverity(findings []Finding, min Severity) []Finding {
	if min == "" {
		return findings
	}

	floor := severityRank(min)
	out := make([]Finding, 0, len(findings))

	for _, f := range findings {
		if severityRank(f.Severity) >= floor {
			out = append(out, f)
		}
	}

	return out
}

func RunRules(snap *snapshot.SchemaSnapshot, config *Config) []Finding {
	return suppressOverlapping(GateDisabled(runAllRules(snap, config), config.DisabledRules))
}

// single disable gate; policy.Apply will replace this
func GateDisabled(findings []Finding, disabled []string) []Finding {
	if len(disabled) == 0 {
		return findings
	}
	skip := make(map[string]bool, len(disabled))
	for _, r := range disabled {
		skip[r] = true
	}
	out := make([]Finding, 0, len(findings))
	for _, f := range findings {
		if !skip[f.Rule] {
			out = append(out, f)
		}
	}
	return out
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
