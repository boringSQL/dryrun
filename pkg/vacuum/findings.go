package vacuum

import (
	"github.com/boringsql/dryrun/pkg/lint"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// map vacuum's scale onto lint's; policy owns loudness from here
func toLintSeverity(s Severity) lint.Severity {
	switch s {
	case SeverityHigh:
		return lint.SeverityError
	case SeverityMedium:
		return lint.SeverityWarning
	default:
		return lint.SeverityInfo
	}
}

// check ids for policy gating; no high_bloat, tables/bloated owns that
var codeRule = map[VacuumCode]string{
	CodeAutovacuumDisabled:     "vacuum/autovacuum_disabled",
	CodeDefaultKnobsLargeTable: "vacuum/large_table_defaults",
	CodeHighDeadTupleRatio:     "vacuum/high_dead_tuples",
	CodeVacuumThresholdHigh:    "vacuum/threshold_too_high",
	CodeFreezeAgeHigh:          "vacuum/freeze_age_high",
	CodeMxidAgeHigh:            "vacuum/mxid_age_high",
}

// ToFindings adapts vacuum concerns into lint findings for the policy pass.
func ToFindings(health []VacuumHealth) []lint.Finding {
	var findings []lint.Finding
	for i := range health {
		vh := &health[i]
		table := snapshot.QualifiedName{Schema: vh.Schema, Name: vh.Table}.String()
		for _, f := range vh.Findings {
			rule, ok := codeRule[f.Code]
			if !ok {
				continue
			}
			findings = append(findings, lint.Finding{
				Rule:     rule,
				Severity: toLintSeverity(f.Severity),
				Tables:   []string{table},
				Message:  f.Message,
			})
		}
	}
	return findings
}

// Findings analyzes and adapts in one step, for the audit lint path.
func Findings(a *snapshot.AnnotatedSchema) []lint.Finding {
	return ToFindings(AnalyzeVacuumHealth(a))
}
