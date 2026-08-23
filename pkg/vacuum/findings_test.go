package vacuum

import (
	"testing"

	"github.com/boringsql/dryrun/pkg/lint"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// The adapter's whole job is to collapse vacuum's four-level scale onto lint's
// three: high is loud enough to be an error, medium is a warning, and anything
// quieter (low/info) is just info. Policy takes over loudness from there.
func TestToFindings_SeverityMapping(t *testing.T) {
	health := []VacuumHealth{{
		Schema: "public", Table: "orders",
		Findings: []VacuumFinding{
			{Code: CodeAutovacuumDisabled, Severity: SeverityHigh, Message: "off"},
			{Code: CodeHighDeadTupleRatio, Severity: SeverityMedium, Message: "dead"},
			{Code: CodeVacuumThresholdHigh, Severity: SeverityLow, Message: "low"},
			{Code: CodeFreezeAgeHigh, Severity: SeverityInfo, Message: "info"},
		},
	}}

	findings := ToFindings(health)
	if len(findings) != 4 {
		t.Fatalf("expected 4 findings, got %d: %+v", len(findings), findings)
	}

	want := []lint.Severity{lint.SeverityError, lint.SeverityWarning, lint.SeverityInfo, lint.SeverityInfo}
	for i, w := range want {
		if findings[i].Severity != w {
			t.Errorf("finding %d: severity = %q, want %q", i, findings[i].Severity, w)
		}
	}
}

// Each vacuum concern code maps to a stable vacuum/* check id so policy can
// gate them independently. The id is what lint_schema and the policy file key
// off, not the internal VacuumCode.
func TestToFindings_CodeToRuleID(t *testing.T) {
	cases := map[VacuumCode]string{
		CodeAutovacuumDisabled:     "vacuum/autovacuum_disabled",
		CodeDefaultKnobsLargeTable: "vacuum/large_table_defaults",
		CodeHighDeadTupleRatio:     "vacuum/high_dead_tuples",
		CodeVacuumThresholdHigh:    "vacuum/threshold_too_high",
		CodeFreezeAgeHigh:          "vacuum/freeze_age_high",
		CodeMxidAgeHigh:            "vacuum/mxid_age_high",
	}
	for code, wantRule := range cases {
		health := []VacuumHealth{{
			Schema: "public", Table: "t",
			Findings: []VacuumFinding{{Code: code, Severity: SeverityMedium, Message: "x"}},
		}}
		got := ToFindings(health)
		if len(got) != 1 {
			t.Fatalf("code %q: expected 1 finding, got %d", code, len(got))
		}
		if got[0].Rule != wantRule {
			t.Errorf("code %q: rule = %q, want %q", code, got[0].Rule, wantRule)
		}
		if got[0].Tables[0] != "public.t" {
			t.Errorf("code %q: table = %q, want public.t", code, got[0].Tables[0])
		}
		if got[0].Message != "x" {
			t.Errorf("code %q: message = %q, want x", code, got[0].Message)
		}
	}
}

// A code with no entry in codeRule is dropped rather than adapted with an empty
// rule. high_bloat used to be the standing example -- detect kind=bloated_tables
// owns table bloat, and it was deleted outright rather than left un-gated here.
func TestToFindings_DropsUnmappedCode(t *testing.T) {
	health := []VacuumHealth{{
		Schema: "public", Table: "orders",
		Findings: []VacuumFinding{
			{Code: VacuumCode("not_a_real_code"), Severity: SeverityHigh, Message: "x"},
			{Code: CodeAutovacuumDisabled, Severity: SeverityHigh, Message: "off"},
		},
	}}

	findings := ToFindings(health)
	if len(findings) != 1 {
		t.Fatalf("expected the unmapped code dropped, got %d findings: %+v", len(findings), findings)
	}
	if findings[0].Rule != "vacuum/autovacuum_disabled" {
		t.Errorf("expected only the autovacuum_disabled finding, got %q", findings[0].Rule)
	}
}

// A health row carrying no concerns produces nothing, and an unmapped code is
// skipped rather than emitted with a blank rule id.
func TestToFindings_EmptyAndUnknown(t *testing.T) {
	if got := ToFindings(nil); got != nil {
		t.Errorf("nil health should yield nil, got %+v", got)
	}
	clean := []VacuumHealth{{Schema: "public", Table: "t"}}
	if got := ToFindings(clean); got != nil {
		t.Errorf("a row with no concerns should yield nil, got %+v", got)
	}
	unknown := []VacuumHealth{{
		Schema: "public", Table: "t",
		Findings: []VacuumFinding{{Code: VacuumCode("vacuum/made_up"), Severity: SeverityHigh, Message: "x"}},
	}}
	if got := ToFindings(unknown); got != nil {
		t.Errorf("an unmapped code should be skipped, got %+v", got)
	}
}

// Findings() is the one-step entry point the audit lint path uses: analyze the
// schema, then adapt. A table with autovacuum switched off must come out as a
// vacuum/autovacuum_disabled error.
func TestFindings_EndToEnd(t *testing.T) {
	a := vacuumFixture("disabled_av", 50_000, 10_000, []string{"autovacuum_enabled=false"})

	findings := Findings(a)
	var found *lint.Finding
	for i := range findings {
		if findings[i].Rule == "vacuum/autovacuum_disabled" {
			found = &findings[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("expected a vacuum/autovacuum_disabled finding, got %+v", findings)
	}
	if found.Severity != lint.SeverityError {
		t.Errorf("autovacuum-off should be an error, got %q", found.Severity)
	}
	if found.Tables[0] != "public.disabled_av" {
		t.Errorf("table = %q, want public.disabled_av", found.Tables[0])
	}
}

// Without planner sizing (a DDL-only snapshot) AnalyzeVacuumHealth finds no
// tables worth judging, so the adapter must stay silent rather than guess.
func TestFindings_NoPlannerYieldsNothing(t *testing.T) {
	a := &snapshot.AnnotatedSchema{
		Schema: &snapshot.SchemaSnapshot{Tables: []snapshot.Table{{Schema: "public", Name: "t"}}},
	}
	if got := Findings(a); got != nil {
		t.Errorf("expected no vacuum findings without planner stats, got %+v", got)
	}
}
