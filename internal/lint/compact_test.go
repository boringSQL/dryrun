package lint

import "testing"

func TestCompactReportGroupsByRule(t *testing.T) {
	findings := []Finding{
		{Rule: "pk/exists", Severity: SeverityError, Tables: []string{"public.a"}, Message: "no PK"},
		{Rule: "pk/exists", Severity: SeverityError, Tables: []string{"public.b"}, Message: "no PK"},
		{Rule: "types/timestamptz", Severity: SeverityWarning, Tables: []string{"public.a"}, Column: new("created_at"), Message: "bad type"},
	}
	report := NewReport(findings, 2, "default")
	compact := CompactReportFromReport(report)

	if len(compact.RuleGroups) != 2 {
		t.Errorf("expected 2 rule groups, got %d", len(compact.RuleGroups))
	}
	if compact.RuleGroups[0].Rule != "pk/exists" {
		t.Errorf("expected first group pk/exists, got %s", compact.RuleGroups[0].Rule)
	}
	if compact.RuleGroups[0].Count != 2 {
		t.Errorf("expected count 2, got %d", compact.RuleGroups[0].Count)
	}
}
