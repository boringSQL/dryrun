package lint

import "testing"

// Pins the sentinel behavior of CompactReportFromReportN: passing 0 as the
// item cap means "keep all items", not "drop all". Count and Items length
// should both equal the input size.
func TestCompactReportFromReportN_ZeroKeepsAll(t *testing.T) {
	findings := []Finding{
		{Rule: "pk/exists", Severity: SeverityError, Tables: []string{"public.a"}},
		{Rule: "pk/exists", Severity: SeverityError, Tables: []string{"public.b"}},
		{Rule: "pk/exists", Severity: SeverityError, Tables: []string{"public.c"}},
		{Rule: "pk/exists", Severity: SeverityError, Tables: []string{"public.d"}},
	}
	report := NewReport(findings, 4, "test")
	compact := CompactReportFromReportN(report, 0)
	if len(compact.RuleGroups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(compact.RuleGroups))
	}
	g := compact.RuleGroups[0]
	if g.Count != 4 {
		t.Errorf("expected count=4, got %d", g.Count)
	}
	if len(g.Items) != 4 {
		t.Errorf("expected all 4 items, got %d", len(g.Items))
	}
}

// verifies that the item cap truncates the Items slice but Count still reports
// the full untruncated finding total. This is the contract MCP clients rely on
// to know how many findings exist when only a sample is returned.
func TestCompactReportFromReportN_CapsItemsKeepsCount(t *testing.T) {
	var findings []Finding
	for i := 0; i < 10; i++ {
		findings = append(findings, Finding{
			Rule: "pk/exists", Severity: SeverityError, Tables: []string{"public.t"},
		})
	}
	report := NewReport(findings, 10, "test")
	compact := CompactReportFromReportN(report, 3)
	if len(compact.RuleGroups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(compact.RuleGroups))
	}
	g := compact.RuleGroups[0]
	if g.Count != 10 {
		t.Errorf("expected count=10, got %d", g.Count)
	}
	if len(g.Items) != 3 {
		t.Errorf("expected items capped at 3, got %d", len(g.Items))
	}
}

// Pins that rule groups appear in first-seen order from the input findings, not
// sorted alphabetically. Input order z, a, z, m must produce groups z, a, m so
// downstream consumers see findings as the engine emitted them.
func TestCompactReportFromReportN_GroupOrderPreserved(t *testing.T) {
	findings := []Finding{
		{Rule: "z/last", Severity: SeverityError, Tables: []string{"public.a"}},
		{Rule: "a/first", Severity: SeverityError, Tables: []string{"public.b"}},
		{Rule: "z/last", Severity: SeverityError, Tables: []string{"public.c"}},
		{Rule: "m/middle", Severity: SeverityError, Tables: []string{"public.d"}},
	}
	report := NewReport(findings, 4, "test")
	compact := CompactReportFromReportN(report, 5)
	if len(compact.RuleGroups) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(compact.RuleGroups))
	}
	want := []string{"z/last", "a/first", "m/middle"}
	for i, w := range want {
		if compact.RuleGroups[i].Rule != w {
			t.Errorf("group[%d]: expected %s, got %s", i, w, compact.RuleGroups[i].Rule)
		}
	}
}

func TestCompactReportGroupsByRule(t *testing.T) {
	findings := []Finding{
		{Rule: "pk/exists", Severity: SeverityError, Tables: []string{"public.a"}, Message: "no PK"},
		{Rule: "pk/exists", Severity: SeverityError, Tables: []string{"public.b"}, Message: "no PK"},
		{Rule: "types/timestamptz", Severity: SeverityWarning, Tables: []string{"public.a"}, Column: new("created_at"), Message: "bad type"},
	}
	report := NewReport(findings, 2, "default")
	compact := CompactReportFromReportN(report, 0)

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
