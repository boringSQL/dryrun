package audit

import (
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/bloat"
)

// A users table with 100k rows whose heap and pkey occupy far more pages than
// the analytical model expects — both should trip the default 4x threshold,
// while a non-bloated index and a non-btree (gin) index stay quiet.
func bloatedAnnotated() *schema.AnnotatedSchema {
	sch := &schema.SchemaSnapshot{Tables: []schema.Table{{
		Schema: "public", Name: "users",
		Columns: []schema.Column{
			{Name: "id", TypeName: "integer"},
			{Name: "email", TypeName: "text"},
			{Name: "doc", TypeName: "jsonb"},
		},
		Indexes: []schema.Index{
			{Name: "users_pkey", Columns: []string{"id"}, IndexType: "btree"},
			{Name: "users_email_idx", Columns: []string{"email"}, IndexType: "btree"},
			{Name: "users_doc_gin", Columns: []string{"doc"}, IndexType: "gin"},
		},
	}}}
	qual := schema.QualifiedName{Schema: "public", Name: "users"}
	planner := &schema.PlannerStatsSnapshot{
		Tables: []schema.TableSizingEntry{
			// expected ~1.5k pages for 100k rows; 10000 is ~6x bloated
			{Table: qual, Sizing: schema.TableSizing{Relpages: 10000, Reltuples: 100000}},
		},
		Indexes: []schema.IndexSizingEntry{
			// expected ~163 pages for an int pkey; 1000 is ~6.1x bloated
			{Table: qual, Index: "users_pkey", Sizing: schema.IndexSizing{Relpages: 1000, Reltuples: 100000}},
			// healthy: close to the expected page count
			{Table: qual, Index: "users_email_idx", Sizing: schema.IndexSizing{Relpages: 200, Reltuples: 100000}},
			// gin has no analytical model, never sized
			{Table: qual, Index: "users_doc_gin", Sizing: schema.IndexSizing{Relpages: 5000, Reltuples: 100000}},
		},
	}
	// Annotate fills planner.Tables[i].Bloat (the table rule reads it)
	bloat.Annotate(planner, sch)
	return &schema.AnnotatedSchema{Schema: sch, Planner: planner}
}

func TestCheckBloatedIndexes_FlagsBtreeOnly(t *testing.T) {
	cfg := DefaultConfig()
	findings := checkBloatedIndexes(bloatedAnnotated(), &cfg)

	if len(findings) != 1 {
		t.Fatalf("expected exactly 1 bloated-index finding (pkey), got %d: %+v", len(findings), findings)
	}
	f := findings[0]
	if f.Rule != "indexes/bloated" {
		t.Errorf("rule = %q, want indexes/bloated", f.Rule)
	}
	if !strings.Contains(f.Message, "users_pkey") {
		t.Errorf("expected the pkey to be named in the message, got %q", f.Message)
	}
	if f.Tables[0] != "public.users" {
		t.Errorf("table = %q, want public.users", f.Tables[0])
	}
	if !strings.Contains(f.Recommendation, "REINDEX") {
		t.Errorf("expected a REINDEX recommendation, got %q", f.Recommendation)
	}
}

func TestCheckBloatedTables_FlagsHeapBloat(t *testing.T) {
	cfg := DefaultConfig()
	findings := checkBloatedTables(bloatedAnnotated(), &cfg)

	if len(findings) != 1 {
		t.Fatalf("expected exactly 1 bloated-table finding, got %d: %+v", len(findings), findings)
	}
	f := findings[0]
	if f.Rule != "tables/bloated" {
		t.Errorf("rule = %q, want tables/bloated", f.Rule)
	}
	if f.Tables[0] != "public.users" {
		t.Errorf("table = %q, want public.users", f.Tables[0])
	}
	if !strings.Contains(f.Recommendation, "VACUUM FULL") {
		t.Errorf("expected a VACUUM FULL recommendation, got %q", f.Recommendation)
	}
}

// The estimate for a partial or expression index is unreliable without
// pgstattuple; the finding must say so out loud.
func TestCheckBloatedIndexes_MarksExpressionApproximate(t *testing.T) {
	sch := &schema.SchemaSnapshot{Tables: []schema.Table{{
		Schema: "public", Name: "t",
		Columns: []schema.Column{{Name: "email", TypeName: "text"}},
		Indexes: []schema.Index{
			{Name: "lower_email", Columns: []string{"email"}, IndexType: "btree", HasExpressions: true},
		},
	}}}
	qual := schema.QualifiedName{Schema: "public", Name: "t"}
	planner := &schema.PlannerStatsSnapshot{
		Indexes: []schema.IndexSizingEntry{
			{Table: qual, Index: "lower_email", Sizing: schema.IndexSizing{Relpages: 3000, Reltuples: 100000}},
		},
	}
	cfg := DefaultConfig()

	findings := checkBloatedIndexes(&schema.AnnotatedSchema{Schema: sch, Planner: planner}, &cfg)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if !strings.Contains(findings[0].Message, "approximate") {
		t.Errorf("expected the expression index to be flagged approximate, got %q", findings[0].Message)
	}
}

// Without planner sizing (a DDL-only snapshot loaded from disk) the bloat rules
// must stay silent rather than guess — this is the offline lint path.
func TestBloatRules_NilPlannerYieldsNothing(t *testing.T) {
	cfg := DefaultConfig()
	ddlOnly := &schema.AnnotatedSchema{Schema: bloatedAnnotated().Schema}

	if got := checkBloatedIndexes(ddlOnly, &cfg); got != nil {
		t.Errorf("expected no index findings without planner stats, got %+v", got)
	}
	if got := checkBloatedTables(ddlOnly, &cfg); got != nil {
		t.Errorf("expected no table findings without planner stats, got %+v", got)
	}

	// the plain DDL-only RunRules entry point must also produce no bloat findings
	for _, f := range RunRules(bloatedAnnotated().Schema, &cfg) {
		if f.Rule == "indexes/bloated" || f.Rule == "tables/bloated" {
			t.Errorf("RunRules (DDL-only) leaked a bloat finding: %+v", f)
		}
	}
}

func TestRunRulesAnnotated_EmitsBloatFindings(t *testing.T) {
	cfg := DefaultConfig()
	findings := RunRulesAnnotated(bloatedAnnotated(), &cfg)

	var sawIndex, sawTable bool
	for _, f := range findings {
		switch f.Rule {
		case "indexes/bloated":
			sawIndex = true
		case "tables/bloated":
			sawTable = true
		}
	}
	if !sawIndex || !sawTable {
		t.Errorf("expected both bloat rules to fire via RunRulesAnnotated; index=%v table=%v", sawIndex, sawTable)
	}
}

// Policy gates rules by id: disabling tables/bloated suppresses it while the
// index rule keeps firing (Layer 3 enable/disable).
func TestRunRulesAnnotated_RespectsDisabledRules(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DisabledRules = []string{"tables/bloated"}
	findings := RunRulesAnnotated(bloatedAnnotated(), &cfg)

	for _, f := range findings {
		if f.Rule == "tables/bloated" {
			t.Errorf("tables/bloated should be suppressed when disabled, got %+v", f)
		}
	}
	var sawIndex bool
	for _, f := range findings {
		if f.Rule == "indexes/bloated" {
			sawIndex = true
		}
	}
	if !sawIndex {
		t.Error("disabling tables/bloated must not affect indexes/bloated")
	}
}
