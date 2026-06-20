package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/pgmustard"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/bloat"
	"github.com/boringsql/dryrun/pkg/lint"
)

// bloatedServer stands up an offline server whose annotated schema carries
// planner sizing for a users table whose heap and pkey are both far larger
// than the analytical model expects. This is the live-stats shape the bloat
// lint rules need; the plain offline harness (setupOfflineTest) loads a
// DDL-only schema and therefore never trips them.
func bloatedServer() *Server {
	snap := &schema.SchemaSnapshot{
		Database: "test",
		Tables: []schema.Table{{
			Schema: "public", Name: "users",
			Columns: []schema.Column{
				{Name: "id", TypeName: "integer"},
				{Name: "email", TypeName: "text"},
				{Name: "doc", TypeName: "jsonb"},
			},
			Indexes: []schema.Index{
				{Name: "users_pkey", Columns: []string{"id"}, IndexType: "btree"},
			},
		}},
	}
	qual := schema.QualifiedName{Schema: "public", Name: "users"}
	planner := &schema.PlannerStatsSnapshot{
		Tables: []schema.TableSizingEntry{
			// ~6x the expected page count for 100k rows
			{Table: qual, Sizing: schema.TableSizing{Relpages: 10000, Reltuples: 100000}},
		},
		Indexes: []schema.IndexSizingEntry{
			// ~6x the expected page count for an int pkey
			{Table: qual, Index: "users_pkey", Sizing: schema.IndexSizing{Relpages: 1000, Reltuples: 100000}},
		},
	}
	bloat.Annotate(planner, snap)

	return &Server{
		annotated:       &schema.AnnotatedSchema{Schema: snap, Planner: planner},
		lintConfig:      lint.DefaultConfig(),
		pgmustardClient: pgmustard.NewClient(""),
	}
}

func lintAuditFindings(t *testing.T, s *Server, args map[string]any) []lint.Finding {
	t.Helper()
	args["scope"] = "audit"
	args["verbosity"] = "full"

	var req mcp.CallToolRequest
	req.Params.Name = "lint_schema"
	req.Params.Arguments = args

	res, err := s.handleLintSchema(context.Background(), req)
	if err != nil {
		t.Fatalf("handleLintSchema: %v", err)
	}
	text, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}

	var payload struct {
		Audit *lint.Report `json:"audit"`
	}
	if err := json.Unmarshal([]byte(text.Text), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, text.Text)
	}
	if payload.Audit == nil {
		t.Fatalf("no audit branch in response: %s", text.Text)
	}
	return payload.Audit.Findings
}

func hasRule(findings []lint.Finding, rule string) bool {
	for _, f := range findings {
		if f.Rule == rule {
			return true
		}
	}
	return false
}

// The whole point of the wiring: bloat must come out of lint_schema, not only
// the detect tools. With planner sizing present, both rules fire.
func TestLintSchema_SurfacesBloat(t *testing.T) {
	findings := lintAuditFindings(t, bloatedServer(), map[string]any{})

	if !hasRule(findings, "tables/bloated") {
		t.Errorf("expected tables/bloated in lint_schema audit output, got %+v", findings)
	}
	if !hasRule(findings, "indexes/bloated") {
		t.Errorf("expected indexes/bloated in lint_schema audit output, got %+v", findings)
	}
}

// A DDL-only schema (the common offline case: a schema.json with no planner
// stats) must not produce bloat findings — the rules can't guess sizing and
// must stay silent rather than emit confidently-wrong warnings.
func TestLintSchema_NoBloatWithoutPlannerStats(t *testing.T) {
	s := NewOfflineServer(bloatedServer().annotated.Schema, lint.DefaultConfig())
	findings := lintAuditFindings(t, s, map[string]any{})

	if hasRule(findings, "tables/bloated") || hasRule(findings, "indexes/bloated") {
		t.Errorf("DDL-only schema must not surface bloat findings, got %+v", findings)
	}
}

// The table/schema filter must reach the bloat rules too: narrowing to a
// schema that holds no tables drops the bloat findings entirely.
func TestLintSchema_BloatRespectsSchemaFilter(t *testing.T) {
	findings := lintAuditFindings(t, bloatedServer(), map[string]any{"schema": "nonexistent"})

	if hasRule(findings, "tables/bloated") || hasRule(findings, "indexes/bloated") {
		t.Errorf("filtering to an empty schema should drop bloat findings, got %+v", findings)
	}
}
