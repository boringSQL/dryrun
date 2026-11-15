package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

func dependentsSnap() *schema.SchemaSnapshot {
	users := testTable("public", "users", testCol("email", "text", true))
	snap := finish(&schema.SchemaSnapshot{
		Tables: []schema.Table{users},
		Views: []schema.View{
			{Schema: "public", Name: "user_emails", Definition: "SELECT email FROM public.users"},
			{Schema: "public", Name: "user_email_mv", IsMaterialized: true, Definition: "SELECT * FROM public.users"},
			{Schema: "public", Name: "email_digest", Definition: "SELECT email FROM public.user_emails"},
			{Schema: "public", Name: "unrelated", Definition: "SELECT id FROM public.accounts"},
		},
	})
	snap.DependenciesHash = "dep-1"
	snap.Dependencies = []schema.Dependency{
		{Deptype: "n", DependentClass: "pg_class", DependentIdentity: "public.user_emails",
			ReferencedClass: "pg_class", ReferencedIdentity: "public.users.email"},
		{Deptype: "n", DependentClass: "pg_class", DependentIdentity: "public.user_email_mv",
			ReferencedClass: "pg_class", ReferencedIdentity: "public.users"},
		{Deptype: "n", DependentClass: "pg_class", DependentIdentity: "public.email_digest",
			ReferencedClass: "pg_class", ReferencedIdentity: "public.user_emails.display_email"},
		{Deptype: "n", DependentClass: "pg_class", DependentIdentity: "public.unrelated",
			ReferencedClass: "pg_class", ReferencedIdentity: "public.accounts.id"},
	}
	return snap
}

func TestDescribeTableDependents(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(
		&schema.AnnotatedSchema{Schema: dependentsSnap()}, lint.DefaultConfig()))

	out := callTool(t, c, "describe_table", map[string]any{
		"table": "users", "fields": []any{"dependents"}, "limit": 0,
	})

	var payload struct {
		Dependents tableDependents `json:"dependents"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	got := payload.Dependents
	if !got.Available || got.Count != 3 || got.Omitted != 0 || len(got.Views) != 3 {
		t.Fatalf("dependents = %+v", got)
	}
	if got.Views[0].Name != "user_email_mv" || got.Views[0].Kind != "materialized_view" ||
		got.Views[0].Depth != 1 || len(got.Views[0].Columns) != 0 {
		t.Errorf("materialized view = %+v", got.Views[0])
	}
	if got.Views[1].Name != "user_emails" || got.Views[1].Kind != "view" || got.Views[1].Depth != 1 ||
		len(got.Views[1].Columns) != 1 || got.Views[1].Columns[0] != "email" {
		t.Errorf("view = %+v", got.Views[1])
	}
	if got.Views[2].Name != "email_digest" || got.Views[2].Depth != 2 || len(got.Views[2].Columns) != 0 ||
		len(got.Views[2].Via) != 1 || got.Views[2].Via[0].Name != "user_emails" ||
		len(got.Views[2].Via[0].Columns) != 1 || got.Views[2].Via[0].Columns[0] != "display_email" {
		t.Errorf("transitive view = %+v", got.Views[2])
	}
	for _, v := range got.Views {
		if v.Name == "unrelated" {
			t.Errorf("unrelated view listed: %+v", got)
		}
	}
}

func TestDescribeTableDependentsCaps(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(
		&schema.AnnotatedSchema{Schema: dependentsSnap()}, lint.DefaultConfig()))

	out := callTool(t, c, "describe_table", map[string]any{
		"table": "users", "fields": []any{"dependents"}, "limit": 1,
	})

	var payload struct {
		Dependents tableDependents `json:"dependents"`
		Meta       map[string]any  `json:"_meta"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if payload.Dependents.Count != 3 || payload.Dependents.Omitted != 2 || len(payload.Dependents.Views) != 1 {
		t.Fatalf("dependents = %+v", payload.Dependents)
	}
	hint, _ := payload.Meta["hint"].(string)
	if !strings.Contains(hint, "Omitted 2 of 3 dependents") {
		t.Errorf("hint = %q", hint)
	}
	next, _ := payload.Meta["next"].([]any)
	if len(next) != 1 {
		t.Fatalf("next = %v, want one dependents call", next)
	}
	call, _ := next[0].(map[string]any)
	args, _ := call["args"].(map[string]any)
	if call["tool"] != "describe_table" || args["limit"] != float64(0) {
		t.Errorf("next = %v", next)
	}
}

func TestDescribeTableDependentsNeedCapture(t *testing.T) {
	snap := finish(&schema.SchemaSnapshot{
		Tables: []schema.Table{testTable("public", "users", testCol("email", "text", true))},
	})
	c := serveOffline(t, NewOfflineServerAnnotated(
		&schema.AnnotatedSchema{Schema: snap}, lint.DefaultConfig()))

	out := callTool(t, c, "describe_table", map[string]any{
		"table": "users", "fields": []any{"dependents"},
	})

	var payload struct {
		Dependents tableDependents `json:"dependents"`
		Meta       map[string]any  `json:"_meta"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if payload.Dependents.Available {
		t.Errorf("dependents claimed available without capture: %+v", payload.Dependents)
	}
	hint, _ := payload.Meta["hint"].(string)
	if !strings.Contains(hint, "predates dependency capture") {
		t.Errorf("hint = %q", hint)
	}
}
