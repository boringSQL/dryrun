package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// Smoke tests for the schema-family tools (list_tables, describe_table,
// search_schema, describe_table detail=relations). Each subtest exercises one tool against the
// offline demo snapshot and asserts the expected substrings appear in the
// rendered text/JSON output.
func TestSchemaHandlers_OfflineSmoke(t *testing.T) {
	c := setupOfflineTest(t)

	t.Run("list_tables", func(t *testing.T) {
		out := callTool(t, c, "list_tables", nil)
		assertContains(t, out, "PostgreSQL 18.3.0")
		assertContains(t, out, "users")
		assertContains(t, out, "tasks")
	})

	t.Run("describe_table", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "users"})
		assertContains(t, out, "pg_version")
		assertContains(t, out, "email")
		assertContains(t, out, "user_id")
	})

	t.Run("search_schema", func(t *testing.T) {
		out := callTool(t, c, "search_schema", map[string]any{"query": "email"})
		assertContains(t, out, "email")
	})

	t.Run("relations", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "users", "detail": "relations"})
		assertContains(t, out, `"table": "public.organizations"`)
		assertContains(t, out, `"join": "JOIN public.organizations ON public.organizations.organization_id = public.users.organization_id"`)
	})
}

// fields whitelist drops all sections except the listed ones (plus identity
// keys schema/name and the always-injected _meta).
func TestDescribeTable_FieldsWhitelist(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "describe_table", map[string]any{
		"table":  "users",
		"fields": []any{"indexes"},
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}

	if _, ok := payload["indexes"]; !ok {
		t.Errorf("expected 'indexes' key, got: %v", keys(payload))
	}
	if _, ok := payload["_meta"]; !ok {
		t.Errorf("expected '_meta' key, got: %v", keys(payload))
	}
	for _, banned := range []string{"columns", "constraints", "stats", "policies"} {
		if _, ok := payload[banned]; ok {
			t.Errorf("expected %q to be filtered out, got: %v", banned, keys(payload))
		}
	}
}

func TestDescribeTable_UnknownFieldErrors(t *testing.T) {
	c := setupOfflineTest(t)
	var req mcp.CallToolRequest
	req.Params.Name = "describe_table"
	req.Params.Arguments = map[string]any{
		"table":  "users",
		"fields": []any{"bogus"},
	}
	result, err := c.CallTool(context.Background(), req)
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected IsError, got success: %+v", result)
	}
	text := result.Content[0].(mcp.TextContent).Text
	if !strings.Contains(text, "unknown field 'bogus'") {
		t.Errorf("expected error text to mention unknown field, got: %s", text)
	}
}

// Default (no fields) preserves the flattened shape — columns and indexes
// both present at the top level.
func TestDescribeTable_DefaultShape(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "describe_table", map[string]any{"table": "users"})

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	for _, want := range []string{"schema", "name", "columns", "indexes", "_meta"} {
		if _, ok := payload[want]; !ok {
			t.Errorf("expected top-level %q in default shape, got: %v", want, keys(payload))
		}
	}
}

// Tables with FKs surface a _meta.next pointing at this tool's own relations
// mode, so chaining clients can follow up without parsing the prose hint.
// Tables without FKs must not surface _meta.next.
func TestDescribeTable_FKSuggestsRelations(t *testing.T) {
	c := setupOfflineTest(t)

	t.Run("with_fk", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "users"})
		var payload map[string]any
		if err := json.Unmarshal([]byte(out), &payload); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, out)
		}
		meta, _ := payload["_meta"].(map[string]any)
		next, ok := meta["next"].([]any)
		if !ok || len(next) == 0 {
			t.Fatalf("expected _meta.next with one entry, got: %v", meta)
		}
		first, _ := next[0].(map[string]any)
		if first["tool"] != "describe_table" {
			t.Errorf("expected tool=describe_table, got %v", first["tool"])
		}
		args, _ := first["args"].(map[string]any)
		if args["table"] != "users" || args["schema"] != "public" || args["detail"] != "relations" {
			t.Errorf("expected args{table:users, schema:public, detail:relations}, got %v", args)
		}
	})

	t.Run("without_fk", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "organizations"})
		var payload map[string]any
		if err := json.Unmarshal([]byte(out), &payload); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, out)
		}
		meta, _ := payload["_meta"].(map[string]any)
		if _, present := meta["next"]; present {
			t.Errorf("expected no _meta.next for table without FKs, got: %v", meta)
		}
	})
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The DDL is what a person shares, diffs, and pastes into a migration, so it is
// asked for by name rather than riding every describe_table.
func TestDescribeTableDDL(t *testing.T) {
	c := setupOfflineTest(t)

	t.Run("absent unless asked for", func(t *testing.T) {
		for _, args := range []map[string]any{
			{"table": "users"},
			{"table": "users", "detail": "full"},
			{"table": "users", "fields": []string{"columns"}},
		} {
			if strings.Contains(callTool(t, c, "describe_table", args), `"ddl"`) {
				t.Errorf("ddl returned unasked for %v", args)
			}
		}
	})

	t.Run("returned when asked for", func(t *testing.T) {
		var decoded map[string]any
		out := callTool(t, c, "describe_table", map[string]any{"table": "users", "fields": []string{"ddl"}})
		if err := json.Unmarshal([]byte(out), &decoded); err != nil {
			t.Fatal(err)
		}
		sql, _ := decoded["ddl"].(string)
		for _, want := range []string{
			"CREATE TABLE public.users (",
			"user_id bigint GENERATED ALWAYS AS IDENTITY NOT NULL",
			"CONSTRAINT users_pkey PRIMARY KEY (user_id)",
			"ALTER TABLE public.users ADD CONSTRAINT users_organization_id_fkey FOREIGN KEY",
		} {
			if !strings.Contains(sql, want) {
				t.Errorf("missing %q in:\n%s", want, sql)
			}
		}
		if _, ok := decoded["ddl_omitted"].([]any); !ok {
			t.Errorf("ddl without the list of what it cannot reproduce: %v", decoded["ddl_omitted"])
		}
	})

	// a capped CREATE TABLE would not create the table
	t.Run("not capped", func(t *testing.T) {
		var decoded map[string]any
		out := callTool(t, c, "describe_table", map[string]any{
			"table": "users", "fields": []string{"ddl"}, "limit": float64(1),
		})
		if err := json.Unmarshal([]byte(out), &decoded); err != nil {
			t.Fatal(err)
		}
		sql, _ := decoded["ddl"].(string)
		for _, want := range []string{"user_id", "email", "name", "organization_id", "created_at"} {
			if !strings.Contains(sql, want) {
				t.Errorf("limit=1 dropped %q from the ddl:\n%s", want, sql)
			}
		}
	})
}

// The other sections were asked for too; a table the renderer cannot build is
// no reason to withhold them.
func TestDescribeTableDDLErrorKeepsTheRest(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "test", Timestamp: time.Now().UTC(),
		Tables: []schema.Table{{
			Schema: "public", Name: "broken",
			// no type, so there is no CREATE TABLE to render
			Columns: []schema.Column{{Name: "a", Ordinal: 1}},
		}},
	}
	c := serveOffline(t, NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: snap}, lint.DefaultConfig()))
	out := callTool(t, c, "describe_table", map[string]any{
		"table": "broken", "fields": []string{"columns", "ddl"},
	})

	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["columns"]; !ok {
		t.Errorf("the columns section was withheld because the ddl failed:\n%s", out)
	}
	msg, _ := decoded["ddl_error"].(string)
	if !strings.Contains(msg, "no type") {
		t.Errorf("ddl_error should say what went wrong, got %q", msg)
	}
	if _, ok := decoded["ddl"]; ok {
		t.Errorf("ddl returned alongside an error:\n%s", out)
	}
}

// detect reports vacuum offenders only, so describe_table is the only place a
// finding-free table's vacuum picture can be read. Uses the planner-bearing
// fixture: the demo snapshot has no sizing, so SizingFor is nil and the
// analyzer produces nothing for any table.
func TestDescribeTable_CarriesVacuumForAHealthyTable(t *testing.T) {
	c := setupVacuumOfflineTest(t)

	for _, tc := range []struct {
		name string
		args map[string]any
		want bool
	}{
		{"detail=stats", map[string]any{"table": "healthy", "detail": "stats"}, true},
		{"fields=vacuum", map[string]any{"table": "healthy", "fields": []string{"vacuum"}}, true},
		{"detail=full", map[string]any{"table": "healthy", "detail": "full"}, true},
		// the default response is already the widest in the product
		{"summary default stays lean", map[string]any{"table": "healthy"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var payload map[string]any
			out := callTool(t, c, "describe_table", tc.args)
			if err := json.Unmarshal([]byte(out), &payload); err != nil {
				t.Fatalf("not JSON: %v\n%s", err, out)
			}
			vac, has := payload["vacuum"].(map[string]any)
			if has != tc.want {
				t.Fatalf("vacuum present=%v, want %v: %s", has, tc.want, out)
			}
			if !tc.want {
				return
			}
			// "healthy" carries no finding -- that is the whole point: detect
			// would not show this table at all
			if fs, _ := vac["findings"].([]any); len(fs) != 0 {
				t.Errorf("fixture table should have no finding, got %v", fs)
			}
			for _, k := range []string{"dead_tuples", "vacuum_trigger_at"} {
				if _, ok := vac[k]; !ok {
					t.Errorf("vacuum section missing %q: %v", k, vac)
				}
			}
		})
	}
}

// detail=relations answers the FK question without dragging anything else
// along -- that is why it is a detail level and not just a fields entry. Pins
// the WHOLE key set, not a few absences: public.events is partitioned, so a
// leak of partition_summary/node_breakdown/column_profiles shows up here.
func TestDescribeTable_RelationsCarriesNothingElse(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(
		&schema.AnnotatedSchema{Schema: relatedSnap()}, lint.DefaultConfig()))

	for _, table := range []string{"users", "events"} {
		t.Run(table, func(t *testing.T) {
			var payload map[string]any
			out := callTool(t, c, "describe_table", map[string]any{"table": table, "detail": "relations"})
			if err := json.Unmarshal([]byte(out), &payload); err != nil {
				t.Fatalf("not JSON: %v\n%s", err, out)
			}
			want := map[string]bool{"schema": true, "name": true, "relations": true, "_meta": true}
			for k := range payload {
				if !want[k] {
					t.Errorf("detail=relations must not carry %q: %s", k, out)
				}
			}
			if _, has := payload["relations"]; !has {
				t.Errorf("expected a relations section: %s", out)
			}
		})
	}
}

// The fields whitelist deletes sections after they are built. If relations is
// filtered out, its delete-cascade hint and follow-up calls must go with it --
// otherwise the response advertises a section it does not contain.
func TestDescribeTable_RelationsFilteredOutTakesItsHintsAlong(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(
		&schema.AnnotatedSchema{Schema: relatedSnap()}, lint.DefaultConfig()))

	var payload map[string]any
	out := callTool(t, c, "describe_table", map[string]any{
		"table": "users", "detail": "relations", "fields": []string{"columns"},
	})
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if _, has := payload["relations"]; has {
		t.Fatal("fields=[columns] must drop the relations section")
	}
	meta, _ := payload["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if strings.Contains(hint, "deletes matching rows") || strings.Contains(hint, "clears the referencing column") {
		t.Errorf("cascade prose survived its section: %q", hint)
	}
	// one suggestion to go get relations is right -- the table has FKs and this
	// response does not show them. What must not survive is the cascade
	// follow-up, which chases a *different* table on behalf of a section that
	// was never returned.
	next, _ := meta["next"].([]any)
	if len(next) != 1 {
		t.Fatalf("want exactly one follow-up, got %v", next)
	}
	call, _ := next[0].(map[string]any)
	args, _ := call["args"].(map[string]any)
	if args["table"] != "users" {
		t.Errorf("the surviving follow-up should point back at this table, not a cascade target: %v", call)
	}
}

// A table with no indexes and no constraints is ordinary (a log table with no
// primary key). Those fields carry no omitempty, so nil marshals as null while
// describeTableOutputSchema requires arrays, and the tool returns a validation
// error instead of the table. It bites hardest on the hosted endpoint, where
// WithOutputSchemaValidation replaces the whole result -- _meta and the
// snapshot identity go with it.
func TestDescribeTable_TableWithNoIndexesOrConstraints(t *testing.T) {
	snap := &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "test",
		Timestamp: time.Now().UTC(), ContentHash: "test",
		Tables: []schema.Table{{
			Schema: "public", Name: "audit_log",
			Columns: []schema.Column{col("id")},
			// Indexes and Constraints deliberately nil
		}},
	}
	c := serveOffline(t, NewOfflineServerAnnotated(
		&schema.AnnotatedSchema{Schema: snap}, lint.DefaultConfig()))

	// every detail level, because they build the payload three different ways:
	// summary through toCompactTable, full by marshalling the raw Table, stats
	// and relations field by field. Pinning only one is how the summary path --
	// the default, and the one the hosted endpoint serves most -- stayed broken
	// after the full path was fixed.
	for _, detail := range []string{"summary", "full", "stats", "relations"} {
		t.Run(detail, func(t *testing.T) {
			args := map[string]any{"table": "audit_log"}
			if detail != "summary" {
				args["detail"] = detail
			}
			out := callTool(t, c, "describe_table", args)
			if strings.Contains(out, "output schema validation failed") {
				t.Fatalf("detail=%s must not fail validation on nil arrays: %s", detail, out)
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(out), &payload); err != nil {
				t.Fatalf("not JSON: %v\n%s", err, out)
			}
			// only the levels that carry the sections at all
			if detail != "summary" && detail != "full" {
				return
			}
			for _, k := range []string{"columns", "indexes", "constraints"} {
				if v, ok := payload[k]; !ok || v == nil {
					t.Errorf("%q must be an empty array, not null/absent: %v", k, v)
				}
			}
		})
	}
}
