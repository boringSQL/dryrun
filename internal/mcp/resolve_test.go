package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/pkg/lint"
)

// list_tables prints schema.name, so an agent hands that string straight back
// to describe_table. Every case below is one an agent reaches by copying what
// a previous call showed it.
func TestResolveTable(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig()))

	t.Run("qualified_name", func(t *testing.T) {
		assertContains(t, callTool(t, c, "describe_table", map[string]any{"table": "app.orders"}), `"tenant_id"`)
	})

	// the fixture holds both public."foo.bar" and foo.bar, so this pins the
	// literal ahead of the split rather than passing for want of an alternative
	t.Run("name_containing_a_dot_outranks_the_split", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "foo.bar"})
		assertContains(t, out, `"name": "foo.bar"`)
		assertContains(t, out, `"schema": "public"`)
	})

	// and the split still reaches the other one when asked for it
	t.Run("split_reaches_the_table_the_literal_shadows", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "bar", "schema": "foo"})
		assertContains(t, out, `"schema": "foo"`)
	})

	t.Run("bare_name_outside_public", func(t *testing.T) {
		assertContains(t, callTool(t, c, "describe_table", map[string]any{"table": "events"}), `"schema": "app"`)
	})

	// public wins on a collision because it is where an unqualified name
	// resolves; the app copy needs asking for
	t.Run("bare_name_prefers_public", func(t *testing.T) {
		assertContains(t, callTool(t, c, "describe_table", map[string]any{"table": "orders"}), `"customer_id"`)
	})

	t.Run("ambiguous_is_reported_not_guessed", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "invoices"})
		assertContains(t, out, "exists in 2 schemas")
		assertContains(t, out, "archive.invoices, billing.invoices")
	})

	// the literal name is tried first, so by the time the split runs there is
	// nothing left for the schema argument to point at
	t.Run("qualified_name_outranks_schema_argument", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "app.orders", "schema": "public"})
		assertContains(t, out, `"tenant_id"`)
		assertContains(t, out, "outranks schema=public")
	})

	t.Run("miss_carries_somewhere_to_go", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "order"})
		assertContains(t, out, "did you mean app.orders, public.orders")
		assertContains(t, out, `search_schema {"query":"order"}`)
	})

	// a view read out of search_schema is a miss with a specific cause
	t.Run("view_says_why", func(t *testing.T) {
		out := callTool(t, c, "describe_table", map[string]any{"table": "app.order_summary"})
		assertContains(t, out, "app.order_summary is a view")
	})

	// query="" matches every object, so it must not be offered as the way out
	t.Run("empty_table_argument", func(t *testing.T) {
		var req mcp.CallToolRequest
		req.Params.Name = "describe_table"
		req.Params.Arguments = map[string]any{"table": ""}
		srv := NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig())
		res, err := srv.handleDescribeTable(context.Background(), req)
		if err != nil {
			t.Fatalf("handleDescribeTable: %v", err)
		}
		text := res.Content[0].(mcp.TextContent).Text
		if !strings.Contains(text, "table argument is required") {
			t.Errorf("want the required-argument error, got: %s", text)
		}
	})

	t.Run("find_related_resolves_the_same_way", func(t *testing.T) {
		assertContains(t, callTool(t, c, "find_related", map[string]any{"table": "public.foo.bar"}),
			"Relationships for public.foo.bar")
	})
}

// A follow-up call built from the raw argument would hand back the same name
// that needed resolving.
func TestResolveTable_NextCallCarriesResolvedName(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig()))

	var decoded struct {
		Meta struct {
			Next []struct {
				Tool string            `json:"tool"`
				Args map[string]string `json:"args"`
			} `json:"next"`
		} `json:"_meta"`
	}
	out := callTool(t, c, "describe_table", map[string]any{"table": "public.orders"})
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("unmarshal: %v\n%.500s", err, out)
	}
	if len(decoded.Meta.Next) == 0 {
		t.Fatalf("want a find_related follow-up, got:\n%.500s", out)
	}
	got := decoded.Meta.Next[0]
	if got.Tool != "find_related" || got.Args["table"] != "orders" || got.Args["schema"] != "public" {
		t.Errorf("want find_related{table: orders, schema: public}, got %s%v", got.Tool, got.Args)
	}
}

// Resolution needs the snapshot, so it must run after the loaded check; moving
// it earlier would dereference nil instead of reporting the real problem.
func TestResolveTable_UninitializedReportsTheRealProblem(t *testing.T) {
	srv := &Server{lintConfig: lint.DefaultConfig()}
	srv.SetUninitialized()

	var req mcp.CallToolRequest
	req.Params.Name = "describe_table"
	req.Params.Arguments = map[string]any{"table": "orders"}
	res, err := srv.handleDescribeTable(context.Background(), req)
	if err != nil {
		t.Fatalf("handleDescribeTable: %v", err)
	}
	text, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	if !strings.Contains(text.Text, "no schema loaded") {
		t.Errorf("want the uninitialized message, got: %.300s", text.Text)
	}
}
