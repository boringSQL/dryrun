package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

// Offline advise returns a JSON wrapper carrying validation results and a
// hint nudging the user to connect a live DB for plan-based advice.
func TestAdvise_OfflineReturnsValidationAndHint(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{
		"sql": "SELECT * FROM users WHERE email = 'a@b'",
	})

	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	if _, has := decoded["valid"]; !has {
		t.Error("expected `valid` key in advise output")
	}
	meta, _ := decoded["_meta"].(map[string]any)
	if hint, _ := meta["hint"].(string); !strings.Contains(hint, "Offline") {
		t.Errorf("expected offline-mode hint, got %q", hint)
	}
}

// include_index_suggestions=false must omit the key from the wrapper.
func TestAdvise_RespectsIncludeIndexSuggestionsFlag(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{
		"sql":                       "SELECT * FROM tasks WHERE status = 'open'",
		"include_index_suggestions": false,
	})
	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	if _, has := decoded["index_suggestions"]; has {
		t.Error("expected no index_suggestions when include_index_suggestions=false")
	}
}

// Malformed SQL must surface a typed parse error, not panic or empty body.
func TestAdvise_MalformedSQLReturnsParseError(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{"sql": "SELEKT broken"})
	if !strings.Contains(out, "parse error") {
		t.Errorf("expected parse error, got: %s", out)
	}
}

// analyze_plan accepts the bare {"Plan": {...}} shape and reports advice on
// the embedded plan tree.
func TestAnalyzePlan_AcceptsBareShape(t *testing.T) {
	c := setupOfflineTest(t)
	plan := map[string]any{
		"Plan": map[string]any{
			"Node Type":     "Seq Scan",
			"Relation Name": "users",
			"Schema":        "public",
			"Plan Rows":     50000.0,
			"Total Cost":    1234.0,
		},
	}
	out := callTool(t, c, "analyze_plan", map[string]any{
		"sql":       "SELECT * FROM users",
		"plan_json": plan,
	})
	var decoded map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("expected JSON output: %v\n%s", err, out)
	}
	if _, has := decoded["plan_warnings"]; !has {
		t.Error("expected plan_warnings key in analyze_plan output")
	}
}

// analyze_plan also accepts the array-wrapped [{"Plan": {...}}] shape that
// EXPLAIN (FORMAT JSON) returns directly.
func TestAnalyzePlan_AcceptsArrayShape(t *testing.T) {
	c := setupOfflineTest(t)
	plan := []any{
		map[string]any{
			"Plan": map[string]any{
				"Node Type":     "Seq Scan",
				"Relation Name": "users",
				"Plan Rows":     1.0,
			},
		},
	}
	out := callTool(t, c, "analyze_plan", map[string]any{
		"sql":       "SELECT 1",
		"plan_json": plan,
	})
	if !strings.Contains(out, "plan_warnings") && !strings.Contains(out, "advice") {
		t.Errorf("expected plan_warnings or advice key in array-shape output: %s", out)
	}
}

// Missing plan_json must produce a typed error, not a panic.
func TestAnalyzePlan_MissingPlanJSONErrors(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "analyze_plan", map[string]any{"sql": "SELECT 1"})
	if !strings.Contains(out, "plan_json") {
		t.Errorf("expected error mentioning plan_json, got: %s", out)
	}
}

// Malformed plan_json (no Plan key, no Node Type) must surface a parse error.
func TestAnalyzePlan_MalformedPlanJSONErrors(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "analyze_plan", map[string]any{
		"sql":       "SELECT 1",
		"plan_json": map[string]any{"unrelated": "garbage"},
	})
	if !strings.Contains(out, "parse error") {
		t.Errorf("expected parse error, got: %s", out)
	}
}

// snapshot_diff in pure-offline mode (no history store) must surface a helpful
// error instead of panicking — there are no snapshots to diff.
func TestSnapshotDiff_NoHistory(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "snapshot_diff", map[string]any{})
	if out == "" {
		t.Fatal("empty result")
	}
	if !strings.Contains(out, "history") {
		t.Errorf("expected guidance about missing history, got: %s", out)
	}
}

// snapshot_diff with `from` set but no history store also errors gracefully,
// pointing the user at the missing history.
func TestSnapshotDiff_HashWithoutHistoryErrors(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "snapshot_diff", map[string]any{
		"from": "deadbeef",
	})
	if !strings.Contains(out, "history") {
		t.Errorf("expected history-related error, got: %s", out)
	}
}

// Sanity: the snapshot_diff tool resolves and the handler is wired. The actual
// JSON-RPC layer would return an error response, but we use TextContent for
// all error paths so the client always gets a body.
func TestSnapshotDiff_HandlerReachable(t *testing.T) {
	c := setupOfflineTest(t)
	var req mcp.CallToolRequest
	req.Params.Name = "snapshot_diff"
	res, err := c.CallTool(context.Background(), req)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res == nil || len(res.Content) == 0 {
		t.Fatal("expected non-empty result content")
	}
}
