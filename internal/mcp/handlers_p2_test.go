package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/pkg/lint"
)

// Offline advise with no plan returns validation plus a hint naming both ways
// to get plan-based advice: bring a plan, or connect a database.
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
	hint, _ := meta["hint"].(string)
	for _, want := range []string{"plan_json", "--db"} {
		if !strings.Contains(hint, want) {
			t.Errorf("hint does not mention %s: %q", want, hint)
		}
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

// plan_warnings is how a caller tells "the plan is clean" from "no plan was
// read" -- the key is present whenever a plan was looked at, empty or not.
// analyze_plan always emitted it; advise now carries that for its callers.
func TestAdvise_PlanWarningsPresenceMarksThatAPlanWasRead(t *testing.T) {
	c := setupOfflineTest(t)
	decode := func(t *testing.T, out string) map[string]any {
		t.Helper()
		var payload map[string]any
		if err := json.Unmarshal([]byte(out), &payload); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, out)
		}
		return payload
	}

	// a plan with nothing wrong in it: the key still has to be there
	clean := map[string]any{"Plan": map[string]any{
		"Node Type": "Index Scan", "Relation Name": "users", "Schema": "public",
		"Plan Rows": 1.0, "Total Cost": 8.0,
	}}
	withPlan := decode(t, callTool(t, c, "advise", map[string]any{
		"sql": "SELECT * FROM users WHERE user_id = 1", "plan_json": clean,
	}))
	warnings, has := withPlan["plan_warnings"]
	if !has {
		t.Fatalf("a plan was read but plan_warnings is absent: %v", withPlan)
	}
	// null would satisfy "present" and break every caller that takes its length
	if _, ok := warnings.([]any); !ok {
		t.Errorf("plan_warnings is %#v, not an array", warnings)
	}

	// no plan: absent, alongside the keys advise always carries
	noPlan := decode(t, callTool(t, c, "advise", map[string]any{
		"sql": "SELECT * FROM users WHERE user_id = 1",
	}))
	for _, key := range []string{"plan_warnings", "advice", "explain_error"} {
		if _, has := noPlan[key]; has {
			t.Errorf("offline advise emitted %q with no plan: %v", key, noPlan)
		}
	}
	for _, key := range []string{"valid", "errors", "warnings"} {
		if _, has := noPlan[key]; !has {
			t.Errorf("advise dropped %q", key)
		}
	}
}

// The capability this adds: offline, dryrun cannot produce a plan, but it can
// read one. A plan the user pasted turns advise from validation-plus-index-
// suggestions into a plan review, with no connection anywhere.
func TestAdvise_ReadsASuppliedPlanOffline(t *testing.T) {
	c := setupOfflineTest(t)
	seqScan := map[string]any{"Plan": map[string]any{
		"Node Type": "Seq Scan", "Relation Name": "users", "Schema": "public",
		"Plan Rows": 50000.0, "Total Cost": 1234.0,
	}}

	var withPlan map[string]any
	out := callTool(t, c, "advise", map[string]any{
		"sql": "SELECT * FROM users WHERE email = 'a@b'", "plan_json": seqScan,
	})
	if err := json.Unmarshal([]byte(out), &withPlan); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if _, has := withPlan["plan_warnings"]; !has {
		t.Errorf("a supplied plan produced no plan_warnings: %v", withPlan)
	}
	// and the same call without it stays validation-only, so the plan is what
	// made the difference
	var without map[string]any
	out = callTool(t, c, "advise", map[string]any{"sql": "SELECT * FROM users WHERE email = 'a@b'"})
	if err := json.Unmarshal([]byte(out), &without); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	if _, has := without["plan_warnings"]; has {
		t.Errorf("plan_warnings without a plan: %v", without)
	}
}

// Both flags name a plan, and they name different ones: a plan captured on
// prod, and whatever EXPLAIN ANALYZE would produce here and now. Silently
// picking one is worse than refusing.
func TestAdvise_RejectsAnalyzeWithASuppliedPlan(t *testing.T) {
	srv := NewOfflineServer(loadDemoSchema(t), lint.DefaultConfig())
	srv.pool = &pgxpool.Pool{}

	var req mcp.CallToolRequest
	req.Params.Arguments = map[string]any{
		"sql": "SELECT 1", "analyze": true,
		"plan_json": map[string]any{"Plan": map[string]any{"Node Type": "Result"}},
	}
	res, err := srv.handleAdvise(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("analyze together with plan_json was accepted")
	}
	if txt, ok := res.Content[0].(mcp.TextContent); !ok || !strings.Contains(txt.Text, "plan_json") {
		t.Errorf("error should name the conflict: %v", res.Content)
	}
}

// A supplied plan wins over EXPLAIN. The proof is mechanical: the pool here is
// a zero value, so taking the EXPLAIN path at all would panic.
func TestAdvise_SuppliedPlanShortCircuitsExplain(t *testing.T) {
	srv := NewOfflineServer(loadDemoSchema(t), lint.DefaultConfig())
	srv.pool = &pgxpool.Pool{}

	var req mcp.CallToolRequest
	req.Params.Arguments = map[string]any{
		"sql": "SELECT * FROM users",
		"plan_json": map[string]any{"Plan": map[string]any{
			"Node Type": "Seq Scan", "Relation Name": "users", "Schema": "public",
			"Plan Rows": 50000.0,
		}},
	}
	res, err := srv.handleAdvise(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("supplied plan rejected: %v", res.Content)
	}

	// advise records an EXPLAIN failure rather than returning an error, so the
	// payload is what says which path ran
	var payload map[string]any
	if txt, ok := res.Content[0].(mcp.TextContent); ok {
		if err := json.Unmarshal([]byte(txt.Text), &payload); err != nil {
			t.Fatalf("not JSON: %v", err)
		}
	}
	if _, attempted := payload["explain_error"]; attempted {
		t.Error("EXPLAIN was attempted despite plan_json")
	}
	if _, read := payload["plan_warnings"]; !read {
		t.Errorf("the supplied plan was not read: %v", payload)
	}
}

// A plan the caller meant to pass but malformed must fail loudly, not fall
// back to "no plan" and answer a smaller question.
func TestAdvise_MalformedPlanIsAnError(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{
		"sql": "SELECT 1", "plan_json": map[string]any{"unrelated": "garbage"},
	})
	if !strings.Contains(out, "parse error") {
		t.Errorf("expected a parse error, got: %s", out)
	}
}

// EXPLAIN (FORMAT JSON) returns an array, and that is what people paste.
func TestAdvise_AcceptsArrayShapeAndEncodedPlan(t *testing.T) {
	c := setupOfflineTest(t)
	array := []any{map[string]any{"Plan": map[string]any{
		"Node Type": "Seq Scan", "Relation Name": "users", "Schema": "public",
		"Plan Rows": 50000.0, "Total Cost": 1234.0,
	}}}
	encoded, err := json.Marshal(array)
	if err != nil {
		t.Fatal(err)
	}

	for name, plan := range map[string]any{
		"array":                        array,
		"array re-encoded as a string": string(encoded),
	} {
		t.Run(name, func(t *testing.T) {
			out := callTool(t, c, "advise", map[string]any{
				"sql": "SELECT * FROM users", "plan_json": plan,
			})
			if !strings.Contains(out, "plan_warnings") {
				t.Errorf("plan not read: %s", out)
			}
		})
	}
}

// A text plan is not JSON, and the unmarshal error it produces explains
// nothing. Say what the field wants.
func TestAdvise_TextPlanSaysWhatIsWanted(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{
		"sql":       "SELECT * FROM users",
		"plan_json": "Seq Scan on users  (cost=0.00..1234.00 rows=50000 width=100)",
	})
	if !strings.Contains(out, "not the text plan") {
		t.Errorf("unhelpful error for a pasted text plan: %s", out)
	}
}

// Offline, analyze cannot produce a plan, so a caller passing both has asked
// for something fully answerable. Refusing it would be pedantry.
func TestAdvise_OfflineAcceptsAnalyzeBesideAPlan(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{
		"sql": "SELECT * FROM users", "analyze": true,
		"plan_json": map[string]any{"Plan": map[string]any{
			"Node Type": "Seq Scan", "Relation Name": "users", "Schema": "public",
			"Plan Rows": 50000.0,
		}},
	})
	if !strings.Contains(out, "plan_warnings") {
		t.Errorf("the supplied plan was refused offline: %s", out)
	}
	if !strings.Contains(out, "analyze needs a database connection") {
		t.Errorf("no note that analyze was ignored: %s", out)
	}
}

// The route to plan advice must survive a query that already has index
// suggestions -- offline those come from the query text alone, so they are the
// common case, not the rare one.
func TestAdvise_RouteHintSurvivesIndexSuggestions(t *testing.T) {
	c := setupOfflineTest(t)
	var decoded map[string]any
	out := callTool(t, c, "advise", map[string]any{"sql": "SELECT * FROM tasks WHERE title = 'x'"})
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if _, has := decoded["index_suggestions"]; !has {
		t.Fatal("fixture no longer produces index suggestions; pick another query")
	}
	meta, _ := decoded["_meta"].(map[string]any)
	hint, _ := meta["hint"].(string)
	if !strings.Contains(hint, "plan_json") {
		t.Errorf("index suggestions buried the route to plan advice: %q", hint)
	}
}

// A plan is the evidence; the sql is context. dryrun's parser refusing the
// statement must not cost the plan review the caller asked for -- analyze_plan
// tolerated this, and its callers land here now.
func TestAdvise_ReadsAPlanForSQLItCannotParse(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "advise", map[string]any{
		"sql": "SELEKT broken",
		"plan_json": map[string]any{"Plan": map[string]any{
			"Node Type": "Seq Scan", "Relation Name": "users", "Schema": "public",
			"Plan Rows": 50000.0, "Total Cost": 1234.0,
		}},
	})
	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("expected a payload, got: %s", out)
	}
	if _, has := payload["plan_warnings"]; !has {
		t.Errorf("the plan was not read: %v", payload)
	}
	if _, has := payload["validation_error"]; !has {
		t.Errorf("the parse failure should be reported, not hidden: %v", payload)
	}
	// and without a plan there is nothing to fall back on, so it stays an error
	if !strings.Contains(callTool(t, c, "advise", map[string]any{"sql": "SELEKT broken"}), "parse error") {
		t.Error("unparseable sql with no plan should still be an error")
	}
}

// A plan whose only finding is a warning must still say so in the hint.
func TestAdvise_HintsOnPlanWarningsAlone(t *testing.T) {
	c := setupOfflineTest(t)
	var payload map[string]any
	out := callTool(t, c, "advise", map[string]any{
		"sql": "SELECT * FROM users",
		"plan_json": map[string]any{"Plan": map[string]any{
			"Node Type": "Seq Scan", "Relation Name": "users", "Schema": "public",
			"Plan Rows": 50000.0, "Total Cost": 1234.0,
		}},
	})
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	warnings, _ := payload["plan_warnings"].([]any)
	if len(warnings) == 0 {
		t.Skip("fixture plan no longer produces warnings")
	}
	meta, _ := payload["_meta"].(map[string]any)
	if hint, _ := meta["hint"].(string); hint == "" {
		t.Errorf("plan warnings with no hint: %v", payload)
	}
}
