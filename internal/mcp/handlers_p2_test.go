package mcp

import (
	"context"
	"encoding/json"
	"reflect"
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

// The two handlers deliberately disagree about empty results, and a merge is
// coming that will be tempted to harmonise them. advise omits plan_warnings
// when there are none; analyze_plan always emits the key, because the caller
// asked about a plan and "none" is the answer. Offline advise has no plan at
// all, so advice and plan_warnings are both absent while index_suggestions
// still are not.
func TestPlanKeysDifferBetweenAdviseAndAnalyzePlan(t *testing.T) {
	c := setupOfflineTest(t)
	decode := func(t *testing.T, out string) map[string]any {
		t.Helper()
		var payload map[string]any
		if err := json.Unmarshal([]byte(out), &payload); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, out)
		}
		return payload
	}

	// a plan with no problems in it: warnings come back empty either way
	clean := map[string]any{"Plan": map[string]any{
		"Node Type": "Index Scan", "Relation Name": "users", "Schema": "public",
		"Plan Rows": 1.0, "Total Cost": 8.0,
	}}

	ap := decode(t, callTool(t, c, "analyze_plan", map[string]any{
		"sql": "SELECT * FROM users WHERE user_id = 1", "plan_json": clean,
	}))
	if _, ok := ap["plan_warnings"]; !ok {
		t.Errorf("analyze_plan dropped the key it was asked about: %v", ap)
	}

	adv := decode(t, callTool(t, c, "advise", map[string]any{
		"sql": "SELECT * FROM users WHERE user_id = 1",
	}))
	for _, key := range []string{"plan_warnings", "advice", "explain_error"} {
		if _, ok := adv[key]; ok {
			t.Errorf("offline advise emitted %q with no plan: %v", key, adv)
		}
	}
	// validation keys are advise's, and unconditional
	for _, key := range []string{"valid", "errors", "warnings"} {
		if _, ok := adv[key]; !ok {
			t.Errorf("advise dropped %q", key)
		}
	}
	// analyze_plan carries them only when sql parses, and never corrected_sql
	noSQL := decode(t, callTool(t, c, "analyze_plan", map[string]any{"sql": "", "plan_json": clean}))
	if _, ok := noSQL["valid"]; ok {
		t.Errorf("analyze_plan validated an empty query: %v", noSQL)
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

// advise and analyze_plan must read the same pasted plan the same way, or
// folding one into the other later changes answers.
func TestAdviseAndAnalyzePlanAgreeOnASuppliedPlan(t *testing.T) {
	c := setupOfflineTest(t)
	args := map[string]any{
		"sql": "SELECT * FROM users WHERE email = 'a@b'",
		"plan_json": map[string]any{"Plan": map[string]any{
			"Node Type": "Seq Scan", "Relation Name": "users", "Schema": "public",
			"Plan Rows": 50000.0, "Total Cost": 1234.0,
		}},
	}
	decode := func(tool string) map[string]any {
		t.Helper()
		var payload map[string]any
		if err := json.Unmarshal([]byte(callTool(t, c, tool, args)), &payload); err != nil {
			t.Fatalf("%s: not JSON: %v", tool, err)
		}
		return payload
	}

	adv, ap := decode("advise"), decode("analyze_plan")
	for _, key := range []string{"plan_warnings", "advice", "index_suggestions"} {
		if !reflect.DeepEqual(adv[key], ap[key]) {
			t.Errorf("%s differs:\nadvise:       %v\nanalyze_plan: %v", key, adv[key], ap[key])
		}
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

// EXPLAIN (FORMAT JSON) returns an array, and that is what people paste. The
// only coverage of the array shape today is analyze_plan's, and analyze_plan is
// about to be folded away.
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
