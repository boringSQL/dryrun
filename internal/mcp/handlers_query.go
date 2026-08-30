package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/query"
	"github.com/boringsql/dryrun/internal/schema"
)

func (s *Server) handleValidateQuery(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	result, err := query.ValidateQuery(getArg(req, "sql"), snap)
	if err != nil {
		return errResult(fmt.Sprintf("SQL parse error: %v", err)), nil
	}

	hint := ""
	var next []NextCall
	switch {
	case result.Valid && len(result.Warnings) > 0:
		hint = "Query is valid but has warnings. Use advise for index suggestions and plan analysis."
	case result.Valid:
		hint = "Query is valid. Use advise if you need optimization suggestions."
	case result.CorrectedSQL != "":
		hint = "Every unknown name had one candidate in the snapshot. corrected_sql is the query with those names replaced and it validates clean -- dryrun matched names, not intent, so read fixes before applying it."
		next = []NextCall{{Tool: "advise", Args: map[string]any{"sql": result.CorrectedSQL}}}
	default:
		hint = "Unknown names with no single obvious candidate. Look them up with find_objects before rewriting the query."
	}
	return s.metaJSONResult(result, "", hint, next), nil
}

func (s *Server) handleExplainQuery(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pool, err := s.requirePool()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap, _ := s.getSchema()

	withStats := getBoolArg(req, "with_stats")

	var injectResult *schema.InjectResult

	if withStats {
		annotated, err := s.getAnnotated()
		if err != nil {
			return errResult("no annotated schema available for stats injection"), nil
		}
		if err := schema.CanInjectStats(annotated); err != nil {
			return errResult(fmt.Sprintf("cannot inject stats: %v", err)), nil
		}
		pgVer, err := dryrun.ParsePgVersion(snap.PgVersion)
		if err != nil {
			return errResult(fmt.Sprintf("cannot parse PG version: %v", err)), nil
		}
		injectResult, err = schema.InjectStats(ctx, pool, annotated, pgVer.Major)
		if err != nil {
			return errResult(fmt.Sprintf("stats injection failed: %v", err)), nil
		}
	}

	result, err := query.ExplainQuery(ctx, pool, getArg(req, "sql"), getBoolArg(req, "analyze"), snap)
	if err != nil {
		return errResult(fmt.Sprintf("EXPLAIN failed: %v", err)), nil
	}

	result.StatsInjected = injectResult

	// surface injection tripwires (pg_regresql not loaded, degenerate reltuples)
	// into plan warnings; StatsInjected.warnings alone never reaches the hint
	if injectResult != nil {
		for _, w := range injectResult.Warnings {
			result.Warnings = append(result.Warnings, query.PlanWarning{
				Severity: "warning", Message: w, NodeType: "stats_injection",
			})
		}
	}

	if getBoolArg(req, "pgmustard") {
		addPgmWarn := func(msg string) {
			result.Warnings = append(result.Warnings, query.PlanWarning{
				Severity: "warning", Message: msg, NodeType: "pgmustard",
			})
		}
		switch {
		case !getBoolArg(req, "analyze"):
			addPgmWarn("pgMustard requires EXPLAIN ANALYZE output with timings; re-run with analyze: true")
		case withStats:
			addPgmWarn("pgMustard tips are not useful with injected stats: ANALYZE timings reflect local data, not production")
		case !s.pgmustardClient.HasKey():
			addPgmWarn("pgMustard API key not configured; set pgmustard_api_key in dryrun.toml [services] or PGMUSTARD_API_KEY env var")
		default:
			tips, err := s.pgmustardClient.AnalyzePlan(result.RawPlanJSON)
			if err != nil {
				addPgmWarn(fmt.Sprintf("pgMustard analysis failed: %v", err))
			} else {
				result.PgMustardTips = tips.Tips
			}
		}
	}

	hint := ""
	if injectResult != nil && len(injectResult.Warnings) > 0 {
		hint = "Stats injection reported warnings; the plan may not reflect production. Check stats_injected.warnings before trusting row estimates."
	} else if len(result.Warnings) > 0 {
		hint = "Warnings detected. Use advise for index suggestions and actionable recommendations."
	}
	return s.metaJSONResult(result, "", hint, nil), nil
}

func (s *Server) handleCheckMigration(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	pgVersion, _ := dryrun.ParsePgVersion(snap.PgVersion)
	checks, err := query.CheckMigration(getArg(req, "ddl"), snap, &pgVersion)
	if err != nil {
		return errResult(fmt.Sprintf("DDL parse error: %v", err)), nil
	}
	if len(checks) == 0 {
		return textResult("Could not identify a specific DDL operation to check."), nil
	}

	var unsafe, rewritten, multiStep int
	concurrentIndex := false
	for _, c := range checks {
		if c.Safety == query.SafetySafe {
			continue
		}
		unsafe++
		if len(c.SaferSQL) == 0 {
			continue
		}
		rewritten++
		if len(c.SaferSQL) > 1 {
			multiStep++
		}
		if c.Operation == "CREATE INDEX" {
			concurrentIndex = true
		}
	}

	hint := ""
	switch {
	case rewritten > 0 && rewritten == unsafe:
		hint = "safer_sql holds the rewrite: run those statements, in that order, instead of the input."
	case rewritten > 0:
		hint = "safer_sql holds the rewrite for the statements that have a mechanical one. The rest carry a recommendation only, because the safe form needs a decision this tool cannot make -- a batch size, a backfill window, a deploy order."
	case unsafe > 0:
		hint = "No mechanical rewrite for these. Read recommendation and rollback_ddl before applying anything."
	}
	// one wrapping transaction would hold the first statement's lock across the
	// scan in the second
	if multiStep > 0 {
		hint = joinHints(hint, "Run each statement in safer_sql in its own transaction. A migration runner that wraps the file in one holds the ACCESS EXCLUSIVE taken by the first statement across the scan in the second, which is worse than the input.")
	}
	if concurrentIndex {
		hint = joinHints(hint, "CREATE INDEX CONCURRENTLY cannot run inside a transaction at all, so that statement has to be outside whatever the runner wraps.")
	}

	wrapper := map[string]any{"checks": checks}
	s.injectMeta(wrapper, hint, nil)
	return jsonResult(wrapper), nil
}

type (
	// planInsights is the plan-derived half of advise.
	planInsights struct {
		warnings []query.PlanWarning
		advice   []query.Advice
		indexes  []query.IndexSuggestion
	}
)

// A nil plan still leaves snapshot-based index suggestions.
func planInsightsFor(annotated *schema.AnnotatedSchema, sql string, plan *query.PlanNode, includeIdx bool, pgVersion *dryrun.PgVersion) planInsights {
	var out planInsights
	if plan != nil {
		out.warnings = query.DetectPlanWarnings(plan, annotated.Schema)
		out.advice = query.Advise(plan, annotated, pgVersion)
	}
	if includeIdx {
		if suggestions, err := query.SuggestIndex(sql, annotated.Schema, plan, pgVersion); err == nil {
			out.indexes = suggestions
		}
	}
	return out
}

// suppliedPlan reads plan_json when passed; a malformed plan is an error, not a silent fallback.
func suppliedPlan(req mcp.CallToolRequest) (plan *query.PlanNode, supplied bool, err error) {
	raw, ok := req.GetArguments()["plan_json"]
	if !ok || raw == nil {
		return nil, false, nil
	}
	planRaw, err := extractPlanNode(raw)
	if err != nil {
		return nil, true, err
	}
	plan, err = query.ParsePlanJSON(planRaw)
	if err != nil {
		return nil, true, err
	}
	return plan, true, nil
}

func (s *Server) handleAdvise(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	annotated, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap := annotated.Schema

	sql := getArg(req, "sql")
	includeIdx := getBoolArgDefault(req, "include_index_suggestions", true)
	analyze := getBoolArg(req, "analyze")
	pgVersion, _ := dryrun.ParsePgVersion(snap.PgVersion)

	plan, supplied, err := suppliedPlan(req)
	if err != nil {
		return errResult(fmt.Sprintf("plan_json parse error: %v", err)), nil
	}

	validation, vErr := query.ValidateQuery(sql, snap)
	// a plan is the evidence and the sql is context: sql the parser chokes on
	// costs the validation half, not the plan review the caller asked for
	if vErr != nil && !supplied {
		return errResult(fmt.Sprintf("SQL parse error: %v", vErr)), nil
	}
	// analyze would re-run EXPLAIN ANALYZE and replace the plan the caller passed
	if supplied && analyze && s.pool != nil {
		return errResult("analyze runs EXPLAIN ANALYZE, which would replace the plan_json you passed; drop one of them"), nil
	}

	var explainErr string
	if !supplied && s.pool != nil {
		result, err := query.ExplainQuery(ctx, s.pool, sql, analyze, snap)
		if err != nil {
			explainErr = err.Error()
		} else {
			// warnings and advice come from planInsightsFor, so an EXPLAINed plan
			// and a pasted one are read by the same code
			plan = &result.Plan
		}
	}
	insights := planInsightsFor(annotated, sql, plan, includeIdx, &pgVersion)

	wrapper := map[string]any{}
	if vErr != nil {
		wrapper["validation_error"] = vErr.Error()
	} else {
		wrapper["valid"] = validation.Valid
		wrapper["errors"] = orEmpty(validation.Errors)
		wrapper["warnings"] = orEmpty(validation.Warnings)
	}
	// present whenever a plan was read, empty or not: absence is how a caller
	// tells "no problems found" from "no plan was looked at", and an empty
	// array is what a caller can call len() on
	if plan != nil {
		wrapper["plan_warnings"] = orEmpty(insights.warnings)
	}
	if len(insights.advice) > 0 {
		wrapper["advice"] = insights.advice
	}
	if len(insights.indexes) > 0 {
		wrapper["index_suggestions"] = insights.indexes
	}
	if explainErr != "" {
		wrapper["explain_error"] = explainErr
	}
	if vErr == nil && validation.CorrectedSQL != "" {
		wrapper["corrected_sql"] = validation.CorrectedSQL
		wrapper["fixes"] = validation.Fixes
	}

	hint := ""
	switch {
	case vErr == nil && !validation.Valid && validation.CorrectedSQL != "":
		hint = "Query has validation errors, and every unknown name had one candidate: corrected_sql is the query with those names replaced. Re-run advise on it once you have checked the fixes."
	case vErr == nil && !validation.Valid:
		hint = "Query has validation errors. Fix referenced tables/columns before running advise again."
	case len(insights.advice) > 0 || len(insights.indexes) > 0:
		hint = "Review advice and index suggestions. Run any DDL through check_migration before applying."
	case len(insights.warnings) > 0:
		hint = "Plan warnings detected. Inspect plan_warnings for the problem nodes."
	}
	// not a case: the advice arm above also wins offline and would bury this
	if plan == nil {
		if explainErr != "" {
			hint = joinHints("EXPLAIN did not run, so this is validation and index suggestions only; see explain_error.", hint)
		} else if s.pool == nil {
			hint = joinHints("No plan was read: pass plan_json to interpret one you already have, or connect with --db to let advise run EXPLAIN itself.", hint)
		}
	}
	// prepended rather than another case: a validation error is the bigger
	// news, and would otherwise swallow the fact that analyze was dropped
	if analyze && s.pool == nil {
		hint = joinHints("analyze needs a database connection and was ignored.", hint)
	}
	s.injectMeta(wrapper, hint, nil)
	return jsonResult(wrapper), nil
}

// A nil slice marshals to null; a key whose presence is the contract has to
// carry something a caller can take the length of.
func orEmpty[T any](in []T) []T {
	if in == nil {
		return []T{}
	}
	return in
}

func extractPlanNode(v any) (json.RawMessage, error) {
	// clients re-encode structured arguments as strings; a pasted text plan is not recoverable
	if str, ok := v.(string); ok {
		var decoded any
		if err := json.Unmarshal([]byte(str), &decoded); err != nil {
			return nil, fmt.Errorf("plan_json must be EXPLAIN (FORMAT JSON) output, not the text plan")
		}
		v = decoded
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 && raw[0] == '[' {
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, err
		}
		if len(arr) == 0 {
			return nil, fmt.Errorf("empty plan_json array")
		}
		raw = arr[0]
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	if planRaw, ok := obj["Plan"]; ok {
		return planRaw, nil
	}
	if _, ok := obj["Node Type"]; ok {
		return raw, nil
	}
	return nil, fmt.Errorf("no Plan key and no Node Type at root")
}
