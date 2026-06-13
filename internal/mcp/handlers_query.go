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
	if result.Valid && len(result.Warnings) > 0 {
		hint = "Query is valid but has warnings. Use advise for index suggestions and plan analysis."
	} else if result.Valid {
		hint = "Query is valid. Use advise if you need optimization suggestions."
	}
	return s.metaJSONResult(result, "", hint, nil), nil
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
	if len(result.Warnings) > 0 {
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

	hint := ""
	for _, c := range checks {
		if c.Safety == query.SafetyDangerous {
			hint = "DANGEROUS operations detected. Check the recommendation and rollback_ddl fields for safe alternatives."
			break
		}
	}
	wrapper := map[string]any{"checks": checks}
	s.injectMeta(wrapper, hint, nil)
	return jsonResult(wrapper), nil
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

	validation, vErr := query.ValidateQuery(sql, snap)
	if vErr != nil {
		return errResult(fmt.Sprintf("SQL parse error: %v", vErr)), nil
	}

	var (
		plan         *query.PlanNode
		planWarnings []query.PlanWarning
		advice       []query.Advice
		explainErr   string
	)
	if s.pool != nil {
		result, err := query.ExplainQuery(ctx, s.pool, sql, analyze, snap)
		if err != nil {
			explainErr = err.Error()
		} else {
			plan = &result.Plan
			planWarnings = result.Warnings
			advice = query.Advise(plan, annotated, &pgVersion)
		}
	}

	wrapper := map[string]any{
		"valid":    validation.Valid,
		"errors":   validation.Errors,
		"warnings": validation.Warnings,
	}
	if len(planWarnings) > 0 {
		wrapper["plan_warnings"] = planWarnings
	}
	if len(advice) > 0 {
		wrapper["advice"] = advice
	}
	var indexSuggestions []query.IndexSuggestion
	if includeIdx {
		if suggestions, err := query.SuggestIndex(sql, snap, plan, &pgVersion); err == nil {
			indexSuggestions = suggestions
		}
	}
	if len(indexSuggestions) > 0 {
		wrapper["index_suggestions"] = indexSuggestions
	}
	if explainErr != "" {
		wrapper["explain_error"] = explainErr
	}

	hint := ""
	switch {
	case !validation.Valid:
		hint = "Query has validation errors. Fix referenced tables/columns before running advise again."
	case len(advice) > 0 || len(indexSuggestions) > 0:
		hint = "Review advice and index suggestions. Run any DDL through check_migration before applying."
	case s.pool == nil:
		hint = "Offline mode: only static analysis available. Connect with --db for plan-based advice."
	}
	s.injectMeta(wrapper, hint, nil)
	return jsonResult(wrapper), nil
}

func (s *Server) handleAnalyzePlan(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	annotated, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap := annotated.Schema

	sql := getArg(req, "sql")
	includeIdx := getBoolArgDefault(req, "include_index_suggestions", true)
	pgVersion, _ := dryrun.ParsePgVersion(snap.PgVersion)

	args := req.GetArguments()
	rawPlan, ok := args["plan_json"]
	if !ok || rawPlan == nil {
		return errResult("plan_json is required"), nil
	}
	planRaw, err := extractPlanNode(rawPlan)
	if err != nil {
		return errResult(fmt.Sprintf("plan_json parse error: %v", err)), nil
	}
	plan, err := query.ParsePlanJSON(planRaw)
	if err != nil {
		return errResult(fmt.Sprintf("plan_json parse error: %v", err)), nil
	}

	planWarnings := query.DetectPlanWarnings(plan, snap)
	advice := query.Advise(plan, annotated, &pgVersion)

	wrapper := map[string]any{
		"plan_warnings": planWarnings,
	}
	if len(advice) > 0 {
		wrapper["advice"] = advice
	}
	if sql != "" {
		if validation, vErr := query.ValidateQuery(sql, snap); vErr == nil {
			wrapper["valid"] = validation.Valid
			if len(validation.Warnings) > 0 {
				wrapper["warnings"] = validation.Warnings
			}
			if len(validation.Errors) > 0 {
				wrapper["errors"] = validation.Errors
			}
		}
	}
	if includeIdx {
		if suggestions, err := query.SuggestIndex(sql, snap, plan, &pgVersion); err == nil && len(suggestions) > 0 {
			wrapper["index_suggestions"] = suggestions
		}
	}

	hint := ""
	switch {
	case len(advice) > 0:
		hint = "Review advice and index suggestions. Run any DDL through check_migration before applying."
	case len(planWarnings) > 0:
		hint = "Plan warnings detected. Inspect plan_warnings for problem nodes."
	}
	s.injectMeta(wrapper, hint, nil)
	return jsonResult(wrapper), nil
}

// Accepts both shapes Postgres emits: {"Plan": {...}} and [{"Plan": {...}, ...}].
func extractPlanNode(v any) (json.RawMessage, error) {
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
