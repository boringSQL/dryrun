package mcp

import (
	"context"
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
	return s.metaJSONResult(result, "", hint), nil
}

func (s *Server) handleExplainQuery(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pool, err := s.requirePool()
	if err != nil {
		return errResult(err.Error()), nil
	}
	snap, _ := s.getSchema()

	withStats := getBoolArg(req, "with_stats")
	node := getArg(req, "node")

	var injectResult *schema.InjectResult

	if withStats {
		if snap == nil {
			return errResult("no schema snapshot available for stats injection"), nil
		}
		snap = snap.CloneForStats()
		if node != "" {
			if err := schema.ApplyNodeStats(snap, node); err != nil {
				return errResult(fmt.Sprintf("node stats: %v", err)), nil
			}
		}
		if err := schema.CanInjectStats(snap); err != nil {
			return errResult(fmt.Sprintf("cannot inject stats: %v", err)), nil
		}
		pgVer, err := dryrun.ParsePgVersion(snap.PgVersion)
		if err != nil {
			return errResult(fmt.Sprintf("cannot parse PG version: %v", err)), nil
		}
		injectResult, err = schema.InjectStats(ctx, pool, snap, pgVer.Major)
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
	return s.metaJSONResult(result, "", hint), nil
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
	s.injectMeta(wrapper, hint)
	return jsonResult(wrapper), nil
}

func (s *Server) handleSuggestIndex(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	sql := getArg(req, "sql")
	pgVersion, _ := dryrun.ParsePgVersion(snap.PgVersion)

	var plan *query.PlanNode
	if s.pool != nil {
		result, err := query.ExplainQuery(ctx, s.pool, sql, false, snap)
		if err == nil {
			plan = &result.Plan
		}
	}

	suggestions, err := query.SuggestIndex(sql, snap, plan, &pgVersion)
	if err != nil {
		return errResult(fmt.Sprintf("analysis failed: %v", err)), nil
	}
	if len(suggestions) == 0 {
		return textResult("No index suggestions."), nil
	}
	hint := ""
	if len(suggestions) > 0 {
		hint = "Index suggestions contain DDL. Run each through check_migration before applying — it checks lock safety and duration."
	}
	wrapper := map[string]any{"index_suggestions": suggestions}
	s.injectMeta(wrapper, hint)
	return jsonResult(wrapper), nil
}
