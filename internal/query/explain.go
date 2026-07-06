package query

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boringsql/dryrun/internal/pgmustard"
	"github.com/boringsql/dryrun/internal/schema"
)

type (
	ExplainResult struct {
		Plan          PlanNode             `json:"plan"`
		TotalCost     float64              `json:"total_cost"`
		EstimatedRows float64              `json:"estimated_rows"`
		Warnings      []PlanWarning        `json:"warnings"`
		Execution     *ExecutionStats      `json:"execution,omitempty"`
		StatsInjected *schema.InjectResult `json:"stats_injected,omitempty"`
		GucsReplayed  []string             `json:"gucs_replayed,omitempty"`
		GucsSkipped   []SkippedGuc         `json:"gucs_skipped,omitempty"`
		PgMustardTips []pgmustard.Tip      `json:"pgmustard_tips,omitempty"`
		RawPlanJSON   json.RawMessage      `json:"-"`
	}

	PlanWarning struct {
		Severity string  `json:"severity"`
		Message  string  `json:"message"`
		NodeType string  `json:"node_type"`
		Detail   *string `json:"detail,omitempty"`
	}

	ExecutionStats struct {
		ExecutionTimeMs float64 `json:"execution_time_ms"`
		PlanningTimeMs  float64 `json:"planning_time_ms"`
	}
)

// runs EXPLAIN, optionally with ANALYZE in a rolled-back tx
func ExplainQuery(ctx context.Context, pool *pgxpool.Pool, sql string, analyze bool, snap *schema.SchemaSnapshot) (*ExplainResult, error) {
	var explainSQL string
	if analyze {
		explainSQL = fmt.Sprintf("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) %s", sql)
	} else {
		explainSQL = fmt.Sprintf("EXPLAIN (FORMAT JSON) %s", sql)
	}

	replayGUCs := snap != nil && len(snap.GUCs) > 0

	var (
		jsonStr  string
		replayed []string
		skipped  []SkippedGuc
	)
	if analyze || replayGUCs {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("begin transaction: %w", err)
		}
		defer tx.Rollback(ctx)

		if replayGUCs {
			replayed, skipped, err = applyPlannerGUCs(ctx, tx, snap.GUCs)
			if err != nil {
				return nil, fmt.Errorf("replay GUCs: %w", err)
			}
		}
		if err := tx.QueryRow(ctx, explainSQL).Scan(&jsonStr); err != nil {
			return nil, fmt.Errorf("EXPLAIN failed: %w", err)
		}
	} else {
		if err := pool.QueryRow(ctx, explainSQL).Scan(&jsonStr); err != nil {
			return nil, fmt.Errorf("EXPLAIN failed: %w", err)
		}
	}

	var planArray []json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &planArray); err != nil {
		return nil, fmt.Errorf("failed to parse EXPLAIN JSON: %w", err)
	}
	if len(planArray) == 0 {
		return nil, fmt.Errorf("empty EXPLAIN result")
	}

	var planObj map[string]json.RawMessage
	if err := json.Unmarshal(planArray[0], &planObj); err != nil {
		return nil, fmt.Errorf("failed to parse EXPLAIN plan object: %w", err)
	}

	planRaw, ok := planObj["Plan"]
	if !ok {
		return nil, fmt.Errorf("no Plan in EXPLAIN output")
	}

	plan, err := ParsePlanJSON(planRaw)
	if err != nil {
		return nil, err
	}

	var execution *ExecutionStats
	if analyze {
		var execTime, planTime float64
		if raw, ok := planObj["Execution Time"]; ok {
			_ = json.Unmarshal(raw, &execTime)
		}
		if raw, ok := planObj["Planning Time"]; ok {
			_ = json.Unmarshal(raw, &planTime)
		}
		execution = &ExecutionStats{
			ExecutionTimeMs: execTime,
			PlanningTimeMs:  planTime,
		}
	}

	warnings := DetectPlanWarnings(plan, snap)

	return &ExplainResult{
		Plan:          *plan,
		TotalCost:     plan.TotalCost,
		EstimatedRows: plan.PlanRows,
		Warnings:      warnings,
		Execution:     execution,
		GucsReplayed:  replayed,
		GucsSkipped:   skipped,
		RawPlanJSON:   planArray[0],
	}, nil
}
