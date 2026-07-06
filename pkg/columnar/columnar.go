// Package columnar analyzes AlloyDB columnar-engine state from the g_columnar_* views.
package columnar

import (
	"fmt"
	"time"
)

type Severity string

const (
	SeverityInfo   Severity = "info"
	SeverityLow    Severity = "low"
	SeverityMedium Severity = "medium"
	SeverityHigh   Severity = "high"
)

type (
	Column struct {
		Schema           string     `json:"schema"`
		Relation         string     `json:"relation"`
		Column           string     `json:"column"`
		Type             string     `json:"type"`
		Status           string     `json:"status"`
		SizeBytes        int64      `json:"size_bytes"`
		NumTimesAccessed int64      `json:"num_times_accessed"`
		LastAccessed     *time.Time `json:"last_accessed,omitempty"`
	}

	Relation struct {
		Schema             string `json:"schema"`
		Relation           string `json:"relation"`
		Status             string `json:"status"`
		SizeBytes          int64  `json:"size_bytes"`
		InvalidBlockCount  int64  `json:"invalid_block_count"`
		TotalBlockCount    int64  `json:"total_block_count"`
		AutoRefreshFailure int64  `json:"auto_refresh_failure_count"`
	}

	State struct {
		Flavor        string     `json:"flavor"`
		EngineEnabled bool       `json:"engine_enabled"`
		Columns       []Column   `json:"columns"`
		Relations     []Relation `json:"relations"`
	}

	Finding struct {
		Code     string   `json:"code"`
		Severity Severity `json:"severity"`
		Relation string   `json:"relation,omitempty"`
		Message  string   `json:"message"`
	}
)

// staleness ratio above which resident blocks are mostly invalidated by writes
const staleBlockRatio = 0.20

func Analyze(s *State) []Finding {
	if s == nil {
		return nil
	}
	var out []Finding

	// engine on but nothing resident: every plan silently falls back to a heap scan
	if s.EngineEnabled && len(s.Columns) == 0 {
		out = append(out, Finding{
			Code:     "columnar/engine_empty",
			Severity: SeverityMedium,
			Message:  "columnar engine is enabled but no columns are registered in the store; queries fall back to heap scans. Add columns with google_columnar_engine_add or wait for auto-columnarization to observe query patterns",
		})
	}

	for _, r := range s.Relations {
		key := r.Schema + "." + r.Relation
		if r.AutoRefreshFailure > 0 {
			out = append(out, Finding{
				Code:     "columnar/auto_refresh_failing",
				Severity: SeverityHigh,
				Relation: key,
				Message:  fmt.Sprintf("columnar auto-refresh has failed %d times; resident data may be stale", r.AutoRefreshFailure),
			})
		}
		if r.TotalBlockCount > 0 {
			ratio := float64(r.InvalidBlockCount) / float64(r.TotalBlockCount)
			if ratio >= staleBlockRatio {
				out = append(out, Finding{
					Code:     "columnar/blocks_stale",
					Severity: SeverityMedium,
					Relation: key,
					Message: fmt.Sprintf("%.0f%% of columnar blocks are invalid (%d of %d) from writes; the store is spending maintenance to keep up and may not be earning its memory",
						ratio*100, r.InvalidBlockCount, r.TotalBlockCount),
				})
			}
		}
	}

	return out
}
