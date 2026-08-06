package mcp

import (
	"encoding/json"

	"github.com/boringsql/dryrun/pkg/vacuum"
)

// Typed output envelopes for tools with a stable result shape. Their output
// schemas are generated via mcp.WithOutputSchema[T]; jsonschema-go emits
// additionalProperties:false, so these must be the exact wire shape.
type (
	tablePartitionSummary struct {
		Strategy string `json:"strategy"`
		Key      string `json:"key"`
		Children int    `json:"children"`
	}

	tableListEntry struct {
		Schema       string                 `json:"schema"`
		Name         string                 `json:"name"`
		RowsEstimate *int64                 `json:"rows_estimate,omitempty"`
		SizeBytes    *int64                 `json:"size_bytes,omitempty"`
		Partitioned  *tablePartitionSummary `json:"partitioned,omitempty"`
		Comment      *string                `json:"comment,omitempty"`
	}

	// Count is the pre-paging total, Tables the returned page.
	listTablesResult struct {
		Tables []tableListEntry `json:"tables"`
		Count  int              `json:"count"`
		Offset int              `json:"offset,omitempty"`
		Meta   *toolMeta        `json:"_meta,omitempty"`
	}

	searchMatch struct {
		Kind   string `json:"kind"`
		Object string `json:"object"`
		Detail string `json:"detail,omitempty"`
	}

	searchSchemaResult struct {
		Query   string        `json:"query"`
		Matches []searchMatch `json:"matches"`
		Count   int           `json:"count"`
		Offset  int           `json:"offset,omitempty"`
		Meta    *toolMeta     `json:"_meta,omitempty"`
	}

	vacuumHealthResult struct {
		Entries   []vacuum.VacuumHealth `json:"vacuum_health"`
		Count     int                   `json:"count"`
		Truncated bool                  `json:"truncated,omitempty"`
		Omitted   int                   `json:"omitted,omitempty"`
		Meta      *toolMeta             `json:"_meta,omitempty"`
	}

	// Key-only observation of a withheld tag key (deny-listed or literal-shaped value).
	dynamicTagKey struct {
		Key                  string `json:"key"`
		ValueCardinalitySeen int    `json:"value_cardinality_seen"`
	}

	// One pg_stat_statements shape, tagged with its capture node; never merged
	// across nodes. PctOfTotalExecTime is computed pre-pagination, against
	// that node's own total exec time — not pooled across nodes.
	// Owners/RegresqlMeta/DynamicTagKeys come from the query's leading comment via
	// qshape's tag policy; RegresqlMeta is unscreened free text, not a vetted label.
	queryStatsEntry struct {
		Node               string            `json:"node"`
		CapturedAt         string            `json:"captured_at"`
		SchemaRefHash      string            `json:"schema_ref_hash"`
		Fingerprint        string            `json:"fingerprint"`
		Canonical          string            `json:"canonical"`
		CanonicalTruncated bool              `json:"canonical_truncated,omitempty"`
		Calls              int64             `json:"calls"`
		TotalExecTimeMs    float64           `json:"total_exec_time_ms,omitempty"`
		MeanExecTimeMs     float64           `json:"mean_exec_time_ms,omitempty"`
		Rows               int64             `json:"rows,omitempty"`
		RowsPerCall        float64           `json:"rows_per_call,omitempty"`
		PctOfTotalExecTime float64           `json:"pct_of_total_exec_time,omitempty"`
		NestedExecTimeMs   float64           `json:"nested_exec_time_ms,omitempty"`
		TopLevelExecTimeMs float64           `json:"toplevel_exec_time_ms,omitempty"`
		Owners             map[string]string `json:"owners,omitempty"`
		RegresqlMeta       map[string]string `json:"regresql_meta,omitempty"`
		DynamicTagKeys     []dynamicTagKey   `json:"dynamic_tag_keys,omitempty"`
	}

	// Count is the pre-paging total (after the min_calls/node filters), Queries the returned page.
	listTopQueriesResult struct {
		Queries []queryStatsEntry `json:"queries"`
		Count   int               `json:"count"`
		Offset  int               `json:"offset,omitempty"`
		Meta    *toolMeta         `json:"_meta,omitempty"`
	}
)

// Hand-written shallow schemas for tools whose payloads are dynamic maps
// (field filtering, per-kind shapes). Generated struct schemas would set
// additionalProperties:false and reject them at output validation, so these
// stay permissive and document the stable keys only.
var (
	metaProperty = `"_meta": {
		"type": "object",
		"description": "pg_version/database/mode envelope; may carry hint (prose) and next (pre-validated follow-up calls)."
	}`

	describeTableOutputSchema = json.RawMessage(`{
		"type": "object",
		"description": "Table detail. Sections vary with detail= and fields=: columns, indexes, constraints, stats, bloat, column_profiles, node_breakdown, partition_summary, partition_child_sizing.",
		"properties": {
			"schema": {"type": "string"},
			"name": {"type": "string"},
			"columns": {"type": "array"},
			"indexes": {"type": "array"},
			"constraints": {"type": "array"},
			"column_profiles": {"type": "array"},
			` + metaProperty + `
		},
		"additionalProperties": true
	}`)

	detectOutputSchema = json.RawMessage(`{
		"type": "object",
		"description": "kind=all: one {entries, count, truncated?, omitted?} block per category (stale_stats, unused_indexes, anomalies, bloated_indexes, bloated_tables). Single kind: the entries under the kind's key plus count/truncated/omitted at the top level.",
		"properties": {
			"count": {"type": "integer"},
			"truncated": {"type": "boolean"},
			"omitted": {"type": "integer"},
			` + metaProperty + `
		},
		"additionalProperties": true
	}`)

	lintSchemaOutputSchema = json.RawMessage(`{
		"type": "object",
		"description": "Requested sections only: conventions (rule_groups; items dropped in summary view) and audit (full report, or by_rule counts in summary view).",
		"properties": {
			"conventions": {"type": "object"},
			"audit": {"type": "object"},
			` + metaProperty + `
		},
		"additionalProperties": true
	}`)

	snapshotDiffOutputSchema = json.RawMessage(`{
		"type": "object",
		"description": "Diff between two snapshots: refs, headline summary, ranked per-object deltas for the requested kind, correlated planner/activity drift; full view adds raw per-row deltas.",
		"properties": {
			` + metaProperty + `
		},
		"additionalProperties": true
	}`)
)
