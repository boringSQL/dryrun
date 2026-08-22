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

	// Columns are on the table asked about, RefColumns on Table.
	relatedEdge struct {
		Table            string   `json:"table"`
		Constraint       string   `json:"constraint"`
		Columns          []string `json:"columns"`
		RefColumns       []string `json:"ref_columns"`
		OnDelete         string   `json:"on_delete,omitempty"`
		OnUpdate         string   `json:"on_update,omitempty"`
		NotValid         bool     `json:"not_valid,omitempty"`
		Join             string   `json:"join,omitempty"`
		PartitionsFolded int      `json:"partitions_folded,omitempty"`

		// unquoted identity, for a follow-up call that has to resolve it
		target   edgeTarget
		deferred bool
	}

	findRelatedResult struct {
		Table           string        `json:"table"`
		Outgoing        []relatedEdge `json:"outgoing"`
		OutgoingCount   int           `json:"outgoing_count"`
		OutgoingOmitted int           `json:"outgoing_omitted,omitempty"`
		Incoming        []relatedEdge `json:"incoming"`
		IncomingCount   int           `json:"incoming_count"`
		IncomingOmitted int           `json:"incoming_omitted,omitempty"`
		Meta            *toolMeta     `json:"_meta,omitempty"`
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
		Node       string `json:"node"`
		CapturedAt string `json:"captured_at"`
		// Different values mean different captured populations: not comparable.
		CaptureRuleVersion int               `json:"capture_rule_version,omitempty"`
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
		"description": "pg_version/database/mode envelope, plus schema_captured_at, planner_captured_at and activity_captured_at (oldest node, named in activity_oldest_node) where the answer comes from a snapshot; newer_snapshot_at when the local history holds a newer one than the one being served; may carry hint (prose) and next (pre-validated follow-up calls)."
	}`

	describeTableOutputSchema = json.RawMessage(`{
		"type": "object",
		"description": "Table detail. Sections vary with detail= and fields=: columns, indexes, constraints, stats, bloat, column_profiles, node_breakdown, partition_summary, partition_child_sizing. fields=[ddl] adds ddl (the CREATE TABLE and what finishes it), ddl_omitted (what the snapshot cannot reproduce) and, where the render failed, ddl_error.",
		"properties": {
			"schema": {"type": "string"},
			"name": {"type": "string"},
			"columns": {"type": "array"},
			"indexes": {"type": "array"},
			"constraints": {"type": "array"},
			"column_profiles": {"type": "array"},
			"ddl": {"type": "string"},
			"ddl_omitted": {"type": "array"},
			"ddl_error": {"type": "string"},
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
		"description": "Diff between two snapshots: refs, headline summary, ranked per-object deltas for the requested kind, correlated planner/activity drift, and per-node query-shape drift (query_delta) whose means are over the window rather than since pg_stat_statements last reset; full view adds raw per-row deltas.",
		"properties": {
			` + metaProperty + `
		},
		"additionalProperties": true
	}`)
)
