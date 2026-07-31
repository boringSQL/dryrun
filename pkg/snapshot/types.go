// Package snapshot is the snapshot wire contract: types + content-hash funcs.
// Stdlib-only so consumers can recompute content_hash without introspection.
package snapshot

import (
	"sort"
	"time"
)

// 0 means legacy. not hashed. 2 = digest covers reloptions.
const FormatVersion = 2

type (

	// DDL-only schema snapshot; sizing/activity live in AnnotatedSchema
	SchemaSnapshot struct {
		FormatVersion    int             `json:"format_version"`
		PgVersion        string          `json:"pg_version"`
		Flavor           Flavor          `json:"flavor,omitempty"`
		Database         string          `json:"database"`
		SystemIdentifier string          `json:"system_identifier,omitempty"`
		Timestamp        time.Time       `json:"timestamp"`
		ContentHash      string          `json:"content_hash"`
		Source           *string         `json:"source,omitempty"`
		Tables           []Table         `json:"tables"`
		Enums            []EnumType      `json:"enums"`
		Domains          []DomainType    `json:"domains"`
		Composites       []CompositeType `json:"composites"`
		Views            []View          `json:"views"`
		Functions        []Function      `json:"functions"`
		Extensions       []Extension     `json:"extensions"`
		GUCs             []GucSetting    `json:"gucs"`
	}

	Table struct {
		OID           uint32         `json:"oid"`
		Schema        string         `json:"schema"`
		Name          string         `json:"name"`
		Columns       []Column       `json:"columns"`
		Constraints   []Constraint   `json:"constraints"`
		Indexes       []Index        `json:"indexes"`
		Comment       *string        `json:"comment,omitempty"`
		PartitionInfo *PartitionInfo `json:"partition_info,omitempty"`
		Policies      []RlsPolicy    `json:"policies"`
		Triggers      []Trigger      `json:"triggers"`
		RLSEnabled    bool           `json:"rls_enabled"`
		Reloptions    []string       `json:"reloptions,omitempty"`
	}
)

func (t *Table) Qual() QualifiedName {
	return QualifiedName{Schema: t.Schema, Name: t.Name}
}

type Column struct {
	Name             string  `json:"name"`
	Ordinal          int16   `json:"ordinal"`
	TypeName         string  `json:"type_name"`
	Nullable         bool    `json:"nullable"`
	Default          *string `json:"default,omitempty"`
	Identity         *string `json:"identity,omitempty"`
	Comment          *string `json:"comment,omitempty"`
	StatisticsTarget *int16  `json:"statistics_target,omitempty"`
	Generated        *string `json:"generated,omitempty"`
}

type Constraint struct {
	Name         string         `json:"name"`
	Kind         ConstraintKind `json:"kind"`
	Columns      []string       `json:"columns"`
	Definition   *string        `json:"definition,omitempty"`
	FKTable      *string        `json:"fk_table,omitempty"`
	FKColumns    []string       `json:"fk_columns"`
	BackingIndex *string        `json:"backing_index,omitempty"`
	Comment      *string        `json:"comment,omitempty"`
}

type ConstraintKind string

const (
	ConstraintPrimaryKey ConstraintKind = "primary_key"
	ConstraintForeignKey ConstraintKind = "foreign_key"
	ConstraintUnique     ConstraintKind = "unique"
	ConstraintCheck      ConstraintKind = "check"
	ConstraintExclusion  ConstraintKind = "exclusion"
)

func ConstraintKindFromPg(contype string) (ConstraintKind, bool) {
	switch contype {
	case "p":
		return ConstraintPrimaryKey, true
	case "f":
		return ConstraintForeignKey, true
	case "u":
		return ConstraintUnique, true
	case "c":
		return ConstraintCheck, true
	case "x":
		return ConstraintExclusion, true
	default:
		return "", false
	}
}

type Index struct {
	Name            string   `json:"name"`
	Columns         []string `json:"columns"`
	IncludeColumns  []string `json:"include_columns"`
	IndexType       string   `json:"index_type"`
	IsUnique        bool     `json:"is_unique"`
	IsPrimary       bool     `json:"is_primary"`
	Predicate       *string  `json:"predicate,omitempty"`
	Definition      string   `json:"definition"`
	IsValid         bool     `json:"is_valid"`
	IsReady         bool     `json:"is_ready"`
	BacksConstraint bool     `json:"backs_constraint,omitempty"`
	HasExpressions  bool     `json:"-"`
}

// Column-level stats from pg_stats
type ColumnStats struct {
	NullFrac        *float64 `json:"null_frac,omitempty"`
	NDistinct       *float64 `json:"n_distinct,omitempty"`
	MostCommonVals  *string  `json:"most_common_vals,omitempty"`
	MostCommonFreqs *string  `json:"most_common_freqs,omitempty"`
	HistogramBounds *string  `json:"histogram_bounds,omitempty"`
	Correlation     *float64 `json:"correlation,omitempty"`
	AvgWidth        *int     `json:"avg_width,omitempty"` // pg_stats avg bytes/value; bloat row-width estimate
}

type PartitionInfo struct {
	Strategy PartitionStrategy `json:"strategy"`
	Key      string            `json:"key"`
	Children []PartitionChild  `json:"children"`
}

type PartitionStrategy string

const (
	PartitionRange PartitionStrategy = "range"
	PartitionList  PartitionStrategy = "list"
	PartitionHash  PartitionStrategy = "hash"
)

func PartitionStrategyFromPg(partstrat string) (PartitionStrategy, bool) {
	switch partstrat {
	case "r":
		return PartitionRange, true
	case "l":
		return PartitionList, true
	case "h":
		return PartitionHash, true
	default:
		return "", false
	}
}

type PartitionChild struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Bound  string `json:"bound"`
}

type RlsPolicy struct {
	Name          string   `json:"name"`
	Command       string   `json:"command"`
	Permissive    bool     `json:"permissive"`
	Roles         []string `json:"roles"`
	UsingExpr     *string  `json:"using_expr,omitempty"`
	WithCheckExpr *string  `json:"with_check_expr,omitempty"`
}

type Trigger struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
}

type EnumType struct {
	Schema string   `json:"schema"`
	Name   string   `json:"name"`
	Labels []string `json:"labels"`
}

type DomainType struct {
	Schema           string   `json:"schema"`
	Name             string   `json:"name"`
	BaseType         string   `json:"base_type"`
	Nullable         bool     `json:"nullable"`
	Default          *string  `json:"default,omitempty"`
	CheckConstraints []string `json:"check_constraints"`
}

type CompositeType struct {
	Schema string           `json:"schema"`
	Name   string           `json:"name"`
	Fields []CompositeField `json:"fields"`
}

type CompositeField struct {
	Name     string `json:"name"`
	TypeName string `json:"type_name"`
}

type View struct {
	Schema         string  `json:"schema"`
	Name           string  `json:"name"`
	Definition     string  `json:"definition"`
	IsMaterialized bool    `json:"is_materialized"`
	Comment        *string `json:"comment,omitempty"`
}

type Function struct {
	Schema          string     `json:"schema"`
	Name            string     `json:"name"`
	IdentityArgs    string     `json:"identity_args"`
	ReturnType      string     `json:"return_type"`
	Language        string     `json:"language"`
	Volatility      Volatility `json:"volatility"`
	SecurityDefiner bool       `json:"security_definer"`
	Comment         *string    `json:"comment,omitempty"`
}

type Volatility string

const (
	VolatilityImmutable Volatility = "immutable"
	VolatilityStable    Volatility = "stable"
	VolatilityVolatile  Volatility = "volatile"
)

func VolatilityFromPg(provolatile string) (Volatility, bool) {
	switch provolatile {
	case "i":
		return VolatilityImmutable, true
	case "s":
		return VolatilityStable, true
	case "v":
		return VolatilityVolatile, true
	default:
		return "", false
	}
}

type Extension struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Schema  string `json:"schema"`
}

type GucSetting struct {
	Name    string  `json:"name"`
	Setting string  `json:"setting"`
	Unit    *string `json:"unit,omitempty"`
}

type StaleStatsEntry struct {
	Node                string `json:"node"`
	Schema              string `json:"schema"`
	Table               string `json:"table"`
	LastAnalyzedDaysAgo *int64 `json:"last_analyzed_days_ago,omitempty"`
}

// Skip standbys: pg_statistic replicates via WAL but pg_stat_user_tables timestamps are per-node and autoanalyze never runs there.
func DetectStaleStats(a *AnnotatedSchema, staleDays int64) []StaleStatsEntry {
	if a == nil || a.Merged == nil {
		return nil
	}
	now := time.Now().UTC()
	threshold := time.Duration(staleDays) * 24 * time.Hour
	var entries []StaleStatsEntry

	for _, n := range a.Merged.Nodes {
		if n.Node.IsStandby {
			continue
		}
		for _, ts := range n.Tables {
			var lastAnalyzed *time.Time
			if ts.Activity.LastAnalyze != nil {
				lastAnalyzed = ts.Activity.LastAnalyze
			}
			if ts.Activity.LastAutoanalyze != nil {
				if lastAnalyzed == nil || ts.Activity.LastAutoanalyze.After(*lastAnalyzed) {
					lastAnalyzed = ts.Activity.LastAutoanalyze
				}
			}
			if lastAnalyzed == nil {
				entries = append(entries, StaleStatsEntry{
					Node: n.Node.Source, Schema: ts.Table.Schema, Table: ts.Table.Name,
				})
			} else if now.Sub(*lastAnalyzed) > threshold {
				days := int64(now.Sub(*lastAnalyzed).Hours() / 24)
				entries = append(entries, StaleStatsEntry{
					Node: n.Node.Source, Schema: ts.Table.Schema, Table: ts.Table.Name,
					LastAnalyzedDaysAgo: &days,
				})
			}
		}
	}
	// Worst-first so a downstream cap keeps the most stale; nil = never analyzed = most stale (might revisit later)
	sort.SliceStable(entries, func(i, j int) bool {
		di, dj := entries[i].LastAnalyzedDaysAgo, entries[j].LastAnalyzedDaysAgo
		switch {
		case di == nil && dj == nil:
			return false
		case di == nil:
			return true
		case dj == nil:
			return false
		default:
			return *di > *dj
		}
	})
	return entries
}

// JSON map keys must be strings, so (schema, name) keying uses entry slices
type QualifiedName struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

func (q QualifiedName) String() string {
	if q.Schema == "" {
		return q.Name
	}
	return q.Schema + "." + q.Name
}

// Sizing inputs the planner uses: row estimate, on-disk footprint
type TableSizing struct {
	Reltuples         float64 `json:"reltuples"`
	Relpages          int64   `json:"relpages"`
	TableSize         int64   `json:"table_size"`
	TotalRelationSize int64   `json:"total_relation_size"`
	IndexesSize       int64   `json:"indexes_size"`
	ToastSize         int64   `json:"toast_size,omitempty"`
	// raw relfrozenxid/relminmxid (0 = partitioned parent); stable, so kept in the hash.
	// ages are derived against DatabaseXid/DatabaseMxid, not stored, to stay dedup-stable.
	RelfrozenXid int64 `json:"relfrozenxid,omitempty"`
	RelminMxid   int64 `json:"relminmxid,omitempty"`
}

// age(relfrozenxid) at capture, vs DatabaseXid (next-xid reference).
func (s TableSizing) FrozenXidAge(databaseXid int64) (int64, bool) {
	return counterAge(s.RelfrozenXid, databaseXid)
}

// mxid_age(relminmxid) at capture, vs DatabaseMxid (next-multixact reference).
func (s TableSizing) MinMxidAge(databaseMxid int64) (int64, bool) {
	return counterAge(s.RelminMxid, databaseMxid)
}

// Reproduces pg's age()/mxid_age(): wraparound-aware forward distance in 32-bit counter
// space. ok=false when value 0 (partitioned parent) or reference 0 (pre-feature snapshot).
// Guard stays at 0, not <=1: relminmxid 1 (FirstMultiXactId) is a real ageable value.
func counterAge(value, reference int64) (int64, bool) {
	if value == 0 || reference == 0 {
		return 0, false
	}
	const modulo = int64(1) << 32
	age := (reference%modulo - value%modulo) % modulo
	if age < 0 {
		age += modulo
	}
	return age, true
}

// Counters and vacuum/analyze timestamps from pg_stat_user_tables
type TableActivity struct {
	SeqScan     int64 `json:"seq_scan"`
	SeqTupRead  int64 `json:"seq_tup_read"`
	IdxScan     int64 `json:"idx_scan"`
	IdxTupFetch int64 `json:"idx_tup_fetch"`
	NTupIns     int64 `json:"n_tup_ins"`
	NTupUpd     int64 `json:"n_tup_upd"`
	NTupDel     int64 `json:"n_tup_del"`
	NTupHotUpd  int64 `json:"n_tup_hot_upd"`
	NLiveTup    int64 `json:"n_live_tup"`
	NDeadTup    int64 `json:"n_dead_tup"`
	// additive field; omitempty so snapshots taken before it existed stay
	// hash-compatible (a zero value serializes identically to its absence).
	NModSinceAnalyze int64      `json:"n_mod_since_analyze,omitempty"`
	LastVacuum       *time.Time `json:"last_vacuum,omitempty"`
	LastAutovacuum   *time.Time `json:"last_autovacuum,omitempty"`
	LastAnalyze      *time.Time `json:"last_analyze,omitempty"`
	LastAutoanalyze  *time.Time `json:"last_autoanalyze,omitempty"`
	VacuumCount      int64      `json:"vacuum_count"`
	AutovacuumCount  int64      `json:"autovacuum_count"`
	AnalyzeCount     int64      `json:"analyze_count"`
	AutoanalyzeCount int64      `json:"autoanalyze_count"`
}

type IndexSizing struct {
	Relpages  int64   `json:"relpages"`
	Reltuples float64 `json:"reltuples"`
	Size      int64   `json:"size"`
}

type IndexActivity struct {
	IdxScan     int64 `json:"idx_scan"`
	IdxTupRead  int64 `json:"idx_tup_read"`
	IdxTupFetch int64 `json:"idx_tup_fetch"`
}

// Identifies the node that produced an ActivityStatsSnapshot
type NodeIdentity struct {
	Source    string    `json:"source"`
	Label     *string   `json:"label,omitempty"`
	IsStandby bool      `json:"is_standby"`
	PgVersion string    `json:"pg_version"`
	Timestamp time.Time `json:"timestamp"`
}

type TableSizingEntry struct {
	Table  QualifiedName `json:"table"`
	Sizing TableSizing   `json:"sizing"`
	// see IndexSizingEntry.Bloat
	Bloat *BloatEstimate `json:"bloat,omitempty"`
}

type IndexSizingEntry struct {
	Table  QualifiedName `json:"table"`
	Index  string        `json:"index"`
	Sizing IndexSizing   `json:"sizing"`
	// derived at capture, kept out of the content hash; nil for non-btree/pre-ANALYZE
	Bloat *BloatEstimate `json:"bloat,omitempty"`
}

type BloatEstimate struct {
	BloatRatio    float64 `json:"bloat_ratio"`
	ExpectedPages int64   `json:"expected_pages"`
	ActualPages   int64   `json:"actual_pages"`
	// index-key width for index entries, heap row width for table entries
	AvgTupleWidth int   `json:"avg_tuple_width"`
	SizeBytes     int64 `json:"size_bytes"`
	Approximate   bool  `json:"approximate,omitempty"`
}

type ColumnStatsEntry struct {
	Table  QualifiedName `json:"table"`
	Column string        `json:"column"`
	Stats  ColumnStats   `json:"stats"`
}

type TableActivityEntry struct {
	Table    QualifiedName `json:"table"`
	Activity TableActivity `json:"activity"`
}

type IndexActivityEntry struct {
	Table    QualifiedName `json:"table"`
	Index    string        `json:"index"`
	Activity IndexActivity `json:"activity"`
}

type (
	// Persisted planner inputs; schema_ref_hash binds rows to a SchemaSnapshot
	PlannerStatsSnapshot struct {
		FormatVersion int       `json:"format_version"`
		SchemaRefHash string    `json:"schema_ref_hash"`
		ContentHash   string    `json:"content_hash"`
		Database      string    `json:"database"`
		Timestamp     time.Time `json:"timestamp"`
		// next-xid/next-multixact at capture; reference points for deriving the per-table
		// ages. volatile, so excluded from ComputePlannerContentHash to preserve dedup.
		DatabaseXid  int64              `json:"database_xid,omitempty"`
		DatabaseMxid int64              `json:"database_mxid,omitempty"`
		Tables       []TableSizingEntry `json:"tables"`
		Indexes      []IndexSizingEntry `json:"indexes"`
		Columns      []ColumnStatsEntry `json:"columns"`
		// Hashed here, not on the schema: the schema digest only moves on DDL, so a
		// postgresql.conf change never reaches a reader through it. SchemaSnapshot keeps its
		// own copy, the values as of that generation.
		GUCs    []GucSetting `json:"gucs,omitempty"`
		Masking *MaskingInfo `json:"masking,omitempty"`
	}

	MaskingInfo struct {
		Applied       bool `json:"applied"` // a policy was in effect
		ColumnsMasked int  `json:"columns_masked,omitempty"`
		JSONBStripped bool `json:"jsonb_mcv_stripped"` // always-on capture-time strip, policy-independent
	}
)

type (
	// Persisted per-node activity counters
	ActivityStatsSnapshot struct {
		FormatVersion int                  `json:"format_version"`
		SchemaRefHash string               `json:"schema_ref_hash"`
		ContentHash   string               `json:"content_hash"`
		Node          NodeIdentity         `json:"node"`
		Tables        []TableActivityEntry `json:"tables"`
		Indexes       []IndexActivityEntry `json:"indexes"`
	}

	// One raw pg_stat_statements row, before qshape grouping; hashed from these.
	QueryStatsMember struct {
		QueryID         int64   `json:"queryid"`
		Calls           int64   `json:"calls"`
		TotalExecTimeMs float64 `json:"total_exec_time_ms,omitempty"`
		Rows            int64   `json:"rows,omitempty"`
	}

	// One qshape cluster: an ORM-collapsed query shape with per-node pg_stat_statements rollup
	QueryStatsEntry struct {
		Fingerprint     string             `json:"fingerprint"`
		Canonical       string             `json:"canonical"`
		Members         []QueryStatsMember `json:"members,omitempty"`
		Calls           int64              `json:"calls"`
		TotalExecTimeMs float64            `json:"total_exec_time_ms,omitempty"`
		MeanExecTimeMs  float64            `json:"mean_exec_time_ms,omitempty"`
		Rows            int64              `json:"rows,omitempty"`
	}

	// Persisted per-node pg_stat_statements rollup, fingerprinted via qshape
	QueryStatsSnapshot struct {
		FormatVersion int    `json:"format_version"`
		SchemaRefHash string `json:"schema_ref_hash"`
		ContentHash   string `json:"content_hash"`
		// qshape.GroupingVersion at capture; 0 for rows predating this field.
		QshapeVersion int               `json:"qshape_version,omitempty"`
		Node          NodeIdentity      `json:"node"`
		Queries       []QueryStatsEntry `json:"queries"`
	}
)
