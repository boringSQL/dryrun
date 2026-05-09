package schema

import "time"

// Point-in-time PG schema snapshot
type SchemaSnapshot struct {
	PgVersion   string          `json:"pg_version"`
	Database    string          `json:"database"`
	Timestamp   time.Time       `json:"timestamp"`
	ContentHash string          `json:"content_hash"`
	Source      *string         `json:"source,omitempty"`
	Tables      []Table         `json:"tables"`
	Enums       []EnumType      `json:"enums"`
	Domains     []DomainType    `json:"domains"`
	Composites  []CompositeType `json:"composites"`
	Views       []View          `json:"views"`
	Functions   []Function      `json:"functions"`
	Extensions  []Extension     `json:"extensions"`
	GUCs        []GucSetting    `json:"gucs"`
	NodeStats   []NodeStats     `json:"node_stats,omitempty"`
}

type Table struct {
	OID           uint32         `json:"oid"`
	Schema        string         `json:"schema"`
	Name          string         `json:"name"`
	Columns       []Column       `json:"columns"`
	Constraints   []Constraint   `json:"constraints"`
	Indexes       []Index        `json:"indexes"`
	Comment       *string        `json:"comment,omitempty"`
	Stats         *TableStats    `json:"stats,omitempty"`
	PartitionInfo *PartitionInfo `json:"partition_info,omitempty"`
	Policies      []RlsPolicy    `json:"policies"`
	Triggers      []Trigger      `json:"triggers"`
	RLSEnabled    bool           `json:"rls_enabled"`
	Reloptions    []string       `json:"reloptions,omitempty"`
}

type Column struct {
	Name              string       `json:"name"`
	Ordinal           int16        `json:"ordinal"`
	TypeName          string       `json:"type_name"`
	Nullable          bool         `json:"nullable"`
	Default           *string      `json:"default,omitempty"`
	Identity          *string      `json:"identity,omitempty"`
	Comment           *string      `json:"comment,omitempty"`
	StatisticsTarget  *int16       `json:"statistics_target,omitempty"`
	Generated         *string      `json:"generated,omitempty"`
	Stats             *ColumnStats `json:"stats,omitempty"`
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
	Name            string      `json:"name"`
	Columns         []string    `json:"columns"`
	IncludeColumns  []string    `json:"include_columns"`
	IndexType       string      `json:"index_type"`
	IsUnique        bool        `json:"is_unique"`
	IsPrimary       bool        `json:"is_primary"`
	Predicate       *string     `json:"predicate,omitempty"`
	Definition      string      `json:"definition"`
	IsValid         bool        `json:"is_valid"`
	BacksConstraint bool        `json:"backs_constraint,omitempty"`
	Stats           *IndexStats `json:"stats,omitempty"`
}

type IndexStats struct {
	IdxScan     int64   `json:"idx_scan"`
	IdxTupRead  int64   `json:"idx_tup_read"`
	IdxTupFetch int64   `json:"idx_tup_fetch"`
	Size        int64   `json:"size"`
	Relpages    int64   `json:"relpages"`
	Reltuples   float64 `json:"reltuples"`
}

// Table-level stats from pg_stat_user_tables
type TableStats struct {
	Reltuples       float64    `json:"reltuples"`
	Relpages        int64      `json:"relpages"`
	DeadTuples      int64      `json:"dead_tuples"`
	LastVacuum      *time.Time `json:"last_vacuum,omitempty"`
	LastAutovacuum  *time.Time `json:"last_autovacuum,omitempty"`
	LastAnalyze     *time.Time `json:"last_analyze,omitempty"`
	LastAutoanalyze *time.Time `json:"last_autoanalyze,omitempty"`
	SeqScan         int64      `json:"seq_scan"`
	IdxScan         int64      `json:"idx_scan"`
	TableSize       int64      `json:"table_size"`
}

// Column-level stats from pg_stats
type ColumnStats struct {
	NullFrac        *float64 `json:"null_frac,omitempty"`
	NDistinct       *float64 `json:"n_distinct,omitempty"`
	MostCommonVals  *string  `json:"most_common_vals,omitempty"`
	MostCommonFreqs *string  `json:"most_common_freqs,omitempty"`
	HistogramBounds *string  `json:"histogram_bounds,omitempty"`
	Correlation     *float64 `json:"correlation,omitempty"`
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

// Per-node stats for multi-node setups
type NodeStats struct {
	Source      string            `json:"source"`
	Timestamp   time.Time         `json:"timestamp"`
	IsStandby   bool              `json:"is_standby,omitempty"`
	TableStats  []NodeTableStats  `json:"table_stats"`
	IndexStats  []NodeIndexStats  `json:"index_stats"`
	ColumnStats []NodeColumnStats `json:"column_stats,omitempty"`
}

type NodeTableStats struct {
	Schema string     `json:"schema"`
	Table  string     `json:"table"`
	Stats  TableStats `json:"stats"`
}

type NodeIndexStats struct {
	Schema    string     `json:"schema"`
	Table     string     `json:"table"`
	IndexName string     `json:"index_name"`
	Stats     IndexStats `json:"stats"`
}

type NodeColumnStats struct {
	Schema string      `json:"schema"`
	Table  string      `json:"table"`
	Column string      `json:"column"`
	Stats  ColumnStats `json:"stats"`
}

func AggregateTableStats(nodeStats []NodeStats, schemaName, tableName string) *TableStats {
	var matching []*TableStats
	for i := range nodeStats {
		for j := range nodeStats[i].TableStats {
			nts := &nodeStats[i].TableStats[j]
			if nts.Schema == schemaName && nts.Table == tableName {
				matching = append(matching, &nts.Stats)
			}
		}
	}
	if len(matching) == 0 {
		return nil
	}

	result := &TableStats{}
	for _, s := range matching {
		if s.Reltuples > result.Reltuples {
			result.Reltuples = s.Reltuples
		}
		if s.Relpages > result.Relpages {
			result.Relpages = s.Relpages
		}
		if s.DeadTuples > result.DeadTuples {
			result.DeadTuples = s.DeadTuples
		}
		result.SeqScan += s.SeqScan
		result.IdxScan += s.IdxScan
		if s.TableSize > result.TableSize {
			result.TableSize = s.TableSize
		}
	}

	// vacuum/analyze timestamps come only from primaries, standbys don't run autovacuum
	maxTime := func(a, b *time.Time) *time.Time {
		if a == nil {
			return b
		}
		if b == nil {
			return a
		}
		if b.After(*a) {
			return b
		}
		return a
	}
	for i := range nodeStats {
		if nodeStats[i].IsStandby {
			continue
		}
		for j := range nodeStats[i].TableStats {
			nts := &nodeStats[i].TableStats[j]
			if nts.Schema != schemaName || nts.Table != tableName {
				continue
			}
			result.LastVacuum = maxTime(result.LastVacuum, nts.Stats.LastVacuum)
			result.LastAutovacuum = maxTime(result.LastAutovacuum, nts.Stats.LastAutovacuum)
			result.LastAnalyze = maxTime(result.LastAnalyze, nts.Stats.LastAnalyze)
			result.LastAutoanalyze = maxTime(result.LastAutoanalyze, nts.Stats.LastAutoanalyze)
		}
	}
	return result
}

type StaleStatsEntry struct {
	Node                string `json:"node"`
	Schema              string `json:"schema"`
	Table               string `json:"table"`
	LastAnalyzedDaysAgo *int64 `json:"last_analyzed_days_ago,omitempty"`
}

func DetectStaleStats(nodeStats []NodeStats, staleDays int64) []StaleStatsEntry {
	now := time.Now().UTC()
	threshold := time.Duration(staleDays) * 24 * time.Hour
	var entries []StaleStatsEntry

	for _, ns := range nodeStats {
		for _, ts := range ns.TableStats {
			var lastAnalyzed *time.Time
			if ts.Stats.LastAnalyze != nil {
				lastAnalyzed = ts.Stats.LastAnalyze
			}
			if ts.Stats.LastAutoanalyze != nil {
				if lastAnalyzed == nil || ts.Stats.LastAutoanalyze.After(*lastAnalyzed) {
					lastAnalyzed = ts.Stats.LastAutoanalyze
				}
			}

			if lastAnalyzed == nil {
				entries = append(entries, StaleStatsEntry{
					Node: ns.Source, Schema: ts.Schema, Table: ts.Table,
				})
			} else if now.Sub(*lastAnalyzed) > threshold {
				days := int64(now.Sub(*lastAnalyzed).Hours() / 24)
				entries = append(entries, StaleStatsEntry{
					Node: ns.Source, Schema: ts.Schema, Table: ts.Table,
					LastAnalyzedDaysAgo: &days,
				})
			}
		}
	}
	return entries
}

// Returns aggregated multi-node stats, else table-level stats
func EffectiveTableStats(t *Table, snap *SchemaSnapshot) *TableStats {
	if len(snap.NodeStats) > 0 {
		if agg := AggregateTableStats(snap.NodeStats, t.Schema, t.Name); agg != nil {
			return agg
		}
	}
	return t.Stats
}
