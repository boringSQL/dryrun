package snapshot

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// SHA-256 over DDL-relevant fields only, runtime stats are stripped
func ComputeContentHash(snap *SchemaSnapshot) string {
	tables := make([]any, len(snap.Tables))
	for i := range snap.Tables {
		tables[i] = tableToStructural(&snap.Tables[i])
	}

	canonical := map[string]any{
		"pg_version": snap.PgVersion,
		"tables":     tables,
		"enums":      snap.Enums,
		"domains":    snap.Domains,
		"composites": snap.Composites,
		"views":      snap.Views,
		"functions":  snap.Functions,
		"extensions": snap.Extensions,
	}

	b, _ := json.Marshal(canonical)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h)
}

func tableToStructural(t *Table) map[string]any {
	cols := make([]map[string]any, len(t.Columns))
	for i := range t.Columns {
		cols[i] = columnToStructural(&t.Columns[i])
	}

	return map[string]any{
		"schema":         t.Schema,
		"name":           t.Name,
		"columns":        cols,
		"constraints":    t.Constraints,
		"indexes":        t.Indexes,
		"comment":        t.Comment,
		"partition_info": t.PartitionInfo,
		"policies":       t.Policies,
		"triggers":       t.Triggers,
		"rls_enabled":    t.RLSEnabled,
	}
}

func columnToStructural(c *Column) map[string]any {
	return map[string]any{
		"name":              c.Name,
		"ordinal":           c.Ordinal,
		"type_name":         c.TypeName,
		"nullable":          c.Nullable,
		"default":           c.Default,
		"identity":          c.Identity,
		"comment":           c.Comment,
		"statistics_target": c.StatisticsTarget,
		"generated":         c.Generated,
	}
}

// SHA-256 over the captured planner data; schema_ref binds to the DDL snapshot.
// Bloat projected out (derived from sizing+schema); hashing it would break
// dedup against older docs.
func ComputePlannerContentHash(p *PlannerStatsSnapshot) string {
	tables := make([]tableSizingStructural, len(p.Tables))
	for i := range p.Tables {
		tables[i] = tableSizingStructural{
			Table:  p.Tables[i].Table,
			Sizing: p.Tables[i].Sizing,
		}
	}
	indexes := make([]indexSizingStructural, len(p.Indexes))
	for i := range p.Indexes {
		indexes[i] = indexSizingStructural{
			Table:  p.Indexes[i].Table,
			Index:  p.Indexes[i].Index,
			Sizing: p.Indexes[i].Sizing,
		}
	}

	canonical := map[string]any{
		"schema_ref_hash": p.SchemaRefHash,
		"tables":          tables,
		"indexes":         indexes,
		"columns":         p.Columns,
	}
	b, _ := json.Marshal(canonical)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h)
}

// *SizingEntry minus Bloat, same field order so bytes match pre-bloat snapshots.
type (
	tableSizingStructural struct {
		Table  QualifiedName `json:"table"`
		Sizing TableSizing   `json:"sizing"`
	}

	indexSizingStructural struct {
		Table  QualifiedName `json:"table"`
		Index  string        `json:"index"`
		Sizing IndexSizing   `json:"sizing"`
	}
)

// Per-node activity; node.source included so two nodes never collide
func ComputeActivityContentHash(a *ActivityStatsSnapshot) string {
	canonical := map[string]any{
		"schema_ref_hash": a.SchemaRefHash,
		"node_source":     a.Node.Source,
		"tables":          a.Tables,
		"indexes":         a.Indexes,
	}
	b, _ := json.Marshal(canonical)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h)
}
