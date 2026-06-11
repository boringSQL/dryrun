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

// SHA-256 over the captured planner data; schema_ref binds to the DDL snapshot
func ComputePlannerContentHash(p *PlannerStatsSnapshot) string {
	canonical := map[string]any{
		"schema_ref_hash": p.SchemaRefHash,
		"tables":          p.Tables,
		"indexes":         p.Indexes,
		"columns":         p.Columns,
	}
	b, _ := json.Marshal(canonical)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h)
}

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
