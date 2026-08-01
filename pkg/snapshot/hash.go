package snapshot

import (
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
)

// The digest algorithm is a wire contract: derive it from the doc, not from this build.
func DigestFor(snap *SchemaSnapshot) string {
	if snap.FormatVersion >= 2 {
		return ComputeContentHashV2(snap)
	}
	return ComputeStructuralHash(snap)
}

// SHA-256 over DDL-relevant fields only, runtime stats are stripped
func ComputeStructuralHash(snap *SchemaSnapshot) string {
	return hashSchema(snap, false)
}

// Legacy (format_version <= 1) digest; new call sites want DigestFor.
func ComputeContentHash(snap *SchemaSnapshot) string {
	return ComputeStructuralHash(snap)
}

// Structural + reloptions. v1 hashed a settings-only ALTER identically, so the new body
// deduped away and vacuum advice kept reading reloptions frozen at the last DDL change.
func ComputeContentHashV2(snap *SchemaSnapshot) string {
	return hashSchema(snap, true)
}

func hashSchema(snap *SchemaSnapshot, withReloptions bool) string {
	tables := make([]any, len(snap.Tables))
	for i := range snap.Tables {
		tables[i] = tableToStructural(&snap.Tables[i], withReloptions)
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

func tableToStructural(t *Table, withReloptions bool) map[string]any {
	cols := make([]map[string]any, len(t.Columns))
	for i := range t.Columns {
		cols[i] = columnToStructural(&t.Columns[i])
	}

	m := map[string]any{
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
	// Omitted, not null, when empty: a table with no storage params keeps its v1 bytes.
	// Sort a copy; pg_class.reloptions is in set-order, which is not identity.
	if withReloptions && len(t.Reloptions) > 0 {
		opts := slices.Clone(t.Reloptions)
		slices.Sort(opts)
		m["reloptions"] = opts
	}
	return m
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
	// Omitted, not null, when absent: planner docs captured before GUCs moved here keep
	// their digest.
	if len(p.GUCs) > 0 {
		canonical["gucs"] = p.GUCs
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

// cmpOptional orders an optional counter, with unknown sorting before any value.
func cmpOptional(a, b *int64) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	return cmp.Compare(*a, *b)
}

// Per-node query stats; node.source included so two nodes never collide.
// Digested from raw pg_stat_statements rows ordered by queryid, so a qshape
// upgrade doesn't move the digest.
func ComputeQueryStatsContentHash(q *QueryStatsSnapshot) string {
	var rows []QueryStatsMember
	for _, e := range q.Queries {
		rows = append(rows, e.Members...)
	}
	// sort on the full tuple: queryid alone isn't unique, and an unstable
	// order would move the digest for identical content
	slices.SortFunc(rows, func(a, b QueryStatsMember) int {
		if a.QueryID != b.QueryID {
			return cmp.Compare(a.QueryID, b.QueryID)
		}
		if a.Calls != b.Calls {
			return cmp.Compare(a.Calls, b.Calls)
		}
		if a.TotalExecTimeMs != b.TotalExecTimeMs {
			return cmp.Compare(a.TotalExecTimeMs, b.TotalExecTimeMs)
		}
		if a.Rows != b.Rows {
			return cmp.Compare(a.Rows, b.Rows)
		}
		// Temp blocks break the remaining ties. Two members identical in every other
		// counter but differing here would otherwise sort unstably, and an unstable
		// order moves the digest for identical content.
		if c := cmpOptional(a.TempBlksRead, b.TempBlksRead); c != 0 {
			return c
		}
		return cmpOptional(a.TempBlksWritten, b.TempBlksWritten)
	})

	canonical := map[string]any{
		"schema_ref_hash": q.SchemaRefHash,
		"node_source":     q.Node.Source,
		"queries":         rows,
	}
	b, _ := json.Marshal(canonical)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h)
}
