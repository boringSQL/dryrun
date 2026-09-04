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
	switch {
	case snap.FormatVersion >= 3:
		return ComputeContentHashV3(snap)
	case snap.FormatVersion == 2:
		return ComputeContentHashV2(snap)
	}
	return ComputeStructuralHash(snap)
}

type (
	// Per-generation digest coverage; a doc hashes under its stamped generation.
	digestOpts struct {
		reloptions        bool // v2+
		stripPartChildren bool // v3+
	}

	// PartitionInfo minus Children, which churn on every partition create/drop — the
	// same reason indexStructural drops Index.Children. v3+ only: stripping it moves
	// the digest of every partitioned table.
	partitionStructural struct {
		Strategy PartitionStrategy `json:"strategy"`
		Key      string            `json:"key"`
	}
)

// SHA-256 over DDL-relevant fields only, runtime stats are stripped
func ComputeStructuralHash(snap *SchemaSnapshot) string {
	return hashSchema(snap, digestOpts{})
}

// Legacy (format_version <= 1) digest; new call sites want DigestFor.
func ComputeContentHash(snap *SchemaSnapshot) string {
	return ComputeStructuralHash(snap)
}

// Structural + reloptions. v1 hashed a settings-only ALTER identically, so the new body
// deduped away and vacuum advice kept reading reloptions frozen at the last DDL change.
func ComputeContentHashV2(snap *SchemaSnapshot) string {
	return hashSchema(snap, digestOpts{reloptions: true})
}

// v2 minus partition children, so a pg_partman rotation no longer re-hashes the
// schema. Children are dropped both from the parent's PartitionInfo.Children and
// from tables[] (introspect.sql takes relkind 'r' and 'p' without filtering
// relispartition). Tradeoff, as with indexStructural: child-local DDL no longer
// moves the digest.
func ComputeContentHashV3(snap *SchemaSnapshot) string {
	return hashSchema(snap, digestOpts{reloptions: true, stripPartChildren: true})
}

func hashSchema(snap *SchemaSnapshot, opts digestOpts) string {
	var children map[QualifiedName]bool
	if opts.stripPartChildren {
		children = partitionChildTables(snap)
	}
	tables := make([]any, 0, len(snap.Tables))
	for i := range snap.Tables {
		if children[QualifiedName{Schema: snap.Tables[i].Schema, Name: snap.Tables[i].Name}] {
			continue
		}
		tables = append(tables, tableToStructural(&snap.Tables[i], opts))
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

func tableToStructural(t *Table, opts digestOpts) map[string]any {
	cols := make([]map[string]any, len(t.Columns))
	for i := range t.Columns {
		cols[i] = columnToStructural(&t.Columns[i])
	}
	indexes := make([]indexStructural, len(t.Indexes))
	for i := range t.Indexes {
		indexes[i] = indexToStructural(&t.Indexes[i])
	}

	m := map[string]any{
		"schema":         t.Schema,
		"name":           t.Name,
		"columns":        cols,
		"constraints":    t.Constraints,
		"indexes":        indexes,
		"comment":        t.Comment,
		"partition_info": partitionForDigest(t.PartitionInfo, opts.stripPartChildren),
		"policies":       t.Policies,
		"triggers":       t.Triggers,
		"rls_enabled":    t.RLSEnabled,
	}
	// Omitted, not null, when empty: a table with no storage params keeps its v1 bytes.
	// Sort a copy; pg_class.reloptions is in set-order, which is not identity.
	if opts.reloptions && len(t.Reloptions) > 0 {
		ro := slices.Clone(t.Reloptions)
		slices.Sort(ro)
		m["reloptions"] = ro
	}
	return m
}

// Tables that are some other table's partition, derived from the doc alone so a
// stored snapshot re-hashes to the same digest.
func partitionChildTables(snap *SchemaSnapshot) map[QualifiedName]bool {
	var out map[QualifiedName]bool
	for i := range snap.Tables {
		pi := snap.Tables[i].PartitionInfo
		if pi == nil {
			continue
		}
		for _, c := range pi.Children {
			if out == nil {
				out = make(map[QualifiedName]bool)
			}
			out[QualifiedName{Schema: c.Schema, Name: c.Name}] = true
		}
	}
	return out
}

// Keeps the typed nil when absent, so an unpartitioned table still hashes
// "partition_info":null under every generation.
func partitionForDigest(pi *PartitionInfo, strip bool) any {
	if pi == nil || !strip {
		return pi
	}
	return partitionStructural{Strategy: pi.Strategy, Key: pi.Key}
}

func columnToStructural(c *Column) map[string]any {
	m := map[string]any{
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
	// keyed only when set: a generation expression is DDL and must move the
	// digest (ALTER ... SET EXPRESSION, PG17), but adding the key
	// unconditionally would move every ordinary column's digest too
	if c.GenerationExpr != nil {
		m["generation_expr"] = *c.GenerationExpr
	}
	return m
}

// Index minus Children, which changes as partitions are created/dropped and
// would otherwise move every partitioned index's table digest. Field order
// mirrors Index so tables without partitioned indexes hash identically.
type indexStructural struct {
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
}

func indexToStructural(idx *Index) indexStructural {
	return indexStructural{
		Name:            idx.Name,
		Columns:         idx.Columns,
		IncludeColumns:  idx.IncludeColumns,
		IndexType:       idx.IndexType,
		IsUnique:        idx.IsUnique,
		IsPrimary:       idx.IsPrimary,
		Predicate:       idx.Predicate,
		Definition:      idx.Definition,
		IsValid:         idx.IsValid,
		IsReady:         idx.IsReady,
		BacksConstraint: idx.BacksConstraint,
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

// Per-node activity; node.source included so two nodes never collide. Database-scoped
// sections join the canonical map only when present — an unconditional key would move
// every historical digest (TestActivityContentHash_OmitsAbsentFieldsSoOldDigestsSurvive).
// Deploy cloud before any CLI carrying a new section, per CLAUDE.md's wire-addition rule.
func ComputeActivityContentHash(a *ActivityStatsSnapshot) string {
	canonical := map[string]any{
		"schema_ref_hash": a.SchemaRefHash,
		"node_source":     a.Node.Source,
		"tables":          a.Tables,
		"indexes":         a.Indexes,
	}
	if a.Database != nil {
		canonical["database"] = a.Database
	}
	// Gated on ReadOK: "checked, zero slots" is content, distinct from "never checked".
	// The slice is hashed only when non-empty — omitempty drops an empty slice from the
	// wire, so hashing it would break digest reproducibility after a decode round-trip.
	if a.ReplicationSlotsReadOK != nil {
		canonical["replication_slots_read_ok"] = *a.ReplicationSlotsReadOK
		if len(a.ReplicationSlots) > 0 {
			canonical["replication_slots"] = a.ReplicationSlots
		}
	}
	if a.ReplicationPeersReadOK != nil {
		canonical["replication_peers_read_ok"] = *a.ReplicationPeersReadOK
		if len(a.ReplicationPeers) > 0 {
			canonical["replication_peers"] = a.ReplicationPeers
		}
	}
	if a.Checkpointer != nil {
		canonical["checkpointer"] = a.Checkpointer
	}
	b, _ := json.Marshal(canonical)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h)
}

func cmpOptionalFloat(a, b *float64) int {
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
		if c := cmpOptional(a.TempBlksWritten, b.TempBlksWritten); c != 0 {
			return c
		}
		if c := cmpOptional(a.SharedBlksHit, b.SharedBlksHit); c != 0 {
			return c
		}
		if c := cmpOptional(a.SharedBlksRead, b.SharedBlksRead); c != 0 {
			return c
		}
		if c := cmpOptional(a.SharedBlksDirtied, b.SharedBlksDirtied); c != 0 {
			return c
		}
		if c := cmpOptional(a.SharedBlksWritten, b.SharedBlksWritten); c != 0 {
			return c
		}
		if c := cmpOptionalFloat(a.SharedBlkReadTimeMs, b.SharedBlkReadTimeMs); c != 0 {
			return c
		}
		if c := cmpOptionalFloat(a.SharedBlkWriteTimeMs, b.SharedBlkWriteTimeMs); c != 0 {
			return c
		}
		return cmpOptionalFloat(a.StddevExecTimeMs, b.StddevExecTimeMs)
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
