package mcp

import (
	"strings"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/bloat"
)

const (
	findingsBloatThreshold = 4.0
	findingsStaleDays      = 7
)

// What detect would report about one table, cheaply enough to run on every
// describe_table (~225us). Stale stats stays whole-snapshot: its rules are
// per node, and scoping would duplicate the standby skip and day arithmetic.
func tableFindings(a *schema.AnnotatedSchema, t *schema.Table) []string {
	if a == nil || a.Schema == nil || t == nil {
		return nil
	}
	qual := t.Qual()
	var kinds []string

	if len(filterByQual(schema.DetectStaleStats(a, findingsStaleDays), qual.Schema, qual.Name, staleKey)) > 0 {
		kinds = append(kinds, "stale_stats")
	}
	// no activity capture means missing data, not a finding
	if a.Merged != nil && hasUnusedIndex(a, t) {
		kinds = append(kinds, "unused_indexes")
	}
	if hasBloatedIndex(a, t) {
		kinds = append(kinds, "bloated_indexes")
	}
	if b := a.TableBloatFor(qual); b != nil && b.BloatRatio > findingsBloatThreshold {
		kinds = append(kinds, "bloated_tables")
	}
	if len(tableFlags(a, qual)) > 0 {
		kinds = append(kinds, "anomalies")
	}
	return kinds
}

// The DetectUnusedIndexes test scoped to one table's indexes.
func hasUnusedIndex(a *schema.AnnotatedSchema, t *schema.Table) bool {
	qual := t.Qual()
	for _, idx := range t.Indexes {
		if !idx.IsPrimary && a.TotalIndexScans(qual, idx.Name) == 0 {
			return true
		}
	}
	return false
}

func hasBloatedIndex(a *schema.AnnotatedSchema, t *schema.Table) bool {
	qual := t.Qual()
	for _, idx := range t.Indexes {
		sz := a.IndexSizingFor(qual, idx.Name)
		if sz == nil {
			continue
		}
		est, ok := bloat.EstimateIndexBloat(*sz, idx.Columns, idx.IncludeColumns, *t, idx.IndexType)
		if ok && est.BloatRatio > findingsBloatThreshold {
			return true
		}
	}
	return false
}

// One table's activity summary and flags, aggregated like SummarizeTableStats.
func tableFlags(a *schema.AnnotatedSchema, qual schema.QualifiedName) []schema.TableFlag {
	if a.Merged == nil {
		return nil
	}
	summary := schema.TableSummary{Schema: qual.Schema, Table: qual.Name}
	var seen bool
	for _, n := range a.Merged.Nodes {
		for _, ts := range n.Tables {
			if ts.Table != qual {
				continue
			}
			seen = true
			summary.TotalSeqScan += ts.Activity.SeqScan
			summary.TotalIdxScan += ts.Activity.IdxScan
		}
	}
	if !seen {
		return nil
	}
	return schema.DetectTableFlags(&summary, a, schema.BuildQueryRefIndex(a))
}

func findingsHint(kinds []string) string {
	if len(kinds) == 0 {
		return ""
	}
	return "detect reports " + strings.Join(kinds, ", ") + " for this table."
}

// One kind when that is all there is, "all" otherwise.
func findingsKind(kinds []string) string {
	if len(kinds) == 1 {
		return kinds[0]
	}
	return "all"
}
