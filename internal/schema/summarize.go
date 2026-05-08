package schema

import (
	"fmt"
	"sort"
)

// Per-table summary aggregated across all nodes
type TableSummary struct {
	Schema       string         `json:"schema"`
	Table        string         `json:"table"`
	TotalSeqScan int64          `json:"total_seq_scan"`
	TotalIdxScan int64          `json:"total_idx_scan"`
	PerNodeSeq   []NodeSeqEntry `json:"per_node_seq"`
}

type NodeSeqEntry struct {
	Source  string `json:"source"`
	SeqScan int64  `json:"seq_scan"`
}

func SummarizeTableStats(nodeStats []NodeStats) []TableSummary {
	type key struct{ schema, table string }
	agg := make(map[key]*TableSummary)
	var order []key

	for _, ns := range nodeStats {
		for _, ts := range ns.TableStats {
			k := key{ts.Schema, ts.Table}
			s, ok := agg[k]
			if !ok {
				s = &TableSummary{Schema: ts.Schema, Table: ts.Table}
				agg[k] = s
				order = append(order, k)
			}
			s.TotalSeqScan += ts.Stats.SeqScan
			s.TotalIdxScan += ts.Stats.IdxScan
			s.PerNodeSeq = append(s.PerNodeSeq, NodeSeqEntry{Source: ns.Source, SeqScan: ts.Stats.SeqScan})
		}
	}

	result := make([]TableSummary, 0, len(order))
	for _, k := range order {
		result = append(result, *agg[k])
	}
	return result
}

type TableFlag string

const (
	FlagHighSeqIdxRatio TableFlag = "high_seq_idx_ratio"
	FlagSeqScanOnly     TableFlag = "seq_scan_only"
	FlagNodeImbalance   TableFlag = "node_imbalance"
)

func DetectTableFlags(summary *TableSummary, nodeStats []NodeStats) []TableFlag {
	var flags []TableFlag

	if summary.TotalSeqScan > 100 && summary.TotalIdxScan > 0 {
		ratio := float64(summary.TotalSeqScan) / float64(summary.TotalIdxScan)
		if ratio > 0.5 {
			flags = append(flags, FlagHighSeqIdxRatio)
		}
	} else if summary.TotalSeqScan > 100 && summary.TotalIdxScan == 0 {
		flags = append(flags, FlagSeqScanOnly)
	}

	if DetectSeqScanImbalance(nodeStats, summary.Schema, summary.Table) != nil {
		flags = append(flags, FlagNodeImbalance)
	}

	return flags
}

type NodeImbalanceInfo struct {
	HotNode    string `json:"hot_node"`
	Multiplier int64  `json:"multiplier"`
}

// Flags when one node carries disproportionate seq_scans
func DetectSeqScanImbalance(nodeStats []NodeStats, schemaName, tableName string) *NodeImbalanceInfo {
	type entry struct {
		source  string
		seqScan int64
	}
	var entries []entry
	for _, ns := range nodeStats {
		for _, ts := range ns.TableStats {
			if ts.Schema == schemaName && ts.Table == tableName {
				entries = append(entries, entry{ns.Source, ts.Stats.SeqScan})
			}
		}
	}
	if len(entries) < 2 {
		return nil
	}

	var nonzero []entry
	for _, e := range entries {
		if e.seqScan > 0 {
			nonzero = append(nonzero, e)
		}
	}
	if len(nonzero) < 2 {
		return nil
	}

	sort.Slice(nonzero, func(i, j int) bool { return nonzero[i].seqScan < nonzero[j].seqScan })
	minVal := nonzero[0].seqScan
	maxEntry := nonzero[len(nonzero)-1]

	if minVal > 0 && maxEntry.seqScan/minVal >= 5 {
		return &NodeImbalanceInfo{
			HotNode:    maxEntry.source,
			Multiplier: maxEntry.seqScan / minVal,
		}
	}
	return nil
}

type UnusedIndexEntry struct {
	Schema         string `json:"schema"`
	Table          string `json:"table"`
	IndexName      string `json:"index_name"`
	TotalIdxScan   int64  `json:"total_idx_scan"`
	TotalSizeBytes int64  `json:"total_size_bytes"`
	IsUnique       bool   `json:"is_unique"`
	Definition     string `json:"definition"`
}

func DetectUnusedIndexes(nodeStats []NodeStats, tables []Table) []UnusedIndexEntry {
	var entries []UnusedIndexEntry

	if len(nodeStats) == 0 {
		// single-node fallback
		for _, t := range tables {
			for _, idx := range t.Indexes {
				if idx.IsPrimary {
					continue
				}
				if idx.Stats != nil && idx.Stats.IdxScan == 0 {
					entries = append(entries, UnusedIndexEntry{
						Schema: t.Schema, Table: t.Name, IndexName: idx.Name,
						TotalSizeBytes: idx.Stats.Size, IsUnique: idx.IsUnique,
						Definition: idx.Definition,
					})
				}
			}
		}
	} else {
		// multi-node: aggregate
		type idxKey struct{ schema, table, name string }
		type agg struct {
			totalScan int64
			maxSize   int64
		}
		aggMap := make(map[idxKey]*agg)
		for _, ns := range nodeStats {
			for _, is := range ns.IndexStats {
				k := idxKey{is.Schema, is.Table, is.IndexName}
				a, ok := aggMap[k]
				if !ok {
					a = &agg{}
					aggMap[k] = a
				}
				a.totalScan += is.Stats.IdxScan
				if is.Stats.Size > a.maxSize {
					a.maxSize = is.Stats.Size
				}
			}
		}

		idxLookup := make(map[string]*Index)
		for i := range tables {
			for j := range tables[i].Indexes {
				key := fmt.Sprintf("%s.%s.%s", tables[i].Schema, tables[i].Name, tables[i].Indexes[j].Name)
				idxLookup[key] = &tables[i].Indexes[j]
			}
		}

		for k, a := range aggMap {
			if a.totalScan != 0 {
				continue
			}
			lookupKey := fmt.Sprintf("%s.%s.%s", k.schema, k.table, k.name)
			idx := idxLookup[lookupKey]
			if idx != nil && idx.IsPrimary {
				continue
			}

			e := UnusedIndexEntry{
				Schema: k.schema, Table: k.table, IndexName: k.name,
				TotalSizeBytes: a.maxSize,
			}
			if idx != nil {
				e.IsUnique = idx.IsUnique
				e.Definition = idx.Definition
			}
			entries = append(entries, e)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TotalSizeBytes > entries[j].TotalSizeBytes
	})
	return entries
}

type BloatedIndexEntry struct {
	Schema        string  `json:"schema"`
	Table         string  `json:"table"`
	IndexName     string  `json:"index_name"`
	BloatRatio    float64 `json:"bloat_ratio"`
	ActualPages   int64   `json:"actual_pages"`
	ExpectedPages int64   `json:"expected_pages"`
	ActualSize    int64   `json:"actual_size_bytes"`
	IndexType     string  `json:"index_type"`
}

func DetectBloatedIndexes(nodeStats []NodeStats, tables []Table, threshold float64) []BloatedIndexEntry {
	var entries []BloatedIndexEntry

	if len(nodeStats) == 0 {
		for _, t := range tables {
			for _, idx := range t.Indexes {
				est, ok := EstimateIndexBloat(idx, t)
				if !ok {
					continue
				}
				if est.BloatRatio > threshold {
					var size int64
					if idx.Stats != nil {
						size = idx.Stats.Size
					}
					entries = append(entries, BloatedIndexEntry{
						Schema: t.Schema, Table: t.Name, IndexName: idx.Name,
						BloatRatio: est.BloatRatio, ActualPages: est.ActualPages,
						ExpectedPages: est.ExpectedPages, ActualSize: size,
						IndexType: idx.IndexType,
					})
				}
			}
		}
	} else {
		// table lookup for column type resolution
		type tblKey struct{ schema, table string }
		tblMap := make(map[tblKey]*Table)
		for i := range tables {
			tblMap[tblKey{tables[i].Schema, tables[i].Name}] = &tables[i]
		}

		// max bloat per index across nodes
		type idxKey struct{ schema, table, name string }
		best := make(map[idxKey]*BloatedIndexEntry)

		for _, ns := range nodeStats {
			for _, is := range ns.IndexStats {
				t := tblMap[tblKey{is.Schema, is.Table}]
				if t == nil {
					continue
				}
				// find index definition for column names and type
				var idxDef *Index
				for j := range t.Indexes {
					if t.Indexes[j].Name == is.IndexName {
						idxDef = &t.Indexes[j]
						break
					}
				}
				if idxDef == nil {
					continue
				}

				est, ok := EstimateIndexBloatFromStats(is.Stats, idxDef.Columns, *t, idxDef.IndexType)
				if !ok || est.BloatRatio <= threshold {
					continue
				}

				k := idxKey{is.Schema, is.Table, is.IndexName}
				if prev, exists := best[k]; !exists || est.BloatRatio > prev.BloatRatio {
					best[k] = &BloatedIndexEntry{
						Schema: is.Schema, Table: is.Table, IndexName: is.IndexName,
						BloatRatio: est.BloatRatio, ActualPages: est.ActualPages,
						ExpectedPages: est.ExpectedPages, ActualSize: is.Stats.Size,
						IndexType: idxDef.IndexType,
					}
				}
			}
		}

		for _, e := range best {
			entries = append(entries, *e)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].BloatRatio > entries[j].BloatRatio
	})
	return entries
}
