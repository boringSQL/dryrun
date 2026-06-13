package schema

import (
	"sort"

	"github.com/boringsql/dryrun/pkg/bloat"
)

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

func SummarizeTableStats(a *AnnotatedSchema) []TableSummary {
	if a == nil || a.Merged == nil {
		return nil
	}
	type key struct{ schema, table string }
	agg := make(map[key]*TableSummary)
	var order []key

	for _, n := range a.Merged.Nodes {
		for _, ts := range n.Tables {
			k := key{ts.Table.Schema, ts.Table.Name}
			s, ok := agg[k]
			if !ok {
				s = &TableSummary{Schema: ts.Table.Schema, Table: ts.Table.Name}
				agg[k] = s
				order = append(order, k)
			}
			s.TotalSeqScan += ts.Activity.SeqScan
			s.TotalIdxScan += ts.Activity.IdxScan
			s.PerNodeSeq = append(s.PerNodeSeq, NodeSeqEntry{Source: n.Node.Source, SeqScan: ts.Activity.SeqScan})
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

func DetectTableFlags(summary *TableSummary, a *AnnotatedSchema) []TableFlag {
	var flags []TableFlag

	if summary.TotalSeqScan > 100 && summary.TotalIdxScan > 0 {
		ratio := float64(summary.TotalSeqScan) / float64(summary.TotalIdxScan)
		if ratio > 0.5 {
			flags = append(flags, FlagHighSeqIdxRatio)
		}
	} else if summary.TotalSeqScan > 100 && summary.TotalIdxScan == 0 {
		flags = append(flags, FlagSeqScanOnly)
	}

	if DetectSeqScanImbalance(a, QualifiedName{Schema: summary.Schema, Name: summary.Table}) != nil {
		flags = append(flags, FlagNodeImbalance)
	}

	return flags
}

type NodeImbalanceInfo struct {
	HotNode    string `json:"hot_node"`
	Multiplier int64  `json:"multiplier"`
}

// Flags when one node carries disproportionate seq_scans
func DetectSeqScanImbalance(a *AnnotatedSchema, q QualifiedName) *NodeImbalanceInfo {
	if a == nil || a.Merged == nil {
		return nil
	}
	type entry struct {
		source  string
		seqScan int64
	}
	var entries []entry
	for _, n := range a.Merged.Nodes {
		for _, ts := range n.Tables {
			if ts.Table == q {
				entries = append(entries, entry{n.Node.Source, ts.Activity.SeqScan})
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

func DetectUnusedIndexes(a *AnnotatedSchema) []UnusedIndexEntry {
	if a == nil || a.Schema == nil {
		return nil
	}
	var entries []UnusedIndexEntry
	for i := range a.Schema.Tables {
		t := &a.Schema.Tables[i]
		qual := t.Qual()
		for _, idx := range t.Indexes {
			if idx.IsPrimary {
				continue
			}
			total := a.TotalIndexScans(qual, idx.Name)
			if total != 0 {
				continue
			}
			var size int64
			if sz := a.IndexSizingFor(qual, idx.Name); sz != nil {
				size = sz.Size
			}
			entries = append(entries, UnusedIndexEntry{
				Schema: t.Schema, Table: t.Name, IndexName: idx.Name,
				TotalSizeBytes: size, IsUnique: idx.IsUnique,
				Definition: idx.Definition,
			})
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

func DetectBloatedIndexes(a *AnnotatedSchema, threshold float64) []BloatedIndexEntry {
	if a == nil || a.Schema == nil {
		return nil
	}
	var entries []BloatedIndexEntry
	for i := range a.Schema.Tables {
		t := &a.Schema.Tables[i]
		qual := t.Qual()
		for _, idx := range t.Indexes {
			sz := a.IndexSizingFor(qual, idx.Name)
			if sz == nil {
				continue
			}
			est, ok := bloat.EstimateIndexBloat(*sz, idx.Columns, *t, idx.IndexType)
			if !ok || est.BloatRatio <= threshold {
				continue
			}
			entries = append(entries, BloatedIndexEntry{
				Schema: t.Schema, Table: t.Name, IndexName: idx.Name,
				BloatRatio: est.BloatRatio, ActualPages: est.ActualPages,
				ExpectedPages: est.ExpectedPages, ActualSize: sz.Size,
				IndexType: idx.IndexType,
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].BloatRatio > entries[j].BloatRatio
	})
	return entries
}
