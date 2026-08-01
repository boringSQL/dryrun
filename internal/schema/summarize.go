package schema

import (
	"sort"
	"strings"

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
	FlagHighSeqIdxRatio   TableFlag = "high_seq_idx_ratio"
	FlagSeqScanOnly       TableFlag = "seq_scan_only"
	FlagNodeImbalance     TableFlag = "node_imbalance"
	FlagUnattributedScans TableFlag = "unattributed_scans"
)

const unattributedScanThreshold = 100_000

const queryStatsRowCap = 500

func DetectTableFlags(summary *TableSummary, a *AnnotatedSchema, ix *QueryRefIndex) []TableFlag {
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

	if ix.Unattributed(summary.Table, summary.TotalSeqScan+summary.TotalIdxScan) {
		flags = append(flags, FlagUnattributedScans)
	}

	return flags
}

type QueryRefIndex struct {
	identifiers       map[string]struct{}
	trackIsTop        bool
	truncated         bool
	partitionChildren map[string]struct{}
}

func BuildQueryRefIndex(a *AnnotatedSchema) *QueryRefIndex {
	if a == nil || len(a.QueryStats) == 0 {
		return nil
	}
	ix := &QueryRefIndex{
		identifiers:       map[string]struct{}{},
		partitionChildren: map[string]struct{}{},
		trackIsTop:        true,
	}
	sawTrack := false
	for _, snap := range a.QueryStats {
		// Every snapshot must state its track. Activity is summed across nodes,
		// so one capture predating the field (nil) could be hiding track = 'all'
		// on the node the scans actually came from.
		if snap.PgssTrack == nil {
			ix.trackIsTop = false
		} else {
			sawTrack = true
			if *snap.PgssTrack != "top" {
				ix.trackIsTop = false
			}
		}
		if snap.RawRows >= queryStatsRowCap {
			ix.truncated = true
		}
		if snap.InfoAfter != nil && snap.InfoAfter.Dealloc > 0 {
			ix.truncated = true
		}
		for _, e := range snap.Queries {
			collectIdentifiers(e.Canonical, ix.identifiers)
		}
	}
	if !sawTrack {
		return nil
	}
	if a.Schema != nil {
		for _, t := range a.Schema.Tables {
			if t.PartitionInfo == nil {
				continue
			}
			for _, c := range t.PartitionInfo.Children {
				ix.partitionChildren[strings.ToLower(c.Name)] = struct{}{}
			}
		}
	}
	return ix
}

func collectIdentifiers(sql string, out map[string]struct{}) {
	start := -1
	for i := 0; i <= len(sql); i++ {
		var c byte
		if i < len(sql) {
			c = sql[i]
		}
		isWord := c == '_' || c == '$' || (c >= '0' && c <= '9') ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		switch {
		case isWord && start < 0:
			start = i
		case !isWord && start >= 0:
			out[strings.ToLower(sql[start:i])] = struct{}{}
			start = -1
		}
	}
}

func (ix *QueryRefIndex) Unattributed(table string, totalScans int64) bool {
	if ix == nil || !ix.trackIsTop || ix.truncated || totalScans < unattributedScanThreshold {
		return false
	}
	lower := strings.ToLower(table)
	if _, isChild := ix.partitionChildren[lower]; isChild {
		return false
	}
	_, referenced := ix.identifiers[lower]
	return !referenced
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

type BloatedTableEntry struct {
	Schema        string  `json:"schema"`
	Table         string  `json:"table"`
	BloatRatio    float64 `json:"bloat_ratio"`
	ActualPages   int64   `json:"actual_pages"`
	ExpectedPages int64   `json:"expected_pages"`
	ActualSize    int64   `json:"actual_size_bytes"`
}

func DetectBloatedTables(a *AnnotatedSchema, threshold float64) []BloatedTableEntry {
	if a == nil || a.Schema == nil {
		return nil
	}
	var entries []BloatedTableEntry
	for i := range a.Schema.Tables {
		t := &a.Schema.Tables[i]
		qual := t.Qual()
		b := a.TableBloatFor(qual)
		if b == nil || b.BloatRatio <= threshold {
			continue
		}
		sz := a.SizingFor(qual)
		var sizeBytes int64
		if sz != nil {
			sizeBytes = sz.TableSize
		}
		entries = append(entries, BloatedTableEntry{
			Schema: t.Schema, Table: t.Name,
			BloatRatio: b.BloatRatio, ActualPages: b.ActualPages,
			ExpectedPages: b.ExpectedPages, ActualSize: sizeBytes,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].BloatRatio > entries[j].BloatRatio
	})
	return entries
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
			est, ok := bloat.EstimateIndexBloat(*sz, idx.Columns, idx.IncludeColumns, *t, idx.IndexType)
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
