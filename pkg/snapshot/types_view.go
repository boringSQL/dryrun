package snapshot

// Not persisted: in-memory join of one SchemaSnapshot, one PlannerStatsSnapshot
// and N ActivityStatsSnapshot rows sharing the same schema_ref_hash
type AnnotatedSchema struct {
	Schema     *SchemaSnapshot
	Planner    *PlannerStatsSnapshot
	Merged     *MergedActivity
	QueryStats []QueryStatsSnapshot
}

// one entry per node, except a label that rotates between servers: one per server
type MergedActivity struct {
	Nodes []NodeActivity
}

// true when several servers share this label, so listings must tell them apart
func (m *MergedActivity) LabelRepeats(source string) bool {
	if m == nil {
		return false
	}
	seen := 0
	for i := range m.Nodes {
		if m.Nodes[i].Node.Source == source {
			seen++
		}
	}
	return seen > 1
}

type NodeActivity struct {
	Node    NodeIdentity
	Tables  []TableActivityEntry
	Indexes []IndexActivityEntry
}

func (a *AnnotatedSchema) SizingFor(q QualifiedName) *TableSizing {
	if a == nil || a.Planner == nil {
		return nil
	}
	for i := range a.Planner.Tables {
		if a.Planner.Tables[i].Table == q {
			return &a.Planner.Tables[i].Sizing
		}
	}
	return nil
}

// TableBloatFor returns the bloat estimate for the table computed at snapshot time, or nil.
func (a *AnnotatedSchema) TableBloatFor(q QualifiedName) *BloatEstimate {
	if a == nil || a.Planner == nil {
		return nil
	}
	for i := range a.Planner.Tables {
		if a.Planner.Tables[i].Table == q {
			return a.Planner.Tables[i].Bloat
		}
	}
	return nil
}

func (a *AnnotatedSchema) IndexSizingFor(table QualifiedName, index string) *IndexSizing {
	if a == nil || a.Planner == nil {
		return nil
	}
	for i := range a.Planner.Indexes {
		if a.Planner.Indexes[i].Table == table && a.Planner.Indexes[i].Index == index {
			return &a.Planner.Indexes[i].Sizing
		}
	}
	return nil
}

// the label's newest row; on a rotating label that is one server, not a sum
func (a *AnnotatedSchema) ActivityForNode(source string, q QualifiedName) *TableActivity {
	if a == nil || a.Merged == nil {
		return nil
	}
	for i := range a.Merged.Nodes {
		if a.Merged.Nodes[i].Node.Source != source {
			continue
		}
		for j := range a.Merged.Nodes[i].Tables {
			if a.Merged.Nodes[i].Tables[j].Table == q {
				return &a.Merged.Nodes[i].Tables[j].Activity
			}
		}
	}
	return nil
}

// the label's newest row; on a rotating label that is one server, not a sum
func (a *AnnotatedSchema) IndexActivityForNode(source string, table QualifiedName, index string) *IndexActivity {
	if a == nil || a.Merged == nil {
		return nil
	}
	for i := range a.Merged.Nodes {
		if a.Merged.Nodes[i].Node.Source != source {
			continue
		}
		for j := range a.Merged.Nodes[i].Indexes {
			e := &a.Merged.Nodes[i].Indexes[j]
			if e.Table == table && e.Index == index {
				return &e.Activity
			}
		}
	}
	return nil
}

// one identity per label: a rotating label's first entry, its newest row
func (a *AnnotatedSchema) Nodes() []NodeIdentity {
	if a == nil || a.Merged == nil {
		return nil
	}
	seen := map[string]bool{}
	out := make([]NodeIdentity, 0, len(a.Merged.Nodes))
	for i := range a.Merged.Nodes {
		if n := a.Merged.Nodes[i].Node; !seen[n.Source] {
			seen[n.Source] = true
			out = append(out, n)
		}
	}
	return out
}

// First non-standby's row; standbys don't run autovacuum so timestamps live on primaries
func (a *AnnotatedSchema) PrimaryActivity(q QualifiedName) *TableActivity {
	if a == nil || a.Merged == nil {
		return nil
	}
	var fallback *TableActivity
	for i := range a.Merged.Nodes {
		n := &a.Merged.Nodes[i]
		for j := range n.Tables {
			if n.Tables[j].Table != q {
				continue
			}
			if !n.Node.IsStandby {
				return &n.Tables[j].Activity
			}
			if fallback == nil {
				fallback = &n.Tables[j].Activity
			}
		}
	}
	return fallback
}

func (a *AnnotatedSchema) PrimaryIndexActivity(table QualifiedName, index string) *IndexActivity {
	if a == nil || a.Merged == nil {
		return nil
	}
	var fallback *IndexActivity
	for i := range a.Merged.Nodes {
		n := &a.Merged.Nodes[i]
		for j := range n.Indexes {
			e := &n.Indexes[j]
			if e.Table != table || e.Index != index {
				continue
			}
			if !n.Node.IsStandby {
				return &e.Activity
			}
			if fallback == nil {
				fallback = &e.Activity
			}
		}
	}
	return fallback
}

// Sums seq_scan / idx_scan across every node for the table
func (a *AnnotatedSchema) TotalTableScans(q QualifiedName) (seq, idx int64) {
	if a == nil || a.Merged == nil {
		return 0, 0
	}
	for i := range a.Merged.Nodes {
		for j := range a.Merged.Nodes[i].Tables {
			if a.Merged.Nodes[i].Tables[j].Table == q {
				seq += a.Merged.Nodes[i].Tables[j].Activity.SeqScan
				idx += a.Merged.Nodes[i].Tables[j].Activity.IdxScan
			}
		}
	}
	return seq, idx
}

// Sums IdxScan across every node for the index
func (a *AnnotatedSchema) TotalIndexScans(table QualifiedName, index string) int64 {
	if a == nil || a.Merged == nil {
		return 0
	}
	var n int64
	for i := range a.Merged.Nodes {
		for j := range a.Merged.Nodes[i].Indexes {
			e := &a.Merged.Nodes[i].Indexes[j]
			if e.Table == table && e.Index == index {
				n += e.Activity.IdxScan
			}
		}
	}
	return n
}

func (a *AnnotatedSchema) ColumnStats(table QualifiedName, column string) *ColumnStats {
	if e := a.ColumnStatsEntry(table, column); e != nil {
		return &e.Stats
	}
	return nil
}

// ColumnStatsEntry is like ColumnStats but also exposes Inherited for injection.
func (a *AnnotatedSchema) ColumnStatsEntry(table QualifiedName, column string) *ColumnStatsEntry {
	if a == nil || a.Planner == nil {
		return nil
	}
	for i := range a.Planner.Columns {
		e := &a.Planner.Columns[i]
		if e.Table == table && e.Column == column {
			return e
		}
	}
	return nil
}
