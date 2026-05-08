package schema

// Shallow copy with fresh Tables/Columns/Indexes slices so ApplyNodeStats can swap Stats pointers without touching original
func (s *SchemaSnapshot) CloneForStats() *SchemaSnapshot {
	clone := *s
	clone.Tables = make([]Table, len(s.Tables))
	for i, t := range s.Tables {
		clone.Tables[i] = t
		clone.Tables[i].Columns = make([]Column, len(t.Columns))
		copy(clone.Tables[i].Columns, t.Columns)
		clone.Tables[i].Indexes = make([]Index, len(t.Indexes))
		copy(clone.Tables[i].Indexes, t.Indexes)
	}
	return &clone
}
