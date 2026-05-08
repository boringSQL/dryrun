package schema

import (
	"math"
	"testing"
)

func TestLookupTypeWidth(t *testing.T) {
	tests := []struct {
		typeName string
		want     int
	}{
		{"integer", 4},
		{"bigint", 8},
		{"uuid", 16},
		{"text", 32},
		{"boolean", 1},
		{"timestamptz", 8},
		{"jsonb", 64},
		// case insensitivity
		{"INTEGER", 4},
		{"UUID", 16},
		// parameterized types
		{"varchar(255)", 32},
		{"numeric(10,2)", 16},
		{"character varying(100)", 32},
		// array suffix
		{"integer[]", 4},
		{"uuid[]", 16},
		// unknown type
		{"hstore", defaultWidth},
		{"custom_type", defaultWidth},
	}
	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			got := lookupTypeWidth(tt.typeName)
			if got != tt.want {
				t.Errorf("lookupTypeWidth(%q) = %d, want %d", tt.typeName, got, tt.want)
			}
		})
	}
}

func TestEstimateIndexBloat_NilStats(t *testing.T) {
	idx := Index{Name: "idx_test", Columns: []string{"id"}, IndexType: "btree", Stats: nil}
	table := Table{Columns: []Column{{Name: "id", TypeName: "integer"}}}
	_, ok := EstimateIndexBloat(idx, table)
	if ok {
		t.Error("expected false for nil stats")
	}
}

func TestEstimateIndexBloat_NonBtree(t *testing.T) {
	for _, idxType := range []string{"hash", "gin", "gist", "brin"} {
		t.Run(idxType, func(t *testing.T) {
			idx := Index{
				Name: "idx_test", Columns: []string{"data"}, IndexType: idxType,
				Stats: &IndexStats{Relpages: 100, Reltuples: 10000},
			}
			table := Table{Columns: []Column{{Name: "data", TypeName: "jsonb"}}}
			_, ok := EstimateIndexBloat(idx, table)
			if ok {
				t.Errorf("expected false for %s index", idxType)
			}
		})
	}
}

func TestEstimateIndexBloat_ZeroTuples(t *testing.T) {
	idx := Index{
		Name: "idx_test", Columns: []string{"id"}, IndexType: "btree",
		Stats: &IndexStats{Relpages: 10, Reltuples: 0},
	}
	table := Table{Columns: []Column{{Name: "id", TypeName: "integer"}}}
	_, ok := EstimateIndexBloat(idx, table)
	if ok {
		t.Error("expected false for zero tuples")
	}
}

func TestEstimateIndexBloat_ZeroPages(t *testing.T) {
	idx := Index{
		Name: "idx_test", Columns: []string{"id"}, IndexType: "btree",
		Stats: &IndexStats{Relpages: 0, Reltuples: 1000},
	}
	table := Table{Columns: []Column{{Name: "id", TypeName: "integer"}}}
	_, ok := EstimateIndexBloat(idx, table)
	if ok {
		t.Error("expected false for zero pages")
	}
}

func TestEstimateIndexBloat_NormalIndex(t *testing.T) {
	// A single integer column: key width = 4, tuple = 12 bytes
	// usable = 8192 * 0.9 = 7372.8
	// tuplesPerPage = 7372.8 / 12 = 614.4
	// 100k tuples → expected = ceil(100000/614.4) = 163 pages
	// Actual pages = 163 → ratio = 1.0
	expected := int64(math.Ceil(100000.0 / (float64(pageSize) * btreeFillfactor / float64(tupleOverhead+4))))

	idx := Index{
		Name: "idx_test", Columns: []string{"id"}, IndexType: "btree",
		Stats: &IndexStats{Relpages: expected, Reltuples: 100000},
	}
	table := Table{Columns: []Column{{Name: "id", TypeName: "integer"}}}
	est, ok := EstimateIndexBloat(idx, table)
	if !ok {
		t.Fatal("expected ok")
	}
	if est.BloatRatio < 0.9 || est.BloatRatio > 1.1 {
		t.Errorf("expected bloat ratio ~1.0, got %.2f", est.BloatRatio)
	}
	if est.AvgKeyWidth != 4 {
		t.Errorf("expected avg key width 4, got %d", est.AvgKeyWidth)
	}
}

func TestEstimateIndexBloat_BloatedIndex(t *testing.T) {
	// Same setup but actual pages = 10x expected
	expected := int64(math.Ceil(100000.0 / (float64(pageSize) * btreeFillfactor / float64(tupleOverhead+4))))
	actualPages := expected * 10

	idx := Index{
		Name: "idx_test", Columns: []string{"id"}, IndexType: "btree",
		Stats: &IndexStats{Relpages: actualPages, Reltuples: 100000},
	}
	table := Table{Columns: []Column{{Name: "id", TypeName: "integer"}}}
	est, ok := EstimateIndexBloat(idx, table)
	if !ok {
		t.Fatal("expected ok")
	}
	if est.BloatRatio < 9.5 || est.BloatRatio > 10.5 {
		t.Errorf("expected bloat ratio ~10.0, got %.2f", est.BloatRatio)
	}
	if est.ActualPages != actualPages {
		t.Errorf("expected actual pages %d, got %d", actualPages, est.ActualPages)
	}
}

func TestEstimateIndexBloat_ExpressionColumn(t *testing.T) {
	// Column "lower_email" not in table → uses defaultWidth
	idx := Index{
		Name: "idx_test", Columns: []string{"lower_email"}, IndexType: "btree",
		Stats: &IndexStats{Relpages: 500, Reltuples: 10000},
	}
	table := Table{Columns: []Column{{Name: "email", TypeName: "text"}}}
	est, ok := EstimateIndexBloat(idx, table)
	if !ok {
		t.Fatal("expected ok")
	}
	if est.AvgKeyWidth != defaultWidth {
		t.Errorf("expected avg key width %d (default), got %d", defaultWidth, est.AvgKeyWidth)
	}
}

func TestEstimateIndexBloat_MultiColumn(t *testing.T) {
	idx := Index{
		Name: "idx_test", Columns: []string{"user_id", "created_at"}, IndexType: "btree",
		Stats: &IndexStats{Relpages: 500, Reltuples: 50000},
	}
	table := Table{Columns: []Column{
		{Name: "user_id", TypeName: "integer"},
		{Name: "created_at", TypeName: "timestamptz"},
	}}
	est, ok := EstimateIndexBloat(idx, table)
	if !ok {
		t.Fatal("expected ok")
	}
	// integer(4) + timestamptz(8) = 12
	if est.AvgKeyWidth != 12 {
		t.Errorf("expected avg key width 12, got %d", est.AvgKeyWidth)
	}
}
