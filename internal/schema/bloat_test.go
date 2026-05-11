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
		{"INTEGER", 4},
		{"UUID", 16},
		{"varchar(255)", 32},
		{"numeric(10,2)", 16},
		{"character varying(100)", 32},
		{"integer[]", 4},
		{"uuid[]", 16},
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

// Non-btree index types skip bloat estimation — there's no analytical model for
// hash/gin/gist/brin tuple packing in this codebase.
func TestEstimateIndexBloat_NonBtree(t *testing.T) {
	for _, idxType := range []string{"hash", "gin", "gist", "brin"} {
		t.Run(idxType, func(t *testing.T) {
			sz := IndexSizing{Relpages: 100, Reltuples: 10000}
			table := Table{Columns: []Column{{Name: "data", TypeName: "jsonb"}}}
			_, ok := EstimateIndexBloat(sz, []string{"data"}, table, idxType)
			if ok {
				t.Errorf("expected false for %s index", idxType)
			}
		})
	}
}

// Zero reltuples / zero relpages are degenerate inputs that mean ANALYZE never ran;
// the estimator must refuse rather than emit a division-by-zero ratio.
func TestEstimateIndexBloat_DegenerateSizing(t *testing.T) {
	table := Table{Columns: []Column{{Name: "id", TypeName: "integer"}}}
	for _, sz := range []IndexSizing{
		{Relpages: 10, Reltuples: 0},
		{Relpages: 0, Reltuples: 1000},
	} {
		if _, ok := EstimateIndexBloat(sz, []string{"id"}, table, "btree"); ok {
			t.Errorf("expected false for %+v", sz)
		}
	}
}

// Healthy single-column integer index: actual pages match the analytical expectation
// to within rounding, so bloat ratio is ~1.0 and avg key width is the int4 byte size.
func TestEstimateIndexBloat_NormalIndex(t *testing.T) {
	expected := int64(math.Ceil(100000.0 / (float64(pageSize) * btreeFillfactor / float64(tupleOverhead+4))))

	sz := IndexSizing{Relpages: expected, Reltuples: 100000, Size: expected * pageSize}
	table := Table{Columns: []Column{{Name: "id", TypeName: "integer"}}}
	est, ok := EstimateIndexBloat(sz, []string{"id"}, table, "btree")
	if !ok {
		t.Fatal("expected ok")
	}
	if est.BloatRatio < 0.9 || est.BloatRatio > 1.1 {
		t.Errorf("expected bloat ratio ~1.0, got %.2f", est.BloatRatio)
	}
	if est.AvgKeyWidth != 4 {
		t.Errorf("expected avg key width 4, got %d", est.AvgKeyWidth)
	}
	if est.SizeBytes != sz.Size {
		t.Errorf("expected size_bytes %d, got %d", sz.Size, est.SizeBytes)
	}
}

// 10x relpages over the analytical expectation should yield a ~10x bloat ratio,
// which is how operators identify candidates for REINDEX CONCURRENTLY.
func TestEstimateIndexBloat_BloatedIndex(t *testing.T) {
	expected := int64(math.Ceil(100000.0 / (float64(pageSize) * btreeFillfactor / float64(tupleOverhead+4))))
	actualPages := expected * 10

	sz := IndexSizing{Relpages: actualPages, Reltuples: 100000}
	table := Table{Columns: []Column{{Name: "id", TypeName: "integer"}}}
	est, ok := EstimateIndexBloat(sz, []string{"id"}, table, "btree")
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

// Expression indexes reference a synthetic column not in the table; the
// estimator falls back to defaultWidth so we still get a bloat estimate.
func TestEstimateIndexBloat_ExpressionColumn(t *testing.T) {
	sz := IndexSizing{Relpages: 500, Reltuples: 10000}
	table := Table{Columns: []Column{{Name: "email", TypeName: "text"}}}
	est, ok := EstimateIndexBloat(sz, []string{"lower_email"}, table, "btree")
	if !ok {
		t.Fatal("expected ok")
	}
	if est.AvgKeyWidth != defaultWidth {
		t.Errorf("expected avg key width %d (default), got %d", defaultWidth, est.AvgKeyWidth)
	}
}

// Multi-column indexes sum the per-column type widths into the avg_key_width;
// for (integer, timestamptz) that's 4 + 8 = 12 bytes.
func TestEstimateIndexBloat_MultiColumn(t *testing.T) {
	sz := IndexSizing{Relpages: 500, Reltuples: 50000}
	table := Table{Columns: []Column{
		{Name: "user_id", TypeName: "integer"},
		{Name: "created_at", TypeName: "timestamptz"},
	}}
	est, ok := EstimateIndexBloat(sz, []string{"user_id", "created_at"}, table, "btree")
	if !ok {
		t.Fatal("expected ok")
	}
	if est.AvgKeyWidth != 12 {
		t.Errorf("expected avg key width 12, got %d", est.AvgKeyWidth)
	}
}
