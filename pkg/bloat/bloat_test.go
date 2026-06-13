package bloat

import (
	"math"
	"testing"

	"github.com/boringsql/dryrun/pkg/snapshot"
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
			sz := snapshot.IndexSizing{Relpages: 100, Reltuples: 10000}
			table := snapshot.Table{Columns: []snapshot.Column{{Name: "data", TypeName: "jsonb"}}}
			_, ok := EstimateIndexBloat(sz, []string{"data"}, nil, table, idxType)
			if ok {
				t.Errorf("expected false for %s index", idxType)
			}
		})
	}
}

// Zero reltuples / zero relpages are degenerate inputs that mean ANALYZE never ran;
// the estimator must refuse rather than emit a division-by-zero ratio.
func TestEstimateIndexBloat_DegenerateSizing(t *testing.T) {
	table := snapshot.Table{Columns: []snapshot.Column{{Name: "id", TypeName: "integer"}}}
	for _, sz := range []snapshot.IndexSizing{
		{Relpages: 10, Reltuples: 0},
		{Relpages: 0, Reltuples: 1000},
	} {
		if _, ok := EstimateIndexBloat(sz, []string{"id"}, nil, table, "btree"); ok {
			t.Errorf("expected false for %+v", sz)
		}
	}
}

// Healthy single-column integer index: actual pages match the analytical expectation
// to within rounding, so bloat ratio is ~1.0 and avg key width is the int4 byte size.
func TestEstimateIndexBloat_NormalIndex(t *testing.T) {
	expected := int64(math.Ceil(100000.0 / (float64(pageSize) * btreeFillfactor / float64(tupleOverhead+4))))

	sz := snapshot.IndexSizing{Relpages: expected, Reltuples: 100000, Size: expected * pageSize}
	table := snapshot.Table{Columns: []snapshot.Column{{Name: "id", TypeName: "integer"}}}
	est, ok := EstimateIndexBloat(sz, []string{"id"}, nil, table, "btree")
	if !ok {
		t.Fatal("expected ok")
	}
	if est.BloatRatio < 0.9 || est.BloatRatio > 1.1 {
		t.Errorf("expected bloat ratio ~1.0, got %.2f", est.BloatRatio)
	}
	if est.AvgTupleWidth != 4 {
		t.Errorf("expected avg key width 4, got %d", est.AvgTupleWidth)
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

	sz := snapshot.IndexSizing{Relpages: actualPages, Reltuples: 100000}
	table := snapshot.Table{Columns: []snapshot.Column{{Name: "id", TypeName: "integer"}}}
	est, ok := EstimateIndexBloat(sz, []string{"id"}, nil, table, "btree")
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
	sz := snapshot.IndexSizing{Relpages: 500, Reltuples: 10000}
	table := snapshot.Table{Columns: []snapshot.Column{{Name: "email", TypeName: "text"}}}
	est, ok := EstimateIndexBloat(sz, []string{"lower_email"}, nil, table, "btree")
	if !ok {
		t.Fatal("expected ok")
	}
	if est.AvgTupleWidth != defaultWidth {
		t.Errorf("expected avg key width %d (default), got %d", defaultWidth, est.AvgTupleWidth)
	}
}

// Annotate joins the planner doc against its schema doc and fills entry.Bloat
// for the table and the btree it can size, while leaving the gin index (no
// analytical model) and the index missing from the schema doc untouched.
func TestAnnotate_FillsTablesAndBtreeOnly(t *testing.T) {
	sch := &snapshot.SchemaSnapshot{Tables: []snapshot.Table{{
		Schema: "public", Name: "users",
		Columns: []snapshot.Column{
			{Name: "id", TypeName: "integer"},
			{Name: "doc", TypeName: "jsonb"},
		},
		Indexes: []snapshot.Index{
			{Name: "users_pkey", Columns: []string{"id"}, IndexType: "btree"},
			{Name: "users_doc_gin", Columns: []string{"doc"}, IndexType: "gin"},
		},
	}}}
	qual := snapshot.QualifiedName{Schema: "public", Name: "users"}
	planner := &snapshot.PlannerStatsSnapshot{
		Tables: []snapshot.TableSizingEntry{
			{Table: qual, Sizing: snapshot.TableSizing{Relpages: 5000, Reltuples: 100000}},
			{Table: snapshot.QualifiedName{Schema: "public", Name: "phantom"}, Sizing: snapshot.TableSizing{Relpages: 10, Reltuples: 1000}},
		},
		Indexes: []snapshot.IndexSizingEntry{
			{Table: qual, Index: "users_pkey", Sizing: snapshot.IndexSizing{Relpages: 1000, Reltuples: 100000}},
			{Table: qual, Index: "users_doc_gin", Sizing: snapshot.IndexSizing{Relpages: 5000, Reltuples: 100000}},
			{Table: qual, Index: "phantom_idx", Sizing: snapshot.IndexSizing{Relpages: 10, Reltuples: 1000}},
		},
	}

	Annotate(planner, sch)

	if planner.Tables[0].Bloat == nil {
		t.Fatal("expected bloat on users table")
	}
	if planner.Tables[0].Bloat.BloatRatio <= 1.0 {
		t.Errorf("expected table bloat ratio > 1.0, got %.2f", planner.Tables[0].Bloat.BloatRatio)
	}
	if planner.Tables[1].Bloat != nil {
		t.Error("expected no bloat on table absent from schema doc")
	}
	if planner.Indexes[0].Bloat == nil {
		t.Fatal("expected bloat on btree pkey")
	}
	if planner.Indexes[0].Bloat.BloatRatio <= 1.0 {
		t.Errorf("expected bloat ratio > 1.0, got %.2f", planner.Indexes[0].Bloat.BloatRatio)
	}
	if planner.Indexes[1].Bloat != nil {
		t.Error("expected no bloat on gin index")
	}
	if planner.Indexes[2].Bloat != nil {
		t.Error("expected no bloat on index absent from schema doc")
	}
}

// Healthy table: actual heap pages match the analytical expectation, so the
// bloat ratio is ~1.0 and avg_tuple_width is the summed column widths.
func TestEstimateTableBloat_NormalTable(t *testing.T) {
	// (integer 4 + timestamptz 8) row → tuple = heapTupleOverhead + 12 = 40 bytes
	rowWidth := 4 + 8
	usable := float64(pageSize-pageHeaderSize) * heapFillfactor
	expected := int64(math.Ceil(100000.0 / (usable / float64(heapTupleOverhead+rowWidth))))

	sz := snapshot.TableSizing{Relpages: expected, Reltuples: 100000, TableSize: expected * pageSize}
	table := snapshot.Table{Columns: []snapshot.Column{
		{Name: "id", TypeName: "integer"},
		{Name: "created_at", TypeName: "timestamptz"},
	}}
	est, ok := EstimateTableBloat(sz, table)
	if !ok {
		t.Fatal("expected ok")
	}
	if est.BloatRatio < 0.9 || est.BloatRatio > 1.1 {
		t.Errorf("expected bloat ratio ~1.0, got %.2f", est.BloatRatio)
	}
	if est.AvgTupleWidth != rowWidth {
		t.Errorf("expected avg tuple width %d, got %d", rowWidth, est.AvgTupleWidth)
	}
	if est.SizeBytes != sz.TableSize {
		t.Errorf("expected size_bytes %d, got %d", sz.TableSize, est.SizeBytes)
	}
}

// 5x relpages over the analytical expectation yields a ~5x bloat ratio — the
// signal that a table needs VACUUM FULL / pg_repack.
func TestEstimateTableBloat_BloatedTable(t *testing.T) {
	rowWidth := 4
	usable := float64(pageSize-pageHeaderSize) * heapFillfactor
	expected := int64(math.Ceil(100000.0 / (usable / float64(heapTupleOverhead+rowWidth))))

	sz := snapshot.TableSizing{Relpages: expected * 5, Reltuples: 100000}
	table := snapshot.Table{Columns: []snapshot.Column{{Name: "id", TypeName: "integer"}}}
	est, ok := EstimateTableBloat(sz, table)
	if !ok {
		t.Fatal("expected ok")
	}
	if est.BloatRatio < 4.5 || est.BloatRatio > 5.5 {
		t.Errorf("expected bloat ratio ~5.0, got %.2f", est.BloatRatio)
	}
}

// A non-default fillfactor packs fewer rows per page, so the same row count is
// expected to span more pages — lowering the bloat ratio for identical relpages.
func TestEstimateTableBloat_FillfactorReloption(t *testing.T) {
	table := snapshot.Table{
		Columns:    []snapshot.Column{{Name: "id", TypeName: "integer"}},
		Reloptions: []string{"fillfactor=50"},
	}
	sz := snapshot.TableSizing{Relpages: 1000, Reltuples: 100000}

	withFF, ok := EstimateTableBloat(sz, table)
	if !ok {
		t.Fatal("expected ok")
	}
	plain, _ := EstimateTableBloat(sz, snapshot.Table{Columns: table.Columns})
	if !(withFF.BloatRatio < plain.BloatRatio) {
		t.Errorf("fillfactor=50 should lower bloat ratio: ff=%.2f plain=%.2f", withFF.BloatRatio, plain.BloatRatio)
	}
}

// Pre-ANALYZE sizing (zero relpages/reltuples) yields no estimate, same contract
// as the index estimator.
func TestEstimateTableBloat_DegenerateSizing(t *testing.T) {
	table := snapshot.Table{Columns: []snapshot.Column{{Name: "id", TypeName: "integer"}}}
	for _, sz := range []snapshot.TableSizing{
		{Relpages: 10, Reltuples: 0},
		{Relpages: 0, Reltuples: 1000},
	} {
		if _, ok := EstimateTableBloat(sz, table); ok {
			t.Errorf("expected false for %+v", sz)
		}
	}
}

// Degenerate sizing (no ANALYZE yet) yields no bloat point, mirroring the
// estimator's ok=false contract.
func TestAnnotate_SkipsDegenerateSizing(t *testing.T) {
	sch := &snapshot.SchemaSnapshot{Tables: []snapshot.Table{{
		Schema: "public", Name: "users",
		Columns: []snapshot.Column{{Name: "id", TypeName: "integer"}},
		Indexes: []snapshot.Index{{Name: "users_pkey", Columns: []string{"id"}, IndexType: "btree"}},
	}}}
	qual := snapshot.QualifiedName{Schema: "public", Name: "users"}
	planner := &snapshot.PlannerStatsSnapshot{Indexes: []snapshot.IndexSizingEntry{
		{Table: qual, Index: "users_pkey", Sizing: snapshot.IndexSizing{Relpages: 0, Reltuples: 0}},
	}}

	Annotate(planner, sch)

	if planner.Indexes[0].Bloat != nil {
		t.Error("expected no bloat for pre-ANALYZE sizing")
	}
}

// Defensive: nil inputs must not panic.
func TestAnnotate_NilInputs(t *testing.T) {
	Annotate(nil, nil)
	Annotate(&snapshot.PlannerStatsSnapshot{}, nil)
	Annotate(nil, &snapshot.SchemaSnapshot{})
}

// Multi-column indexes sum the per-column type widths into the avg_key_width;
// for (integer, timestamptz) that's 4 + 8 = 12 bytes.
func TestEstimateIndexBloat_MultiColumn(t *testing.T) {
	sz := snapshot.IndexSizing{Relpages: 500, Reltuples: 50000}
	table := snapshot.Table{Columns: []snapshot.Column{
		{Name: "user_id", TypeName: "integer"},
		{Name: "created_at", TypeName: "timestamptz"},
	}}
	est, ok := EstimateIndexBloat(sz, []string{"user_id", "created_at"}, nil, table, "btree")
	if !ok {
		t.Fatal("expected ok")
	}
	if est.AvgTupleWidth != 12 {
		t.Errorf("expected avg key width 12, got %d", est.AvgTupleWidth)
	}
}

// INCLUDE columns live in the btree leaf tuple, so they widen avg_tuple_width
// on top of the key: key (integer 4) + INCLUDE (uuid 16) = 20 bytes.
func TestEstimateIndexBloat_IncludeColumns(t *testing.T) {
	sz := snapshot.IndexSizing{Relpages: 500, Reltuples: 50000}
	table := snapshot.Table{Columns: []snapshot.Column{
		{Name: "user_id", TypeName: "integer"},
		{Name: "token", TypeName: "uuid"},
	}}
	est, ok := EstimateIndexBloat(sz, []string{"user_id"}, []string{"token"}, table, "btree")
	if !ok {
		t.Fatal("expected ok")
	}
	if est.AvgTupleWidth != 20 {
		t.Errorf("expected avg tuple width 20 (4 key + 16 include), got %d", est.AvgTupleWidth)
	}
}

// Malformed or out-of-range fillfactor reloptions fall back to the heap default
// (100%), so the estimate matches a table with no reloptions at all.
func TestEstimateTableBloat_FillfactorFallback(t *testing.T) {
	cols := []snapshot.Column{{Name: "id", TypeName: "integer"}}
	sz := snapshot.TableSizing{Relpages: 1000, Reltuples: 100000}
	plain, _ := EstimateTableBloat(sz, snapshot.Table{Columns: cols})

	for _, opt := range []string{"fillfactor=0", "fillfactor=200", "fillfactor=abc", "autovacuum_enabled=false"} {
		est, ok := EstimateTableBloat(sz, snapshot.Table{Columns: cols, Reloptions: []string{opt}})
		if !ok {
			t.Fatalf("expected ok for reloption %q", opt)
		}
		if est.BloatRatio != plain.BloatRatio {
			t.Errorf("reloption %q should fall back to default: got %.4f, want %.4f", opt, est.BloatRatio, plain.BloatRatio)
		}
	}
}
