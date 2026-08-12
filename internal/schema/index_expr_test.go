package schema

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// Regression: expression entries used to be dropped from the index column array
// while the key/include split still happened positionally at indnkeyatts, so an
// index like (a, lower(b), d) INCLUDE (c) was captured as Columns=[a d c],
// IncludeColumns=[] — a plausible index that does not exist.
func TestFetchIndexes_ExpressionColumnsKeepTheirSlot(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()

	setup := []string{
		`DROP SCHEMA IF EXISTS dryrun_idx_expr_test CASCADE`,
		`CREATE SCHEMA dryrun_idx_expr_test`,
		`CREATE TABLE dryrun_idx_expr_test.t (a int, b text, c text, d int)`,
		`CREATE INDEX i_expr_mixed ON dryrun_idx_expr_test.t (a, lower(b), d) INCLUDE (c)`,
		`CREATE INDEX i_plain ON dryrun_idx_expr_test.t (a, b) INCLUDE (c, d)`,
		`CREATE INDEX i_expr_only ON dryrun_idx_expr_test.t ((a + d))`,
		`CREATE INDEX i_ops ON dryrun_idx_expr_test.t (b text_pattern_ops, a DESC NULLS LAST)`,
		`CREATE INDEX i_partial ON dryrun_idx_expr_test.t (lower(c), a) WHERE d > 0`,
		`CREATE INDEX i_multiline ON dryrun_idx_expr_test.t ((CASE WHEN d > 0 THEN b ELSE c END))`,
		`ALTER TABLE dryrun_idx_expr_test.t ADD CONSTRAINT t_excl EXCLUDE USING btree (c WITH =)`,
		`CREATE TABLE dryrun_idx_expr_test.p (a int, b text, c text) PARTITION BY RANGE (a)`,
		`CREATE TABLE dryrun_idx_expr_test.p1 PARTITION OF dryrun_idx_expr_test.p FOR VALUES FROM (0) TO (10)`,
		`CREATE INDEX p_expr ON dryrun_idx_expr_test.p (a, lower(b)) INCLUDE (c)`,
	}
	for _, stmt := range setup {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS dryrun_idx_expr_test CASCADE`); err != nil {
			t.Logf("cleanup: %v", err)
		}
	})

	snap, err := IntrospectSchema(ctx, pool)
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}

	findTable := func(name string) *snapshot.Table {
		t.Helper()
		for i := range snap.Tables {
			if snap.Tables[i].Schema == "dryrun_idx_expr_test" && snap.Tables[i].Name == name {
				return &snap.Tables[i]
			}
		}
		t.Fatalf("table dryrun_idx_expr_test.%s not found in snapshot", name)
		return nil
	}

	table := findTable("t")

	byName := make(map[string]snapshot.Index, len(table.Indexes))
	for _, idx := range table.Indexes {
		byName[idx.Name] = idx
	}

	cases := []struct {
		name     string
		columns  []string
		include  []string
		hasExprs bool
	}{
		{name: "i_expr_mixed", columns: []string{"a", "lower(b)", "d"}, include: []string{"c"}, hasExprs: true},
		{name: "i_plain", columns: []string{"a", "b"}, include: []string{"c", "d"}},
		{name: "i_expr_only", columns: []string{"(a + d)"}, hasExprs: true},
		// opclass / ordering must not leak into the column name
		{name: "i_ops", columns: []string{"b", "a"}},
		// nor must the predicate
		{name: "i_partial", columns: []string{"lower(c)", "a"}, hasExprs: true},
		// exclusion-constraint index: no "WITH =" leak
		{name: "t_excl", columns: []string{"c"}},
	}
	for _, tc := range cases {
		idx, ok := byName[tc.name]
		if !ok {
			t.Errorf("index %s not captured", tc.name)
			continue
		}
		if !reflect.DeepEqual(idx.Columns, tc.columns) {
			t.Errorf("%s Columns=%v want=%v", tc.name, idx.Columns, tc.columns)
		}
		if len(idx.IncludeColumns) != len(tc.include) || (len(tc.include) > 0 && !reflect.DeepEqual(idx.IncludeColumns, tc.include)) {
			t.Errorf("%s IncludeColumns=%v want=%v", tc.name, idx.IncludeColumns, tc.include)
		}
		if idx.HasExpressions != tc.hasExprs {
			t.Errorf("%s HasExpressions=%v want=%v", tc.name, idx.HasExpressions, tc.hasExprs)
		}
	}

	// deparsed expressions are indented by the server; entries must stay
	// single-line so the content hash cannot move on formatting alone
	ml, ok := byName["i_multiline"]
	if !ok {
		t.Fatalf("index i_multiline not captured")
	}
	if len(ml.Columns) != 1 {
		t.Fatalf("i_multiline Columns=%v want one entry", ml.Columns)
	}
	if strings.ContainsAny(ml.Columns[0], "\n\r\t") || strings.Contains(ml.Columns[0], "  ") {
		t.Errorf("i_multiline column not whitespace-normalized: %q", ml.Columns[0])
	}
	if !strings.Contains(ml.Columns[0], "CASE") {
		t.Errorf("i_multiline column lost its expression: %q", ml.Columns[0])
	}

	// partitioned parent and its physical child index take the same path
	for _, tn := range []string{"p", "p1"} {
		pt := findTable(tn)
		if len(pt.Indexes) != 1 {
			t.Fatalf("%s: expected 1 index, got %d", tn, len(pt.Indexes))
		}
		idx := pt.Indexes[0]
		if !reflect.DeepEqual(idx.Columns, []string{"a", "lower(b)"}) {
			t.Errorf("%s Columns=%v want=[a lower(b)]", tn, idx.Columns)
		}
		if !reflect.DeepEqual(idx.IncludeColumns, []string{"c"}) {
			t.Errorf("%s IncludeColumns=%v want=[c]", tn, idx.IncludeColumns)
		}
	}
}

func TestSplitKeyInclude(t *testing.T) {
	cases := []struct {
		name        string
		all         []string
		nKey        int
		wantKey     []string
		wantInclude []string
	}{
		{"key only", []string{"a", "b"}, 2, []string{"a", "b"}, []string{}},
		{"with include", []string{"a", "lower(b)", "c"}, 2, []string{"a", "lower(b)"}, []string{"c"}},
		{"single expression", []string{"(a + d)"}, 1, []string{"(a + d)"}, []string{}},
		// degenerate catalog readings fall back to "everything is a key column"
		{"nkey overflows", []string{"a"}, 3, []string{"a"}, nil},
		{"nkey zero", []string{"a"}, 0, []string{"a"}, nil},
		{"empty", nil, 1, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, include := splitKeyInclude(tc.all, tc.nKey)
			if !reflect.DeepEqual(key, tc.wantKey) {
				t.Errorf("key=%v want=%v", key, tc.wantKey)
			}
			if !reflect.DeepEqual(include, tc.wantInclude) {
				t.Errorf("include=%v want=%v", include, tc.wantInclude)
			}
		})
	}
}
