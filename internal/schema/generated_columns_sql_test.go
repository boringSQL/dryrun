package schema

import (
	"regexp"
	"strings"
	"testing"
)

// pg_attrdef holds the generation expression for stored AND virtual generated
// columns. Reading it as default_expr made a generated column look writable --
// on PG18+, where virtual became the default for GENERATED ALWAYS AS, a
// virtual column was reported as a plain column with a DEFAULT. Verified
// against PostgreSQL 16.14 and 19beta3.
func TestIntrospectSeparatesGenerationExprFromDefault(t *testing.T) {
	body, err := sqlFS.ReadFile("sql/introspect.sql")
	if err != nil {
		t.Fatalf("read embedded introspect.sql: %v", err)
	}
	sql := string(body)

	for _, want := range []string{
		// default_expr is gated on the column not being generated
		"CASE WHEN a.attgenerated = ''",
		// and the expression is kept, under a name that says what it is
		"END AS generation_expr",
		// PG18 made virtual the default for GENERATED ALWAYS AS
		"WHEN 'v' THEN 'virtual'",
		"WHEN 's' THEN 'stored'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("introspect.sql no longer contains %q", want)
		}
	}

	// the ungated form is what produced the phantom DEFAULT
	if strings.Contains(sql, "pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expr") {
		t.Error("default_expr is ungated again: generated columns will report a DEFAULT")
	}
}

// fetch-columns is scanned positionally in introspect.go, so a reordered
// SELECT list silently lands each value in the wrong Go field -- default_expr
// and generation_expr are both nullable text and would swap without a single
// compile error. This pins the order the Scan call depends on.
func TestFetchColumnsSelectOrder(t *testing.T) {
	body, err := sqlFS.ReadFile("sql/introspect.sql")
	if err != nil {
		t.Fatalf("read embedded introspect.sql: %v", err)
	}
	block, _, ok := strings.Cut(strings.SplitN(string(body), "-- name: fetch-columns", 2)[1], "-- name:")
	if !ok {
		t.Fatal("could not isolate the fetch-columns query")
	}

	var got []string
	for _, m := range regexp.MustCompile(`AS (\w+)`).FindAllStringSubmatch(block, -1) {
		got = append(got, m[1])
	}
	want := []string{
		"table_oid", "column_name", "ordinal", "type_name", "nullable",
		"default_expr", "generation_expr", "identity", "statistics_target", "generated",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("fetch-columns select order changed; introspect.go Scan expects\n  %v\ngot\n  %v", want, got)
	}
}
