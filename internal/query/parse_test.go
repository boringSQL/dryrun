package query

import "testing"

func TestParseSimpleSelect(t *testing.T) {
	q, err := ParseSQL("SELECT id, name FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if q.Info.StatementType != "SELECT" {
		t.Errorf("got %q, want SELECT", q.Info.StatementType)
	}
	if !q.Info.HasWhere {
		t.Error("expected HasWhere")
	}
	if q.Info.HasSelectStar {
		t.Error("did not expect HasSelectStar")
	}
	if q.Info.HasJoin {
		t.Error("did not expect HasJoin")
	}
	if len(q.Info.Tables) != 1 || q.Info.Tables[0].Name != "users" {
		t.Errorf("unexpected tables: %v", q.Info.Tables)
	}
}

func TestDetectSelectStar(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM orders")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasSelectStar {
		t.Error("expected HasSelectStar")
	}
	if q.Info.HasWhere {
		t.Error("did not expect HasWhere")
	}
	if q.Info.HasLimit {
		t.Error("did not expect HasLimit")
	}
}

func TestDetectJoin(t *testing.T) {
	q, err := ParseSQL("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 100")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasJoin {
		t.Error("expected HasJoin")
	}
	if !q.Info.HasWhere {
		t.Error("expected HasWhere")
	}
	if len(q.Info.Tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(q.Info.Tables))
	}
}

func TestDetectLimit(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM users LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasLimit {
		t.Error("expected HasLimit")
	}
}

func TestSelectStarInFromSubquery(t *testing.T) {
	q, err := ParseSQL("SELECT id FROM (SELECT * FROM users) t")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasSelectStar {
		t.Error("expected HasSelectStar from nested FROM subquery")
	}
}

func TestLimitInFromSubquery(t *testing.T) {
	q, err := ParseSQL("SELECT a FROM (SELECT a FROM big LIMIT 1) t")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasLimit {
		t.Error("expected HasLimit from nested FROM subquery")
	}
}

func TestCountStarIsNotSelectStar(t *testing.T) {
	q, err := ParseSQL("SELECT count(*) FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if q.Info.HasSelectStar {
		t.Error("count(*) must not count as SELECT *")
	}
}

func TestSelectStarAndLimitInCteBody(t *testing.T) {
	q, err := ParseSQL("WITH x AS (SELECT * FROM u LIMIT 1) SELECT id FROM x")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasSelectStar {
		t.Error("expected HasSelectStar from CTE body")
	}
	if !q.Info.HasLimit {
		t.Error("expected HasLimit from CTE body")
	}
}

func TestSelectStarInSetOpBranch(t *testing.T) {
	q, err := ParseSQL("SELECT id FROM a UNION SELECT * FROM b")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasSelectStar {
		t.Error("expected HasSelectStar from set-op branch")
	}
}

func TestNestedWhereDoesNotLeakToHasWhere(t *testing.T) {
	q, err := ParseSQL("WITH x AS (SELECT id FROM o WHERE q = 1) UPDATE users SET n = 1")
	if err != nil {
		t.Fatal(err)
	}
	if q.Info.HasWhere {
		t.Error("nested CTE WHERE must not set HasWhere for an UPDATE without WHERE")
	}
}

func TestParseError(t *testing.T) {
	_, err := ParseSQL("SELEC broken")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestDetectUpdateWithoutWhere(t *testing.T) {
	q, err := ParseSQL("UPDATE users SET name = 'test'")
	if err != nil {
		t.Fatal(err)
	}
	if q.Info.StatementType != "UPDATE" {
		t.Errorf("got %q, want UPDATE", q.Info.StatementType)
	}
	if q.Info.HasWhere {
		t.Error("did not expect HasWhere")
	}
}

func TestDetectDeleteWithWhere(t *testing.T) {
	q, err := ParseSQL("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if q.Info.StatementType != "DELETE" {
		t.Errorf("got %q, want DELETE", q.Info.StatementType)
	}
	if !q.Info.HasWhere {
		t.Error("expected HasWhere")
	}
}

func TestFuncWrappedExtract(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE EXTRACT(year FROM created_at) = 2025")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Column != "created_at" || fwc.FuncName != "extract" {
		t.Errorf("got column=%q func=%q, want created_at/extract", fwc.Column, fwc.FuncName)
	}
}

func TestFuncWrappedTypeCast(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE created_at::date = '2025-01-01'")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Column != "created_at" || fwc.FuncName != "::date" {
		t.Errorf("got column=%q func=%q, want created_at/::date", fwc.Column, fwc.FuncName)
	}
}

func TestFuncWrappedDateTrunc(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE date_trunc('month', created_at) = '2025-01-01'")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Column != "created_at" || fwc.FuncName != "date_trunc" {
		t.Errorf("got column=%q func=%q, want created_at/date_trunc", fwc.Column, fwc.FuncName)
	}
}

func TestFuncWrappedToChar(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE to_char(created_at, 'YYYY-MM') = '2025-01'")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Column != "created_at" || fwc.FuncName != "to_char" {
		t.Errorf("got column=%q func=%q, want created_at/to_char", fwc.Column, fwc.FuncName)
	}
}

func TestFuncWrappedQualifiedColumn(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events e WHERE e.created_at::date = '2025-01-01'")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn, got %d", len(q.Info.FuncWrappedColumns))
	}
	fwc := q.Info.FuncWrappedColumns[0]
	if fwc.Table == nil || *fwc.Table != "e" {
		t.Errorf("expected table=e, got %v", fwc.Table)
	}
	if fwc.Column != "created_at" {
		t.Errorf("expected column=created_at, got %q", fwc.Column)
	}
}

func TestNoFuncWrappedForLiteralFunction(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM events WHERE created_at > now()")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 0 {
		t.Errorf("expected no FuncWrappedColumns, got %d: %v", len(q.Info.FuncWrappedColumns), q.Info.FuncWrappedColumns)
	}
}

func TestNoFuncWrappedInSelect(t *testing.T) {
	q, err := ParseSQL("SELECT lower(name) FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 0 {
		t.Errorf("expected no FuncWrappedColumns for SELECT-only function, got %d", len(q.Info.FuncWrappedColumns))
	}
}

func TestSelectColumnsAreNotFilterColumns(t *testing.T) {
	q, err := ParseSQL("SELECT LOWER(email) FROM auth.user_account WHERE user_id = ANY($1)")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FilterColumns) != 1 {
		t.Fatalf("got %d filter columns, want 1: %+v", len(q.Info.FilterColumns), q.Info.FilterColumns)
	}
	if q.Info.FilterColumns[0].Column != "user_id" {
		t.Errorf("got filter column %q, want user_id", q.Info.FilterColumns[0].Column)
	}
	for _, fc := range q.Info.FilterColumns {
		if fc.Column == "email" {
			t.Errorf("SELECT-list column reported as a filter column: %+v", fc)
		}
	}
}

func TestNoFilterColumnsWithoutWhere(t *testing.T) {
	q, err := ParseSQL("SELECT email FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FilterColumns) != 0 {
		t.Errorf("got %d filter columns, want 0: %+v", len(q.Info.FilterColumns), q.Info.FilterColumns)
	}
	found := false
	for _, rc := range q.Info.ReferencedColumns {
		if rc.Column == "email" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected email in ReferencedColumns, got %+v", q.Info.ReferencedColumns)
	}
}

func TestReferencedColumnsIncludeSelectList(t *testing.T) {
	q, err := ParseSQL("SELECT u.emial FROM users u")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FilterColumns) != 0 {
		t.Errorf("got filter columns for a query with no predicate: %+v", q.Info.FilterColumns)
	}
	found := false
	for _, rc := range q.Info.ReferencedColumns {
		if rc.Column == "emial" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected emial in ReferencedColumns, got %+v", q.Info.ReferencedColumns)
	}
}

func TestJoinQualsAreFilterColumns(t *testing.T) {
	q, err := ParseSQL("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, fc := range q.Info.FilterColumns {
		got[fc.Column] = true
	}
	for _, want := range []string{"id", "user_id"} {
		if !got[want] {
			t.Errorf("expected %q in filter columns from JOIN quals, got %+v", want, q.Info.FilterColumns)
		}
	}
}

func TestSubqueryTargetNotFilterColumn(t *testing.T) {
	q, err := ParseSQL("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FilterColumns) != 1 {
		t.Fatalf("got %d filter columns, want 1: %+v", len(q.Info.FilterColumns), q.Info.FilterColumns)
	}
	if q.Info.FilterColumns[0].Column != "id" {
		t.Errorf("got filter column %q, want id", q.Info.FilterColumns[0].Column)
	}
}

func TestSubqueryPredicateIsFilterColumn(t *testing.T) {
	q, err := ParseSQL("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE user_id > 5)")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, fc := range q.Info.FilterColumns {
		got[fc.Column] = true
	}
	if !got["id"] || !got["user_id"] {
		t.Errorf("expected id and user_id from outer and subquery predicates, got %+v", q.Info.FilterColumns)
	}
}

func filterColumnSet(t *testing.T, q *ParsedQuery) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	for _, fc := range q.Info.FilterColumns {
		got[fc.Column] = true
	}
	return got
}

func TestCaseExpressionFilterColumns(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM t WHERE CASE WHEN x > 1 THEN a ELSE b END = 2")
	if err != nil {
		t.Fatal(err)
	}
	got := filterColumnSet(t, q)
	for _, want := range []string{"x", "a", "b"} {
		if !got[want] {
			t.Errorf("expected %q from CASE expression, got %+v", want, q.Info.FilterColumns)
		}
	}
}

func TestRowExpressionFilterColumns(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM t WHERE (a, b) IN (SELECT a, b FROM u WHERE u.c = 1)")
	if err != nil {
		t.Fatal(err)
	}
	got := filterColumnSet(t, q)
	for _, want := range []string{"a", "b", "c"} {
		if !got[want] {
			t.Errorf("expected %q from row expression, got %+v", want, q.Info.FilterColumns)
		}
	}
}

func TestMinMaxCollateIndirectionFilterColumns(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"SELECT * FROM t WHERE GREATEST(a, b) > 5", "a"},
		{"SELECT * FROM t WHERE LEAST(a, b) > 5", "b"},
		{"SELECT * FROM t WHERE name COLLATE \"C\" = 'x'", "name"},
		{"SELECT * FROM t WHERE arr[1] = 5", "arr"},
	}
	for _, tc := range cases {
		q, err := ParseSQL(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if !filterColumnSet(t, q)[tc.want] {
			t.Errorf("%s: expected %q in filter columns, got %+v", tc.sql, tc.want, q.Info.FilterColumns)
		}
	}
}

func TestHavingIsNotFilterColumn(t *testing.T) {
	q, err := ParseSQL("SELECT sum(y) FROM t GROUP BY z HAVING sum(y) > 10 AND q = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FilterColumns) != 0 {
		t.Errorf("expected no filter columns from HAVING, got %+v", q.Info.FilterColumns)
	}
	referenced := map[string]bool{}
	for _, rc := range q.Info.ReferencedColumns {
		referenced[rc.Column] = true
	}
	for _, want := range []string{"y", "q"} {
		if !referenced[want] {
			t.Errorf("expected %q in ReferencedColumns, got %+v", want, q.Info.ReferencedColumns)
		}
	}
}

func TestMergeParsed(t *testing.T) {
	q, err := ParseSQL("MERGE INTO t USING u ON t.id = u.id WHEN MATCHED AND u.q = 5 THEN UPDATE SET x = 1")
	if err != nil {
		t.Fatal(err)
	}
	if q.Info.StatementType != "MERGE" {
		t.Errorf("got statement type %q, want MERGE", q.Info.StatementType)
	}
	tables := map[string]bool{}
	for _, tb := range q.Info.Tables {
		tables[tb.Name] = true
	}
	for _, want := range []string{"t", "u"} {
		if !tables[want] {
			t.Errorf("expected table %q, got %+v", want, q.Info.Tables)
		}
	}
	got := filterColumnSet(t, q)
	for _, want := range []string{"id", "q"} {
		if !got[want] {
			t.Errorf("expected %q from MERGE predicates, got %+v", want, q.Info.FilterColumns)
		}
	}
}

func TestOnConflictFilterColumns(t *testing.T) {
	q, err := ParseSQL("INSERT INTO t (a) SELECT a FROM u WHERE u.q = 5 ON CONFLICT (a) WHERE a > 0 DO UPDATE SET a = 1 WHERE t.a = 2")
	if err != nil {
		t.Fatal(err)
	}
	got := filterColumnSet(t, q)
	for _, want := range []string{"q", "a"} {
		if !got[want] {
			t.Errorf("expected %q from ON CONFLICT predicates, got %+v", want, q.Info.FilterColumns)
		}
	}
}

func TestFromSubselectTablesCollected(t *testing.T) {
	q, err := ParseSQL("SELECT * FROM (SELECT user_id FROM orders WHERE total > 5) o WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]bool{}
	for _, tb := range q.Info.Tables {
		tables[tb.Name] = true
	}
	if !tables["orders"] {
		t.Errorf("expected orders from FROM subselect, got %+v", q.Info.Tables)
	}
	got := filterColumnSet(t, q)
	for _, want := range []string{"total", "id"} {
		if !got[want] {
			t.Errorf("expected %q in filter columns, got %+v", want, q.Info.FilterColumns)
		}
	}
}

func TestCteBodyTablesAndColumnsCollected(t *testing.T) {
	q, err := ParseSQL("WITH x AS (SELECT id, emial FROM ghost WHERE id > 1) SELECT * FROM x")
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]bool{}
	for _, tb := range q.Info.Tables {
		tables[tb.Name] = true
	}
	if !tables["ghost"] {
		t.Errorf("expected ghost from CTE body, got %+v", q.Info.Tables)
	}
	var sawEmial bool
	for _, fc := range q.Info.ReferencedColumns {
		if fc.Column == "emial" {
			sawEmial = true
		}
	}
	if !sawEmial {
		t.Errorf("expected emial from CTE body in referenced columns, got %+v", q.Info.ReferencedColumns)
	}
}

func TestCteBodyFilterColumns(t *testing.T) {
	q, err := ParseSQL("WITH x AS (SELECT id FROM orders WHERE user_id = 5) SELECT id FROM x")
	if err != nil {
		t.Fatal(err)
	}
	if got := filterColumnSet(t, q); !got["user_id"] {
		t.Errorf("expected user_id from CTE predicate, got %+v", q.Info.FilterColumns)
	}
}

func TestCteBodyFuncWrappedColumns(t *testing.T) {
	q, err := ParseSQL("WITH x AS (SELECT id FROM events WHERE date_trunc('day', created_at) = now()) SELECT id FROM x")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Info.FuncWrappedColumns) != 1 {
		t.Fatalf("expected 1 FuncWrappedColumn from CTE body, got %d: %+v", len(q.Info.FuncWrappedColumns), q.Info.FuncWrappedColumns)
	}
	if got := q.Info.FuncWrappedColumns[0].Column; got != "created_at" {
		t.Errorf("got column %q, want created_at", got)
	}
}

func TestNestedCteBodiesCollected(t *testing.T) {
	q, err := ParseSQL("WITH a AS (WITH b AS (SELECT id FROM ghost) SELECT id FROM b) SELECT id FROM a")
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]bool{}
	for _, tb := range q.Info.Tables {
		tables[tb.Name] = true
	}
	if !tables["ghost"] {
		t.Errorf("expected ghost from nested CTE body, got %+v", q.Info.Tables)
	}
}

func TestDmlCteBodyCollected(t *testing.T) {
	q, err := ParseSQL("WITH x AS (SELECT id FROM ghost) UPDATE orders SET user_id = 1 WHERE id IN (SELECT id FROM x)")
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]bool{}
	for _, tb := range q.Info.Tables {
		tables[tb.Name] = true
	}
	if !tables["ghost"] {
		t.Errorf("expected ghost from DML CTE body, got %+v", q.Info.Tables)
	}
}

func TestSelectStarInExistsIgnored(t *testing.T) {
	for _, sql := range []string{
		"SELECT id FROM users u WHERE EXISTS (SELECT * FROM orders o WHERE o.user_id = u.id)",
		"SELECT id FROM users u WHERE NOT EXISTS (SELECT * FROM a UNION SELECT * FROM b)",
	} {
		q, err := ParseSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if q.Info.HasSelectStar {
			t.Errorf("SELECT * inside EXISTS must not count: %s", sql)
		}
	}
}

func TestSelectStarInInSubqueryStillCounts(t *testing.T) {
	q, err := ParseSQL("SELECT id FROM users WHERE (id, email) IN (SELECT * FROM archived_users)")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasSelectStar {
		t.Error("expected HasSelectStar from IN subquery")
	}
}

func TestLimitInSublinkDoesNotBoundOuter(t *testing.T) {
	for _, sql := range []string{
		"SELECT id FROM users WHERE id IN (SELECT user_id FROM orders LIMIT 10)",
		"SELECT id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id LIMIT 1)",
		"SELECT id, (SELECT total FROM orders o WHERE o.user_id = u.id ORDER BY id LIMIT 1) FROM users u",
		"SELECT id FROM users WHERE id IN (SELECT user_id FROM (SELECT * FROM orders LIMIT 5) s)",
		"SELECT id FROM users WHERE id IN (WITH x AS (SELECT user_id FROM orders LIMIT 5) SELECT user_id FROM x)",
	} {
		q, err := ParseSQL(sql)
		if err != nil {
			t.Fatal(err)
		}
		if q.Info.HasLimit {
			t.Errorf("LIMIT inside a sublink must not count as bounding the query: %s", sql)
		}
	}
}

func TestOuterLimitWithSublinkStillCounts(t *testing.T) {
	q, err := ParseSQL("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders) LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Info.HasLimit {
		t.Error("expected HasLimit from outer LIMIT")
	}
}

// The walk must reach every clause (the old hand-listed switch kept missing
// some) and must not report targets or DDL relations as reads.
func TestParseSQL_WalkCoverage(t *testing.T) {
	cases := []struct {
		sql       string
		tables    []string // exactly these tables, when set
		hasTable  string
		hasColumn string
	}{
		{sql: "DELETE FROM orders USING bogus_table WHERE orders.id = bogus_table.id", hasTable: "bogus_table"},
		{sql: "INSERT INTO users (id) VALUES ((SELECT max(id) FROM bogus_table))", hasTable: "bogus_table"},
		{sql: "SELECT * FROM generate_series(1, (SELECT count(*) FROM bogus_table)) g", hasTable: "bogus_table"},
		{sql: "SELECT id FROM users ORDER BY c", hasColumn: "c"},
		{sql: "SELECT count(*) FILTER (WHERE c > 1) FROM users", hasColumn: "c"},
		{sql: "SELECT sum(id) OVER (ORDER BY c) FROM users", hasColumn: "c"},
		{sql: "UPDATE users SET email = c", hasColumn: "c"},
		{sql: "UPDATE users SET email = 'x' RETURNING c", hasColumn: "c"},
		{sql: "SELECT * INTO new_t FROM users", tables: []string{"users"}},
		{sql: "SELECT * FROM users u FOR UPDATE OF u", tables: []string{"users"}},
		{sql: "CREATE TABLE new_t (id int)", tables: []string{}},
	}
	for _, tc := range cases {
		q, err := ParseSQL(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		tables := map[string]bool{}
		for _, tb := range q.Info.Tables {
			tables[tb.Name] = true
		}
		columns := map[string]bool{}
		for _, c := range q.Info.ReferencedColumns {
			columns[c.Column] = true
		}
		if tc.hasTable != "" && !tables[tc.hasTable] {
			t.Errorf("%s: table %s missing, got %v", tc.sql, tc.hasTable, tables)
		}
		if tc.hasColumn != "" && !columns[tc.hasColumn] {
			t.Errorf("%s: column %s missing, got %v", tc.sql, tc.hasColumn, columns)
		}
		if tc.tables != nil {
			if len(tables) != len(tc.tables) {
				t.Errorf("%s: tables %v, want %v", tc.sql, tables, tc.tables)
			}
			for _, name := range tc.tables {
				if !tables[name] {
					t.Errorf("%s: tables %v, want %v", tc.sql, tables, tc.tables)
				}
			}
		}
	}
}

// The SET (a, b) = (SELECT ...) source is shared by every column; one visit,
// or duplicate locs break the table rewrite and errors double.
func TestParseSQL_MultiAssignSourceWalkedOnce(t *testing.T) {
	q, err := ParseSQL("UPDATE users SET (email, id) = (SELECT o.nocol, o.id FROM orders o)")
	if err != nil {
		t.Fatal(err)
	}
	for _, tb := range q.Info.Tables {
		if tb.Name == "orders" && len(tb.locs) != 1 {
			t.Errorf("orders locs=%v, want one", tb.locs)
		}
	}
	n := 0
	for _, c := range q.Info.ReferencedColumns {
		if c.Column == "nocol" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("nocol referenced %d times, want 1", n)
	}
}

// A CTE named like a real table carries its own columns.
func TestValidate_CteShadowColumns(t *testing.T) {
	r, err := ValidateQuery("WITH users AS (SELECT 1 AS foo) SELECT users.foo FROM users ORDER BY users.foo", testSchema())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Errors) != 0 || r.CorrectedSQL != "" {
		t.Errorf("errors=%v corrected=%q, want none", r.Errors, r.CorrectedSQL)
	}
}
