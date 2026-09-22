package query

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func testSchema() *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion:   "PostgreSQL 17.0",
		Database:    "test",
		Timestamp:   time.Now().UTC(),
		ContentHash: "test",
		Tables: []schema.Table{
			{
				OID:    1,
				Schema: "public",
				Name:   "users",
				Columns: []schema.Column{
					{Name: "id", Ordinal: 1, TypeName: "bigint"},
					{Name: "email", Ordinal: 2, TypeName: "text"},
				},
			},
			{
				OID:    2,
				Schema: "public",
				Name:   "orders",
				Columns: []schema.Column{
					{Name: "id", Ordinal: 1, TypeName: "bigint"},
					{Name: "user_id", Ordinal: 2, TypeName: "bigint"},
				},
			},
			{
				OID:    3,
				Schema: "public",
				Name:   "events",
				Columns: []schema.Column{
					{Name: "id", Ordinal: 1, TypeName: "bigint"},
					{Name: "created_at", Ordinal: 2, TypeName: "timestamptz"},
					{Name: "user_id", Ordinal: 3, TypeName: "bigint"},
				},
				PartitionInfo: &schema.PartitionInfo{
					Strategy: schema.PartitionRange,
					Key:      "created_at",
					Children: []schema.PartitionChild{
						{Schema: "public", Name: "events_2025_01", Bound: "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"},
						{Schema: "public", Name: "events_2025_02", Bound: "FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')"},
						{Schema: "public", Name: "events_2025_03", Bound: "FOR VALUES FROM ('2025-03-01') TO ('2025-04-01')"},
					},
				},
			},
		},
	}
}

func TestValidQuery(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT id, email FROM users WHERE id = 1", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid {
		t.Errorf("expected valid, got errors: %v", result.Errors)
	}
}

func TestNonexistentTable(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM nonexistent", snap)
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid {
		t.Error("expected invalid")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e, "does not exist") {
			found = true
		}
	}
	if !found {
		t.Error("expected 'does not exist' error")
	}
}

func TestNonexistentColumnInWhere(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT id FROM users u WHERE u.fake_col = 1", snap)
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid {
		t.Error("expected invalid")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e, "fake_col") {
			found = true
		}
	}
	if !found {
		t.Error("expected error mentioning fake_col")
	}
}

func TestSelectStarResolved(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM users", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid {
		t.Errorf("expected valid, got errors: %v", result.Errors)
	}
	if len(result.ResolvedStarColumns) == 0 {
		t.Error("expected resolved star columns")
	}
	if len(result.ResolvedStarColumns[0].Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.ResolvedStarColumns[0].Columns))
	}
}

func TestSelectStarWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM users", snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "SELECT *") {
			found = true
		}
	}
	if !found {
		t.Error("expected SELECT * warning")
	}
}

// Unbounded-query warnings depend on table row counts; ValidateQuery no
// longer carries an AnnotatedSchema, so the heuristic is dormant until a
// future migration plumbs annotated through. Coverage will follow.
func TestUnboundedQueryWarning(t *testing.T) {
	t.Skip("unbounded-query heuristic disabled until ValidateQuery accepts AnnotatedSchema")
}

func TestCartesianJoinWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM users, orders", snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "artesian") {
			found = true
		}
	}
	if !found {
		t.Error("expected Cartesian join warning")
	}
}

func hasCartesianWarning(result *ValidationResult) bool {
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "artesian") {
			return true
		}
	}
	return false
}

func TestCartesianJoinImplicitWhereNoWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT u.id FROM users u, orders o WHERE u.id = o.user_id", snap)
	if err != nil {
		t.Fatal(err)
	}
	if hasCartesianWarning(result) {
		t.Errorf("implicit join written in WHERE reported as Cartesian: %+v", result.Warnings)
	}
}

func TestCartesianJoinSubqueryNoWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)", snap)
	if err != nil {
		t.Fatal(err)
	}
	if hasCartesianWarning(result) {
		t.Errorf("subquery table reported as Cartesian: %+v", result.Warnings)
	}
}

func TestCartesianJoinOnClauseNoWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id", snap)
	if err != nil {
		t.Fatal(err)
	}
	if hasCartesianWarning(result) {
		t.Errorf("JOIN ... ON reported as Cartesian: %+v", result.Warnings)
	}
}

func TestCartesianJoinCteNoWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("WITH recent AS (SELECT id FROM orders WHERE user_id = 5) SELECT u.id FROM users u JOIN recent r ON u.id = r.id", snap)
	if err != nil {
		t.Fatal(err)
	}
	if hasCartesianWarning(result) {
		t.Errorf("CTE query reported as Cartesian: %+v", result.Warnings)
	}
}

func TestCartesianJoinPartialConnectivityWarns(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM users u, orders o, events e WHERE u.id = o.user_id", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCartesianWarning(result) {
		t.Errorf("disconnected events not reported as Cartesian: %+v", result.Warnings)
	}
}

func TestCartesianJoinCrossJoinWarns(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM users u CROSS JOIN orders o", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCartesianWarning(result) {
		t.Errorf("CROSS JOIN not reported as Cartesian: %+v", result.Warnings)
	}
}

func TestCartesianJoinNoWarning(t *testing.T) {
	snap := testSchema()
	cases := map[string]string{
		"using":            "SELECT * FROM users u JOIN orders o USING (id)",
		"natural":          "SELECT * FROM users u NATURAL JOIN orders o",
		"range predicate":  "SELECT * FROM users u, orders o WHERE u.id > o.user_id",
		"derived linked":   "SELECT * FROM users u, (SELECT * FROM orders) x WHERE u.id = x.user_id",
		"wrapped columns":  "SELECT * FROM users u JOIN orders o ON LOWER(u.email) = o.x",
		"full outer":       "SELECT * FROM users u FULL JOIN orders o ON u.id = o.user_id",
		"three way linked": "SELECT * FROM users u, orders o, events e WHERE u.id = o.user_id AND o.user_id = e.user_id",
	}
	for name, sql := range cases {
		result, err := ValidateQuery(sql, snap)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if hasCartesianWarning(result) {
			t.Errorf("%s: %q reported as Cartesian: %+v", name, sql, result.Warnings)
		}
	}
}

func TestCartesianJoinWarns(t *testing.T) {
	snap := testSchema()
	cases := map[string]string{
		"comma separated":  "SELECT * FROM users, orders",
		"cross join":       "SELECT * FROM users u CROSS JOIN orders o",
		"derived unlinked": "SELECT * FROM users u, (SELECT * FROM orders) x",
		"partially linked": "SELECT * FROM a, b JOIN c USING (x)",
	}
	for name, sql := range cases {
		result, err := ValidateQuery(sql, snap)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !hasCartesianWarning(result) {
			t.Errorf("%s: %q not reported as Cartesian: %+v", name, sql, result.Warnings)
		}
	}
}

func TestCartesianJoinMultiStatement(t *testing.T) {
	snap := testSchema()
	// the first statement is a Cartesian product even though the second is joined
	result, err := ValidateQuery("SELECT * FROM users, orders; SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCartesianWarning(result) {
		t.Errorf("first statement's Cartesian product masked by second statement: %+v", result.Warnings)
	}
}

func TestPartitionKeyMissingWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM events WHERE user_id = 5", snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "partition key") {
			found = true
		}
	}
	if !found {
		t.Error("expected partition key warning for query missing partition key filter")
	}
}

func TestPartitionKeyPresentNoWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM events WHERE created_at > '2025-01-01'", snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "partition key") {
			t.Error("unexpected partition key warning when partition key is in WHERE")
		}
	}
}

func TestPartitionKeyPresentWithOtherFilter(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM events WHERE created_at > '2025-01-01' AND user_id = 5", snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "partition key") {
			t.Error("unexpected partition key warning when partition key is in WHERE")
		}
	}
}

func TestFuncWrapPartitionKeyExtract(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM events WHERE EXTRACT(year FROM created_at) = 2025", snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "wrapped in extract") {
			found = true
		}
	}
	if !found {
		t.Error("expected func-wrap warning for EXTRACT on partition key")
	}
}

func TestFuncWrapPartitionKeyTypeCast(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM events WHERE created_at::date = '2025-01-01'", snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "wrapped in ::date") {
			found = true
		}
	}
	if !found {
		t.Error("expected func-wrap warning for ::date on partition key")
	}
}

func TestFuncWrapPartitionKeyDateTrunc(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM events WHERE date_trunc('month', created_at) = '2025-01-01'", snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "wrapped in date_trunc") {
			found = true
		}
	}
	if !found {
		t.Error("expected func-wrap warning for date_trunc on partition key")
	}
}

func TestNoFuncWrapWarningForDirectFilter(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM events WHERE created_at > '2025-01-01'", snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "wrapped in") {
			t.Error("unexpected func-wrap warning for direct partition key filter")
		}
	}
}

func TestNoFuncWrapWarningForLiteralFunc(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM events WHERE created_at > now()", snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "wrapped in") {
			t.Error("unexpected func-wrap warning when function is on literal side")
		}
	}
}

func TestUpdatePartitionKeyWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("UPDATE events SET created_at = '2026-01-01' WHERE id = 1", snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "UPDATE changes partition key") {
			found = true
		}
	}
	if !found {
		t.Error("expected warning when UPDATE changes partition key")
	}
}

func TestUpdateNonPartitionKeyNoWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("UPDATE events SET user_id = 99 WHERE id = 1", snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "UPDATE changes partition key") {
			t.Error("unexpected partition key update warning when SET does not touch partition key")
		}
	}
}

func TestUpdatePartitionKeyOnNonPartitionedTable(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("UPDATE users SET email = 'new@test.com' WHERE id = 1", snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "UPDATE changes partition key") {
			t.Error("unexpected partition key update warning on non-partitioned table")
		}
	}
}

func TestUpdateTargetsParsed(t *testing.T) {
	parsed, err := ParseSQL("UPDATE events SET created_at = '2026-01-01', user_id = 5 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Info.UpdateTargets) != 2 {
		t.Fatalf("expected 2 update targets, got %d", len(parsed.Info.UpdateTargets))
	}
	expected := map[string]bool{"created_at": true, "user_id": true}
	for _, ut := range parsed.Info.UpdateTargets {
		if !expected[ut] {
			t.Errorf("unexpected update target: %s", ut)
		}
	}
}

func TestNonPartitionedTableNoWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT * FROM users WHERE email = 'test@test.com'", snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "partition key") {
			t.Error("unexpected partition key warning on non-partitioned table")
		}
	}
}

// issue #32: pg_query treats DO/function bodies as opaque strings, so the
// body escapes static validation. We can't catch the runtime error, but we
// must at least warn that the body wasn't checked instead of claiming clean.
func TestDoBlockBodyNotValidatedWarning(t *testing.T) {
	snap := testSchema()
	sql := `DO $$
BEGIN
  RAISE EXCEPTION 'bad format output: %', format('value %s %s', 'one');
END
$$;`
	result, err := ValidateQuery(sql, snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "DO body") && strings.Contains(w.Message, "not statically validated") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected DO body not-validated warning, got %+v", result.Warnings)
	}
}

func TestPlpgsqlFunctionBodyNotValidatedWarning(t *testing.T) {
	snap := testSchema()
	cases := []struct {
		sql  string
		kind string
	}{
		{`CREATE FUNCTION f() RETURNS void AS $$ BEGIN RETURN; END $$ LANGUAGE plpgsql;`, "CREATE FUNCTION body"},
		{`CREATE PROCEDURE p() AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql;`, "CREATE PROCEDURE body"},
	}
	for _, tc := range cases {
		result, err := ValidateQuery(tc.sql, snap)
		if err != nil {
			t.Fatalf("%s: %v", tc.kind, err)
		}
		found := false
		for _, w := range result.Warnings {
			if strings.Contains(w.Message, tc.kind) && strings.Contains(w.Message, "plpgsql") {
				found = true
			}
		}
		if !found {
			t.Errorf("expected %q warning, got %+v", tc.kind, result.Warnings)
		}
	}
}

func TestPlainSelectNoBodyWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT id FROM users", snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "not statically validated") {
			t.Errorf("unexpected body warning on plain SELECT: %s", w.Message)
		}
	}
}

func TestCteBodyGhostTableReported(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("WITH x AS (SELECT * FROM ghost) SELECT * FROM x", snap)
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid {
		t.Fatal("expected invalid: ghost inside CTE body was not checked")
	}
	found := false
	for _, e := range result.Errors {
		if strings.Contains(e, "ghost") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a ghost error, got %v", result.Errors)
	}
}

func TestCteBodyColumnTypoCorrected(t *testing.T) {
	res, err := ValidateQuery("WITH x AS (SELECT u.emial FROM users u) SELECT * FROM x", correctSchema())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.CorrectedSQL, "u.email") {
		t.Fatalf("expected CTE body column typo corrected, got %q (errors: %v)", res.CorrectedSQL, res.Errors)
	}
	again, err := ValidateQuery(res.CorrectedSQL, correctSchema())
	if err != nil || !again.Valid {
		t.Fatalf("corrected CTE query does not validate: %v %v", err, again.Errors)
	}
}

func TestRecursiveCteSelfReferenceValid(t *testing.T) {
	snap := testSchema()
	res, err := ValidateQuery("WITH RECURSIVE x AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM x WHERE id < 5) SELECT id FROM x", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid {
		t.Fatalf("recursive CTE self-reference reported missing: %v", res.Errors)
	}
}
