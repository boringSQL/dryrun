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
				Stats: &schema.TableStats{
					Reltuples: 1_000_000,
					TableSize: 100_000_000,
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
				Stats: &schema.TableStats{
					Reltuples: 50,
					TableSize: 8192,
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
				Stats: &schema.TableStats{
					Reltuples: 50_000_000,
					TableSize: 5_000_000_000,
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

func TestUnboundedQueryWarning(t *testing.T) {
	snap := testSchema()
	result, err := ValidateQuery("SELECT id FROM users", snap)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w.Message, "unbounded") {
			found = true
		}
	}
	if !found {
		t.Error("expected unbounded query warning")
	}
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
