package schema

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Environment-contract tripwires for stats injection. Without them the injected
// plan silently lies: autovacuum overwrites injected stats, or the injected
// relpages/reltuples get rescaled away when pg_regresql is not loaded. All are
// DB-coupled (they assert catalog side effects and the pg_regresql probe), so
// they run only against a throwaway Postgres pointed at by TEST_DATABASE_URL,
// same gate as stats_test.go.

func serverMajor(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var num int
	if err := pool.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&num); err != nil {
		t.Fatalf("server_version_num: %v", err)
	}
	return num / 10000
}

func reloptions(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rel string) []string {
	t.Helper()
	var opts []string
	err := pool.QueryRow(ctx,
		"SELECT COALESCE(reloptions, '{}') FROM pg_class WHERE oid = $1::regclass",
		rel).Scan(&opts)
	if err != nil {
		t.Fatalf("reloptions(%s): %v", rel, err)
	}
	return opts
}

func hasWarning(warns []string, substr string) bool {
	for _, w := range warns {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}

// The two load-bearing tripwires on a plain injected table: autovacuum must be
// turned off before injecting (else a background analyze overwrites the injected
// pg_statistic/pg_class with local toy-data stats), and — because a stock
// Postgres has no pg_regresql loaded — the result must warn that the injected
// relpages/reltuples will be rescaled to the empty relation's physical size and
// ignored by the planner.
func TestInjectStats_DisablesAutovacuumAndFlagsMissingHook(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS a0_plain;
		CREATE TABLE a0_plain (id int PRIMARY KEY, v text);
		INSERT INTO a0_plain SELECT g, g::text FROM generate_series(1, 50) g;
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { pool.Exec(ctx, "DROP TABLE IF EXISTS a0_plain") })

	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{
			{Schema: "public", Name: "a0_plain", Columns: []Column{{Name: "id"}, {Name: "v"}}},
		}},
		Planner: &PlannerStatsSnapshot{Tables: []TableSizingEntry{
			{Table: qn("public", "a0_plain"), Sizing: TableSizing{Reltuples: 1_000_000, Relpages: 8000}},
		}},
	}

	res, err := InjectStats(ctx, pool, a, major)
	if err != nil {
		t.Fatalf("InjectStats: %v", err)
	}

	if res.AutovacuumDisabled != 1 {
		t.Errorf("AutovacuumDisabled = %d, want 1", res.AutovacuumDisabled)
	}
	if opts := reloptions(t, ctx, pool, "public.a0_plain"); !contains(opts, "autovacuum_enabled=false") {
		t.Errorf("reloptions = %v, want autovacuum_enabled=false", opts)
	}
	if res.PgRegresqlLoaded {
		t.Error("PgRegresqlLoaded = true on a stock server; probe is wrong")
	}
	if res.TablesUpdated != 1 {
		t.Errorf("TablesUpdated = %d, want 1", res.TablesUpdated)
	}
	if !hasWarning(res.Warnings, "pg_regresql") {
		t.Errorf("expected pg_regresql tripwire, warnings = %v", res.Warnings)
	}
}

// A partitioned parent (relkind 'p') rejects storage params, so the disable
// must skip it — no error, no spurious warning, no reloptions change — while a
// plain table alongside it is still disabled.
func TestInjectStats_SkipsPartitionedParent(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS a0_part;
		CREATE TABLE a0_part (id int, d date) PARTITION BY RANGE (d);
		CREATE TABLE a0_part_2026 PARTITION OF a0_part FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { pool.Exec(ctx, "DROP TABLE IF EXISTS a0_part") })

	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{
			{
				Schema: "public", Name: "a0_part",
				Columns:       []Column{{Name: "id"}, {Name: "d"}},
				PartitionInfo: &PartitionInfo{Strategy: PartitionRange, Key: "d"},
			},
		}},
		Planner: &PlannerStatsSnapshot{Tables: []TableSizingEntry{
			{Table: qn("public", "a0_part"), Sizing: TableSizing{Reltuples: 1000, Relpages: 10}},
		}},
	}

	res, err := InjectStats(ctx, pool, a, major)
	if err != nil {
		t.Fatalf("InjectStats: %v", err)
	}

	if res.AutovacuumDisabled != 0 {
		t.Errorf("AutovacuumDisabled = %d, want 0 (parent skipped)", res.AutovacuumDisabled)
	}
	if opts := reloptions(t, ctx, pool, "public.a0_part"); contains(opts, "autovacuum_enabled=false") {
		t.Errorf("partitioned parent got reloptions %v; should be untouched", opts)
	}
	if hasWarning(res.Warnings, "disable autovacuum") {
		t.Errorf("partitioned parent produced a disable-autovacuum warning: %v", res.Warnings)
	}
}

// Regression guard for the loop restructure: index sizing is keyed independently
// of table sizing, so a table carrying only index sizing (no table sizing, no
// column stats) must still have its index injected — an early skip on missing
// table sizing would wrongly drop these.
func TestInjectStats_InjectsIndexWithoutTableSizing(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS a0_idx;
		CREATE TABLE a0_idx (id int PRIMARY KEY, v text);
		CREATE INDEX a0_idx_v ON a0_idx (v);
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { pool.Exec(ctx, "DROP TABLE IF EXISTS a0_idx") })

	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{
			{
				Schema: "public", Name: "a0_idx",
				Columns: []Column{{Name: "id"}, {Name: "v"}},
				Indexes: []Index{{Name: "a0_idx_v"}},
			},
		}},
		// no TableSizingEntry for a0_idx — only the index
		Planner: &PlannerStatsSnapshot{Indexes: []IndexSizingEntry{
			{Table: qn("public", "a0_idx"), Index: "a0_idx_v", Sizing: IndexSizing{Reltuples: 500_000, Relpages: 2000}},
		}},
	}

	res, err := InjectStats(ctx, pool, a, major)
	if err != nil {
		t.Fatalf("InjectStats: %v", err)
	}

	if res.IndexesUpdated != 1 {
		t.Errorf("IndexesUpdated = %d, want 1 (index injected despite no table sizing)", res.IndexesUpdated)
	}
	// the table is still touched (via its index), so autovacuum is disabled
	if res.AutovacuumDisabled != 1 {
		t.Errorf("AutovacuumDisabled = %d, want 1", res.AutovacuumDisabled)
	}
}

// A snapshot with reltuples <= 0 injects a degenerate size; the plan built on it
// is worthless, so it must warn rather than silently produce a bad plan.
func TestInjectStats_WarnsOnNonPositiveReltuples(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS a0_zero;
		CREATE TABLE a0_zero (id int PRIMARY KEY);
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { pool.Exec(ctx, "DROP TABLE IF EXISTS a0_zero") })

	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{
			{Schema: "public", Name: "a0_zero", Columns: []Column{{Name: "id"}}},
		}},
		Planner: &PlannerStatsSnapshot{Tables: []TableSizingEntry{
			{Table: qn("public", "a0_zero"), Sizing: TableSizing{Reltuples: 0, Relpages: 0}},
		}},
	}

	res, err := InjectStats(ctx, pool, a, major)
	if err != nil {
		t.Fatalf("InjectStats: %v", err)
	}
	if !hasWarning(res.Warnings, "reltuples") {
		t.Errorf("expected non-positive reltuples warning, warnings = %v", res.Warnings)
	}
}

func qn(schema, name string) QualifiedName { return QualifiedName{Schema: schema, Name: name} }

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
