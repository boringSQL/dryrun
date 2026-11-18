package schema

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
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
			{Table: qn("public", "a0_plain"), Sizing: TableSizing{Reltuples: 1_000_000, Relpages: 8000, RelAllVisible: ptrInt64(6000)}},
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

	// relallvisible must reach pg_class on both paths: pg_restore_relation_stats
	// on PG18, direct UPDATE below it. Without it the planner costs index-only
	// scans as full heap fetches.
	var visible int64
	if err := pool.QueryRow(ctx,
		`SELECT relallvisible FROM pg_class WHERE oid = 'public.a0_plain'::regclass`).Scan(&visible); err != nil {
		t.Fatalf("query relallvisible: %v", err)
	}
	if visible != 6000 {
		t.Errorf("injected relallvisible = %d, want 6000", visible)
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

	// a snapshot predating relallvisible must not touch the target's value —
	// NULL into pg_restore_relation_stats is a skip, and the legacy path's
	// COALESCE keeps the existing 0
	var visible int64
	if err := pool.QueryRow(ctx,
		`SELECT relallvisible FROM pg_class WHERE oid = 'public.a0_zero'::regclass`).Scan(&visible); err != nil {
		t.Fatalf("query relallvisible: %v", err)
	}
	if visible != 0 {
		t.Errorf("nil relallvisible overwrote target value: got %d, want 0", visible)
	}
}

// A partitioned parent has only inherited=true pg_stats rows (see stats_test.go's
// DedupsInheritedRows), and injection must round-trip that flag rather than always
// writing stainherit=false — otherwise the injected stats land where the planner
// never looks for a partitioned parent. Legacy (pre-PG18) path only, since it writes
// pg_statistic directly; the PG18 path goes through pg_restore_attribute_stats.
func TestInjectStats_PartitionedParentColumnStatsAreInherited(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)
	if major >= 18 {
		t.Skip("legacy pg_statistic path only; PG18 uses pg_restore_attribute_stats")
	}

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS a0_part_stats;
		CREATE TABLE a0_part_stats (id int, v text) PARTITION BY RANGE (id);
		CREATE TABLE a0_part_stats_p1 PARTITION OF a0_part_stats FOR VALUES FROM (0) TO (1000);
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { pool.Exec(ctx, "DROP TABLE IF EXISTS a0_part_stats") })

	nullFrac, nDistinct := 0.0, -1.0
	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{
			{
				Schema: "public", Name: "a0_part_stats",
				Columns:       []Column{{Name: "id"}, {Name: "v"}},
				PartitionInfo: &PartitionInfo{Strategy: PartitionRange, Key: "id"},
			},
		}},
		Planner: &PlannerStatsSnapshot{Columns: []ColumnStatsEntry{
			{
				Table: qn("public", "a0_part_stats"), Column: "id", Inherited: true,
				Stats: ColumnStats{NullFrac: &nullFrac, NDistinct: &nDistinct},
			},
		}},
	}

	res, err := InjectStats(ctx, pool, a, major)
	if err != nil {
		t.Fatalf("InjectStats: %v", err)
	}
	if res.ColumnsUpdated != 1 {
		t.Fatalf("ColumnsUpdated = %d, want 1; warnings = %v", res.ColumnsUpdated, res.Warnings)
	}

	var inherited bool
	err = pool.QueryRow(ctx, `
		SELECT stainherit FROM pg_statistic
		 WHERE starelid = 'public.a0_part_stats'::regclass
		   AND staattnum = (SELECT attnum FROM pg_attribute
		                      WHERE attrelid = 'public.a0_part_stats'::regclass AND attname = 'id')
	`).Scan(&inherited)
	if err != nil {
		t.Fatalf("query pg_statistic: %v", err)
	}
	if !inherited {
		t.Errorf("stainherit = false, want true for a partitioned parent's stats")
	}
}

// PG18 companion of the above: injectColumnStatsPG18 must pass the Inherited
// flag through to pg_restore_attribute_stats's 'inherited' argument instead of
// hard-coding false.
func TestInjectStats_PartitionedParentColumnStatsAreInherited_PG18(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)
	if major < 18 {
		t.Skip("pg_restore_attribute_stats path only; PG < 18 uses direct pg_statistic writes")
	}

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS a0_part_stats18;
		CREATE TABLE a0_part_stats18 (id int, v text) PARTITION BY RANGE (id);
		CREATE TABLE a0_part_stats18_p1 PARTITION OF a0_part_stats18 FOR VALUES FROM (0) TO (1000);
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { pool.Exec(ctx, "DROP TABLE IF EXISTS a0_part_stats18") })

	nullFrac, nDistinct := 0.0, -1.0
	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{
			{
				Schema: "public", Name: "a0_part_stats18",
				Columns:       []Column{{Name: "id"}, {Name: "v"}},
				PartitionInfo: &PartitionInfo{Strategy: PartitionRange, Key: "id"},
			},
		}},
		Planner: &PlannerStatsSnapshot{Columns: []ColumnStatsEntry{
			{
				Table: qn("public", "a0_part_stats18"), Column: "id", Inherited: true,
				Stats: ColumnStats{NullFrac: &nullFrac, NDistinct: &nDistinct},
			},
		}},
	}

	res, err := InjectStats(ctx, pool, a, major)
	if err != nil {
		t.Fatalf("InjectStats: %v", err)
	}
	if res.ColumnsUpdated != 1 {
		t.Fatalf("ColumnsUpdated = %d, want 1; warnings = %v", res.ColumnsUpdated, res.Warnings)
	}

	var inherited bool
	err = pool.QueryRow(ctx, `
		SELECT inherited FROM pg_stats
		 WHERE schemaname = 'public' AND tablename = 'a0_part_stats18' AND attname = 'id'
	`).Scan(&inherited)
	if err != nil {
		t.Fatalf("query pg_statistic: %v", err)
	}
	if !inherited {
		t.Errorf("stainherit = false, want true for a partitioned parent's stats (pg_restore_attribute_stats path)")
	}
}

func qn(schema, name string) QualifiedName { return QualifiedName{Schema: schema, Name: name} }

func ptrInt64(v int64) *int64 { return &v }

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func TestInjectStats_InjectsColumnStats(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS a0_colstats;
		CREATE TABLE a0_colstats (id int PRIMARY KEY, v text);
		INSERT INTO a0_colstats SELECT g, g::text FROM generate_series(1, 50) g;
		ANALYZE a0_colstats;
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { pool.Exec(ctx, "DROP TABLE IF EXISTS a0_colstats") })

	nullFrac := 0.25
	nDistinct := -0.5
	correlation := 0.75
	avgWidth := 4
	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{
			{Schema: "public", Name: "a0_colstats", Columns: []Column{{Name: "id"}, {Name: "v"}}},
		}},
		Planner: &PlannerStatsSnapshot{
			Tables: []TableSizingEntry{
				{Table: qn("public", "a0_colstats"), Sizing: TableSizing{Reltuples: 1_000_000, Relpages: 8000}},
			},
			Columns: []ColumnStatsEntry{{
				Table:  qn("public", "a0_colstats"),
				Column: "id",
				Stats: ColumnStats{
					NullFrac:    &nullFrac,
					NDistinct:   &nDistinct,
					Correlation: &correlation,
					AvgWidth:    &avgWidth,
				},
			}},
		},
	}

	res, err := InjectStats(ctx, pool, a, major)
	if err != nil {
		t.Fatalf("InjectStats: %v", err)
	}
	if res.ColumnsUpdated != 1 {
		t.Fatalf("ColumnsUpdated = %d, want 1 (method %s, warnings %v)", res.ColumnsUpdated, res.Method, res.Warnings)
	}

	var gotNullFrac, gotNDistinct, gotCorrelation float64
	err = pool.QueryRow(ctx, `
		SELECT null_frac, n_distinct, correlation
		  FROM pg_catalog.pg_stats
		 WHERE schemaname = 'public' AND tablename = 'a0_colstats' AND attname = 'id'`,
	).Scan(&gotNullFrac, &gotNDistinct, &gotCorrelation)
	if err != nil {
		t.Fatalf("read back pg_stats (method %s): %v", res.Method, err)
	}
	for _, c := range []struct {
		name      string
		got, want float64
	}{
		{"null_frac", gotNullFrac, nullFrac},
		{"n_distinct", gotNDistinct, nDistinct},
		{"correlation", gotCorrelation, correlation},
	} {
		if diff := c.got - c.want; diff > 1e-6 || diff < -1e-6 {
			t.Errorf("%s = %v, want %v (method %s)", c.name, c.got, c.want, res.Method)
		}
	}
}

// Round-trips captured objects through pg_restore_extended_stats: clear the
// target data, inject a normal and an inherited entry, and recapture. Also
// pins the warning for a statistics object the target does not have.
func TestInjectStats_ExtendedStatistics(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)
	if major < 19 {
		t.Skipf("pg_restore_extended_stats is PostgreSQL 19+; server is %d", major)
	}

	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS a0_extstats CASCADE;
		DROP TABLE IF EXISTS a0_extstats_part CASCADE;
		CREATE TABLE a0_extstats (a int, b text);
		INSERT INTO a0_extstats
		SELECT (g % 2) + 1, CASE WHEN g % 2 = 0 THEN NULL ELSE 'x' END
		  FROM generate_series(1, 400) g;
		CREATE STATISTICS a0_extstats_all (ndistinct, dependencies, mcv) ON a, b FROM a0_extstats;
		CREATE STATISTICS a0_extstats_masked (mcv) ON a, b FROM a0_extstats;
		ANALYZE a0_extstats;

		CREATE TABLE a0_extstats_part (a int, b int) PARTITION BY RANGE (a);
		CREATE TABLE a0_extstats_part_1 PARTITION OF a0_extstats_part FOR VALUES FROM (0) TO (100);
		INSERT INTO a0_extstats_part SELECT g, g FROM generate_series(1, 50) g;
		CREATE STATISTICS a0_extstats_part_s (ndistinct) ON a, b FROM a0_extstats_part;
		ANALYZE a0_extstats_part;
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DROP TABLE IF EXISTS a0_extstats CASCADE; DROP TABLE IF EXISTS a0_extstats_part CASCADE")
	})

	planner, err := CapturePlannerStats(ctx, pool, "sref")
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	find := func(p *PlannerStatsSnapshot, name string, inherited bool) *ExtendedStatsEntry {
		t.Helper()
		for i := range p.ExtendedStats {
			if p.ExtendedStats[i].Statistics.Name == name && p.ExtendedStats[i].Inherited == inherited {
				return &p.ExtendedStats[i]
			}
		}
		t.Fatalf("%s (inherited=%v) missing from capture: %+v", name, inherited, p.ExtendedStats)
		return nil
	}
	plain := find(planner, "a0_extstats_all", false)
	if plain.Definition != "a, b" || plain.MCVValues == nil {
		t.Fatalf("plain layout = %q, mcv = %v", plain.Definition, plain.MCVValues)
	}
	inherited := find(planner, "a0_extstats_part_s", true)
	// as left by a masking policy; the target's own ANALYZE MCV must not survive
	masked := *find(planner, "a0_extstats_masked", false)
	masked.MCVValues, masked.MCVFreqs, masked.MCVBaseFreqs = nil, nil, nil

	for _, stmt := range []string{
		`SELECT pg_clear_extended_stats('public', 'a0_extstats', 'public', 'a0_extstats_all', false)`,
		`SELECT pg_clear_extended_stats('public', 'a0_extstats_part', 'public', 'a0_extstats_part_s', true)`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("clear target stats: %v", err)
		}
	}

	missing := *plain
	missing.Statistics.Name = "a0_extstats_missing"
	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{Tables: []Table{
			{Schema: "public", Name: "a0_extstats", Columns: []Column{{Name: "a"}, {Name: "b"}}},
			{Schema: "public", Name: "a0_extstats_part", Columns: []Column{{Name: "a"}, {Name: "b"}},
				PartitionInfo: &PartitionInfo{Strategy: PartitionRange, Key: "a"}},
		}},
		Planner: &PlannerStatsSnapshot{ExtendedStats: []ExtendedStatsEntry{*plain, *inherited, masked, missing}},
	}

	res, err := InjectStats(ctx, pool, a, major)
	if err != nil {
		t.Fatalf("InjectStats: %v", err)
	}
	if res.ExtendedStatsUpdated != 2 {
		t.Fatalf("ExtendedStatsUpdated = %d, want 2; warnings %v", res.ExtendedStatsUpdated, res.Warnings)
	}
	if res.AutovacuumDisabled != 1 {
		t.Errorf("AutovacuumDisabled = %d, want 1 (partitioned parent must be skipped)", res.AutovacuumDisabled)
	}
	if !hasWarning(res.Warnings, "a0_extstats_missing") {
		t.Errorf("missing object produced no warning; warnings = %v", res.Warnings)
	}

	after, err := CapturePlannerStats(ctx, pool, "sref")
	if err != nil {
		t.Fatalf("recapture: %v", err)
	}
	if got := find(after, "a0_extstats_masked", false); got.MCVValues != nil {
		t.Errorf("masked object kept the target's local MCV: %s", *got.MCVValues)
	}
	for _, want := range []*ExtendedStatsEntry{plain, inherited} {
		got := find(after, want.Statistics.Name, want.Inherited)
		if !reflect.DeepEqual(*got, *want) {
			t.Errorf("%s round trip:\n got %+v\nwant %+v", want.Statistics.Name, *got, *want)
		}
	}
}

// Pre-19 targets skip extended statistics with a reason, not a warning: a
// warning would mark every MCP explain as untrustworthy.
func TestInjectStats_ExtendedStatisticsSkippedBefore19(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)
	if major >= 19 {
		t.Skipf("target has pg_restore_extended_stats; server is %d", major)
	}
	a := &AnnotatedSchema{
		Schema: &SchemaSnapshot{},
		Planner: &PlannerStatsSnapshot{ExtendedStats: []ExtendedStatsEntry{{
			Table:      qn("public", "t"),
			Statistics: qn("public", "s"),
			Definition: "a, b",
		}}},
	}
	res, err := InjectStats(ctx, pool, a, major)
	if err != nil {
		t.Fatalf("InjectStats: %v", err)
	}
	if res.ExtendedStatsSkipped == "" || res.ExtendedStatsUpdated != 0 || len(res.Warnings) != 0 {
		t.Errorf("want a skip reason and no warnings, got %+v", res)
	}
}

// The restore payloads carry no column names, so only the target's key layout
// decides what is safe. A skipped object must leave autovacuum alone, and a
// dropped column must remap attnums onto the right columns, not the reverse.
func TestInjectStats_ExtendedStatisticsTargetLayout(t *testing.T) {
	pool := livePool(t)
	ctx := context.Background()
	major := serverMajor(t, ctx, pool)
	if major < 19 {
		t.Skipf("pg_restore_extended_stats is PostgreSQL 19+; server is %d", major)
	}

	// a determines b, so the dependency degree is asymmetric and a swapped
	// remap cannot pass by coincidence
	if _, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS a0_extlayout CASCADE;
		CREATE TABLE a0_extlayout (a int, b text);
		INSERT INTO a0_extlayout
		SELECT g % 10, CASE WHEN g % 10 < 5 THEN 'lo' END FROM generate_series(1, 1000) g;
		CREATE STATISTICS a0_extlayout_s (ndistinct, dependencies, mcv) ON a, b FROM a0_extlayout;
		ANALYZE a0_extlayout;
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { pool.Exec(ctx, "DROP TABLE IF EXISTS a0_extlayout CASCADE") })

	capture := func() ExtendedStatsEntry {
		t.Helper()
		p, err := CapturePlannerStats(ctx, pool, "sref")
		if err != nil {
			t.Fatalf("capture: %v", err)
		}
		for _, e := range p.ExtendedStats {
			if e.Statistics.Name == "a0_extlayout_s" {
				return e
			}
		}
		t.Fatalf("a0_extlayout_s missing from capture")
		return ExtendedStatsEntry{}
	}
	build := func(table string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			DROP TABLE a0_extlayout CASCADE;
			CREATE TABLE a0_extlayout `+table+`;
			ALTER TABLE a0_extlayout DROP COLUMN IF EXISTS gone;
			CREATE STATISTICS a0_extlayout_s (ndistinct, dependencies, mcv) ON a, b FROM a0_extlayout;
		`); err != nil {
			t.Fatalf("build target %s: %v", table, err)
		}
	}
	// captured before any rebuild: a fresh statistics object has no data yet
	src := capture()
	if src.NDistinct == nil || src.Dependencies == nil || src.MCVValues == nil {
		t.Fatalf("source capture incomplete: %+v", src)
	}
	inject := func() (*InjectResult, error) {
		t.Helper()
		a := &AnnotatedSchema{
			Schema: &SchemaSnapshot{Tables: []Table{
				{Schema: "public", Name: "a0_extlayout", Columns: []Column{{Name: "a"}, {Name: "b"}}},
			}},
			Planner: &PlannerStatsSnapshot{ExtendedStats: []ExtendedStatsEntry{src}},
		}
		return InjectStats(ctx, pool, a, major)
	}

	// same columns, different order: positional MCV would land under the wrong
	// column, so the entry must be skipped and the table left to autovacuum
	t.Run("column order swapped", func(t *testing.T) {
		build("(b text, a int)")
		res, err := inject()
		if err != nil {
			t.Fatalf("InjectStats: %v", err)
		}
		if res.ExtendedStatsUpdated != 0 || res.AutovacuumDisabled != 0 {
			t.Errorf("skipped entry wrote stats or disabled autovacuum: %+v", res)
		}
		if !hasWarning(res.Warnings, "key layout differs") {
			t.Errorf("no layout warning; warnings = %v", res.Warnings)
		}
	})

	// a dropped column shifts a=2, b=3: the payload must follow the target's
	// numbering or dependencies are rejected and MCV is mislabeled
	t.Run("dropped column", func(t *testing.T) {
		build("(gone int, a int, b text)")
		res, err := inject()
		if err != nil {
			t.Fatalf("InjectStats: %v", err)
		}
		if res.ExtendedStatsUpdated != 1 {
			t.Fatalf("ExtendedStatsUpdated = %d, want 1; warnings %v", res.ExtendedStatsUpdated, res.Warnings)
		}
		got := capture()
		if got.MCVValues == nil || *got.MCVValues != *src.MCVValues {
			t.Errorf("mcv = %v, want %v", got.MCVValues, *src.MCVValues)
		}
		if got.NDistinct == nil || got.Dependencies == nil {
			t.Fatalf("kinds missing after injection: n_distinct %v, dependencies %v", got.NDistinct, got.Dependencies)
		}

		// one shift direction only: the source attnums go into the expected
		// keys, the stored ones are used as they are
		key := func(attrs []int16, shift int16) string {
			parts := make([]string, len(attrs))
			for i, a := range attrs {
				parts[i] = strconv.Itoa(int(a + shift))
			}
			return strings.Join(parts, ",")
		}
		var gotND, srcND []ndistinctPayloadItem
		if err := json.Unmarshal([]byte(*got.NDistinct), &gotND); err != nil {
			t.Fatalf("parse stored n_distinct: %v", err)
		}
		if err := json.Unmarshal([]byte(*src.NDistinct), &srcND); err != nil {
			t.Fatalf("parse source n_distinct: %v", err)
		}
		gotNDMap, srcNDMap := map[string]int64{}, map[string]int64{}
		for _, it := range gotND {
			gotNDMap[key(it.Attributes, 0)] = it.NDistinct
		}
		for _, it := range srcND {
			srcNDMap[key(it.Attributes, 1)] = it.NDistinct
		}
		if !reflect.DeepEqual(gotNDMap, srcNDMap) {
			t.Errorf("n_distinct = %v, want %v", gotNDMap, srcNDMap)
		}

		var gotDeps, srcDeps []dependencyPayloadItem
		if err := json.Unmarshal([]byte(*got.Dependencies), &gotDeps); err != nil {
			t.Fatalf("parse stored dependencies: %v", err)
		}
		if err := json.Unmarshal([]byte(*src.Dependencies), &srcDeps); err != nil {
			t.Fatalf("parse source dependencies: %v", err)
		}
		gotDepMap, srcDepMap := map[string]float64{}, map[string]float64{}
		for _, it := range gotDeps {
			gotDepMap[key(it.Attributes, 0)+"=>"+key([]int16{it.Dependency}, 0)] = it.Degree
		}
		for _, it := range srcDeps {
			srcDepMap[key(it.Attributes, 1)+"=>"+key([]int16{it.Dependency}, 1)] = it.Degree
		}
		if !reflect.DeepEqual(gotDepMap, srcDepMap) {
			t.Errorf("dependencies = %v, want %v", gotDepMap, srcDepMap)
		}
	})
}
