package query

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boringsql/dryrun/internal/schema"
)

// Every fixture migration runs on a real server and dryrun's verdict is checked
// against what the engine did. Measured, not declared: whether the table file was
// rewritten (relfilenode) and whether the statement failed. `scan` is declared,
// for the statements that scan under lock without rewriting (nothing observable
// separates those). Rule: safe => no rewrite, no failure, no scan; dangerous =>
// one of them (else a false alarm). Caution is unconstrained.
// Gated on TEST_DATABASE_URL; CI runs it on every supported major.

type engineFixture struct {
	name  string
	setup string // %[1]s = the fixture's table; default baseTable
	ddl   string
	drop  string // default: drop table and its _ref sibling

	noTx    bool // cannot run in a transaction block: run for real, no rollback
	rewrite bool // expected: table file rewritten
	fail    bool // expected: statement fails
	scan    bool // declared: scans the table under lock without rewriting

	verdict SafetyRating
}

const (
	baseTable = `CREATE TABLE %[1]s (id bigint PRIMARY KEY, code varchar(20), slug varchar, tag char(4), n int NOT NULL DEFAULT 0, email text);
		INSERT INTO %[1]s SELECT g, 'c' || g, 's' || g, 't', g, 'e' || g FROM generate_series(1, 200) g;
		CREATE TABLE %[1]s_ref (id int PRIMARY KEY);
		INSERT INTO %[1]s_ref SELECT g FROM generate_series(1, 200) g;`
	defaultDrop = `DROP TABLE IF EXISTS %[1]s CASCADE; DROP TABLE IF EXISTS %[1]s_ref CASCADE`
)

var engineFixtures = []engineFixture{
	{name: "add nullable column", ddl: "ALTER TABLE %[1]s ADD COLUMN x int", verdict: SafetySafe},
	{name: "add constant default", ddl: "ALTER TABLE %[1]s ADD COLUMN x int NOT NULL DEFAULT 0", verdict: SafetySafe},
	{name: "add stable default now()", ddl: "ALTER TABLE %[1]s ADD COLUMN x timestamptz NOT NULL DEFAULT now()", verdict: SafetySafe},
	{name: "add volatile default", ddl: "ALTER TABLE %[1]s ADD COLUMN x double precision DEFAULT random()", rewrite: true, verdict: SafetyDangerous},
	{name: "add serial", ddl: "ALTER TABLE %[1]s ADD COLUMN x serial", rewrite: true, verdict: SafetyDangerous},
	{name: "add bigserial", ddl: "ALTER TABLE %[1]s ADD COLUMN x bigserial", rewrite: true, verdict: SafetyDangerous},
	{name: "add identity", ddl: "ALTER TABLE %[1]s ADD COLUMN x bigint GENERATED ALWAYS AS IDENTITY", rewrite: true, verdict: SafetyDangerous},
	{name: "add stored generated", ddl: "ALTER TABLE %[1]s ADD COLUMN x int GENERATED ALWAYS AS (n * 2) STORED", rewrite: true, verdict: SafetyDangerous},

	{name: "varchar widen", ddl: "ALTER TABLE %[1]s ALTER COLUMN code TYPE varchar(50)", verdict: SafetySafe},
	{name: "varchar to unbounded", ddl: "ALTER TABLE %[1]s ALTER COLUMN code TYPE varchar", verdict: SafetySafe},
	{name: "varchar to text", ddl: "ALTER TABLE %[1]s ALTER COLUMN code TYPE text", verdict: SafetySafe},
	{name: "unbounded varchar to text", ddl: "ALTER TABLE %[1]s ALTER COLUMN slug TYPE text", verdict: SafetySafe},
	{name: "varchar narrow", ddl: "ALTER TABLE %[1]s ALTER COLUMN code TYPE varchar(10)", rewrite: true, verdict: SafetyDangerous},
	{name: "text to varchar", ddl: "ALTER TABLE %[1]s ALTER COLUMN email TYPE varchar(100)", rewrite: true, verdict: SafetyDangerous},
	{name: "char to text", ddl: "ALTER TABLE %[1]s ALTER COLUMN tag TYPE text", rewrite: true, verdict: SafetyDangerous},
	{name: "int to bigint", ddl: "ALTER TABLE %[1]s ALTER COLUMN n TYPE bigint", rewrite: true, verdict: SafetyDangerous},

	{name: "drop column", ddl: "ALTER TABLE %[1]s DROP COLUMN email", verdict: SafetySafe},
	{name: "set not null scans", ddl: "ALTER TABLE %[1]s ALTER COLUMN email SET NOT NULL", scan: true, verdict: SafetyDangerous},
	{
		name:    "set not null skips the scan behind a validated check",
		setup:   baseTable + "ALTER TABLE %[1]s ADD CONSTRAINT %[1]s_email_nn CHECK (email IS NOT NULL);",
		ddl:     "ALTER TABLE %[1]s ALTER COLUMN email SET NOT NULL",
		verdict: SafetySafe,
	},
	{name: "add check scans", ddl: "ALTER TABLE %[1]s ADD CONSTRAINT %[1]s_c CHECK (n >= 0)", scan: true, verdict: SafetyDangerous},
	{name: "add check not valid", ddl: "ALTER TABLE %[1]s ADD CONSTRAINT %[1]s_c CHECK (n >= 0) NOT VALID", verdict: SafetySafe},
	{name: "add foreign key scans", ddl: "ALTER TABLE %[1]s ADD CONSTRAINT %[1]s_fk FOREIGN KEY (n) REFERENCES %[1]s_ref (id)", scan: true, verdict: SafetyDangerous},
	{name: "add foreign key not valid", ddl: "ALTER TABLE %[1]s ADD CONSTRAINT %[1]s_fk FOREIGN KEY (n) REFERENCES %[1]s_ref (id) NOT VALID", verdict: SafetySafe},
	{name: "add unique builds an index", ddl: "ALTER TABLE %[1]s ADD CONSTRAINT %[1]s_u UNIQUE (email)", scan: true, verdict: SafetyDangerous},
	{
		name: "primary key using index",
		setup: `CREATE TABLE %[1]s (id bigint NOT NULL, n int);
			INSERT INTO %[1]s SELECT g, g FROM generate_series(1, 200) g;
			CREATE UNIQUE INDEX %[1]s_uidx ON %[1]s (id);`,
		ddl:     "ALTER TABLE %[1]s ADD CONSTRAINT %[1]s_pk PRIMARY KEY USING INDEX %[1]s_uidx",
		verdict: SafetySafe,
	},
	{name: "create index blocks writes", ddl: "CREATE INDEX %[1]s_i ON %[1]s (n)", scan: true, verdict: SafetyDangerous},
	{name: "create index concurrently", ddl: "CREATE INDEX CONCURRENTLY %[1]s_i ON %[1]s (n)", noTx: true, verdict: SafetySafe},
	{
		name: "create index concurrently on a partitioned parent",
		setup: `CREATE TABLE %[1]s (id bigint, ts timestamptz) PARTITION BY RANGE (ts);
			CREATE TABLE %[1]s_p PARTITION OF %[1]s FOR VALUES FROM ('2020-01-01') TO ('2030-01-01');`,
		ddl:  "CREATE INDEX CONCURRENTLY %[1]s_i ON %[1]s (ts)",
		noTx: true, fail: true, verdict: SafetyDangerous,
	},
	{
		name: "create index concurrently inside begin/commit",
		ddl:  "BEGIN; CREATE INDEX CONCURRENTLY %[1]s_i ON %[1]s (n); COMMIT;",
		noTx: true, fail: true, verdict: SafetyDangerous,
	},

	{name: "vacuum full", ddl: "VACUUM FULL %[1]s", noTx: true, rewrite: true, verdict: SafetyDangerous},
	{name: "cluster", ddl: "CLUSTER %[1]s USING %[1]s_pkey", rewrite: true, verdict: SafetyDangerous},
	{
		name:    "refresh materialized view",
		setup:   "CREATE MATERIALIZED VIEW %[1]s AS SELECT g AS id FROM generate_series(1, 200) g;",
		ddl:     "REFRESH MATERIALIZED VIEW %[1]s",
		drop:    "DROP MATERIALIZED VIEW IF EXISTS %[1]s CASCADE",
		rewrite: true, verdict: SafetyDangerous,
	},
}

type engineFacts struct {
	rewrote bool
	failed  bool
	err     error
	locks   []string
}

func (f engineFixture) sql(s string, table string) string { return fmt.Sprintf(s, table) }

func TestVerdictsMatchTheEngine(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping engine verdict harness")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	table := func(i int) string { return fmt.Sprintf("eh_%02d", i) }
	dropAll := func() {
		for i, f := range engineFixtures {
			drop := f.drop
			if drop == "" {
				drop = defaultDrop
			}
			if _, err := pool.Exec(ctx, f.sql(drop, table(i)), pgx.QueryExecModeSimpleProtocol); err != nil {
				t.Logf("drop %s: %v", table(i), err)
			}
		}
	}
	dropAll()
	t.Cleanup(dropAll)

	for i, f := range engineFixtures {
		setup := f.setup
		if setup == "" {
			setup = baseTable
		}
		if _, err := pool.Exec(ctx, f.sql(setup, table(i)), pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("setup %q: %v", f.name, err)
		}
	}

	snap, err := schema.IntrospectSchema(ctx, pool)
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	annotated := &schema.AnnotatedSchema{Schema: snap}

	var version string
	if err := pool.QueryRow(ctx, "SHOW server_version").Scan(&version); err != nil {
		t.Fatal(err)
	}

	for i, f := range engineFixtures {
		t.Run(f.name, func(t *testing.T) {
			tbl := table(i)
			ddl := f.sql(f.ddl, tbl)

			checks, err := CheckMigration(ddl, annotated)
			if err != nil {
				t.Fatalf("CheckMigration(%s): %v", ddl, err)
			}
			verdict := worstVerdict(checks)

			facts := runOnEngine(ctx, t, pool, tbl, ddl, f.noTx)
			t.Logf("PG %s | verdict=%s rewrote=%v failed=%v locks=%v err=%v", version, verdict, facts.rewrote, facts.failed, facts.locks, facts.err)

			if facts.rewrote != f.rewrite {
				t.Errorf("engine disagrees with the fixture: rewrote=%v, fixture says %v", facts.rewrote, f.rewrite)
			}
			if facts.failed != f.fail {
				t.Errorf("engine disagrees with the fixture: failed=%v (%v), fixture says %v", facts.failed, facts.err, f.fail)
			}
			if verdict != f.verdict {
				t.Errorf("verdict %s, fixture expects %s", verdict, f.verdict)
			}

			longWork := facts.rewrote || f.scan
			switch {
			case verdict == SafetySafe && (facts.failed || longWork):
				t.Errorf("FALSE SAFE: engine rewrote=%v failed=%v scan=%v, dryrun said safe", facts.rewrote, facts.failed, f.scan)
			case verdict == SafetyDangerous && !facts.failed && !longWork:
				t.Errorf("FALSE DANGEROUS: engine did no rewrite, no scan and no failure, dryrun said dangerous")
			}
		})
	}
}

func worstVerdict(checks []MigrationCheck) SafetyRating {
	worst := SafetySafe
	for _, c := range checks {
		if c.Operation == "TRANSACTION CONTROL" {
			continue
		}
		switch {
		case c.Safety == SafetyDangerous:
			return SafetyDangerous
		case c.Safety == SafetyCaution:
			worst = SafetyCaution
		}
	}
	return worst
}

// Transactional statements run in a transaction that is rolled back, so the
// fixture survives; the strongest lock is read from pg_locks before that.
func runOnEngine(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table, ddl string, noTx bool) engineFacts {
	t.Helper()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	filenode := func() int64 {
		var n *int64
		if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT pg_relation_filenode('%s'::regclass)::bigint", table)).Scan(&n); err != nil || n == nil {
			return 0
		}
		return *n
	}

	var facts engineFacts
	if !noTx {
		if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer conn.Exec(ctx, "ROLLBACK")
	}
	before := filenode()
	_, runErr := conn.Exec(ctx, ddl, pgx.QueryExecModeSimpleProtocol)
	if runErr != nil {
		facts.failed, facts.err = true, runErr
		conn.Exec(ctx, "ROLLBACK")
		return facts
	}
	facts.rewrote = before != 0 && filenode() != before

	if !noTx {
		rows, err := conn.Query(ctx, fmt.Sprintf(
			"SELECT mode FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'relation' AND relation = '%s'::regclass", table))
		if err == nil {
			for rows.Next() {
				var m string
				if rows.Scan(&m) == nil {
					facts.locks = append(facts.locks, strings.TrimSuffix(m, "Lock"))
				}
			}
			rows.Close()
		}
	}
	return facts
}
