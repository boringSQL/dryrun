package jit

import "fmt"

// JIT error dictionary entry returned inline with tool responses
type Entry struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
	Fix    string `json:"fix"`
	Note   string `json:"note,omitempty"`
}

// Warning renders status and reason without the fix, for callers that return
// a real rewrite instead.
func (e Entry) Warning() string {
	s := fmt.Sprintf("STATUS: %s\nREASON: %s", e.Status, e.Reason)
	if e.Note != "" {
		s += "\nNOTE: " + e.Note
	}
	return s
}

func (e Entry) String() string {
	s := fmt.Sprintf("STATUS: %s\nREASON: %s\nFIX:\n%s", e.Status, e.Reason, e.Fix)
	if e.Note != "" {
		s += "\nNOTE: " + e.Note
	}
	return s
}

func AddColumnVolatileDefault(table, col, colType, defaultExpr string) Entry {
	return Entry{
		Status: "DANGEROUS, full table rewrite",
		Reason: fmt.Sprintf("Volatile DEFAULT (%s) rewrites every row under ACCESS EXCLUSIVE lock.", defaultExpr),
		Fix: fmt.Sprintf(
			"  1. ALTER TABLE %s ADD COLUMN %s %s;\n"+
				"  2. ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;\n"+
				"  3. UPDATE %s SET %s = %s WHERE %s IS NULL AND id BETWEEN ... AND ...; -- backfill in batches",
			table, col, colType,
			table, col, defaultExpr,
			table, col, defaultExpr, col),
	}
}

func AlterColumnType(table, col, newType string) Entry {
	return Entry{
		Status: "DANGEROUS: full table rewrite under ACCESS EXCLUSIVE",
		Reason: "Rewrites every row. Rebuilds all indexes on this column. Views, FKs, and generated columns that reference it will block the change.",
		Fix: fmt.Sprintf(
			"  Safe exceptions (metadata-only): varchar(N)→text, varchar(N)→varchar(M) where M>N, numeric precision increase.\n"+
				"  For unsafe changes, use expand-then-swap:\n"+
				"  1. ALTER TABLE %s ADD COLUMN %s_new %s;\n"+
				"  2. Backfill in batches: UPDATE %s SET %s_new = %s::%s WHERE %s_new IS NULL AND id BETWEEN ... AND ...;\n"+
				"  3. Add trigger to keep both columns in sync during migration.\n"+
				"  4. ALTER TABLE %s DROP COLUMN %s;\n"+
				"  5. ALTER TABLE %s RENAME COLUMN %s_new TO %s;",
			table, col, newType,
			table, col, col, newType, col,
			table, col,
			table, col, col),
	}
}

func SetNotNull(table, col string) Entry {
	return Entry{
		Status: "DANGEROUS; full table scan under ACCESS EXCLUSIVE",
		Reason: "SET NOT NULL scans every row to verify no NULLs. All queries block until the scan completes.",
		Fix: fmt.Sprintf(
			"  Safe pattern:\n"+
				"  1. ALTER TABLE %s ADD CONSTRAINT chk_%s_nn CHECK (%s IS NOT NULL) NOT VALID;\n"+
				"  2. ALTER TABLE %s VALIDATE CONSTRAINT chk_%s_nn;  -- allows concurrent DML\n"+
				"  3. ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;    -- instant, skips scan\n"+
				"  4. ALTER TABLE %s DROP CONSTRAINT chk_%s_nn;       -- cleanup",
			table, col, col,
			table, col,
			table, col,
			table, col),
	}
}

func AddForeignKeyUnsafe(table, col, refTable, refCol string) Entry {
	return Entry{
		Status: "DANGEROUS: scans entire table under ACCESS EXCLUSIVE",
		Reason: "Without NOT VALID, Postgres validates all existing rows while holding a lock that blocks everything.",
		Fix: fmt.Sprintf(
			"  1. ALTER TABLE %s ADD CONSTRAINT fk_%s_%s FOREIGN KEY (%s) REFERENCES %s(%s) NOT VALID;\n"+
				"  2. ALTER TABLE %s VALIDATE CONSTRAINT fk_%s_%s;  -- SHARE UPDATE EXCLUSIVE, allows concurrent DML",
			table, table, col, col, refTable, refCol,
			table, table, col),
	}
}

func AddCheckConstraintUnsafe(table, constraintExpr string) Entry {
	return Entry{
		Status: "DANGEROUS: scans entire table under ACCESS EXCLUSIVE",
		Reason: "Without NOT VALID, Postgres validates all existing rows while holding a lock that blocks everything.",
		Fix: fmt.Sprintf(
			"  1. ALTER TABLE %s ADD CONSTRAINT chk_%s CHECK (%s) NOT VALID;\n"+
				"  2. ALTER TABLE %s VALIDATE CONSTRAINT chk_%s;  -- SHARE UPDATE EXCLUSIVE, allows concurrent DML",
			table, table, constraintExpr,
			table, table),
	}
}

// PRIMARY KEY, UNIQUE and EXCLUDE build an index while holding the lock; none
// accepts NOT VALID, so the constraint must attach to an existing index.
func AddIndexBackedConstraint(table, kind, columns string) Entry {
	e := Entry{
		Status: fmt.Sprintf("DANGEROUS: builds an index on %s under ACCESS EXCLUSIVE", table),
		Reason: fmt.Sprintf("ADD %s builds the backing index with the table locked against reads and writes for the whole build. There is no NOT VALID form.", kind),
		Fix: fmt.Sprintf(
			"  1. CREATE UNIQUE INDEX CONCURRENTLY %s_idx ON %s (%s);\n"+
				"  2. ALTER TABLE %s ADD CONSTRAINT %s_idx %s USING INDEX %s_idx;",
			stripSchema(table), table, columns,
			table, stripSchema(table), kind, stripSchema(table)),
	}
	if kind == "PRIMARY KEY" {
		e.Note = "Step 2 also sets NOT NULL on the columns, which scans the table under ACCESS EXCLUSIVE unless they are NOT NULL already. Make them NOT NULL first."
	}
	if kind == "EXCLUDE" {
		e.Fix = "  Build the backing index with CREATE INDEX CONCURRENTLY first, then ADD CONSTRAINT ... USING INDEX. The operator classes must match the exclusion elements exactly."
	}
	return e
}

func CreateIndexBlocking(table, idxName, method, columns string) Entry {
	return Entry{
		Status: "DANGEROUS, blocks all writes for entire build duration",
		Reason: "Non-concurrent index build acquires SHARE lock, blocking INSERT/UPDATE/DELETE until complete.",
		Fix: fmt.Sprintf(
			"  CREATE INDEX CONCURRENTLY %s ON %s USING %s(%s);",
			idxName, table, method, columns),
		Note: fmt.Sprintf("Cannot run inside a transaction. Takes ~2-3x longer. If it fails: DROP INDEX IF EXISTS %s;", idxName),
	}
}

func Rename(oldName, newName string) Entry {
	return Entry{
		Status: "DANGEROUS. Instant but breaks all callers",
		Reason: "Rename is metadata-only (milliseconds) but silently breaks every query, view, function, and ORM mapping that uses the old name.",
		Fix: fmt.Sprintf(
			"  Option A (rolling deploy):\n"+
				"  1. Deploy app code that supports BOTH %s and %s.\n"+
				"  2. Run the RENAME.\n"+
				"  3. Remove old-name support from app.\n"+
				"  Option B (compatibility view):\n"+
				"  CREATE VIEW %s AS SELECT * FROM %s;",
			oldName, newName,
			oldName, newName),
	}
}

func CTEMaterialized(cteName string, rows int64) Entry {
	return Entry{
		Status: "CAUTION: materialized CTE with no statistics",
		Reason: fmt.Sprintf("CTE '%s' is materialized (~%d rows). The planner has no statistics for materialized CTEs; it uses hardcoded defaults (e.g., 0.33 selectivity), leading to bad join order and memory estimates.", cteName, rows),
		Fix: fmt.Sprintf(
			"  If referenced only once:\n"+
				"    WITH %s AS NOT MATERIALIZED (...)  -- allows inlining and predicate pushdown\n"+
				"  If referenced multiple times with expensive computation:\n"+
				"    Materialization is correct, but for large results consider a temporary table (gets real statistics).",
			cteName),
	}
}

func CTEOverPartitionedTable(cteName, table string) Entry {
	return Entry{
		Status: "CAUTION: materialized CTE defeats partition pruning",
		Reason: fmt.Sprintf("Materialized CTE '%s' over partitioned table '%s' scans ALL partitions to build the result, even when predicates would normally eliminate most of them.", cteName, table),
		Fix: fmt.Sprintf(
			"  Add NOT MATERIALIZED to preserve pruning:\n"+
				"    WITH %s AS NOT MATERIALIZED (...)\n"+
				"  Or restructure to push WHERE filters inside the CTE body.",
			cteName),
	}
}

func NoPartitionPruning(table, partitionKey string, scanned, total int) Entry {
	return Entry{
		Status: fmt.Sprintf("WARNING: scanning all %d of %d partitions", scanned, total),
		Reason: fmt.Sprintf("no partition pruning on '%s'. The query scans every partition because no filter on the partition key '%s' was found.", table, partitionKey),
		Fix: fmt.Sprintf(
			"  Add a WHERE clause on the partition key:\n"+
				"    WHERE %s = ... or WHERE %s BETWEEN ... AND ...",
			partitionKey, partitionKey),
		Note: "Functions on the partition key (EXTRACT, ::date, date_trunc) prevent pruning. Filter on the raw column value.",
	}
}

func SuggestGIN(table, col, colType string) Entry {
	return Entry{
		Status: "INFO: GIN index recommended",
		Reason: fmt.Sprintf("Column '%s.%s' is %s. GIN indexes support containment (@>), existence (?), and full-text operators.", table, col, colType),
		Fix: fmt.Sprintf(
			"  CREATE INDEX CONCURRENTLY idx_%s_%s_gin ON %s USING gin(%s);",
			stripSchema(table), col, table, col),
		Note: "For JSONB: use jsonb_path_ops operator class if you only need @> (smaller, faster). " +
			"Default jsonb_ops supports ?, ?|, ?& too. " +
			"WARNING: = ANY(...) does NOT use GIN. Rewrite as @> for containment queries.",
	}
}

func SuggestGiST(table, col, colType string) Entry {
	return Entry{
		Status: "INFO: GiST index recommended",
		Reason: fmt.Sprintf("Column '%s.%s' is %s. GiST indexes support overlap (&&), containment (@>), and nearest-neighbor (<->) operators.", table, col, colType),
		Fix: fmt.Sprintf(
			"  CREATE INDEX CONCURRENTLY idx_%s_%s_gist ON %s USING gist(%s);",
			stripSchema(table), col, table, col),
	}
}

func MissingPrimaryKey(table string) Entry {
	return Entry{
		Status: "ERROR: table has no primary key",
		Reason: "Without a primary key, there is no guaranteed unique row identifier. Replication, upserts, and ORM mappings require one.",
		Fix: fmt.Sprintf(
			"  ALTER TABLE %s ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY;",
			table),
	}
}

func TextOverVarchar(table, col string) Entry {
	return Entry{
		Status: "WARNING: prefer text over varchar",
		Reason: fmt.Sprintf("Column '%s.%s' uses varchar. In PostgreSQL, text and varchar have identical performance. varchar(N) only adds a length check.", table, col),
		Fix: fmt.Sprintf(
			"  ALTER TABLE %s ALTER COLUMN %s TYPE text;  -- safe, metadata-only, no rewrite",
			table, col),
	}
}

func TimestampToTimestamptz(table, col string) Entry {
	return Entry{
		Status: "WARNING: use timestamptz",
		Reason: fmt.Sprintf("Column '%s.%s' uses timestamp without time zone. This silently drops timezone information, causing bugs across timezones.", table, col),
		Fix: fmt.Sprintf(
			"  ALTER TABLE %s ALTER COLUMN %s TYPE timestamptz USING %s AT TIME ZONE 'UTC';",
			table, col, col),
		Note: "DANGEROUS: this is a full table rewrite. Use the expand-then-swap pattern on large tables.",
	}
}

func MissingTimestamp(table, colName string) Entry {
	return Entry{
		Status: fmt.Sprintf("WARNING: missing %s column", colName),
		Reason: fmt.Sprintf("Table '%s' has no '%s' column. Audit trails and debugging rely on knowing when rows were created/modified.", table, colName),
		Fix: fmt.Sprintf(
			"  ALTER TABLE %s ADD COLUMN %s timestamptz NOT NULL DEFAULT now();",
			table, colName),
	}
}

func PartitionTooManyChildren(table string, count int) Entry {
	return Entry{
		Status: fmt.Sprintf("WARNING: %d partitions", count),
		Reason: "Planning time scales with partition count. Each partition's constraints must be checked during pruning.",
		Fix:    "  Consider sub-partitioning or coarser time granularity to reduce the number of direct children.",
		Note:   "PG 14+ handles large partition counts better with improved pruning algorithms, but >100 partitions still adds measurable planning overhead.",
	}
}

func PartitionRangeGap(parent, fromBound, toBound string) Entry {
	return Entry{
		Status: "WARNING: gap in partition range",
		Reason: fmt.Sprintf("Gap between '%s' and '%s'. INSERTs for values in this range will fail unless a DEFAULT partition exists.", fromBound, toBound),
		Fix: fmt.Sprintf(
			"  CREATE TABLE %s_gap PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s');",
			parent, parent, fromBound, toBound),
	}
}

func PartitionNoDefault(parent string) Entry {
	return Entry{
		Status: "INFO: no DEFAULT partition",
		Reason: "Without a DEFAULT partition, INSERTs for unmapped values fail with an error. This might be intentional (strict partitioning) or a safety gap.",
		Fix: fmt.Sprintf(
			"  CREATE TABLE %s_default PARTITION OF %s DEFAULT;",
			parent, parent),
	}
}

func stripSchema(qualified string) string {
	for i := len(qualified) - 1; i >= 0; i-- {
		if qualified[i] == '.' {
			return qualified[i+1:]
		}
	}
	return qualified
}
