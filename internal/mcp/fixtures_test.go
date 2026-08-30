package mcp

import (
	"fmt"
	"strings"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// Shapes examples/demo cannot express: schemas other than public, one table
// name living in two of them, a name containing a dot, and tables wide or
// partitioned enough to matter for output caps.

const testPageSize = 8192

// a day after the schema, as separate cron entries would produce
var fixtureActivityTime = time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

// pg keeps a generated column's expression in pg_attrdef, next to real
// defaults; the capture separates them so the column does not read as writable.
func generatedCol() schema.Column {
	kind, expr := "virtual", "lower(email)"
	return schema.Column{
		Name: "email_lower", TypeName: "text", Nullable: true,
		Generated: &kind, GenerationExpr: &expr,
	}
}

func testCol(name, typ string, nullable bool) schema.Column {
	return schema.Column{Name: name, TypeName: typ, Nullable: nullable}
}

// pg_get_indexdef qualifies and quotes; find_objects matches this text.
func indexDef(unique bool, name, schemaName, table string, cols ...string) string {
	kind := "INDEX"
	if unique {
		kind = "UNIQUE INDEX"
	}
	return fmt.Sprintf("CREATE %s %s ON %s.%s USING btree (%s)",
		kind, quoteIdent(name), schemaName, quoteIdent(table), strings.Join(cols, ", "))
}

// Fixture-only: covers the punctuation these names use, not reserved words or
// embedded quotes.
func quoteIdent(s string) string {
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return `"` + s + `"`
		}
	}
	return s
}

func testTable(schemaName, name string, cols ...schema.Column) schema.Table {
	return testTablePK(schemaName, name, []string{"id"}, cols...)
}

// One table whose primary key is pkCols; ordinals follow position.
func testTablePK(schemaName, name string, pkCols []string, cols ...schema.Column) schema.Table {
	t := schema.Table{
		Schema: schemaName, Name: name,
		Columns: append([]schema.Column{testCol("id", "integer", false)}, cols...),
		Constraints: []schema.Constraint{{
			Name: name + "_pkey", Kind: schema.ConstraintPrimaryKey, Columns: pkCols,
			BackingIndex: strPtr(name + "_pkey"),
		}},
		Indexes: []schema.Index{{
			Name: name + "_pkey", Columns: pkCols, IndexType: "btree",
			IsUnique: true, IsPrimary: true, IsValid: true, IsReady: true,
			BacksConstraint: true,
			Definition:      indexDef(true, name+"_pkey", schemaName, name, pkCols...),
		}},
	}
	setOrdinals(&t)
	return t
}

func setOrdinals(t *schema.Table) {
	for i := range t.Columns {
		t.Columns[i].Ordinal = int16(i + 1)
	}
}

// public.orders and app.orders collide on the bare name; app.events is unique
// outside public; "foo.bar" is a legal name that contains its own separator,
// and carries an incoming FK because that is where the ambiguity bites.
func multiSchemaSnap() *schema.SchemaSnapshot {
	orders := testTable("public", "orders",
		testCol("customer_id", "integer", false),
		testCol("ref_id", "integer", true),
		testCol("note", "text", true))
	orders.Constraints = append(orders.Constraints,
		fkTo("orders", "customer_id", "public.customers"),
		fkTo("orders", "ref_id", "public.foo.bar"))

	return finish(&schema.SchemaSnapshot{
		Tables: []schema.Table{
			orders,
			testTable("public", "customers", testCol("email", "text", true), generatedCol()),
			testTable("public", "foo.bar", testCol("note", "text", true)),
			testTable("app", "orders", testCol("tenant_id", "integer", false)),
			testTable("app", "events", testCol("payload", "jsonb", true)),
			// same name, neither in public: a bare lookup cannot pick one
			testTable("billing", "invoices", testCol("total", "numeric", false)),
			testTable("archive", "invoices", testCol("total", "numeric", false)),
			// with public."foo.bar" above, "foo.bar" reads two ways
			testTable("foo", "bar", testCol("note", "text", true)),
		},
		Views: []schema.View{{
			Schema: "app", Name: "order_summary",
			Definition: "SELECT id FROM app.orders",
		}},
	})
}

// FKTable carries the same nspname.relname form the capture builds, which is
// what makes public."foo.bar" indistinguishable from public.foo.bar.
func fkTo(table, col, target string) schema.Constraint {
	return schema.Constraint{
		Name: table + "_" + col + "_fkey", Kind: schema.ConstraintForeignKey,
		Columns: []string{col}, FKTable: strPtr(target), FKColumns: []string{"id"},
	}
}

// wideSnap builds one table of cols columns beyond its key, and one
// partitioned table of parts children. The trailing column carries the only
// non-primary index, so a cap that keeps the first N by ordinal drops it.
func wideSnap(cols, parts int) *schema.SchemaSnapshot {
	wide := testTable("public", "wide")
	for i := 1; i <= cols; i++ {
		wide.Columns = append(wide.Columns, testCol(fmt.Sprintf("c%04d", i), "text", i%3 == 0))
	}
	setOrdinals(&wide)
	last := wide.Columns[len(wide.Columns)-1].Name
	wide.Indexes = append(wide.Indexes, schema.Index{
		Name: "wide_" + last + "_idx", Columns: []string{last}, IndexType: "btree",
		IsValid: true, IsReady: true,
		Definition: indexDef(false, "wide_"+last+"_idx", "public", "wide", last),
	})

	// a partitioned table's unique constraint must contain the partition key,
	// and each leaf inherits that key verbatim
	pk := []string{"id", "at"}
	at := testCol("at", "timestamptz", false)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	children := make([]schema.PartitionChild, parts)
	childIdx := make([]schema.IndexPartitionChild, parts)
	leaves := make([]schema.Table, parts)
	for i := range children {
		name := fmt.Sprintf("events_p%03d", i)
		children[i] = schema.PartitionChild{
			Schema: "public", Name: name,
			Bound: fmt.Sprintf("FOR VALUES FROM ('%s') TO ('%s')",
				pgTimestamp(base.AddDate(0, 0, i)), pgTimestamp(base.AddDate(0, 0, i+1))),
		}
		childIdx[i] = schema.IndexPartitionChild{Schema: "public", Table: name, Index: name + "_pkey"}
		leaves[i] = testTablePK("public", name, pk, at)
	}

	events := testTablePK("public", "events", pk, at)
	events.PartitionInfo = &schema.PartitionInfo{
		Strategy: schema.PartitionRange, Key: "RANGE (at)", Children: children,
	}
	events.Indexes[0].Children = childIdx

	return finish(&schema.SchemaSnapshot{
		Tables: append([]schema.Table{wide, events}, leaves...),
	})
}

func pgTimestamp(t time.Time) string { return t.Format("2006-01-02 15:04:05-07") }

func finish(snap *schema.SchemaSnapshot) *schema.SchemaSnapshot {
	snap.FormatVersion = schema.FormatVersion
	snap.PgVersion = "PostgreSQL 18.3"
	snap.Database = "fixture"
	snap.Timestamp = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	snap.ContentHash = schema.DigestFor(snap)
	return snap
}

// Planner sizing for every table, internally consistent so anything deriving
// bytes-per-page or index share gets a plausible number. No bloat.Annotate:
// these fixtures must not trip the bloat rules.
func annotate(snap *schema.SchemaSnapshot, rows float64) *schema.AnnotatedSchema {
	planner := &schema.PlannerStatsSnapshot{}
	for _, t := range snap.Tables {
		sizing := schema.TableSizing{Reltuples: rows}
		// a partitioned parent holds no heap: relpages 0, relfrozenxid 0
		if t.PartitionInfo == nil {
			rowWidth := int64(64 * len(t.Columns))
			sizing.Relpages = (int64(rows)*rowWidth + testPageSize - 1) / testPageSize
			sizing.TableSize = sizing.Relpages * testPageSize
			// pg_indexes_size is always a whole number of pages
			sizing.IndexesSize = int64(len(t.Indexes)) * (sizing.Relpages / 10) * testPageSize
			sizing.RelfrozenXid = 1000
		}
		sizing.TotalRelationSize = sizing.TableSize + sizing.IndexesSize
		planner.Tables = append(planner.Tables, schema.TableSizingEntry{Table: t.Qual(), Sizing: sizing})
	}
	return &schema.AnnotatedSchema{Schema: snap, Planner: planner}
}

// The production paths (history.GetAnnotated, snapshot.AssembleAnnotated) roll
// partition children up into their parent before anything reads the sizing, so
// a fixture that skips it makes partitioned parents look empty.
func annotateRolled(snap *schema.SchemaSnapshot, rows float64) *schema.AnnotatedSchema {
	a := annotate(snap, rows)
	return &schema.AnnotatedSchema{Schema: snap, Planner: schema.RollUpPartitionSizing(snap, a.Planner)}
}

// withColumnStats gives every column of one table a pg_stats row, which is
// what makes describe_table emit column_profiles.
func withColumnStats(a *schema.AnnotatedSchema, q schema.QualifiedName) *schema.AnnotatedSchema {
	nullFrac, nDistinct := 0.0, -1.0
	for _, t := range a.Schema.Tables {
		if t.Qual() != q {
			continue
		}
		for _, c := range t.Columns {
			a.Planner.Columns = append(a.Planner.Columns, schema.ColumnStatsEntry{
				Table: q, Column: c.Name,
				Stats: schema.ColumnStats{NullFrac: &nullFrac, NDistinct: &nDistinct},
			})
		}
	}
	return a
}

// withActivity attaches one node's counters for a single table.
func withActivity(a *schema.AnnotatedSchema, q schema.QualifiedName, act schema.TableActivity) *schema.AnnotatedSchema {
	return withActivityAt(a, "primary", fixtureActivityTime, q, act)
}

// Activity is captured per node on its own schedule, so each node carries its
// own timestamp and two nodes need not agree.
func withActivityAt(a *schema.AnnotatedSchema, source string, at time.Time, q schema.QualifiedName, act schema.TableActivity) *schema.AnnotatedSchema {
	if a.Merged == nil {
		a.Merged = &schema.MergedActivity{}
	}
	for i := range a.Merged.Nodes {
		if a.Merged.Nodes[i].Node.Source == source {
			a.Merged.Nodes[i].Tables = append(a.Merged.Nodes[i].Tables,
				schema.TableActivityEntry{Table: q, Activity: act})
			return a
		}
	}
	a.Merged.Nodes = append(a.Merged.Nodes, schema.NodeActivity{
		Node:   schema.NodeIdentity{Source: source, Timestamp: at},
		Tables: []schema.TableActivityEntry{{Table: q, Activity: act}},
	})
	return a
}

func strPtr(s string) *string { return &s }
