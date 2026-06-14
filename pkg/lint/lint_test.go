package lint

import (
	"fmt"
	"testing"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

func emptySnapshot() *snapshot.SchemaSnapshot {
	return &snapshot.SchemaSnapshot{
		PgVersion:   "PostgreSQL 17.0",
		Database:    "test",
		Timestamp:   time.Now().UTC(),
		ContentHash: "abc",
	}
}

func makeCol(name, typeName string) snapshot.Column {
	return snapshot.Column{Name: name, TypeName: typeName}
}

func makePK(name string, columns ...string) snapshot.Constraint {
	return snapshot.Constraint{Name: name, Kind: snapshot.ConstraintPrimaryKey, Columns: columns}
}

func makeFK(name string, columns []string, fkTable string) snapshot.Constraint {
	return snapshot.Constraint{
		Name: name, Kind: snapshot.ConstraintForeignKey,
		Columns: columns, FKTable: new(fkTable), FKColumns: []string{"id"},
	}
}

func makeIndex(name string, columns ...string) snapshot.Index {
	return snapshot.Index{Name: name, Columns: columns, IndexType: "btree"}
}

func TestCleanSchemaNoErrors(t *testing.T) {
	snap := emptySnapshot()
	idCol := makeCol("id", "bigint")
	idCol.Identity = new("ALWAYS")
	snap.Tables = []snapshot.Table{{
		Schema: "public", Name: "user",
		Columns: []snapshot.Column{idCol, makeCol("email", "text"),
			makeCol("created_at", "timestamp with time zone"),
			makeCol("updated_at", "timestamp with time zone")},
		Constraints: []snapshot.Constraint{makePK("pk_user", "id")},
	}}

	report := LintSchema(snap, &Config{TableNameStyle: "snake_singular", ColumnNameStyle: "snake_case", PKType: "bigint_identity", RequireTimestamps: true, TimestampType: "timestamptz", PreferTextOverVarchar: true, FKPattern: "fk_{table}_{column}", IndexPattern: "idx_{table}_{columns}"})
	for _, v := range report.Findings {
		if v.Severity == SeverityError {
			t.Errorf("unexpected error: %s - %s", v.Rule, v.Message)
		}
	}
}

func TestMissingPK(t *testing.T) {
	snap := emptySnapshot()
	snap.Tables = []snapshot.Table{{
		Schema: "public", Name: "log",
		Columns: []snapshot.Column{makeCol("message", "text"),
			makeCol("created_at", "timestamp with time zone"),
			makeCol("updated_at", "timestamp with time zone")},
	}}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	found := false
	for _, v := range report.Findings {
		if v.Rule == "pk/exists" {
			found = true
		}
	}
	if !found {
		t.Error("expected pk/exists violation")
	}
}

func TestFKWithoutIndex(t *testing.T) {
	snap := emptySnapshot()
	idCol := makeCol("id", "bigint")
	idCol.Identity = new("ALWAYS")
	snap.Tables = []snapshot.Table{{
		Schema: "public", Name: "order_item",
		Columns: []snapshot.Column{idCol, makeCol("order_id", "bigint"),
			makeCol("created_at", "timestamp with time zone"),
			makeCol("updated_at", "timestamp with time zone")},
		Constraints: []snapshot.Constraint{
			makePK("pk_order_item", "id"),
			makeFK("fk_order_item_order_id", []string{"order_id"}, "public.order"),
		},
	}}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	found := false
	for _, v := range report.Findings {
		if v.Rule == "constraints/fk_has_index" {
			found = true
		}
	}
	if !found {
		t.Error("expected constraints/fk_has_index violation")
	}
}

func TestFKWithPrefixIndexPasses(t *testing.T) {
	snap := emptySnapshot()
	idCol := makeCol("id", "bigint")
	idCol.Identity = new("ALWAYS")
	snap.Tables = []snapshot.Table{{
		Schema: "public", Name: "order_item",
		Columns: []snapshot.Column{idCol, makeCol("order_id", "bigint"), makeCol("product_id", "bigint"),
			makeCol("created_at", "timestamp with time zone"),
			makeCol("updated_at", "timestamp with time zone")},
		Constraints: []snapshot.Constraint{
			makePK("pk_order_item", "id"),
			makeFK("fk_order_item_order_id", []string{"order_id"}, "public.order"),
		},
		Indexes: []snapshot.Index{
			makeIndex("idx_order_item_order_id_product_id", "order_id", "product_id"),
		},
	}}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	for _, v := range report.Findings {
		if v.Rule == "constraints/fk_has_index" {
			t.Error("FK with covering prefix index should not be flagged")
		}
	}
}

func TestMultiColumnFKNeedsPrefixIndex(t *testing.T) {
	snap := emptySnapshot()
	idCol := makeCol("id", "bigint")
	idCol.Identity = new("ALWAYS")
	snap.Tables = []snapshot.Table{{
		Schema: "public", Name: "shipment",
		Columns: []snapshot.Column{idCol, makeCol("order_id", "bigint"), makeCol("warehouse_id", "bigint"),
			makeCol("created_at", "timestamp with time zone"),
			makeCol("updated_at", "timestamp with time zone")},
		Constraints: []snapshot.Constraint{
			makePK("pk_shipment", "id"),
			makeFK("fk_shipment_order_warehouse", []string{"order_id", "warehouse_id"}, "public.order_warehouse"),
		},
		Indexes: []snapshot.Index{
			// Index on (warehouse_id, order_id) does NOT cover FK (order_id, warehouse_id) as prefix
			makeIndex("idx_shipment_wh_order", "warehouse_id", "order_id"),
		},
	}}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	found := false
	for _, v := range report.Findings {
		if v.Rule == "constraints/fk_has_index" {
			found = true
		}
	}
	if !found {
		t.Error("multi-column FK with wrong prefix order should be flagged")
	}
}

func TestDisabledRulesSkipped(t *testing.T) {
	snap := emptySnapshot()
	snap.Tables = []snapshot.Table{{
		Schema: "public", Name: "log",
		Columns: []snapshot.Column{makeCol("message", "text")},
	}}

	config := DefaultConfig()
	config.DisabledRules = []string{"pk/exists", "timestamps/has_created_at", "timestamps/has_updated_at"}
	report := LintSchema(snap, &config)
	for _, v := range report.Findings {
		if v.Rule == "pk/exists" {
			t.Error("pk/exists should be disabled")
		}
	}
}

func TestPartitionChildSkipped(t *testing.T) {
	snap := emptySnapshot()
	idCol := makeCol("id", "bigint")
	idCol.Identity = new("ALWAYS")
	snap.Tables = []snapshot.Table{
		{
			Schema: "public", Name: "events",
			Columns:     []snapshot.Column{idCol, makeCol("created_at", "timestamp with time zone"), makeCol("updated_at", "timestamp with time zone")},
			Constraints: []snapshot.Constraint{makePK("pk_events", "id")},
			PartitionInfo: &snapshot.PartitionInfo{
				Strategy: snapshot.PartitionRange, Key: "created_at",
				Children: []snapshot.PartitionChild{{Schema: "public", Name: "events_2024"}},
			},
		},
		{
			Schema: "public", Name: "events_2024",
			Columns: []snapshot.Column{makeCol("id", "bigint"), makeCol("created_at", "timestamp with time zone"), makeCol("updated_at", "timestamp with time zone")},
		},
	}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	// events_2024 should not generate separate violations
	for _, v := range report.Findings {
		if len(v.Tables) > 0 && v.Tables[0] == "public.events_2024" {
			t.Errorf("partition child should be skipped, got violation: %s", v.Rule)
		}
	}
}

func TestAutoDetectTableNameStyle(t *testing.T) {
	snap := emptySnapshot()
	// Majority plural tables
	for _, name := range []string{"users", "orders", "products", "item"} {
		idCol := makeCol("id", "bigint")
		idCol.Identity = new("ALWAYS")
		snap.Tables = append(snap.Tables, snapshot.Table{
			Schema: "public", Name: name,
			Columns:     []snapshot.Column{idCol, makeCol("created_at", "timestamp with time zone"), makeCol("updated_at", "timestamp with time zone")},
			Constraints: []snapshot.Constraint{makePK("pk_"+name, "id")},
		})
	}

	config := DefaultConfig()
	config.TableNameStyle = "auto"
	report := LintSchema(snap, &config)

	// With auto-detect to snake_plural, no table_style violations expected
	// (snake_plural only checks isSnakeCase, all names pass)
	for _, v := range report.Findings {
		if v.Rule == "naming/table_style" {
			t.Errorf("unexpected naming/table_style violation with auto-detect: %v", v.Tables)
		}
	}
}

func makePartitionedTable(children []snapshot.PartitionChild) snapshot.Table {
	idCol := makeCol("id", "bigint")
	idCol.Identity = new("ALWAYS")
	return snapshot.Table{
		Schema:      "public",
		Name:        "events",
		Columns:     []snapshot.Column{idCol, makeCol("created_at", "timestamp with time zone"), makeCol("updated_at", "timestamp with time zone")},
		Constraints: []snapshot.Constraint{makePK("pk_events", "id", "created_at")},
		PartitionInfo: &snapshot.PartitionInfo{
			Strategy: snapshot.PartitionRange,
			Key:      "created_at",
			Children: children,
		},
	}
}

func TestPartitionTooManyChildren(t *testing.T) {
	children := make([]snapshot.PartitionChild, 501)
	for i := range children {
		children[i] = snapshot.PartitionChild{Schema: "public", Name: fmt.Sprintf("events_%d", i), Bound: "FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')"}
	}
	snap := emptySnapshot()
	snap.Tables = []snapshot.Table{makePartitionedTable(children)}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	found := false
	for _, v := range report.Findings {
		if v.Rule == "partition/too_many_children" {
			found = true
		}
	}
	if !found {
		t.Error("expected partition/too_many_children violation for 501 partitions")
	}
}

func TestPartitionTooManyChildrenNoWarning(t *testing.T) {
	children := make([]snapshot.PartitionChild, 100)
	for i := range children {
		children[i] = snapshot.PartitionChild{Schema: "public", Name: fmt.Sprintf("events_%d", i), Bound: "FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')"}
	}
	snap := emptySnapshot()
	snap.Tables = []snapshot.Table{makePartitionedTable(children)}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	for _, v := range report.Findings {
		if v.Rule == "partition/too_many_children" {
			t.Error("unexpected partition/too_many_children violation for 100 partitions")
		}
	}
}

func TestPartitionRangeGap(t *testing.T) {
	snap := emptySnapshot()
	snap.Tables = []snapshot.Table{makePartitionedTable([]snapshot.PartitionChild{
		{Schema: "public", Name: "events_2025_01", Bound: "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"},
		{Schema: "public", Name: "events_2025_03", Bound: "FOR VALUES FROM ('2025-03-01') TO ('2025-04-01')"},
	})}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	found := false
	for _, v := range report.Findings {
		if v.Rule == "partition/range_gaps" {
			found = true
		}
	}
	if !found {
		t.Error("expected partition/range_gaps violation for gap between Feb and Mar")
	}
}

func TestPartitionRangeNoGap(t *testing.T) {
	snap := emptySnapshot()
	snap.Tables = []snapshot.Table{makePartitionedTable([]snapshot.PartitionChild{
		{Schema: "public", Name: "events_2025_01", Bound: "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"},
		{Schema: "public", Name: "events_2025_02", Bound: "FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')"},
	})}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	for _, v := range report.Findings {
		if v.Rule == "partition/range_gaps" {
			t.Error("unexpected partition/range_gaps violation for contiguous partitions")
		}
	}
}

func TestPartitionRangeGapUnsortedChildren(t *testing.T) {
	snap := emptySnapshot()
	// Children listed out of order - sorting must handle this
	snap.Tables = []snapshot.Table{makePartitionedTable([]snapshot.PartitionChild{
		{Schema: "public", Name: "events_2025_03", Bound: "FOR VALUES FROM ('2025-03-01') TO ('2025-04-01')"},
		{Schema: "public", Name: "events_2025_01", Bound: "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"},
	})}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	found := false
	for _, v := range report.Findings {
		if v.Rule == "partition/range_gaps" {
			found = true
		}
	}
	if !found {
		t.Error("expected partition/range_gaps violation even when children are listed out of order")
	}
}

func TestPartitionNoDefault(t *testing.T) {
	snap := emptySnapshot()
	snap.Tables = []snapshot.Table{makePartitionedTable([]snapshot.PartitionChild{
		{Schema: "public", Name: "events_2025_01", Bound: "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"},
	})}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	found := false
	for _, v := range report.Findings {
		if v.Rule == "partition/no_default" {
			found = true
		}
	}
	if !found {
		t.Error("expected partition/no_default violation")
	}
}

func TestPartitionWithDefault(t *testing.T) {
	snap := emptySnapshot()
	snap.Tables = []snapshot.Table{makePartitionedTable([]snapshot.PartitionChild{
		{Schema: "public", Name: "events_2025_01", Bound: "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"},
		{Schema: "public", Name: "events_default", Bound: "DEFAULT"},
	})}

	config := DefaultConfig()
	report := LintSchema(snap, &config)
	for _, v := range report.Findings {
		if v.Rule == "partition/no_default" {
			t.Error("unexpected partition/no_default violation when DEFAULT partition exists")
		}
	}
}

// strp is defined in rules.go
