package schema

import "testing"

func ptr(f float64) *float64 { return &f }

func TestCloneForStats_IsolatesStatsMutation(t *testing.T) {
	origTableStats := &TableStats{Reltuples: 1000, Relpages: 50}
	origIndexStats := &IndexStats{IdxScan: 42}
	origColStats := &ColumnStats{NullFrac: ptr(0.1), NDistinct: ptr(-0.5)}

	snap := &SchemaSnapshot{
		PgVersion: "PostgreSQL 17.2",
		Database:  "testdb",
		Tables: []Table{
			{
				Schema:  "public",
				Name:    "orders",
				Stats:   origTableStats,
				Columns: []Column{{Name: "id", Stats: origColStats}},
				Indexes: []Index{{Name: "orders_pkey", Stats: origIndexStats}},
			},
		},
		NodeStats: []NodeStats{
			{
				Source:      "replica",
				TableStats:  []NodeTableStats{{Schema: "public", Table: "orders", Stats: TableStats{Reltuples: 9999}}},
				IndexStats:  []NodeIndexStats{{Schema: "public", Table: "orders", IndexName: "orders_pkey", Stats: IndexStats{IdxScan: 999}}},
				ColumnStats: []NodeColumnStats{{Schema: "public", Table: "orders", Column: "id", Stats: ColumnStats{NullFrac: ptr(0.9)}}},
			},
		},
	}

	clone := snap.CloneForStats()

	if err := ApplyNodeStats(clone, "replica"); err != nil {
		t.Fatalf("ApplyNodeStats: %v", err)
	}

	if clone.Tables[0].Stats.Reltuples != 9999 {
		t.Errorf("clone table reltuples = %v, want 9999", clone.Tables[0].Stats.Reltuples)
	}
	if clone.Tables[0].Indexes[0].Stats.IdxScan != 999 {
		t.Errorf("clone index idx_scan = %v, want 999", clone.Tables[0].Indexes[0].Stats.IdxScan)
	}
	if *clone.Tables[0].Columns[0].Stats.NullFrac != 0.9 {
		t.Errorf("clone column null_frac = %v, want 0.9", *clone.Tables[0].Columns[0].Stats.NullFrac)
	}

	// original untouched
	if snap.Tables[0].Stats.Reltuples != 1000 {
		t.Errorf("original table reltuples = %v, want 1000", snap.Tables[0].Stats.Reltuples)
	}
	if snap.Tables[0].Indexes[0].Stats.IdxScan != 42 {
		t.Errorf("original index idx_scan = %v, want 42", snap.Tables[0].Indexes[0].Stats.IdxScan)
	}
	if *snap.Tables[0].Columns[0].Stats.NullFrac != 0.1 {
		t.Errorf("original column null_frac = %v, want 0.1", *snap.Tables[0].Columns[0].Stats.NullFrac)
	}
}

func TestCloneForStats_PreservesScalarFields(t *testing.T) {
	snap := &SchemaSnapshot{
		PgVersion: "PostgreSQL 16.1",
		Database:  "mydb",
		Tables:    []Table{{Schema: "public", Name: "users"}},
	}

	clone := snap.CloneForStats()

	if clone.PgVersion != snap.PgVersion {
		t.Errorf("PgVersion = %q, want %q", clone.PgVersion, snap.PgVersion)
	}
	if clone.Database != snap.Database {
		t.Errorf("Database = %q, want %q", clone.Database, snap.Database)
	}
	if len(clone.Tables) != 1 || clone.Tables[0].Name != "users" {
		t.Errorf("Tables not preserved")
	}
}
