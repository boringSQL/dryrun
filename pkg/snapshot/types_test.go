package snapshot

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Column.StatisticsTarget and Column.Generated must omit when nil so the
// on-disk JSON stays compatible with snapshots produced before v0.6.
func TestColumn_JSONOmitsUnsetStatisticsTargetAndGenerated(t *testing.T) {
	c := Column{Name: "id", Ordinal: 1, TypeName: "bigint", Nullable: false}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	if strings.Contains(s, "statistics_target") {
		t.Errorf("unset statistics_target leaked into JSON: %s", s)
	}
	if strings.Contains(s, "generated") {
		t.Errorf("unset generated leaked into JSON: %s", s)
	}
}

// When set, both fields must round-trip verbatim through JSON.
func TestColumn_JSONRoundTripStatisticsTargetAndGenerated(t *testing.T) {
	target := int16(1000)
	gen := "stored"
	in := Column{
		Name:             "computed",
		Ordinal:          2,
		TypeName:         "text",
		Nullable:         true,
		StatisticsTarget: &target,
		Generated:        &gen,
	}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Column
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.StatisticsTarget == nil || *out.StatisticsTarget != 1000 {
		t.Errorf("statistics_target round-trip: got %v want 1000", out.StatisticsTarget)
	}
	if out.Generated == nil || *out.Generated != "stored" {
		t.Errorf("generated round-trip: got %v want \"stored\"", out.Generated)
	}
}

// QualifiedName.String renders as "schema.name" with an empty-schema fallback
// so callers building error messages and cache keys get a single, unambiguous
// form. The empty case keeps logs readable for the `pg_catalog`-less paths
// we sometimes hit in tests.
func TestQualifiedName_String(t *testing.T) {
	cases := []struct {
		q    QualifiedName
		want string
	}{
		{QualifiedName{Schema: "public", Name: "users"}, "public.users"},
		{QualifiedName{Schema: "", Name: "loose"}, "loose"},
		{QualifiedName{Schema: "tenant_42", Name: "events_2026_05"}, "tenant_42.events_2026_05"},
	}
	for _, c := range cases {
		if got := c.q.String(); got != c.want {
			t.Errorf("String() = %q, want %q", got, c.want)
		}
	}
}

// Equality is plain struct comparison — we rely on this when collecting
// entries into maps keyed by QualifiedName, so two values with the same
// fields must compare equal regardless of construction order.
func TestQualifiedName_Equality(t *testing.T) {
	a := QualifiedName{Schema: "public", Name: "users"}
	b := QualifiedName{Name: "users", Schema: "public"}
	if a != b {
		t.Errorf("expected equality: %+v != %+v", a, b)
	}
	if a == (QualifiedName{Schema: "public", Name: "USERS"}) {
		t.Errorf("case-sensitive comparison expected")
	}
}

// Planner snapshots round-trip through JSON with all the entry shapes —
// nil slices must marshal as null (or be elided gracefully) so on-disk
// payloads remain compact when a database has no indexes or stats.
func TestPlannerStatsSnapshot_JSONRoundTrip(t *testing.T) {
	in := &PlannerStatsSnapshot{
		SchemaRefHash: "ddl-hash",
		ContentHash:   "planner-hash",
		Database:      "test",
		Timestamp:     time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Tables: []TableSizingEntry{
			{Table: QualifiedName{Schema: "public", Name: "users"}, Sizing: TableSizing{Reltuples: 1000, Relpages: 10, TableSize: 8192}},
		},
		Indexes: []IndexSizingEntry{
			{Table: QualifiedName{Schema: "public", Name: "users"}, Index: "users_pkey", Sizing: IndexSizing{Relpages: 2, Reltuples: 1000, Size: 16384}},
		},
		Columns: nil,
	}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out PlannerStatsSnapshot
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.SchemaRefHash != in.SchemaRefHash || out.ContentHash != in.ContentHash {
		t.Errorf("hashes drifted: %+v", out)
	}
	if len(out.Tables) != 1 || out.Tables[0].Table.Name != "users" || out.Tables[0].Sizing.Reltuples != 1000 {
		t.Errorf("table entry didn't round-trip: %+v", out.Tables)
	}
	if len(out.Indexes) != 1 || out.Indexes[0].Index != "users_pkey" {
		t.Errorf("index entry didn't round-trip: %+v", out.Indexes)
	}
}

// Activity snapshots round-trip including the nullable vacuum timestamps —
// these are the trickiest fields because Postgres returns NULL until the
// first (auto)vacuum runs, and a stray non-nil zero time would silently
// pollute drift reports.
func TestActivityStatsSnapshot_JSONRoundTrip(t *testing.T) {
	vac := time.Date(2026, 5, 1, 9, 30, 0, 0, time.UTC)
	in := &ActivityStatsSnapshot{
		SchemaRefHash: "ddl-hash",
		ContentHash:   "activity-hash",
		Node: NodeIdentity{
			Source:    "primary",
			IsStandby: false,
			PgVersion: "PostgreSQL 17.0",
			Timestamp: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		},
		Tables: []TableActivityEntry{
			{Table: QualifiedName{Schema: "public", Name: "users"}, Activity: TableActivity{SeqScan: 42, IdxScan: 100, LastVacuum: &vac}},
		},
		Indexes: nil,
	}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ActivityStatsSnapshot
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.Node.Source != "primary" || out.Node.IsStandby {
		t.Errorf("node identity drifted: %+v", out.Node)
	}
	if len(out.Tables) != 1 || out.Tables[0].Activity.SeqScan != 42 {
		t.Errorf("table activity didn't round-trip: %+v", out.Tables)
	}
	if out.Tables[0].Activity.LastVacuum == nil || !out.Tables[0].Activity.LastVacuum.Equal(vac) {
		t.Errorf("last_vacuum didn't round-trip: %+v", out.Tables[0].Activity.LastVacuum)
	}
}

// A standby snapshot has IsStandby=true and typically nil vacuum timestamps;
// it must still round-trip and the boolean must serialize even at its zero
// value so consumers don't misclassify the node.
func TestActivityStatsSnapshot_StandbyShape(t *testing.T) {
	in := &ActivityStatsSnapshot{
		SchemaRefHash: "h",
		ContentHash:   "c",
		Node:          NodeIdentity{Source: "replica-1", IsStandby: true},
		Tables:        []TableActivityEntry{},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"is_standby":true`) {
		t.Errorf("is_standby missing or wrong in JSON: %s", b)
	}

	var out ActivityStatsSnapshot
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !out.Node.IsStandby {
		t.Errorf("is_standby lost in round-trip")
	}
}
