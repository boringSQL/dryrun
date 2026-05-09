package schema

import (
	"encoding/json"
	"strings"
	"testing"
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
