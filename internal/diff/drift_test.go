package diff

import (
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func emptySnap(hash string) *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.0", Database: "test",
		Timestamp: time.Now().UTC(), ContentHash: hash,
	}
}

func TestIdentical(t *testing.T) {
	a := emptySnap("abc")
	b := emptySnap("abc")
	report := ClassifyDrift(a, b)
	if report.Direction != DriftIdentical {
		t.Errorf("expected identical, got %s", report.Direction)
	}
}

func TestAhead(t *testing.T) {
	saved := emptySnap("old")
	live := emptySnap("new")
	live.Tables = []schema.Table{{Schema: "public", Name: "users"}}
	report := ClassifyDrift(saved, live)
	if report.Direction != DriftAhead {
		t.Errorf("expected ahead, got %s", report.Direction)
	}
	if report.AddedCount != 1 {
		t.Errorf("expected 1 added, got %d", report.AddedCount)
	}
}

func TestBehind(t *testing.T) {
	saved := emptySnap("old")
	saved.Tables = []schema.Table{{Schema: "public", Name: "users"}}
	live := emptySnap("new")
	report := ClassifyDrift(saved, live)
	if report.Direction != DriftBehind {
		t.Errorf("expected behind, got %s", report.Direction)
	}
}

func TestDiverged(t *testing.T) {
	saved := emptySnap("old")
	saved.Tables = []schema.Table{{Schema: "public", Name: "old_table"}}
	live := emptySnap("new")
	live.Tables = []schema.Table{{Schema: "public", Name: "new_table"}}
	report := ClassifyDrift(saved, live)
	if report.Direction != DriftDiverged {
		t.Errorf("expected diverged, got %s", report.Direction)
	}
}
