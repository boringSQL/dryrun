package columnar

import (
	"strings"
	"testing"
)

func hasCode(fs []Finding, code string) *Finding {
	for i := range fs {
		if fs[i].Code == code {
			return &fs[i]
		}
	}
	return nil
}

func TestAnalyze_EngineEnabledButEmpty(t *testing.T) {
	s := &State{EngineEnabled: true}
	f := hasCode(Analyze(s), "columnar/engine_empty")
	if f == nil {
		t.Fatal("expected engine_empty finding when enabled with no resident columns")
	}
	if f.Severity != SeverityMedium {
		t.Errorf("severity = %q, want medium", f.Severity)
	}
	if !strings.Contains(f.Message, "google_columnar_engine_add") {
		t.Errorf("message should point at the populate function, got: %s", f.Message)
	}
}

func TestAnalyze_EngineDisabledIsSilent(t *testing.T) {
	s := &State{EngineEnabled: false}
	if f := hasCode(Analyze(s), "columnar/engine_empty"); f != nil {
		t.Errorf("engine disabled should not flag an empty store, got: %+v", f)
	}
}

func TestAnalyze_PopulatedEngineNoEmptyFinding(t *testing.T) {
	s := &State{
		EngineEnabled: true,
		Columns:       []Column{{Schema: "public", Relation: "lineitem", Column: "l_discount"}},
	}
	if f := hasCode(Analyze(s), "columnar/engine_empty"); f != nil {
		t.Errorf("populated store should not flag empty, got: %+v", f)
	}
}

func TestAnalyze_StaleBlocks(t *testing.T) {
	s := &State{
		Relations: []Relation{
			{Schema: "public", Relation: "hot", InvalidBlockCount: 30, TotalBlockCount: 100},
			{Schema: "public", Relation: "cold", InvalidBlockCount: 5, TotalBlockCount: 100},
		},
	}
	fs := Analyze(s)
	stale := hasCode(fs, "columnar/blocks_stale")
	if stale == nil {
		t.Fatal("expected blocks_stale for the 30%-invalid relation")
	}
	if stale.Relation != "public.hot" {
		t.Errorf("stale relation = %q, want public.hot", stale.Relation)
	}
	// cold at 5% is below threshold; only one stale finding expected
	count := 0
	for _, f := range fs {
		if f.Code == "columnar/blocks_stale" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly one stale finding, got %d", count)
	}
}

func TestAnalyze_AutoRefreshFailure(t *testing.T) {
	s := &State{
		Relations: []Relation{{Schema: "public", Relation: "r", AutoRefreshFailure: 3, TotalBlockCount: 100}},
	}
	f := hasCode(Analyze(s), "columnar/auto_refresh_failing")
	if f == nil {
		t.Fatal("expected auto_refresh_failing finding")
	}
	if f.Severity != SeverityHigh {
		t.Errorf("severity = %q, want high", f.Severity)
	}
}

func TestAnalyze_NilAndZeroBlocks(t *testing.T) {
	if Analyze(nil) != nil {
		t.Error("nil state should yield no findings")
	}
	// TotalBlockCount 0 must not divide by zero
	s := &State{Relations: []Relation{{Schema: "public", Relation: "empty"}}}
	if got := Analyze(s); len(got) != 0 {
		t.Errorf("relation with zero blocks should be silent, got: %+v", got)
	}
}
