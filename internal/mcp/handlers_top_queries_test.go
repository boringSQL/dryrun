package mcp

import (
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

// The regression this guards: pgss under track = 'all' bills a nested
// statement's time twice — once to its own row, once to the caller it ran
// inside. Summing raw totals inflates the per-node denominator, which silently
// understates every reported share.
func TestTopLevelExecTimeExcludesNestedTime(t *testing.T) {
	tests := []struct {
		name  string
		entry schema.QueryStatsEntry
		want  float64
	}{
		{
			name:  "track=top leaves nested at zero, so the total passes through",
			entry: schema.QueryStatsEntry{TotalExecTimeMs: 500},
			want:  500,
		},
		{
			name:  "a shape run both directly and inside a function keeps only its direct share",
			entry: schema.QueryStatsEntry{TotalExecTimeMs: 500, NestedExecTimeMs: 200},
			want:  300,
		},
		{
			name:  "a purely nested shape contributes nothing to the top-level total",
			entry: schema.QueryStatsEntry{TotalExecTimeMs: 200, NestedExecTimeMs: 200},
			want:  0,
		},
		{
			name:  "corrupt snapshot: nested above total must clamp, never go negative",
			entry: schema.QueryStatsEntry{TotalExecTimeMs: 100, NestedExecTimeMs: 250},
			want:  0,
		},
		{
			name:  "corrupt snapshot: negative nested must not inflate the total",
			entry: schema.QueryStatsEntry{TotalExecTimeMs: 100, NestedExecTimeMs: -50},
			want:  100,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := topLevelExecTime(tc.entry); got != tc.want {
				t.Errorf("topLevelExecTime = %v, want %v", got, tc.want)
			}
		})
	}
}

// The percentages are the point: a trigger body counted into the denominator
// drags every share down, so the arithmetic is asserted end to end.
func TestNestedTimeDoesNotDilutePercentages(t *testing.T) {
	// pgss has already charged the trigger body's 300ms into the parent's 800ms;
	// the trigger's own row repeats it, and only that repeat must be dropped.
	parent := schema.QueryStatsEntry{TotalExecTimeMs: 800}
	sibling := schema.QueryStatsEntry{TotalExecTimeMs: 200}
	triggerBody := schema.QueryStatsEntry{TotalExecTimeMs: 300, NestedExecTimeMs: 300}

	all := []schema.QueryStatsEntry{parent, sibling, triggerBody}

	var denom float64
	for _, e := range all {
		denom += topLevelExecTime(e)
	}
	// 800 (parent, nested time included) + 200 (sibling) + 0 = 1000ms of wall time
	if denom != 1000 {
		t.Fatalf("denominator = %v, want 1000", denom)
	}
	if got := topLevelExecTime(parent) / denom * 100; got != 80 {
		t.Errorf("parent share = %v%%, want 80%%", got)
	}
	// summing raw totals would give 1300 and report the parent at ~61%
	raw := parent.TotalExecTimeMs + sibling.TotalExecTimeMs + triggerBody.TotalExecTimeMs
	if parent.TotalExecTimeMs/raw*100 >= 80 {
		t.Fatal("test is not exercising the bug: raw denominator should understate the share")
	}

	// Numerator and denominator must use the same measure, or a nested-heavy
	// shape reports a share of a total it was never part of.
	var sumPct float64
	for _, e := range all {
		sumPct += topLevelExecTime(e) / denom * 100
	}
	if sumPct != 100 {
		t.Errorf("shares sum to %v%%, want exactly 100%%", sumPct)
	}
}

// Regression for the mismatched-measure bug: full total over a top-level
// denominator lets one entry exceed 100%.
func TestNestedHeavyShapeCannotExceedFullShare(t *testing.T) {
	triggerBody := schema.QueryStatsEntry{TotalExecTimeMs: 5000, NestedExecTimeMs: 5000}
	caller := schema.QueryStatsEntry{TotalExecTimeMs: 6000}
	other := schema.QueryStatsEntry{TotalExecTimeMs: 4000}

	var denom float64
	for _, e := range []schema.QueryStatsEntry{triggerBody, caller, other} {
		denom += topLevelExecTime(e)
	}
	if denom != 10000 {
		t.Fatalf("denominator = %v, want 10000", denom)
	}
	// the old shape: 5000/10000 = 50% for the trigger plus 60% for its caller
	if triggerBody.TotalExecTimeMs/denom*100+caller.TotalExecTimeMs/denom*100 <= 100 {
		t.Fatal("test is not exercising the bug: raw numerators should overflow 100%")
	}
	for _, e := range []schema.QueryStatsEntry{triggerBody, caller, other} {
		if pct := topLevelExecTime(e) / denom * 100; pct > 100 {
			t.Errorf("share %v%% exceeds 100%%", pct)
		}
	}
}
