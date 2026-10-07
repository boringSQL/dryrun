package snapdiff

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/pkg/diff"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// day i of a daily series; counters grow by 2000 calls a day at the given blocks per call
func planQuery(day int, perCall []int64, started time.Time) *snapshot.QueryStatsSnapshot {
	ts := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC).Add(time.Duration(day) * 24 * time.Hour)
	return planQueryAt(fmt.Sprintf("q-%d", day), "primary", ts, day, perCall, started)
}

// idx windows of perCall have accumulated by this capture
func planQueryAt(hash, source string, ts time.Time, idx int, perCall []int64, started time.Time) *snapshot.QueryStatsSnapshot {
	var calls, shared int64
	for i := 0; i < idx; i++ {
		calls += 2000
		shared += 2000 * perCall[i]
	}
	zero := int64(0)
	return &snapshot.QueryStatsSnapshot{
		SchemaRefHash: "sch", ContentHash: hash, QshapeVersion: 3, CaptureRuleVersion: 3,
		RowCap: 500, RawRows: 1,
		Node: snapshot.NodeIdentity{
			Source: source, PgVersion: "PostgreSQL 16.4", PostmasterStartTime: &started, Timestamp: ts,
		},
		Queries: []snapshot.QueryStatsEntry{{
			Fingerprint: "fp-users", Canonical: "SELECT * FROM users WHERE id = $1",
			Calls: calls, Rows: calls,
			Members: []snapshot.QueryStatsMember{{
				QueryID: 1, Calls: calls, Rows: calls,
				SharedBlksHit: &shared, SharedBlksRead: &zero, SharedBlksDirtied: &zero, SharedBlksWritten: &zero,
				TempBlksRead: &zero, TempBlksWritten: &zero,
			}},
		}},
	}
}

func TestPlanShiftsForPair_ReadsTheBaselineFromTheStore(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	started := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	perCall := []int64{100, 100, 100, 100, 1000}
	var snaps []*snapshot.QueryStatsSnapshot
	for day := 0; day <= 5; day++ {
		q := planQuery(day, perCall, started)
		snaps = append(snaps, q)
		if _, err := store.PutQueryStats(ctx, k, q); err != nil {
			t.Fatal(err)
		}
	}

	res := PlanShiftsForPair(ctx, store, k, snaps[4], snaps[5])
	if res.Suppressed != "" || len(res.Shifts) != 1 {
		t.Fatalf("want one shift, got %+v", res)
	}
	if res.Shifts[0].BaselineBlksPerCall != 100 || res.Shifts[0].BlksPerCall != 1000 {
		t.Fatalf("unexpected shift %+v", res.Shifts[0])
	}

	// the pair as the caller names it, newer first, reads the same
	if rev := PlanShiftsForPair(ctx, store, k, snaps[5], snaps[4]); len(rev.Shifts) != 1 {
		t.Fatalf("a reversed pair must read the same, got %+v", rev)
	}

	// day 2 has only two earlier captures: the baseline is incomplete, and says so
	early := PlanShiftsForPair(ctx, store, k, snaps[2], snaps[3])
	if early.Suppressed == "" || len(early.Shifts) != 0 {
		t.Fatalf("want a suppression, got %+v", early)
	}
}

func TestPlanShiftsForPair_IgnoresAnotherServersCaptures(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	started := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	other := started.Add(24 * time.Hour)
	perCall := []int64{100, 100, 100, 100, 1000}
	var snaps []*snapshot.QueryStatsSnapshot
	for day := 0; day <= 5; day++ {
		s := started
		if day == 1 || day == 2 {
			s = other
		}
		q := planQuery(day, perCall, s)
		snaps = append(snaps, q)
		if _, err := store.PutQueryStats(ctx, k, q); err != nil {
			t.Fatal(err)
		}
	}
	res := PlanShiftsForPair(ctx, store, k, snaps[4], snaps[5])
	// days 1 and 2 are skipped, leaving day 0 as the only same-server capture
	// before day 3: a 72h gap against a 24h window. A filter that let them in
	// would fail on the restart instead.
	if len(res.Shifts) != 0 || !strings.Contains(res.Suppressed, "too sparse") {
		t.Fatalf("captures from another server must not join the baseline, got %+v", res)
	}
}

// One label over two servers: the capture before the newer one is the other
// server's, so the pair is re-anchored on the newer server's own earlier capture
// and the baseline is read from that server alone.
func TestPlanShiftsForPair_RotatingLabelUsesTheNewerServersOwnCaptures(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	t0 := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	bootA, bootB := t0.Add(-72*time.Hour), t0.Add(-48*time.Hour)
	// server A runs the flip in its last window; B never moves
	perA := []int64{100, 100, 100, 1000}
	perB := []int64{100, 100, 100, 100}

	var snaps []*snapshot.QueryStatsSnapshot
	for day := 0; day <= 8; day++ {
		ts := t0.Add(time.Duration(day) * 24 * time.Hour)
		hash := fmt.Sprintf("p-%d", day)
		var q *snapshot.QueryStatsSnapshot
		if day%2 == 0 {
			q = planQueryAt(hash, "pool", ts, day/2, perA, bootA)
		} else {
			q = planQueryAt(hash, "pool", ts, day/2, perB, bootB)
		}
		snaps = append(snaps, q)
		if _, err := store.PutQueryStats(ctx, k, q); err != nil {
			t.Fatal(err)
		}
	}

	// asked for day 7 (server B) to day 8 (server A)
	res := PlanShiftsForPair(ctx, store, k, snaps[7], snaps[8])
	if res.Suppressed != "" || len(res.Shifts) != 1 {
		t.Fatalf("want the flip on server A found, got %+v", res)
	}
	if res.Window != 48*time.Hour {
		t.Errorf("window %s, want 48h: day 6 to day 8 on server A", res.Window)
	}
	if got := res.Shifts[0]; got.BaselineBlksPerCall != 100 || got.BlksPerCall != 1000 {
		t.Errorf("unexpected shift %+v", got)
	}
	if !strings.Contains(strings.Join(res.Notes, "|"), "rotates between servers") {
		t.Errorf("the re-anchoring is not noted: %v", res.Notes)
	}
}

func TestBuild_ReportsPlanShiftsInBothViews(t *testing.T) {
	ctx := context.Background()
	store := openStore(t)
	k := key()
	started := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	perCall := []int64{100, 100, 100, 100, 1000}
	for day := 0; day <= 5; day++ {
		if _, err := store.PutQueryStats(ctx, k, planQuery(day, perCall, started)); err != nil {
			t.Fatal(err)
		}
	}

	res, err := Build(ctx, store, k, Options{From: "latest~1", To: "latest", Kind: "query", Node: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary.PlanShifts != 1 || res.IsEmpty() {
		t.Fatalf("want one plan shift counted, got %d (empty=%v)", res.Summary.PlanShifts, res.IsEmpty())
	}
	if !strings.Contains(res.Summary.Headline, "possible plan flip") {
		t.Errorf("headline %q omits the plan shift", res.Summary.Headline)
	}

	sum := res.ForView("summary", 50)
	if len(sum.PlanShifts) != 1 || len(sum.PlanShifts[0].Result.Shifts) != 1 {
		t.Fatalf("summary must carry the flag, got %+v", sum.PlanShifts)
	}
	if sum.PlanShifts[0].Result.Shifts[0].Canonical != "" {
		t.Errorf("summary carries SQL text")
	}
	if res.PlanShifts[0].Result.Shifts[0].Canonical == "" {
		t.Errorf("summarising mutated the full result")
	}

	full := res.ForView("full", 50)
	if full.PlanShifts[0].Result.Shifts[0].Canonical == "" {
		t.Errorf("full view dropped the SQL text")
	}
}

func TestForView_CapsPlanShifts(t *testing.T) {
	var shifts []diff.PlanShift
	for i := 0; i < summaryPlanShifts+3; i++ {
		shifts = append(shifts, diff.PlanShift{QueryID: int64(i), Canonical: "SELECT 1"})
	}
	r := &Result{PlanShifts: []NodePlanShifts{{Node: "primary", Result: &diff.PlanShiftResult{Node: "primary", Shifts: shifts}}}}

	if got := len(r.ForView("summary", 50).PlanShifts[0].Result.Shifts); got != summaryPlanShifts {
		t.Errorf("summary kept %d flags, want %d", got, summaryPlanShifts)
	}
	full := r.ForView("full", 2)
	if got := len(full.PlanShifts[0].Result.Shifts); got != 2 {
		t.Errorf("full kept %d flags, want the limit of 2", got)
	}
	if full.OmittedRows != len(shifts)-2 || !full.Truncated {
		t.Errorf("omitted rows %d truncated %v", full.OmittedRows, full.Truncated)
	}
}
