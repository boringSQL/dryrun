package diff

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// one window of one queryid; shared is blocks per call
type pwin struct {
	calls, rows, shared int64
}

func steady(shared int64) pwin      { return pwin{2000, 2000, shared} }
func busy(calls, shared int64) pwin { return pwin{calls, calls, shared} }

var planT0 = time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)

// a member carrying cumulative totals; the counters a plan shift ignores are zero
func memberTotal(qid, calls, rows, shared int64) snapshot.QueryStatsMember {
	zero, hit := int64(0), shared
	return snapshot.QueryStatsMember{
		QueryID: qid, Calls: calls, Rows: rows,
		SharedBlksHit: &hit, SharedBlksRead: &zero, SharedBlksDirtied: &zero, SharedBlksWritten: &zero,
		TempBlksRead: &zero, TempBlksWritten: &zero,
	}
}

// psnap builds a capture at day d (24h apart) with the given entries
func psnap(day int, ents ...snapshot.QueryStatsEntry) *snapshot.QueryStatsSnapshot {
	s := qSnap("primary", planT0.Add(time.Duration(day)*24*time.Hour), ents...)
	s.CaptureRuleVersion = 3
	start := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	s.Node.PostmasterStartTime = &start
	return s
}

func pent(fp string, ms ...snapshot.QueryStatsMember) snapshot.QueryStatsEntry {
	e := snapshot.QueryStatsEntry{Fingerprint: fp, Canonical: "SELECT " + fp, Members: ms}
	for _, m := range ms {
		e.Calls += m.Calls
		e.Rows += m.Rows
	}
	return e
}

// chain builds captures 0..len(wins) for one queryid whose window i is wins[i]
func chain(qid int64, wins ...pwin) []*snapshot.QueryStatsSnapshot {
	return chainMulti(false, []int64{qid}, wins)
}

// chainMulti accumulates several queryids, wins[k] being the windows of qids[k],
// in one shared shape or one shape each
func chainMulti(sameShape bool, qids []int64, wins ...[]pwin) []*snapshot.QueryStatsSnapshot {
	n := len(wins[0])
	totals := make([]pwin, len(qids))
	out := make([]*snapshot.QueryStatsSnapshot, 0, n+1)
	for day := 0; day <= n; day++ {
		var members []snapshot.QueryStatsMember
		for k, qid := range qids {
			if day > 0 {
				w := wins[k][day-1]
				totals[k].calls += w.calls
				totals[k].rows += w.rows
				totals[k].shared += w.calls * w.shared
			}
			members = append(members, memberTotal(qid, totals[k].calls, totals[k].rows, totals[k].shared))
		}
		if sameShape {
			out = append(out, psnap(day, pent("shape", members...)))
			continue
		}
		var ents []snapshot.QueryStatsEntry
		for k, m := range members {
			ents = append(ents, pent([]string{"fp", "fp2"}[k], m))
		}
		out = append(out, psnap(day, ents...))
	}
	return out
}

// diff of the last two captures against the three windows before them
func runChain(caps []*snapshot.QueryStatsSnapshot) *PlanShiftResult {
	n := len(caps)
	return PlanShifts(caps[n-5:n-2], caps[n-2], caps[n-1])
}

func TestPlanShiftsFlagsAFlipWithStableRows(t *testing.T) {
	r := runChain(chain(7, steady(100), steady(100), steady(100), steady(1000)))
	if r.Suppressed != "" || len(r.Shifts) != 1 {
		t.Fatalf("want one shift, got %+v", r)
	}
	s := r.Shifts[0]
	if s.QueryID != 7 || s.BaselineBlksPerCall != 100 || s.BlksPerCall != 1000 || s.ExcessBlks != 900.0*2000 {
		t.Fatalf("unexpected shift %+v", s)
	}
	if len(s.BaselineCalls) != PlanShiftBaselineWindows {
		t.Fatalf("baseline calls %v", s.BaselineCalls)
	}

	if r := runChain(chain(7, steady(100), steady(100), steady(100), steady(110))); len(r.Shifts) != 0 || r.Suppressed != "" {
		t.Fatalf("a steady query must not flag, got %+v", r)
	}

	// 4x on a hot query clears both excess floors, so only the ratio holds it back
	hot := func(shared int64) pwin { return busy(100000, shared) }
	if r := runChain(chain(7, hot(100), hot(100), hot(100), hot(400))); len(r.Shifts) != 0 {
		t.Fatalf("a 4x step is under the ratio, got %+v", r.Shifts)
	}
	if r := runChain(chain(7, hot(100), hot(100), hot(100), hot(600))); len(r.Shifts) != 1 {
		t.Fatalf("a 6x step must flag, got %+v", r)
	}
}

func TestPlanShiftsCatchesAFlipLandingMidWindow(t *testing.T) {
	// a 10x flip with 30% of the window's calls on the bad plan reads 3.7x
	// against a one-window baseline and then hides in the next diff's baseline;
	// the median baseline reports it once the flip has taken over
	mixed := pwin{2000, 2000, 100*7/10 + 1000*3/10}
	caps := chain(7, steady(100), steady(100), steady(100), mixed, steady(1000))

	if first := PlanShifts(caps[0:3], caps[3], caps[4]); len(first.Shifts) != 0 {
		t.Fatalf("mixed window is under the ratio, got %+v", first.Shifts)
	}
	second := PlanShifts(caps[1:4], caps[4], caps[5])
	if len(second.Shifts) != 1 || second.Shifts[0].BaselineBlksPerCall != 100 {
		t.Fatalf("the next diff must flag against the median, got %+v", second)
	}
}

func TestPlanShiftsIsPerQueryidNotPerShape(t *testing.T) {
	// one queryid flips 10x on a tenth of the shape's calls, which moves the
	// shape's mean by under 2x
	a := []pwin{busy(18000, 100), busy(18000, 100), busy(18000, 100), busy(18000, 100), busy(18000, 100)}
	b := []pwin{busy(2000, 100), busy(2000, 100), busy(2000, 100), busy(2000, 100), busy(2000, 1000)}
	caps := chainMulti(true, []int64{1, 2}, a, b)

	if r := PlanShifts(caps[0:3], caps[3], caps[4]); len(r.Shifts) != 0 {
		t.Fatalf("unexpected shift before the flip: %+v", r.Shifts)
	}
	r := PlanShifts(caps[1:4], caps[4], caps[5])
	if len(r.Shifts) != 1 || r.Shifts[0].QueryID != 2 {
		t.Fatalf("want queryid 2 flagged, got %+v", r.Shifts)
	}
}

func TestPlanShiftsRanksByExcessBlocks(t *testing.T) {
	// the modest query has the bigger ratio (50x), the hot one the bigger excess
	a := []pwin{steady(100), steady(100), steady(100), steady(100), steady(5000)}
	b := []pwin{busy(100000, 10), busy(100000, 10), busy(100000, 10), busy(100000, 10), busy(100000, 210)}
	caps := chainMulti(false, []int64{1, 2}, a, b)
	res := PlanShifts(caps[1:4], caps[4], caps[5])
	if len(res.Shifts) != 2 || res.Shifts[0].QueryID != 2 {
		t.Fatalf("want the hot query first (20M excess blocks against 9.8M), got %+v", res.Shifts)
	}
}

func TestPlanShiftsCountsANewQueryidAsNoBaseline(t *testing.T) {
	caps := chain(7, steady(100), steady(100), steady(100), steady(100))
	last := caps[len(caps)-1]
	last.Queries = append(last.Queries, pent("new", memberTotal(99, 5000, 5000, 5000*5000)))
	r := runChain(caps)
	if len(r.Shifts) != 0 || r.NoBaseline != 1 {
		t.Fatalf("want no shift and one no_baseline, got %+v", r)
	}
}

func TestPlanShiftsRowsGuard(t *testing.T) {
	cases := []struct {
		name string
		base pwin
		cur  pwin
		want int
	}{
		{"rows grew with blocks", pwin{2000, 2000, 100}, pwin{2000, 20000, 1000}, 0},
		{"zero rows both sides is stable", pwin{2000, 0, 100}, pwin{2000, 0, 1000}, 1},
		{"zero to non-zero rows is unstable", pwin{2000, 0, 100}, pwin{2000, 2000, 1000}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := runChain(chain(7, c.base, c.base, c.base, c.cur))
			if len(r.Shifts) != c.want {
				t.Fatalf("got %d shifts, want %d: %+v", len(r.Shifts), c.want, r.Shifts)
			}
		})
	}
}

func TestPlanShiftsBackwardsCounterIsUnassessable(t *testing.T) {
	caps := chain(7, steady(100), steady(100), steady(100), steady(1000))
	// the counters restarted for this queryid inside a baseline window
	caps[2].Queries[0].Members[0] = memberTotal(7, 10, 10, 10)
	r := runChain(caps)
	if len(r.Shifts) != 0 || r.Unassessable != 1 {
		t.Fatalf("want unassessable, got %+v", r)
	}
}

func TestPlanShiftsNeedsCallsInEveryWindow(t *testing.T) {
	r := runChain(chain(7, steady(100), pwin{10, 10, 100}, steady(100), steady(1000)))
	if len(r.Shifts) != 0 {
		t.Fatalf("a baseline window under the call floor must not flag: %+v", r.Shifts)
	}
}

func TestPlanShiftsRefusals(t *testing.T) {
	cases := []struct {
		name string
		mut  func([]*snapshot.QueryStatsSnapshot)
		want string
	}{
		{"restart", func(c []*snapshot.QueryStatsSnapshot) {
			ts := planT0.Add(48 * time.Hour)
			c[1].Node.PostmasterStartTime = &ts
		}, "restarted"},
		{"stats reset", func(c []*snapshot.QueryStatsSnapshot) {
			c[2].InfoAfter = &snapshot.QueryStatsInfo{StatsReset: planT0}
			c[3].InfoBefore = &snapshot.QueryStatsInfo{StatsReset: planT0.Add(time.Hour)}
		}, "reset"},
		{"old capture rule", func(c []*snapshot.QueryStatsSnapshot) { c[0].CaptureRuleVersion = 1 }, "predates"},
		{"window lengths", func(c []*snapshot.QueryStatsSnapshot) {
			c[4].Node.Timestamp = c[3].Node.Timestamp.Add(5 * time.Hour)
		}, "not comparable"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			caps := chain(7, steady(100), steady(100), steady(100), steady(1000))
			c.mut(caps)
			r := runChain(caps)
			if !strings.Contains(r.Suppressed, c.want) || len(r.Shifts) != 0 {
				t.Fatalf("want suppression containing %q, got %+v", c.want, r)
			}
		})
	}
}

func TestPlanShiftsUnknownInfoIsNotedNotRefused(t *testing.T) {
	r := runChain(chain(7, steady(100), steady(100), steady(100), steady(1000)))
	notes := strings.Join(r.Notes, "|")
	if !strings.Contains(notes, "no reset or eviction info") || len(r.Shifts) != 1 {
		t.Fatalf("want a note and a shift, got %+v", r)
	}
}

// before capture rule v3 the cap fell on each role's pgss row, so a queryid's
// total can jump by a whole role row's lifetime between two captures
func TestPlanShiftsRoleRowChurnIsNotAFlip(t *testing.T) {
	split := func(m snapshot.QueryStatsMember) []snapshot.QueryStatsMember {
		a, b := m, m
		a.Calls, a.Rows = m.Calls/2, m.Rows/2
		b.Calls, b.Rows = m.Calls-a.Calls, m.Rows-a.Rows
		ha := *m.SharedBlksHit / 2
		hb := *m.SharedBlksHit - ha
		a.SharedBlksHit, b.SharedBlksHit = &ha, &hb
		return []snapshot.QueryStatsMember{a, b}
	}
	old := func(caps []*snapshot.QueryStatsSnapshot, rows func(i int) bool) {
		for i, c := range caps {
			c.CaptureRuleVersion = 2
			if rows(i) {
				c.Queries[0].Members = split(c.Queries[0].Members[0])
			}
		}
	}

	t.Run("a role row that appears mid-span is unassessable", func(t *testing.T) {
		caps := chain(7, steady(100), steady(100), steady(100), steady(100))
		old(caps, func(i int) bool { return i >= 2 })
		r := runChain(caps)
		if len(r.Shifts) != 0 || r.Unassessable != 1 {
			t.Fatalf("want unassessable and no flag, got %+v", r)
		}
	})

	t.Run("the v2 to v3 boundary is unassessable", func(t *testing.T) {
		caps := chain(7, steady(100), steady(100), steady(100), steady(100))
		folded := caps[4].Queries[0].Members[0]
		old(caps, func(i int) bool { return true })
		// v3 folds the role rows into one with the same totals, so nothing goes backwards
		caps[4].CaptureRuleVersion = 3
		caps[4].Queries[0].Members = []snapshot.QueryStatsMember{folded}
		r := runChain(caps)
		if len(r.Shifts) != 0 || r.Unassessable != 1 {
			t.Fatalf("want unassessable and no flag, got %+v", r)
		}
	})

	t.Run("a stable role-row count still flags a real flip", func(t *testing.T) {
		caps := chain(7, steady(100), steady(100), steady(100), steady(1000))
		old(caps, func(i int) bool { return true })
		if r := runChain(caps); len(r.Shifts) != 1 {
			t.Fatalf("want the flip flagged, got %+v", r)
		}
	})
}

func TestPickPlanShiftBaselineRefusesSparseHistory(t *testing.T) {
	from := qSnap("primary", planT0.Add(200*time.Hour))
	to := qSnap("primary", planT0.Add(201*time.Hour))
	old := qSnap("primary", planT0)
	prevAt := func(time.Time) (*snapshot.QueryStatsSnapshot, bool, error) { return old, true, nil }
	if _, why, _ := PickPlanShiftBaseline(from, to, prevAt); !strings.Contains(why, "too sparse") {
		t.Fatalf("want a sparse refusal, got %q", why)
	}
	none := func(time.Time) (*snapshot.QueryStatsSnapshot, bool, error) { return nil, false, nil }
	if _, why, _ := PickPlanShiftBaseline(from, to, none); !strings.Contains(why, "incomplete") {
		t.Fatalf("want an incomplete refusal, got %q", why)
	}
}
