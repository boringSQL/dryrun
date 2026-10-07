package diff

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

type (
	// plan-flip symptom: per-call buffer traffic up while rows per call held;
	// PG16 has no plan id, so this is never a plan comparison
	PlanShift struct {
		QueryID     int64  `json:"queryid"`
		Fingerprint string `json:"fingerprint"`
		Canonical   string `json:"canonical,omitempty"`

		// shared hit+read blocks per call: median baseline, then the window under test
		BaselineBlksPerCall float64 `json:"baseline_blks_per_call"`
		BlksPerCall         float64 `json:"blks_per_call"`
		BaselineRowsPerCall float64 `json:"baseline_rows_per_call"`
		RowsPerCall         float64 `json:"rows_per_call"`

		Calls         int64   `json:"calls"`
		BaselineCalls []int64 `json:"baseline_calls"`
		// (current - baseline) blocks per call times calls; the ranking key
		ExcessBlks float64 `json:"excess_blks"`
	}

	PlanShiftResult struct {
		Node   string        `json:"node"`
		Window time.Duration `json:"window"`
		Shifts []PlanShift   `json:"shifts"`
		// busy queryids absent from every baseline capture (new, or under the cap
		// until now); a lower bound, since an absent newer queryid is invisible
		NoBaseline int `json:"no_baseline,omitempty"`
		// no usable window: counters went backwards, block counters missing, or a
		// pre-v3 capture's per-role rows changed
		Unassessable int `json:"unassessable,omitempty"`
		// why nothing was assessed; set so silence never reads as "no flips"
		Suppressed string   `json:"suppressed,omitempty"`
		Notes      []string `json:"notes,omitempty"`
	}

	// one queryid's counters in one capture, summed over its entries and role rows
	planTotals struct {
		fingerprint, canonical string
		calls, rows, shared    int64
		// every member carried the block counters
		known bool
		// before capture rule v3 the cap fell on each role's row, so a role row
		// crossing it moves a whole lifetime into one window; a count change is that
		memberRows int
		roleRows   bool
	}

	planWindow struct {
		calls, rows, shared int64
	}
)

const (
	// preceding windows whose median is the baseline. One cannot be: a flip
	// landing mid-window is half-visible here and half-absorbed into the next.
	PlanShiftBaselineWindows = 3

	// baseline windows must be about as long as the window under test
	planShiftMinLenRatio = 0.5
	planShiftMaxLenRatio = 2.0

	planShiftMinCalls       = 50
	planShiftRatio          = 5.0
	planShiftMinExcessPer   = 20.0
	planShiftMinTotalExcess = 1_000_000.0
	planShiftRowsStable     = 1.5

	// the oldest rule whose per-queryid sums are usable
	planShiftMinRule = 2
)

func planShiftMinLen(window time.Duration) time.Duration {
	return time.Duration(float64(window) * planShiftMinLenRatio)
}

func planShiftMaxLen(window time.Duration) time.Duration {
	return time.Duration(float64(window) * planShiftMaxLenRatio)
}

// PlanShifts compares the window from..to against the PlanShiftBaselineWindows
// before it; base holds their start captures, oldest first. It refuses rather
// than guesses, saying why in Suppressed; missing reset/eviction info is a note,
// not a refusal, since a restart the counters do not betray goes unseen.
func PlanShifts(base []*snapshot.QueryStatsSnapshot, from, to *snapshot.QueryStatsSnapshot) *PlanShiftResult {
	res := &PlanShiftResult{Shifts: []PlanShift{}}
	if from == nil || to == nil {
		res.Suppressed = "both captures are required"
		return res
	}
	res.Node = to.Node.Source
	if from.Node.Timestamp.After(to.Node.Timestamp) {
		from, to = to, from
	}
	res.Window = to.Node.Timestamp.Sub(from.Node.Timestamp)
	if len(base) != PlanShiftBaselineWindows {
		res.Suppressed = fmt.Sprintf("need %d earlier captures of the same server to form the baseline, have %d",
			PlanShiftBaselineWindows, len(base))
		return res
	}
	caps := make([]*snapshot.QueryStatsSnapshot, 0, len(base)+2)
	caps = append(caps, base...)
	caps = append(caps, from, to)

	if why := planShiftEligible(caps, res); why != "" {
		res.Suppressed = why
		return res
	}

	tot := make([]map[int64]planTotals, len(caps))
	for i, c := range caps {
		tot[i] = captureTotals(c)
	}
	last := len(caps) - 1

	for qid, now := range tot[last] {
		cur, curOK := planWindow{calls: now.calls, rows: now.rows, shared: now.shared}, now.known
		if prior, inFrom := tot[last-1][qid]; inFrom {
			cur, curOK = planWindowOf(prior, now)
			if !curOK {
				res.Unassessable++
				continue
			}
		}
		if !curOK || cur.calls < planShiftMinCalls {
			continue
		}
		wins, status := baselineWindows(tot, qid)
		switch status {
		case baselineMissing:
			res.NoBaseline++
			continue
		case baselineBackwards:
			res.Unassessable++
			continue
		}
		if s, ok := evaluatePlanShift(qid, now, cur, wins); ok {
			res.Shifts = append(res.Shifts, s)
		}
	}
	sort.Slice(res.Shifts, func(i, j int) bool {
		a, b := res.Shifts[i], res.Shifts[j]
		if a.ExcessBlks != b.ExcessBlks {
			return a.ExcessBlks > b.ExcessBlks
		}
		return a.QueryID < b.QueryID
	})
	return res
}

const (
	baselineOK = iota
	baselineMissing
	baselineBackwards
)

// the window of each baseline interval for one queryid, or why it has none
func baselineWindows(tot []map[int64]planTotals, qid int64) ([]planWindow, int) {
	wins := make([]planWindow, 0, PlanShiftBaselineWindows)
	for i := 0; i < PlanShiftBaselineWindows; i++ {
		a, okA := tot[i][qid]
		b, okB := tot[i+1][qid]
		if !okA || !okB {
			return nil, baselineMissing
		}
		w, ok := planWindowOf(a, b)
		if !ok {
			return nil, baselineBackwards
		}
		wins = append(wins, w)
	}
	return wins, baselineOK
}

func planWindowOf(a, b planTotals) (planWindow, bool) {
	if !a.known || !b.known {
		return planWindow{}, false
	}
	if (a.roleRows || b.roleRows) && a.memberRows != b.memberRows {
		return planWindow{}, false
	}
	w := planWindow{calls: b.calls - a.calls, rows: b.rows - a.rows, shared: b.shared - a.shared}
	if w.calls < 0 || w.rows < 0 || w.shared < 0 {
		return planWindow{}, false
	}
	return w, true
}

func evaluatePlanShift(qid int64, now planTotals, cur planWindow, base []planWindow) (PlanShift, bool) {
	for _, w := range base {
		if w.calls < planShiftMinCalls {
			return PlanShift{}, false
		}
	}
	perCall := func(w planWindow, f func(planWindow) int64) float64 {
		return float64(f(w)) / float64(w.calls)
	}
	shared := func(w planWindow) int64 { return w.shared }
	rows := func(w planWindow) int64 { return w.rows }
	median := func(f func(planWindow) int64) float64 {
		v := make([]float64, len(base))
		for i, w := range base {
			v[i] = perCall(w, f)
		}
		slices.Sort(v)
		return v[len(v)/2]
	}

	baseRows, curRows := median(rows), perCall(cur, rows)
	if !rowsStable(baseRows, curRows) {
		return PlanShift{}, false
	}
	baseShared, curShared := median(shared), perCall(cur, shared)
	excess := (curShared - baseShared) * float64(cur.calls)
	if curShared-baseShared < planShiftMinExcessPer || excess < planShiftMinTotalExcess ||
		(baseShared > 0 && curShared/baseShared < planShiftRatio) {
		return PlanShift{}, false
	}

	s := PlanShift{
		QueryID: qid, Fingerprint: now.fingerprint, Canonical: now.canonical,
		BaselineBlksPerCall: baseShared, BlksPerCall: curShared,
		BaselineRowsPerCall: baseRows, RowsPerCall: curRows,
		Calls: cur.calls, ExcessBlks: excess,
	}
	for _, w := range base {
		s.BaselineCalls = append(s.BaselineCalls, w.calls)
	}
	return s, true
}

// both zero is stable; zero against non-zero is not
func rowsStable(base, cur float64) bool {
	if base == 0 && cur == 0 {
		return true
	}
	if base == 0 || cur == 0 {
		return false
	}
	r := cur / base
	return r <= planShiftRowsStable && r >= 1/planShiftRowsStable
}

// refusals and notes: what makes the captures incomparable, or merely untrusted
func planShiftEligible(caps []*snapshot.QueryStatsSnapshot, res *PlanShiftResult) string {
	for _, c := range caps {
		if c == nil {
			return "a baseline capture is missing"
		}
	}
	node := caps[0].Node.Source
	for i, c := range caps {
		if c.Node.Source != node {
			return fmt.Sprintf("captures come from different nodes (%s and %s)", node, c.Node.Source)
		}
		if i > 0 && !c.Node.Timestamp.After(caps[i-1].Node.Timestamp) {
			return "captures are not in strictly increasing time order"
		}
	}
	last := caps[len(caps)-1]
	for _, c := range caps {
		if c.CaptureRuleVersion < planShiftMinRule {
			return fmt.Sprintf("a capture predates capture rule v%d (has v%d)", planShiftMinRule, c.CaptureRuleVersion)
		}
		if c.ToplevelOnly != last.ToplevelOnly {
			return "toplevel filtering differs between captures"
		}
		if a, b := trackOf(c), trackOf(last); a != b && a != "" && b != "" {
			return fmt.Sprintf("pg_stat_statements.track changed (%s to %s)", a, b)
		}
	}

	var started *time.Time
	unknownServer := 0
	for _, c := range caps {
		ts := c.Node.PostmasterStartTime
		if ts == nil {
			unknownServer++
			continue
		}
		if started != nil && !ts.Equal(*started) {
			return "the server restarted or changed inside the span: counters are not comparable"
		}
		started = ts
	}
	if unknownServer > 0 {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"%d of %d captures carry no server start time: a restart between them cannot be ruled out except by counters going backwards",
			unknownServer, len(caps)))
	}

	cur := last.Node.Timestamp.Sub(caps[len(caps)-2].Node.Timestamp)
	if cur <= 0 {
		return "the window under test has no duration"
	}
	for i := 0; i < PlanShiftBaselineWindows; i++ {
		d := caps[i+1].Node.Timestamp.Sub(caps[i].Node.Timestamp)
		if d < planShiftMinLen(cur) || d > planShiftMaxLen(cur) {
			return fmt.Sprintf("baseline window %d is %s against a %s window under test: lengths are not comparable",
				i+1, d.Round(time.Minute), cur.Round(time.Minute))
		}
	}

	unknownInfo := 0
	for i, c := range caps {
		if i > 0 && (statsWasReset(caps[i-1], c) || straddledReset(c)) {
			return "pg_stat_statements was reset inside the span: counters restarted"
		}
		if earliestInfo(c) == nil {
			unknownInfo++
		}
	}
	if unknownInfo > 0 {
		res.Notes = append(res.Notes, fmt.Sprintf(
			"%d of %d captures carry no reset or eviction info: a reset or eviction is detectable only as a counter going backwards",
			unknownInfo, len(caps)))
	}
	return ""
}

// queryid -> counters, summed over the entries and per-role rows that carry it
func captureTotals(q *snapshot.QueryStatsSnapshot) map[int64]planTotals {
	out := make(map[int64]planTotals)
	for _, e := range q.Queries {
		for _, m := range e.Members {
			t, ok := out[m.QueryID]
			if !ok {
				t = planTotals{
					fingerprint: e.Fingerprint, canonical: e.Canonical, known: true,
					roleRows: q.CaptureRuleVersion < 3,
				}
			}
			t.memberRows++
			t.calls += m.Calls
			t.rows += m.Rows
			if b := memberBlocks(m); b.known {
				t.shared += b.hit + b.read
			} else {
				t.known = false
			}
			out[m.QueryID] = t
		}
	}
	return out
}

// PickPlanShiftBaseline walks back from from, taking the newest capture at least
// half a window earlier, so baseline windows track the window under test whatever
// the cadence. prevAt returns the newest same-server capture at or before its arg.
func PickPlanShiftBaseline(from, to *snapshot.QueryStatsSnapshot, prevAt func(time.Time) (*snapshot.QueryStatsSnapshot, bool, error)) ([]*snapshot.QueryStatsSnapshot, string, error) {
	window := to.Node.Timestamp.Sub(from.Node.Timestamp)
	if window <= 0 {
		return nil, "the window under test has no duration", nil
	}
	base := make([]*snapshot.QueryStatsSnapshot, PlanShiftBaselineWindows)
	cur := from.Node.Timestamp
	for i := PlanShiftBaselineWindows - 1; i >= 0; i-- {
		c, ok, err := prevAt(cur.Add(-planShiftMinLen(window)))
		if err != nil {
			return nil, "", err
		}
		if !ok {
			return nil, fmt.Sprintf("no capture of the same server %s or more before %s: the baseline is incomplete",
				planShiftMinLen(window).Round(time.Minute), cur.UTC().Format(time.RFC3339)), nil
		}
		if gap := cur.Sub(c.Node.Timestamp); gap > planShiftMaxLen(window) {
			return nil, fmt.Sprintf("the nearest earlier capture is %s before %s, against a %s window under test: capture cadence is too sparse for a baseline",
				gap.Round(time.Minute), cur.UTC().Format(time.RFC3339), window.Round(time.Minute)), nil
		}
		base[i], cur = c, c.Node.Timestamp
	}
	return base, "", nil
}

const planShiftConsoleRows = 10

func RenderPlanShifts(w io.Writer, r *PlanShiftResult) {
	if r == nil {
		return
	}
	fmt.Fprintf(w, "\nPlan shifts: node=%s, window %s against the median of %d earlier windows\n",
		r.Node, humanWindow(r.Window), PlanShiftBaselineWindows)
	if r.Suppressed != "" {
		fmt.Fprintf(w, "  not assessed: %s\n", r.Suppressed)
		return
	}
	if len(r.Shifts) == 0 {
		fmt.Fprintln(w, "  no queryid stepped up its per-call cost")
	} else {
		fmt.Fprintf(w, "  %-22s %12s %14s  %s\n", "BLKS/CALL", "ROWS/CALL", "EXCESS BLKS", "QUERY")
		for i, s := range r.Shifts {
			if i == planShiftConsoleRows {
				fmt.Fprintf(w, "  ... %d more\n", len(r.Shifts)-i)
				break
			}
			fmt.Fprintf(w, "  %-22s %12s %14.0f  %s\n",
				fmt.Sprintf("%.0f<-%.0f", s.BlksPerCall, s.BaselineBlksPerCall),
				fmt.Sprintf("%.1f<-%.1f", s.RowsPerCall, s.BaselineRowsPerCall),
				s.ExcessBlks, truncateQuery(s.Canonical, 50))
		}
	}
	if r.NoBaseline > 0 {
		fmt.Fprintf(w, "  %d busy queryid(s) have no baseline (new, or below the capture cap until now)\n", r.NoBaseline)
	}
	if r.Unassessable > 0 {
		fmt.Fprintf(w, "  %d queryid(s) were not assessed (counters went backwards, block counters missing, or role rows changed)\n", r.Unassessable)
	}
	renderNotes(w, r.Notes)
}
