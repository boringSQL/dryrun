package diff

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

type (
	QueryDelta struct {
		Node string `json:"node"`
		// wall time between the two captures; the window every delta is over
		Window time.Duration `json:"window"`
		// set when the two captures cannot be subtracted at all
		Incomparable string `json:"incomparable,omitempty"`
		// pg_stat_statements was reset between the captures: counters restarted
		// from zero, so growth is unknowable for every shape
		StatsReset bool `json:"stats_reset,omitempty"`
		// either capture hit its row cap, so a shape missing from one side may
		// simply have fallen out of the top-N
		Truncated bool `json:"truncated,omitempty"`
		// the OLDER capture was capped: a shape absent there may have been
		// running below the cap, so its first appearance is not growth
		FromTruncated bool `json:"from_truncated,omitempty"`
		// entries whose window delta is unknowable (reset, truncated baseline)
		// and therefore excluded from the totals
		NotSubtractable int               `json:"not_subtractable,omitempty"`
		Entries         []QueryEntryDelta `json:"entries"`
		CallsDelta      int64             `json:"calls_delta"`
		TimeDelta       float64           `json:"total_exec_time_ms_delta"`
		Caveats         []string          `json:"caveats,omitempty"`
	}

	// One query shape between two captures. Deltas are over the window, which
	// is what makes the mean meaningful: pg_stat_statements' own mean_exec_time
	// averages since its last reset and answers a different question.
	QueryEntryDelta struct {
		Fingerprint string `json:"fingerprint"`
		Canonical   string `json:"canonical,omitempty"`
		Status      string `json:"status"`

		Calls      int64 `json:"calls"`
		CallsDelta int64 `json:"calls_delta"`

		TotalExecTimeMs float64 `json:"total_exec_time_ms"`
		TimeDelta       float64 `json:"total_exec_time_ms_delta"`

		Rows      int64 `json:"rows"`
		RowsDelta int64 `json:"rows_delta"`

		// mean over the window, and over the previous window when both are
		// known; nil when the call count did not move
		WindowMeanMs *float64 `json:"window_mean_ms,omitempty"`
		PriorMeanMs  *float64 `json:"prior_mean_ms,omitempty"`
	}
)

// Statuses a shape can carry between two captures.
const (
	QueryGrew   = "grew"
	QueryShrank = "shrank"
	QueryFlat   = "flat"
	QueryNew    = "new"
	QueryGone   = "gone"
	// counters went backwards: a reset, or an entry evicted and re-added.
	// The window's work is unknowable, so the delta is not a difference.
	QueryReset = "reset"
	// absent from a capped capture: may still be running, just below the cap
	QueryEvicted = "evicted"
	// present only in the newer capture, but the older one was capped, so
	// "new" would be a guess
	QueryTruncated = "truncated"

	// console rows; --json carries every entry
	queryConsoleRows = 25
)

// DiffQueryStats subtracts two captures of one node's pg_stat_statements.
//
// It refuses rather than guesses. Counters are cumulative since pgss last
// reset, so a reset, a grouping-version change, or two different nodes make
// subtraction meaningless, and saying so is more useful than a plausible
// number.
func DiffQueryStats(from, to *snapshot.QueryStatsSnapshot) (*QueryDelta, error) {
	if from == nil || to == nil {
		return nil, fmt.Errorf("both captures are required")
	}
	d := &QueryDelta{
		Node:   to.Node.Source,
		Window: to.Node.Timestamp.Sub(from.Node.Timestamp),
	}
	if from.Node.Source != to.Node.Source {
		d.Incomparable = fmt.Sprintf("different nodes (%s and %s): counters are per node",
			from.Node.Source, to.Node.Source)
		return d, nil
	}
	if d.Window < 0 {
		from, to = to, from
		d.Window = -d.Window
	}
	// grouping decides what a fingerprint means, so shapes are not the same
	// objects across a version change
	if from.QshapeVersion != to.QshapeVersion {
		d.Incomparable = fmt.Sprintf("query grouping changed (v%d to v%d): fingerprints are not comparable",
			from.QshapeVersion, to.QshapeVersion)
		return d, nil
	}

	// the rule selecting which statements are captured decides the population;
	// across a change the two sets are not the same thing
	if from.CaptureRuleVersion != to.CaptureRuleVersion &&
		from.CaptureRuleVersion != 0 && to.CaptureRuleVersion != 0 {
		d.Incomparable = fmt.Sprintf("capture rules changed (v%d to v%d): the statement sets do not correspond",
			from.CaptureRuleVersion, to.CaptureRuleVersion)
		return d, nil
	}

	d.StatsReset = statsWasReset(from, to)
	d.FromTruncated = hitRowCap(from)
	d.Truncated = hitRowCap(from) || hitRowCap(to)
	d.Caveats = queryCaveats(from, to, d)

	prev := make(map[string]snapshot.QueryStatsEntry, len(from.Queries))
	for _, e := range from.Queries {
		prev[e.Fingerprint] = e
	}

	for _, cur := range to.Queries {
		before, seen := prev[cur.Fingerprint]
		delete(prev, cur.Fingerprint)
		d.Entries = append(d.Entries, entryDelta(before, cur, seen, d.StatsReset, d.FromTruncated))
	}
	// left in prev: shapes the newer capture does not carry
	for _, before := range prev {
		d.Entries = append(d.Entries, QueryEntryDelta{
			Fingerprint:     before.Fingerprint,
			Canonical:       before.Canonical,
			Status:          missingStatus(d.Truncated),
			Calls:           before.Calls,
			TotalExecTimeMs: before.TotalExecTimeMs,
			Rows:            before.Rows,
		})
	}

	// a reset or a truncated baseline has no window delta to add; summing
	// them would put a shape's whole lifetime into an hour's total
	for _, e := range d.Entries {
		if e.Status == QueryReset || e.Status == QueryTruncated {
			d.NotSubtractable++
			continue
		}
		d.CallsDelta += e.CallsDelta
		d.TimeDelta += e.TimeDelta
	}
	sortQueryEntries(d.Entries)
	return d, nil
}

func entryDelta(before, cur snapshot.QueryStatsEntry, seen, reset, fromTruncated bool) QueryEntryDelta {
	e := QueryEntryDelta{
		Fingerprint:     cur.Fingerprint,
		Canonical:       cur.Canonical,
		Calls:           cur.Calls,
		TotalExecTimeMs: cur.TotalExecTimeMs,
		Rows:            cur.Rows,
	}
	switch {
	case !seen && fromTruncated:
		// the older capture was capped, so this shape may have been running
		// all along below the cap; its cumulative counter is not window growth
		e.Status = QueryTruncated
	case !seen:
		e.Status = QueryNew
		e.CallsDelta, e.TimeDelta, e.RowsDelta = cur.Calls, cur.TotalExecTimeMs, cur.Rows
	case reset || wentBackwards(before, cur):
		// after a reset the new value IS the window's work; before a reset we
		// cannot know how much ran, so report what we can see and say why
		e.Status = QueryReset
		e.CallsDelta, e.TimeDelta, e.RowsDelta = cur.Calls, cur.TotalExecTimeMs, cur.Rows
	default:
		e.CallsDelta = cur.Calls - before.Calls
		e.TimeDelta = cur.TotalExecTimeMs - before.TotalExecTimeMs
		e.RowsDelta = cur.Rows - before.Rows
		switch {
		case e.CallsDelta > 0:
			e.Status = QueryGrew
		case e.CallsDelta == 0 && e.TimeDelta == 0:
			e.Status = QueryFlat
		default:
			e.Status = QueryShrank
		}
		if before.Calls > 0 {
			m := before.TotalExecTimeMs / float64(before.Calls)
			e.PriorMeanMs = &m
		}
	}
	if e.CallsDelta > 0 {
		m := e.TimeDelta / float64(e.CallsDelta)
		e.WindowMeanMs = &m
	}
	return e
}

// A shape the newer capture lacks either stopped running or fell out of the
// top-N. With a truncated capture we cannot tell which, so we do not claim to.
func missingStatus(truncated bool) string {
	if truncated {
		return QueryEvicted
	}
	return QueryGone
}

// stats_reset moving is the only proof of a reset. dealloc moves under
// ordinary eviction pressure, so it says nothing about resets.
//
// The comparison spans the pair: from's earliest read against to's latest, so
// a reset that landed inside from's own fetch still counts.
func statsWasReset(from, to *snapshot.QueryStatsSnapshot) bool {
	a, b := earliestInfo(from), latestInfo(to)
	if a == nil || b == nil || a.StatsReset.IsZero() || b.StatsReset.IsZero() {
		return false
	}
	return b.StatsReset.After(a.StatsReset)
}

// pgss is not MVCC-consistent with its own info view, so a capture whose two
// reads disagree straddled a reset: its rows are part pre- and part post-.
func straddledReset(q *snapshot.QueryStatsSnapshot) bool {
	if q.InfoBefore == nil || q.InfoAfter == nil {
		return false
	}
	return q.InfoAfter.StatsReset.After(q.InfoBefore.StatsReset)
}

func latestInfo(q *snapshot.QueryStatsSnapshot) *snapshot.QueryStatsInfo {
	if q.InfoAfter != nil {
		return q.InfoAfter
	}
	return q.InfoBefore
}

func earliestInfo(q *snapshot.QueryStatsSnapshot) *snapshot.QueryStatsInfo {
	if q.InfoBefore != nil {
		return q.InfoBefore
	}
	return q.InfoAfter
}

// Any counter going backwards makes the pair unsubtractable. An entry is a
// group over several queryids, so one member being evicted can pull time down
// while calls still rise.
func wentBackwards(before, cur snapshot.QueryStatsEntry) bool {
	return cur.Calls < before.Calls ||
		cur.TotalExecTimeMs < before.TotalExecTimeMs ||
		cur.Rows < before.Rows
}

// The fetch is capped, so a saturated capture is a view of the top-N, not of
// everything that ran.
func hitRowCap(q *snapshot.QueryStatsSnapshot) bool {
	limit := q.RowCap
	if limit == 0 {
		limit = 500 // captures predating RowCap used this fixed limit
	}
	// RawRows is omitempty, so a capture predating it reads as 0 with shapes
	// present: unknown, and unknown has to mean "might be capped"
	if q.RawRows == 0 && len(q.Queries) > 0 {
		return true
	}
	return q.RawRows >= limit
}

func queryCaveats(from, to *snapshot.QueryStatsSnapshot, d *QueryDelta) []string {
	var out []string
	if d.StatsReset {
		out = append(out, "pg_stat_statements was reset during the window; deltas are since the reset, not since the earlier capture")
	}
	if hitRowCap(to) {
		out = append(out, "the newer capture hit its row cap; shapes outside the top-N are absent, so 'evicted' may mean 'still running, just smaller'")
	}
	if d.FromTruncated {
		out = append(out, "the older capture hit its row cap; shapes marked 'truncated' have no baseline to subtract, so no growth is claimed for them")
	}
	if straddledReset(from) || straddledReset(to) {
		out = append(out, "pg_stat_statements was reset while a capture was reading it; that capture's rows are part pre-reset and part post-reset")
	}
	if from.ToplevelOnly != to.ToplevelOnly {
		out = append(out, "toplevel filtering differs between the captures; nested statements are counted on one side only")
	}
	if a, b := trackOf(from), trackOf(to); a != b && a != "" && b != "" {
		out = append(out, fmt.Sprintf("pg_stat_statements.track changed (%s to %s)", a, b))
	}
	if d.Window <= 0 {
		out = append(out, "both captures carry the same timestamp; rates over the window are not meaningful")
	}
	return out
}

func trackOf(q *snapshot.QueryStatsSnapshot) string {
	if q.PgssTrack == nil {
		return ""
	}
	return *q.PgssTrack
}

// Biggest movers first: added time is what a reader is looking for, and ties
// break on fingerprint so output is stable.
func sortQueryEntries(rows []QueryEntryDelta) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.TimeDelta != b.TimeDelta {
			return a.TimeDelta > b.TimeDelta
		}
		if a.CallsDelta != b.CallsDelta {
			return a.CallsDelta > b.CallsDelta
		}
		return a.Fingerprint < b.Fingerprint
	})
}

func RenderQueryConsole(w io.Writer, env *SnapshotDiff) {
	d := env.Query
	if d == nil {
		return
	}
	if d.Incomparable != "" {
		fmt.Fprintf(w, "Query stats not comparable: %s\n", d.Incomparable)
		return
	}
	older, newer := env.FromTakenAt, env.ToTakenAt
	if newer.Before(older) {
		older, newer = newer, older
	}
	fmt.Fprintf(w, "Query stats: node=%s, window %s (%s -> %s)\n",
		d.Node, humanWindow(d.Window),
		older.UTC().Format("2006-01-02 15:04:05"),
		newer.UTC().Format("2006-01-02 15:04:05"))
	if len(d.Entries) == 0 {
		fmt.Fprintln(w, "  no query shapes in either capture")
		return
	}
	fmt.Fprintf(w, "  %+d calls, %+.0f ms total\n", d.CallsDelta, d.TimeDelta)
	if d.NotSubtractable > 0 {
		fmt.Fprintf(w, "  %d shape(s) had no subtractable baseline and are excluded from those totals\n", d.NotSubtractable)
	}
	fmt.Fprintln(w)

	// a busy server carries hundreds of shapes and most do not move; listing
	// them buries the ones that did
	movers := make([]QueryEntryDelta, 0, len(d.Entries))
	for _, e := range d.Entries {
		if e.Status != QueryFlat {
			movers = append(movers, e)
		}
	}
	unchanged := len(d.Entries) - len(movers)
	if len(movers) == 0 {
		fmt.Fprintf(w, "  no shape moved (%d unchanged)\n", unchanged)
		return
	}

	shown := movers
	if len(shown) > queryConsoleRows {
		shown = shown[:queryConsoleRows]
	}
	fmt.Fprintf(w, "  %-9s %12s %14s %-17s  %s\n", "STATUS", "CALLS", "TIME(ms)", "MEAN(ms)", "QUERY")
	for _, e := range shown {
		fmt.Fprintf(w, "  %-9s %12s %14s %-17s  %s\n",
			e.Status, signedInt(e.CallsDelta), signedFloat(e.TimeDelta),
			meanCell(e), truncateQuery(e.Canonical, 60))
	}
	if len(movers) > len(shown) {
		fmt.Fprintf(w, "  ... %d more moved\n", len(movers)-len(shown))
	}
	if unchanged > 0 {
		fmt.Fprintf(w, "  (%d unchanged)\n", unchanged)
	}
	for _, c := range d.Caveats {
		fmt.Fprintf(w, "\n  note: %s", c)
	}
	if len(d.Caveats) > 0 {
		fmt.Fprintln(w)
	}
}

// Shows the window mean and, when the shape ran before too, where it moved
// from: a query that doubled in cost is the finding, not its total.
func meanCell(e QueryEntryDelta) string {
	if e.WindowMeanMs == nil {
		return "-"
	}
	if e.PriorMeanMs == nil {
		return fmt.Sprintf("%.2f", *e.WindowMeanMs)
	}
	return fmt.Sprintf("%.2f<-%.2f", *e.WindowMeanMs, *e.PriorMeanMs)
}

func signedInt(v int64) string {
	if v == 0 {
		return "0"
	}
	return fmt.Sprintf("%+d", v)
}

func signedFloat(v float64) string {
	if v == 0 {
		return "0"
	}
	return fmt.Sprintf("%+.0f", v)
}

func humanWindow(d time.Duration) string {
	if d <= 0 {
		return "none"
	}
	// sub-second windows round to "0s", which reads as no time at all
	if d < 10*time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}

func truncateQuery(q string, n int) string {
	q = collapseSpace(q)
	r := []rune(q)
	if len(r) <= n {
		return q
	}
	return string(r[:n-1]) + "…"
}

func collapseSpace(s string) string {
	out := make([]rune, 0, len(s))
	space := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !space && len(out) > 0 {
				out = append(out, ' ')
			}
			space = true
			continue
		}
		space = false
		out = append(out, r)
	}
	for len(out) > 0 && out[len(out)-1] == ' ' {
		out = out[:len(out)-1]
	}
	return string(out)
}
