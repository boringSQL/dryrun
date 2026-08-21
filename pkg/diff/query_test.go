package diff

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

func qEntry(fp, sql string, calls int64, ms float64, rows int64) snapshot.QueryStatsEntry {
	return snapshot.QueryStatsEntry{
		Fingerprint:     fp,
		Canonical:       sql,
		Calls:           calls,
		TotalExecTimeMs: ms,
		Rows:            rows,
		Members:         []snapshot.QueryStatsMember{{QueryID: 1, Calls: calls}},
	}
}

func qSnap(node string, at time.Time, entries ...snapshot.QueryStatsEntry) *snapshot.QueryStatsSnapshot {
	return &snapshot.QueryStatsSnapshot{
		SchemaRefHash: "sr",
		QshapeVersion: 3,
		RowCap:        500,
		RawRows:       len(entries),
		Node:          snapshot.NodeIdentity{Source: node, Timestamp: at},
		Queries:       entries,
	}
}

func findEntry(t *testing.T, d *QueryDelta, fp string) QueryEntryDelta {
	t.Helper()
	for _, e := range d.Entries {
		if e.Fingerprint == fp {
			return e
		}
	}
	t.Fatalf("fingerprint %q not in delta", fp)
	return QueryEntryDelta{}
}

func TestDiffQueryStats(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)

	from := qSnap("primary", t0,
		qEntry("fp-hot", "SELECT * FROM orders WHERE id = $1", 1000, 2000, 1000),
		qEntry("fp-cold", "SELECT 1", 10, 5, 10),
		qEntry("fp-gone", "SELECT * FROM legacy", 50, 100, 50),
	)
	to := qSnap("primary", t1,
		qEntry("fp-hot", "SELECT * FROM orders WHERE id = $1", 1500, 8000, 1500),
		qEntry("fp-cold", "SELECT 1", 10, 5, 10),
		qEntry("fp-new", "SELECT * FROM invoices", 200, 400, 200),
	)

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("window", func(t *testing.T) {
		if d.Window != time.Hour {
			t.Errorf("window %s, want 1h", d.Window)
		}
		if d.Incomparable != "" {
			t.Errorf("unexpectedly incomparable: %s", d.Incomparable)
		}
	})

	t.Run("growth is the difference, not the total", func(t *testing.T) {
		e := findEntry(t, d, "fp-hot")
		if e.Status != QueryGrew {
			t.Errorf("status %q, want %q", e.Status, QueryGrew)
		}
		if e.CallsDelta != 500 || e.TimeDelta != 6000 {
			t.Errorf("delta calls=%d time=%.0f, want 500 and 6000", e.CallsDelta, e.TimeDelta)
		}
	})

	// the reason this diff exists: pgss' own mean is since its last reset, so
	// a query that got 4x slower this hour is invisible in the raw numbers
	t.Run("window mean exposes a regression the totals hide", func(t *testing.T) {
		e := findEntry(t, d, "fp-hot")
		if e.WindowMeanMs == nil || e.PriorMeanMs == nil {
			t.Fatalf("means missing: %+v", e)
		}
		if *e.PriorMeanMs != 2 {
			t.Errorf("prior mean %.2f, want 2", *e.PriorMeanMs)
		}
		if *e.WindowMeanMs != 12 {
			t.Errorf("window mean %.2f, want 12 (6000ms over 500 calls)", *e.WindowMeanMs)
		}
	})

	t.Run("new and gone", func(t *testing.T) {
		if e := findEntry(t, d, "fp-new"); e.Status != QueryNew || e.CallsDelta != 200 {
			t.Errorf("new entry %+v", e)
		}
		if e := findEntry(t, d, "fp-gone"); e.Status != QueryGone {
			t.Errorf("status %q, want %q on an untruncated capture", e.Status, QueryGone)
		}
	})

	t.Run("unchanged shape is flat, not missing", func(t *testing.T) {
		if e := findEntry(t, d, "fp-cold"); e.Status != QueryFlat || e.CallsDelta != 0 {
			t.Errorf("flat entry %+v", e)
		}
	})

	t.Run("biggest mover first", func(t *testing.T) {
		if d.Entries[0].Fingerprint != "fp-hot" {
			t.Errorf("first entry %q, want fp-hot", d.Entries[0].Fingerprint)
		}
	})

	t.Run("totals", func(t *testing.T) {
		// 500 + 200 (new) + 0 + 0 (gone contributes nothing)
		if d.CallsDelta != 700 {
			t.Errorf("calls delta %d, want 700", d.CallsDelta)
		}
	})
}

// Counters are cumulative since pgss last reset, so subtracting across one is
// nonsense. Only stats_reset proves a reset happened -- dealloc moves under
// ordinary eviction pressure.
func TestDiffQueryStats_Reset(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	resetAt := t0.Add(30 * time.Minute)

	from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 1000, 2000, 1000))
	from.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: t0.Add(-24 * time.Hour), Dealloc: 0}
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 30, 60, 30))
	to.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: resetAt, Dealloc: 0}

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if !d.StatsReset {
		t.Fatal("a moved stats_reset was not detected")
	}
	e := findEntry(t, d, "fp")
	if e.Status != QueryReset {
		t.Errorf("status %q, want %q", e.Status, QueryReset)
	}
	// post-reset the counter IS the window's work, so no negative delta
	if e.CallsDelta != 30 {
		t.Errorf("calls delta %d, want 30 (counted from the reset)", e.CallsDelta)
	}
	if len(d.Caveats) == 0 {
		t.Error("a reset must be said out loud, not just flagged in a field")
	}
}

// dealloc alone is eviction pressure, not a reset: claiming a reset here would
// make every busy server look like it restarted.
func TestDiffQueryStats_DeallocIsNotAReset(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	same := t0.Add(-48 * time.Hour)

	from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 200, 100))
	from.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: same, Dealloc: 5}
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 150, 300, 150))
	to.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: same, Dealloc: 900}

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if d.StatsReset {
		t.Error("dealloc growth was mistaken for a reset")
	}
	if e := findEntry(t, d, "fp"); e.Status != QueryGrew {
		t.Errorf("status %q, want %q", e.Status, QueryGrew)
	}
}

// A counter that fell without a recorded reset still cannot be subtracted --
// most likely the entry was evicted and re-added.
func TestDiffQueryStats_DecreaseWithoutRecordedReset(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 1000, 2000, 1000))
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 5, 10, 5))

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	e := findEntry(t, d, "fp")
	if e.Status != QueryReset {
		t.Errorf("status %q, want %q -- a negative delta must never be reported as growth", e.Status, QueryReset)
	}
	if e.CallsDelta < 0 {
		t.Errorf("calls delta %d is negative", e.CallsDelta)
	}
}

func TestDiffQueryStats_Incomparable(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)

	t.Run("different nodes", func(t *testing.T) {
		from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 10, 10, 10))
		to := qSnap("replica", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 20, 20, 20))
		d, err := DiffQueryStats(from, to)
		if err != nil {
			t.Fatal(err)
		}
		if d.Incomparable == "" {
			t.Error("subtracted two different nodes' counters")
		}
		if len(d.Entries) != 0 {
			t.Error("produced entries for an incomparable pair")
		}
	})

	// a fingerprint means whatever the grouping said at capture time
	t.Run("grouping version changed", func(t *testing.T) {
		from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 10, 10, 10))
		to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 20, 20, 20))
		to.QshapeVersion = from.QshapeVersion + 1
		d, err := DiffQueryStats(from, to)
		if err != nil {
			t.Fatal(err)
		}
		if d.Incomparable == "" {
			t.Error("compared fingerprints across a grouping change")
		}
	})

	t.Run("nil operand is an error, not an empty diff", func(t *testing.T) {
		if _, err := DiffQueryStats(nil, qSnap("primary", t0)); err == nil {
			t.Error("want an error for a nil operand")
		}
	})
}

// A capped fetch is a view of the top-N. A shape missing from it may still be
// running, so "gone" would be a lie.
func TestDiffQueryStats_TruncatedCapture(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	from := qSnap("primary", t0,
		qEntry("fp-a", "SELECT 1", 10, 10, 10),
		qEntry("fp-b", "SELECT 2", 10, 10, 10),
	)
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp-a", "SELECT 1", 20, 20, 20))
	to.RowCap, to.RawRows = 1, 1 // saturated

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Truncated {
		t.Fatal("a saturated capture was not flagged")
	}
	if e := findEntry(t, d, "fp-b"); e.Status != QueryEvicted {
		t.Errorf("status %q, want %q on a truncated capture", e.Status, QueryEvicted)
	}
}

func TestDiffQueryStats_ArgumentOrder(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	older := qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 100, 100))
	newer := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 300, 300, 300))

	// passed backwards, it must still report growth over a positive window
	d, err := DiffQueryStats(newer, older)
	if err != nil {
		t.Fatal(err)
	}
	if d.Window != time.Hour {
		t.Errorf("window %s, want a positive 1h", d.Window)
	}
	if e := findEntry(t, d, "fp"); e.CallsDelta != 200 {
		t.Errorf("calls delta %d, want 200", e.CallsDelta)
	}
}

func TestRenderQueryConsole(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	from := qSnap("primary", t0, qEntry("fp", "SELECT  *\n FROM orders WHERE id = $1", 100, 200, 100))
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT  *\n FROM orders WHERE id = $1", 300, 3000, 300))
	d, _ := DiffQueryStats(from, to)

	var buf bytes.Buffer
	RenderQueryConsole(&buf, &SnapshotDiff{Kind: "query", Query: d, FromTakenAt: t0, ToTakenAt: t0.Add(time.Hour)})
	out := buf.String()

	for _, want := range []string{"primary", "1h0m0s", "grew", "+200", "SELECT * FROM orders"} {
		if !bytes.Contains(buf.Bytes(), []byte(want)) {
			t.Errorf("console output missing %q:\n%s", want, out)
		}
	}
	// 2800ms over 200 calls = 14ms, up from 2ms
	if !bytes.Contains(buf.Bytes(), []byte("14.00<-2.00")) {
		t.Errorf("output does not show the mean moving:\n%s", out)
	}
}

func TestRenderQueryConsole_Incomparable(t *testing.T) {
	var buf bytes.Buffer
	RenderQueryConsole(&buf, &SnapshotDiff{Kind: "query", Query: &QueryDelta{Incomparable: "different nodes"}})
	if !bytes.Contains(buf.Bytes(), []byte("not comparable")) {
		t.Errorf("an incomparable diff must say so, got: %s", buf.String())
	}
}

// A busy server carries hundreds of shapes and most do not move; a console
// that lists them all buries the one that did.
func TestRenderQueryConsole_ShowsMoversOnly(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	var fromEntries, toEntries []snapshot.QueryStatsEntry
	for i := 0; i < 40; i++ {
		fp := fmt.Sprintf("flat-%02d", i)
		fromEntries = append(fromEntries, qEntry(fp, "SELECT "+fp, 10, 10, 10))
		toEntries = append(toEntries, qEntry(fp, "SELECT "+fp, 10, 10, 10))
	}
	fromEntries = append(fromEntries, qEntry("mover", "SELECT * FROM orders", 10, 10, 10))
	toEntries = append(toEntries, qEntry("mover", "SELECT * FROM orders", 60, 5000, 60))

	d, err := DiffQueryStats(qSnap("primary", t0, fromEntries...), qSnap("primary", t0.Add(time.Hour), toEntries...))
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	RenderQueryConsole(&buf, &SnapshotDiff{Kind: "query", Query: d, FromTakenAt: t0, ToTakenAt: t0.Add(time.Hour)})
	out := buf.String()

	if !bytes.Contains(buf.Bytes(), []byte("SELECT * FROM orders")) {
		t.Errorf("the one moving shape is missing:\n%s", out)
	}
	if bytes.Contains(buf.Bytes(), []byte("flat-00")) {
		t.Errorf("unchanged shapes were listed:\n%s", out)
	}
	if !bytes.Contains(buf.Bytes(), []byte("(40 unchanged)")) {
		t.Errorf("unchanged shapes were not accounted for:\n%s", out)
	}
}

func TestRenderQueryConsole_NothingMoved(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	e := qEntry("fp", "SELECT 1", 10, 10, 10)
	d, err := DiffQueryStats(qSnap("primary", t0, e), qSnap("primary", t0.Add(time.Hour), e))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	RenderQueryConsole(&buf, &SnapshotDiff{Kind: "query", Query: d, FromTakenAt: t0, ToTakenAt: t0.Add(time.Hour)})
	if !bytes.Contains(buf.Bytes(), []byte("no shape moved")) {
		t.Errorf("want an explicit no-change line, got:\n%s", buf.String())
	}
}

// An entry is a qshape group over several queryids, so one member being
// evicted can pull time down while calls still rise. Reporting that as growth
// would put a negative delta -- and a negative mean -- in front of the user.
func TestDiffQueryStats_CallsUpTimeDown(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 5000, 100))
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 150, 900, 150))

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	e := findEntry(t, d, "fp")
	if e.TimeDelta < 0 {
		t.Errorf("negative time delta %.0f reported", e.TimeDelta)
	}
	if e.Status == QueryGrew {
		t.Error("a backwards time counter was reported as growth")
	}
	if e.WindowMeanMs != nil && *e.WindowMeanMs < 0 {
		t.Errorf("negative window mean %.2f", *e.WindowMeanMs)
	}
}

// A capped OLDER capture has no baseline: a shape appearing in the newer one
// may have been running below the cap all along, so its cumulative counter is
// not an hour's growth.
func TestDiffQueryStats_TruncatedBaseline(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	from := qSnap("primary", t0, qEntry("fp-a", "SELECT 1", 10, 10, 10))
	from.RowCap, from.RawRows = 1, 1 // saturated
	to := qSnap("primary", t0.Add(time.Hour),
		qEntry("fp-a", "SELECT 1", 20, 20, 20),
		qEntry("fp-old", "SELECT * FROM big", 900000, 5000000, 900000),
	)

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if !d.FromTruncated {
		t.Fatal("a capped older capture was not flagged")
	}
	e := findEntry(t, d, "fp-old")
	if e.Status != QueryTruncated {
		t.Errorf("status %q, want %q", e.Status, QueryTruncated)
	}
	if e.TimeDelta != 0 || e.CallsDelta != 0 {
		t.Errorf("claimed a delta (%d calls, %.0f ms) against no baseline", e.CallsDelta, e.TimeDelta)
	}
	// and it must not drag the window totals with it
	if d.TimeDelta >= 5000000 {
		t.Errorf("totals absorbed the unsubtractable shape: %.0f ms", d.TimeDelta)
	}
	if d.NotSubtractable != 1 {
		t.Errorf("NotSubtractable=%d, want 1", d.NotSubtractable)
	}
}

// pgss is not MVCC-consistent with its own info view, which is why both reads
// are captured. A reset inside the older capture's own fetch tears its rows.
func TestDiffQueryStats_ResetStraddledTheOlderCapture(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	old, mid := t0.Add(-24*time.Hour), t0.Add(-time.Minute)

	from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 200, 100))
	from.InfoBefore = &snapshot.QueryStatsInfo{StatsReset: old}
	from.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: mid}
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 300, 600, 300))
	to.InfoBefore = &snapshot.QueryStatsInfo{StatsReset: mid}
	to.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: mid}

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if !d.StatsReset {
		t.Error("a reset inside the older capture's own fetch was missed")
	}
	var straddle bool
	for _, c := range d.Caveats {
		if strings.Contains(c, "while a capture was reading") {
			straddle = true
		}
	}
	if !straddle {
		t.Errorf("the torn capture was not called out: %v", d.Caveats)
	}
}

// A zero stats_reset is "unknown", not "epoch": treating it as a real value
// makes every entry look reset.
func TestDiffQueryStats_ZeroStatsResetIsNotAReset(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 200, 100))
	from.InfoAfter = &snapshot.QueryStatsInfo{} // zero value
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 150, 300, 150))
	to.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: t0.Add(-48 * time.Hour)}

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if d.StatsReset {
		t.Error("a zero stats_reset was read as a reset")
	}
	if e := findEntry(t, d, "fp"); e.Status != QueryGrew {
		t.Errorf("status %q, want %q", e.Status, QueryGrew)
	}
}

// The rule deciding which statements get captured defines the population;
// across a change the two sets are not the same thing.
func TestDiffQueryStats_CaptureRuleVersion(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)

	from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 10, 10, 10))
	from.CaptureRuleVersion = 1
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 20, 20, 20))
	to.CaptureRuleVersion = 2

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if d.Incomparable == "" {
		t.Error("differenced two different statement populations")
	}

	// one side unversioned is unknown, not proof of a change: still comparable
	to.CaptureRuleVersion = 0
	d2, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if d2.Incomparable != "" {
		t.Errorf("refused on an unversioned capture: %s", d2.Incomparable)
	}
}

func TestRenderQueryConsole_CapsRows(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	var fromEntries, toEntries []snapshot.QueryStatsEntry
	for i := 0; i < queryConsoleRows+10; i++ {
		fp := fmt.Sprintf("m-%02d", i)
		fromEntries = append(fromEntries, qEntry(fp, "SELECT "+fp, 10, 10, 10))
		toEntries = append(toEntries, qEntry(fp, "SELECT "+fp, 20, float64(100+i), 20))
	}
	d, err := DiffQueryStats(qSnap("primary", t0, fromEntries...), qSnap("primary", t0.Add(time.Hour), toEntries...))
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	RenderQueryConsole(&buf, &SnapshotDiff{Kind: "query", Query: d, FromTakenAt: t0, ToTakenAt: t0.Add(time.Hour)})
	if !bytes.Contains(buf.Bytes(), []byte("10 more moved")) {
		t.Errorf("truncation was silent:\n%s", buf.String())
	}
}

// Passed (newer, older) the diff normalises the window; the header must not
// then print the arrow backwards.
func TestRenderQueryConsole_ArrowNeverBackwards(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	d, err := DiffQueryStats(
		qSnap("primary", t1, qEntry("fp", "SELECT 1", 300, 300, 300)),
		qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 100, 100)),
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	RenderQueryConsole(&buf, &SnapshotDiff{Kind: "query", Query: d, FromTakenAt: t1, ToTakenAt: t0})
	if !bytes.Contains(buf.Bytes(), []byte("09:00:00 -> 2026-08-21 10:00:00")) {
		t.Errorf("header runs backwards:\n%s", buf.String())
	}
}

func TestTruncateQuery_KeepsRunesIntact(t *testing.T) {
	got := truncateQuery("SELECT * FROM \"naïve_café_ütförsäljning_tabell\" WHERE x = $1 AND y = $2", 20)
	if !utf8.ValidString(got) {
		t.Errorf("truncation split a rune: %q", got)
	}
}

// dealloc proves eviction, never a reset (that is stats_reset's job). Over a
// long window entries leave pgss under pgss.max pressure, so a shape absent
// from one side may have been evicted and re-added rather than started.
func TestDiffQueryStats_DeallocCaveatsNewAndGone(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	same := t0.Add(-72 * time.Hour)

	from := qSnap("primary", t0, qEntry("fp-old", "SELECT 1", 100, 100, 100))
	from.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: same, Dealloc: 10}
	to := qSnap("primary", t0.Add(24*time.Hour), qEntry("fp-fresh", "SELECT 2", 500, 900, 500))
	to.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: same, Dealloc: 4210}

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if d.StatsReset {
		t.Fatal("dealloc growth was read as a reset")
	}
	var said bool
	for _, c := range d.Caveats {
		if strings.Contains(c, "evicted 4200 entries") {
			said = true
		}
	}
	if !said {
		t.Errorf("eviction pressure was not reported: %v", d.Caveats)
	}
}

// A reset zeroes dealloc too, so the difference across one means nothing.
func TestDiffQueryStats_DeallocIgnoredAcrossAReset(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 100, 100))
	from.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: t0.Add(-48 * time.Hour), Dealloc: 900}
	to := qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 5, 5, 5))
	to.InfoAfter = &snapshot.QueryStatsInfo{StatsReset: t0.Add(-time.Minute), Dealloc: 3}

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range d.Caveats {
		if strings.Contains(c, "evicted") {
			t.Errorf("claimed eviction across a reset: %q", c)
		}
	}
}

// The label is a name, not proof of one machine -- which is why captures
// record which server answered. Two servers' cumulative counters are
// unrelated, so subtracting them fabricates growth.
func TestDiffQueryStats_ServerChanged(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	bootA := t0.Add(-72 * time.Hour)
	bootB := t0.Add(-2 * time.Hour)

	withServer := func(q *snapshot.QueryStatsSnapshot, boot time.Time, addr string) *snapshot.QueryStatsSnapshot {
		b := boot
		q.Node.PostmasterStartTime = &b
		q.Node.ServerAddr = addr
		return q
	}

	t.Run("two addresses under one label is refused", func(t *testing.T) {
		from := withServer(qSnap("pool", t0, qEntry("fp", "SELECT 1", 1000, 2000, 1000)), bootA, "10.0.0.1")
		to := withServer(qSnap("pool", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 40, 80, 40)), bootB, "10.0.0.2")

		d, err := DiffQueryStats(from, to)
		if err != nil {
			t.Fatal(err)
		}
		if d.Incomparable == "" {
			t.Fatal("subtracted two servers' counters under one label")
		}
		if len(d.Entries) != 0 {
			t.Error("produced entries for two unrelated servers")
		}
	})

	// pg_stat_statements survives a clean restart, so a moved boot time alone
	// is not grounds to refuse -- but it is worth saying out loud
	t.Run("same address, new boot time still diffs, with a caveat", func(t *testing.T) {
		from := withServer(qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 200, 100)), bootA, "10.0.0.1")
		to := withServer(qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 300, 600, 300)), bootB, "10.0.0.1")

		d, err := DiffQueryStats(from, to)
		if err != nil {
			t.Fatal(err)
		}
		if d.Incomparable != "" {
			t.Fatalf("refused a restart of the same machine: %s", d.Incomparable)
		}
		if d.ServerChanged == "" {
			t.Error("the restart was not reported")
		}
		var said bool
		for _, c := range d.Caveats {
			if strings.Contains(c, "restarted or was replaced") {
				said = true
			}
		}
		if !said {
			t.Errorf("caveats do not mention the restart: %v", d.Caveats)
		}
		if e := findEntry(t, d, "fp"); e.CallsDelta != 200 {
			t.Errorf("calls delta %d, want 200", e.CallsDelta)
		}
	})

	// an address is NULL over a Unix socket and identical behind a tunnel, so
	// it can never be the thing that clears a diff
	t.Run("unknown addresses caveat rather than refuse", func(t *testing.T) {
		from := withServer(qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 200, 100)), bootA, "")
		to := withServer(qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 300, 600, 300)), bootB, "")

		d, err := DiffQueryStats(from, to)
		if err != nil {
			t.Fatal(err)
		}
		if d.Incomparable != "" || d.ServerChanged == "" {
			t.Errorf("incomparable=%q serverChanged=%q, want a caveat", d.Incomparable, d.ServerChanged)
		}
	})

	t.Run("same server says nothing", func(t *testing.T) {
		from := withServer(qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 200, 100)), bootA, "10.0.0.1")
		to := withServer(qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 300, 600, 300)), bootA, "10.0.0.1")

		d, err := DiffQueryStats(from, to)
		if err != nil {
			t.Fatal(err)
		}
		if d.ServerChanged != "" {
			t.Errorf("reported a change on an unchanged server: %s", d.ServerChanged)
		}
	})

	// captures predating the fingerprint carry none; the diff must still work
	t.Run("no fingerprint on either side is silent", func(t *testing.T) {
		d, err := DiffQueryStats(
			qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 200, 100)),
			qSnap("primary", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 300, 600, 300)),
		)
		if err != nil {
			t.Fatal(err)
		}
		if d.Incomparable != "" || d.ServerChanged != "" {
			t.Errorf("unfingerprinted captures were flagged: %q / %q", d.Incomparable, d.ServerChanged)
		}
	})
}

// PostmasterStartTime and ServerAddr are independently optional, so a pair
// with two addresses and no boot time on one side must still be refused --
// gating the strong signal behind the weak one silently subtracts two
// machines' counters.
func TestDiffQueryStats_TwoAddressesWithoutBootTime(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	from := qSnap("pool", t0, qEntry("fp", "SELECT 1", 1000, 2000, 1000))
	from.Node.ServerAddr = "10.0.0.1"
	to := qSnap("pool", t0.Add(time.Hour), qEntry("fp", "SELECT 1", 40, 80, 40))
	to.Node.ServerAddr = "10.0.0.2"
	boot := t0.Add(-time.Hour)
	to.Node.PostmasterStartTime = &boot // only one side carries it

	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if d.Incomparable == "" {
		t.Fatal("subtracted two servers' counters because one boot time was missing")
	}
}
