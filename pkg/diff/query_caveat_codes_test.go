package diff

import (
	"bytes"
	"slices"
	"strings"
	"testing"
	"time"
)

func codesFor(t *testing.T, from, to time.Time) *QueryDelta {
	t.Helper()
	d, err := DiffQueryStats(
		qSnap("primary", from, qEntry("fp", "SELECT 1", 100, 100, 100)),
		qSnap("primary", to, qEntry("fp", "SELECT 1", 200, 200, 200)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestCaveatCodes_ShortWindow(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC) // Friday
	for _, tc := range []struct {
		name   string
		window time.Duration
		want   bool
	}{
		{"26 minutes", 26 * time.Minute, true},
		{"29m59s", 30*time.Minute - time.Second, true},
		{"exactly 30m", 30 * time.Minute, false},
		{"zero keeps its own caveat", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := codesFor(t, t0, t0.Add(tc.window))
			if got := slices.Contains(d.CaveatCodes, CaveatShortWindow); got != tc.want {
				t.Errorf("short_window = %v, want %v (codes %v)", got, tc.want, d.CaveatCodes)
			}
		})
	}
}

func TestCaveatCodes_Weekend(t *testing.T) {
	at := func(day, hour, min int) time.Time { return time.Date(2026, 8, day, hour, min, 0, 0, time.UTC) }
	// 2026-08-21 is a Friday, 22 Saturday, 23 Sunday, 24 Monday
	for _, tc := range []struct {
		name     string
		from, to time.Time
		want     bool
	}{
		{"sunday", at(23, 9, 0), at(23, 11, 0), true},
		{"monday", at(24, 9, 0), at(24, 11, 0), false},
		{"friday night into saturday, mostly weekend", at(21, 23, 50), at(22, 10, 0), true},
		{"friday into saturday, mostly weekday", at(21, 12, 0), at(22, 2, 0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := codesFor(t, tc.from, tc.to)
			if got := slices.Contains(d.CaveatCodes, CaveatWeekend); got != tc.want {
				t.Errorf("weekend = %v, want %v (codes %v)", got, tc.want, d.CaveatCodes)
			}
		})
	}
}

func TestCaveatCodes_WeekendIgnoresZoneOfTimestamp(t *testing.T) {
	// Monday morning at UTC+10 is still Sunday evening in UTC
	aest := time.FixedZone("AEST", 10*3600)
	from := time.Date(2026, 8, 24, 8, 0, 0, 0, aest)
	d := codesFor(t, from, from.Add(2*time.Hour))
	if !slices.Contains(d.CaveatCodes, CaveatWeekend) {
		t.Errorf("codes %v, want weekend", d.CaveatCodes)
	}
}

func TestCaveatCodes_ResetKeepsWindowCodes(t *testing.T) {
	t0 := time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC)
	from := qSnap("primary", t0, qEntry("fp", "SELECT 1", 100, 5000, 100))
	to := qSnap("primary", t0.Add(10*time.Minute), qEntry("fp", "SELECT 1", 2, 4000, 2))
	// lower counters with no recorded reset still read as a reset entry
	d, err := DiffQueryStats(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(d.CaveatCodes, CaveatShortWindow) || !slices.Contains(d.CaveatCodes, CaveatWeekend) {
		t.Errorf("window codes lost on a reset pair: %v", d.CaveatCodes)
	}
	if slices.Contains(d.CaveatCodes, CaveatOneOffHeavy) || len(findEntry(t, d, "fp").CaveatCodes) > 0 {
		t.Errorf("reset entry must not be one_off_heavy: %v", d.CaveatCodes)
	}
}

func TestCaveatCodes_OneOffHeavy(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	build := func(calls int64, ms float64) *QueryDelta {
		d, err := DiffQueryStats(
			qSnap("primary", t0,
				qEntry("backfill", "UPDATE t SET x = 1", 10, 10, 10),
				qEntry("steady", "SELECT 1", 1000, 1000, 1000)),
			qSnap("primary", t0.Add(time.Hour),
				qEntry("backfill", "UPDATE t SET x = 1", 10+calls, 10+ms, 10),
				qEntry("steady", "SELECT 1", 2000, 3000, 2000)),
		)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}

	t.Run("two calls, most of the time", func(t *testing.T) {
		d := build(2, 6000)
		if !slices.Contains(findEntry(t, d, "backfill").CaveatCodes, CaveatOneOffHeavy) {
			t.Errorf("backfill not flagged: %+v", findEntry(t, d, "backfill"))
		}
		if len(findEntry(t, d, "steady").CaveatCodes) != 0 {
			t.Error("steady shape flagged")
		}
		if !slices.Contains(d.CaveatCodes, CaveatOneOffHeavy) {
			t.Errorf("rollup missing: %v", d.CaveatCodes)
		}
	})
	t.Run("many calls is load", func(t *testing.T) {
		if d := build(50, 6000); slices.Contains(d.CaveatCodes, CaveatOneOffHeavy) {
			t.Error("50 calls flagged")
		}
	})
	t.Run("small share", func(t *testing.T) {
		if d := build(2, 100); slices.Contains(d.CaveatCodes, CaveatOneOffHeavy) {
			t.Error("2 calls at 5% flagged")
		}
	})
	t.Run("idle window below the floor", func(t *testing.T) {
		d, err := DiffQueryStats(
			qSnap("primary", t0, qEntry("a", "SELECT 1", 10, 10, 10)),
			qSnap("primary", t0.Add(time.Hour), qEntry("a", "SELECT 1", 12, 18, 12)),
		)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(d.CaveatCodes, CaveatOneOffHeavy) {
			t.Error("8ms of window time flagged")
		}
	})
}

func TestRenderQueryConsole_ShowsCaveatCodes(t *testing.T) {
	t0 := time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC)
	d := codesFor(t, t0, t0.Add(26*time.Minute))
	var buf bytes.Buffer
	RenderQueryConsole(&buf, &SnapshotDiff{Query: d, FromTakenAt: t0, ToTakenAt: t0.Add(26 * time.Minute)})
	out := buf.String()
	for _, want := range []string{"[short_window]", "[weekend]"} {
		if !strings.Contains(out, want) {
			t.Errorf("console lacks %s:\n%s", want, out)
		}
	}
}

func TestCaveatCodes_WeekendMultiDay(t *testing.T) {
	at := func(m time.Month, day int) time.Time { return time.Date(2026, m, day, 0, 0, 0, 0, time.UTC) }
	for _, tc := range []struct {
		name     string
		from, to time.Time
		want     bool
	}{
		{"fri to mon", at(8, 21), at(8, 24), true},
		{"exactly half is not most", at(8, 21).Add(12 * time.Hour), at(8, 22).Add(12 * time.Hour), false},
		{"month boundary, sat to sun", at(10, 31), at(11, 1).Add(12 * time.Hour), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := codesFor(t, tc.from, tc.to)
			if got := slices.Contains(d.CaveatCodes, CaveatWeekend); got != tc.want {
				t.Errorf("weekend = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCaveatCodes_OneOffHeavySkipsAndNew(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)

	t.Run("a new shape with two heavy calls", func(t *testing.T) {
		d, err := DiffQueryStats(
			qSnap("primary", t0, qEntry("steady", "SELECT 1", 100, 100, 100)),
			qSnap("primary", t0.Add(time.Hour),
				qEntry("steady", "SELECT 1", 200, 200, 200),
				qEntry("alter", "ALTER TABLE t ADD COLUMN c int", 2, 9000, 0)),
		)
		if err != nil {
			t.Fatal(err)
		}
		if e := findEntry(t, d, "alter"); e.Status != QueryNew || !slices.Contains(e.CaveatCodes, CaveatOneOffHeavy) {
			t.Errorf("new heavy shape not flagged: %+v", e)
		}
	})

	t.Run("truncated baseline entry is neither flagged nor in the denominator", func(t *testing.T) {
		from := capped(qSnap("primary", t0, qEntry("steady", "SELECT 1", 100, 100, 100)))
		to := qSnap("primary", t0.Add(time.Hour),
			qEntry("steady", "SELECT 1", 200, 2100, 200),
			qEntry("cold", "SELECT 2", 2, 50000, 2))
		d, err := DiffQueryStats(from, to)
		if err != nil {
			t.Fatal(err)
		}
		if e := findEntry(t, d, "cold"); e.Status != QueryTruncated || len(e.CaveatCodes) != 0 {
			t.Errorf("truncated entry: %+v", e)
		}
		if slices.Contains(d.CaveatCodes, CaveatOneOffHeavy) {
			t.Errorf("flagged off a truncated cumulative counter: %v", d.CaveatCodes)
		}
	})

	t.Run("zero calls is not a few calls", func(t *testing.T) {
		d := &QueryDelta{TimeDelta: 5000, Entries: []QueryEntryDelta{
			{Fingerprint: "x", Status: QueryGrew, CallsDelta: 0, TimeDelta: 4000},
		}}
		if markOneOffHeavy(d) {
			t.Error("flagged an entry with no calls")
		}
	})

	t.Run("unmatched members make the delta a lower bound", func(t *testing.T) {
		d := &QueryDelta{TimeDelta: 5000, Entries: []QueryEntryDelta{
			{Fingerprint: "x", Status: QueryGrew, CallsDelta: 2, TimeDelta: 4000, UnmatchedMembers: 1},
		}}
		if markOneOffHeavy(d) {
			t.Error("flagged with unmatched members")
		}
	})
}

func TestRenderQueryConsole_TagsOneOffHeavyRow(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	d, err := DiffQueryStats(
		qSnap("primary", t0, qEntry("a", "UPDATE t SET x = 1", 10, 10, 10)),
		qSnap("primary", t0.Add(time.Hour), qEntry("a", "UPDATE t SET x = 1", 12, 6010, 12)),
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	RenderQueryConsole(&buf, &SnapshotDiff{Query: d, FromTakenAt: t0, ToTakenAt: t0.Add(time.Hour)})
	if !strings.Contains(buf.String(), "[one_off_heavy] UPDATE") {
		t.Errorf("row not tagged:\n%s", buf.String())
	}
}
