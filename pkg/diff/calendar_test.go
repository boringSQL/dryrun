package diff

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func useCalendar(t *testing.T, c Calendar) {
	t.Helper()
	SetCalendar(c)
	t.Cleanup(func() { SetCalendar(DefaultCalendar()) })
}

func mustCalendar(t *testing.T, weekend []string, tz string) Calendar {
	t.Helper()
	c, err := ParseCalendar(weekend, tz)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestParseCalendar(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weekend []string
		tz      string
		want    string
	}{
		{"defaults", nil, "", "Sat/Sun, UTC"},
		{"short and full names, any case", []string{"FRI", "Saturday"}, "", "Fri/Sat, UTC"},
		{"empty list is no weekend", []string{}, "", "no weekend, UTC"},
		{"duplicates collapse", []string{"sun", "sun"}, "", "Sun, UTC"},
		{"zone", []string{"fri", "sat"}, "Asia/Riyadh", "Fri/Sat, Asia/Riyadh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustCalendar(t, tc.weekend, tc.tz).String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		name    string
		weekend []string
		tz      string
	}{
		{"unknown day", []string{"funday"}, ""},
		{"unknown zone", nil, "Mars/Olympus"},
		{"Local is not reproducible", nil, "Local"},
		{"local in any case", nil, "LOCAL"},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			if _, err := ParseCalendar(tc.weekend, tc.tz); err == nil {
				t.Error("want an error")
			}
		})
	}
}

func TestCalendar_WeekendFraction(t *testing.T) {
	at := func(m time.Month, d, h int) time.Time { return time.Date(2026, m, d, h, 0, 0, 0, time.UTC) }
	// 2026-08-21 is a Friday

	t.Run("fri/sat weekend flags friday, not sunday", func(t *testing.T) {
		c := mustCalendar(t, []string{"fri", "sat"}, "")
		if f := c.weekendFraction(at(8, 21, 9), at(8, 21, 11)); f != 1 {
			t.Errorf("friday = %v, want 1", f)
		}
		if f := c.weekendFraction(at(8, 23, 9), at(8, 23, 11)); f != 0 {
			t.Errorf("sunday = %v, want 0", f)
		}
	})

	t.Run("no weekend never flags", func(t *testing.T) {
		if f := mustCalendar(t, []string{}, "").weekendFraction(at(8, 22, 0), at(8, 24, 0)); f != 0 {
			t.Errorf("got %v, want 0", f)
		}
	})

	t.Run("a three-day weekend over a long window", func(t *testing.T) {
		c := mustCalendar(t, []string{"fri", "sat", "sun"}, "")
		// Fri 21st -> Mon 31st: 6 of 10 days
		if f := c.weekendFraction(at(8, 21, 0), at(8, 31, 0)); f < 0.59 || f > 0.61 {
			t.Errorf("got %v, want 0.6", f)
		}
	})

	t.Run("a day starts in the calendar's zone", func(t *testing.T) {
		// Thursday 22:00 UTC is Friday 01:00 in Riyadh (UTC+3)
		riyadh := mustCalendar(t, []string{"fri", "sat"}, "Asia/Riyadh")
		utc := mustCalendar(t, []string{"fri", "sat"}, "")
		if f := riyadh.weekendFraction(at(8, 20, 22), at(8, 20, 23)); f != 1 {
			t.Errorf("riyadh = %v, want 1", f)
		}
		if f := utc.weekendFraction(at(8, 20, 22), at(8, 20, 23)); f != 0 {
			t.Errorf("utc = %v, want 0", f)
		}
	})

	t.Run("a window crossing local midnight splits there", func(t *testing.T) {
		// Sat 20:00-22:00 UTC is Sat 23:00 - Sun 01:00 in Riyadh: one hour of
		// Saturday (weekend) and one of Sunday (not)
		c := mustCalendar(t, []string{"fri", "sat"}, "Asia/Riyadh")
		if f := c.weekendFraction(at(8, 22, 20), at(8, 22, 22)); f != 0.5 {
			t.Errorf("got %v, want 0.5", f)
		}
	})

	// every day a weekend: the fraction is 1 only if the day loop covers the
	// span exactly, which DST days and skipped midnights make hard
	all := []string{"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
	for _, tc := range []struct {
		name     string
		tz       string
		from, to time.Time
	}{
		{"DST change", "America/Santiago", at(9, 4, 0), at(9, 9, 0)},
		{"midnight that does not exist", "America/Sao_Paulo", time.Date(2018, 11, 3, 0, 0, 0, 0, time.UTC), time.Date(2018, 11, 6, 0, 0, 0, 0, time.UTC)},
		{"a skipped calendar day", "Pacific/Apia", time.Date(2011, 12, 28, 0, 0, 0, 0, time.UTC), time.Date(2012, 1, 2, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if f := mustCalendar(t, all, tc.tz).weekendFraction(tc.from, tc.to); f < 0.999999 || f > 1.000001 {
				t.Errorf("got %v, want 1", f)
			}
		})
	}
}

func TestSetCalendar_ZeroValueFallsBackToUTC(t *testing.T) {
	useCalendar(t, Calendar{})
	// would panic on a nil location
	d := codesFor(t, time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC), time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC))
	if slices.Contains(d.CaveatCodes, CaveatWeekend) {
		t.Error("an empty calendar has no weekend")
	}
}

func TestDiffQueryStats_UsesConfiguredCalendar(t *testing.T) {
	useCalendar(t, mustCalendar(t, []string{"fri", "sat"}, ""))
	fri := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	sun := fri.AddDate(0, 0, 2)

	if d := codesFor(t, fri, fri.Add(time.Hour)); !slices.Contains(d.CaveatCodes, CaveatWeekend) {
		t.Errorf("friday not flagged: %v", d.CaveatCodes)
	}
	if d := codesFor(t, sun, sun.Add(time.Hour)); slices.Contains(d.CaveatCodes, CaveatWeekend) {
		t.Errorf("sunday flagged: %v", d.CaveatCodes)
	}
	if note := CaveatNote(CaveatWeekend); !strings.Contains(note, "Fri/Sat, UTC") {
		t.Errorf("note does not name the calendar: %s", note)
	}
}
