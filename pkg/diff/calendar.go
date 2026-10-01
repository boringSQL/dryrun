package diff

import (
	"fmt"
	"strings"
	"time"
)

type (
	Calendar struct {
		weekend [7]bool
		loc     *time.Location
	}
)

// commands that diff set this once at startup; other importers get the default
var active = DefaultCalendar()

func DefaultCalendar() Calendar {
	return Calendar{weekend: [7]bool{time.Saturday: true, time.Sunday: true}, loc: time.UTC}
}

// weekend nil means Sat/Sun, an empty list means none; empty timezone means UTC
func ParseCalendar(weekend []string, tz string) (Calendar, error) {
	c := DefaultCalendar()
	if weekend != nil {
		c.weekend = [7]bool{}
		for _, name := range weekend {
			d, ok := parseWeekday(name)
			if !ok {
				return Calendar{}, fmt.Errorf("[calendar].weekend: unknown day %q (use mon..sun)", name)
			}
			c.weekend[d] = true
		}
	}
	if tz != "" {
		// "Local" would make the same captures flag differently per host
		if strings.EqualFold(tz, "local") {
			return Calendar{}, fmt.Errorf("[calendar].timezone: use an IANA name such as Europe/Prague, not %q", tz)
		}
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return Calendar{}, fmt.Errorf("[calendar].timezone: %w", err)
		}
		c.loc = loc
	}
	return c, nil
}

func SetCalendar(c Calendar) {
	if c.loc == nil {
		c.loc = time.UTC
	}
	active = c
}

func parseWeekday(name string) (time.Weekday, bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	for d := time.Sunday; d <= time.Saturday; d++ {
		full := strings.ToLower(d.String())
		if name == full || name == full[:3] {
			return d, true
		}
	}
	return 0, false
}

// "Sat/Sun, UTC", Monday first
func (c Calendar) String() string {
	var days []string
	for i := 1; i <= 7; i++ {
		if d := time.Weekday(i % 7); c.weekend[d] {
			days = append(days, d.String()[:3])
		}
	}
	if len(days) == 0 {
		return "no weekend, " + c.loc.String()
	}
	return strings.Join(days, "/") + ", " + c.loc.String()
}

// fraction of [start, end) that falls on a weekend day of the calendar's zone
func (c Calendar) weekendFraction(start, end time.Time) float64 {
	start, end = start.In(c.loc), end.In(c.loc)
	if !end.After(start) {
		return 0
	}
	var weekend time.Duration
	for t := start; t.Before(end); {
		next := time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, c.loc)
		// a zone that skips a midnight can normalise to the same instant
		if !next.After(t) {
			next = t.Add(time.Hour)
		}
		if next.After(end) {
			next = end
		}
		if c.weekend[t.Weekday()] {
			weekend += next.Sub(t)
		}
		t = next
	}
	return float64(weekend) / float64(end.Sub(start))
}
