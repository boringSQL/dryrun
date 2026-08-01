package mcp

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func trackPtr(s string) *string { return &s }

// snap builds a capture whose counters ran from reset until captureAt.
func snap(node string, reset, captureAt time.Time) schema.QueryStatsSnapshot {
	return schema.QueryStatsSnapshot{
		Node:       schema.NodeIdentity{Source: node, Timestamp: captureAt},
		InfoBefore: &schema.QueryStatsInfo{StatsReset: reset},
		InfoAfter:  &schema.QueryStatsInfo{StatsReset: reset},
	}
}

func hasBand(caveats []string, band string) bool {
	for _, c := range caveats {
		if strings.HasPrefix(c, band) {
			return true
		}
	}
	return false
}

func matching(caveats []string, substr string) []string {
	var out []string
	for _, c := range caveats {
		if strings.Contains(c, substr) {
			out = append(out, c)
		}
	}
	return out
}

// The incident this whole file exists to prevent: a replica accumulating for
// weeks ranked in one list against a primary reset hours ago. list_top_queries
// sorts across nodes, so unequal windows make that ordering meaningless.
func TestCrossNodeWindowCaveatFiresOnUnequalWindows(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	latest := []schema.QueryStatsSnapshot{
		snap("replica", now.Add(-663*time.Hour), now),
		snap("primary", now.Add(-11*time.Hour), now),
	}

	got := queryStatsCaveats(latest, nil)
	if !hasBand(got, "BLOCKING:") {
		t.Fatalf("unequal windows must produce a BLOCKING caveat, got %#v", got)
	}
	blocking := matching(got, "different counter windows")
	if len(blocking) != 1 {
		t.Fatalf("want exactly one cross-node caveat, got %#v", got)
	}
	// Both nodes and both spans must appear, or the reader cannot act on it.
	for _, want := range []string{"replica", "primary", "27d", "11h"} {
		if !strings.Contains(blocking[0], want) {
			t.Errorf("caveat missing %q: %s", want, blocking[0])
		}
	}
}

// Comparable windows are the normal case and must stay silent; an always-on
// warning is one nobody reads.
func TestCrossNodeWindowCaveatSilentWhenWindowsMatch(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	latest := []schema.QueryStatsSnapshot{
		snap("replica", now.Add(-100*time.Hour), now),
		snap("primary", now.Add(-100*time.Hour-30*time.Minute), now),
	}
	if c := crossNodeWindowCaveat(latest); c != "" {
		t.Errorf("30m of skew is under tolerance and must not warn: %s", c)
	}
	if got := queryStatsCaveats(latest, nil); len(got) != 0 {
		t.Errorf("nothing to say, want no caveats, got %#v", got)
	}
}

// Equal reset epochs are not enough: a stale capture of one node covers a
// different span than a fresh capture of another, and LatestQueryStats has no
// recency bound, so this is reachable simply by a node stopping.
func TestCrossNodeWindowCaveatCatchesStaleCaptureAtEqualEpochs(t *testing.T) {
	reset := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	latest := []schema.QueryStatsSnapshot{
		snap("primary", reset, reset.Add(30*24*time.Hour)),
		snap("replica", reset, reset.Add(2*time.Hour)), // capture stopped weeks ago
	}
	if c := crossNodeWindowCaveat(latest); c == "" {
		t.Error("identical reset epochs with capture times weeks apart are still incomparable")
	}
}

// A single node cannot be compared against anything, and filtering to one node
// must therefore suppress the caveat rather than warn about a ranking that
// cannot happen.
func TestCrossNodeWindowCaveatNeedsTwoKnownWindows(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	one := []schema.QueryStatsSnapshot{snap("primary", now.Add(-11*time.Hour), now)}
	if c := crossNodeWindowCaveat(one); c != "" {
		t.Errorf("single node must not warn about cross-node comparison: %s", c)
	}

	// A node whose window is unknown cannot contribute one either.
	unknown := schema.QueryStatsSnapshot{Node: schema.NodeIdentity{Source: "replica", Timestamp: now}}
	if c := crossNodeWindowCaveat(append(one, unknown)); c != "" {
		t.Errorf("an unknown window is not a second comparable window: %s", c)
	}
}

func TestNodeWindowRejectsUnusableTimestamps(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	cases := map[string]schema.QueryStatsSnapshot{
		"no info view (PG13)": {Node: schema.NodeIdentity{Source: "n", Timestamp: now}},
		"zero reset": {
			Node:      schema.NodeIdentity{Source: "n", Timestamp: now},
			InfoAfter: &schema.QueryStatsInfo{},
		},
		"zero capture time": {
			Node:      schema.NodeIdentity{Source: "n"},
			InfoAfter: &schema.QueryStatsInfo{StatsReset: now},
		},
		// clock skew between the two readings; a negative span is not a window
		"reset after capture": snap("n", now, now.Add(-time.Hour)),
	}
	for name, s := range cases {
		if _, ok := nodeWindow(s); ok {
			t.Errorf("%s: want no usable window", name)
		}
	}

	if d, ok := nodeWindow(snap("n", now.Add(-5*time.Hour), now)); !ok || d != 5*time.Hour {
		t.Errorf("window = %v (ok=%v), want 5h", d, ok)
	}
}

// track = 'none' records nothing, so a short list reads as an idle database.
func TestTrackNoneIsBlocking(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	s := snap("primary", now.Add(-100*time.Hour), now)
	s.PgssTrack = trackPtr("none")

	got := queryStatsCaveats([]schema.QueryStatsSnapshot{s}, nil)
	if len(matching(got, "track = 'none'")) != 1 {
		t.Fatalf("want a track='none' caveat, got %#v", got)
	}
	if !hasBand(got, "BLOCKING:") {
		t.Errorf("track='none' invalidates the obvious reading and must be BLOCKING: %#v", got)
	}
}

// The denominator only excludes nested time when pgss 1.9's split was
// captured. Claiming otherwise on an older pgss would state the opposite of
// the truth, so the wording turns on evidence that the split exists.
func TestTrackAllWordingFollowsTheNestedSplit(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)

	withSplit := snap("primary", now.Add(-100*time.Hour), now)
	withSplit.PgssTrack = trackPtr("all")
	withSplit.Queries = []schema.QueryStatsEntry{{TotalExecTimeMs: 10, NestedExecTimeMs: 4}}
	if c := trackAllCaveat("primary", withSplit); !strings.Contains(c, "top-level time only") {
		t.Errorf("with a split present, say the denominator excludes nested time: %s", c)
	}

	// NestedCalls alone proves the split was captured: a nested statement can
	// be too cheap to accumulate a whole millisecond.
	cheapNested := snap("primary", now.Add(-100*time.Hour), now)
	cheapNested.PgssTrack = trackPtr("all")
	cheapNested.Queries = []schema.QueryStatsEntry{{TotalExecTimeMs: 10, NestedCalls: 3}}
	if c := trackAllCaveat("primary", cheapNested); !strings.Contains(c, "top-level time only") {
		t.Errorf("nested calls with zero accrued time still prove the split: %s", c)
	}

	noSplit := snap("primary", now.Add(-100*time.Hour), now)
	noSplit.PgssTrack = trackPtr("all")
	noSplit.Queries = []schema.QueryStatsEntry{{TotalExecTimeMs: 10}}
	c := trackAllCaveat("primary", noSplit)
	if strings.Contains(c, "top-level time only") {
		t.Errorf("without a split the denominator claim is false and must not be made: %s", c)
	}
	if !strings.Contains(c, "no top-level split") {
		t.Errorf("say the split is absent: %s", c)
	}
}

// Only the immediately-previous capture is compared, and each moved setting
// changes what the counters mean rather than what the workload did.
func TestChangeCaveatsReportMovedSettings(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	from := snap("primary", now.Add(-200*time.Hour), now.Add(-time.Hour))
	to := snap("primary", now.Add(-200*time.Hour), now)

	from.PgssTrack, to.PgssTrack = trackPtr("top"), trackPtr("all")
	from.QshapeVersion, to.QshapeVersion = 1, 2
	to.InfoAfter = &schema.QueryStatsInfo{StatsReset: now.Add(-30 * time.Minute)}

	got := changeCaveats("primary", from, to)
	if len(got) != 3 {
		t.Fatalf("want caveats for track, reset and qshape, got %#v", got)
	}
	for _, want := range []string{"track changed", "were reset at", "regrouped"} {
		if len(matching(got, want)) != 1 {
			t.Errorf("missing a caveat for %q: %#v", want, got)
		}
	}
	// Scoped to the previous capture, not to every earlier response.
	for _, c := range got {
		if !strings.Contains(c, "previous capture") {
			t.Errorf("caveat overpromises its range: %s", c)
		}
	}
}

// Version 0 means unversioned, not "same as" — and it is the least trustworthy
// boundary, so it must warn rather than be skipped.
func TestChangeCaveatsWarnOnUnversionedQshapeBoundary(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	from := snap("primary", now.Add(-200*time.Hour), now.Add(-time.Hour))
	to := snap("primary", now.Add(-200*time.Hour), now)
	from.QshapeVersion, to.QshapeVersion = 0, 2

	got := changeCaveats("primary", from, to)
	if len(matching(got, "predates qshape versioning")) != 1 {
		t.Fatalf("a 0 -> N transition must warn, got %#v", got)
	}

	// Equal versions say nothing, including 0 on both sides.
	from.QshapeVersion, to.QshapeVersion = 0, 0
	if got := changeCaveats("primary", from, to); len(got) != 0 {
		t.Errorf("unchanged version must be silent, got %#v", got)
	}
}

// Only a window too short to represent normal load is worth interrupting for;
// the reset epoch alone is metadata the tool description already covers.
func TestYoungCountersWarnButOldOnesDoNot(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)

	young := queryStatsCaveats([]schema.QueryStatsSnapshot{snap("primary", now.Add(-2*time.Hour), now)}, nil)
	if len(matching(young, "cover only")) != 1 {
		t.Errorf("a 2h window is too short to stand in for steady state: %#v", young)
	}

	old := queryStatsCaveats([]schema.QueryStatsSnapshot{snap("primary", now.Add(-200*time.Hour), now)}, nil)
	if len(old) != 0 {
		t.Errorf("an established window needs no caveat, got %#v", old)
	}
}

// fetchPgssInfo returns nil independently for each probe, so InfoAfter can be
// nil on PG14 too. Silence there would read as "nothing to qualify" when it
// means "we cannot tell".
func TestUnknownEpochIsReportedNotSilent(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)
	cases := map[string]schema.QueryStatsSnapshot{
		"no info view at all": {Node: schema.NodeIdentity{Source: "pg13", Timestamp: now}},
		"second probe failed": {
			Node:       schema.NodeIdentity{Source: "partial", Timestamp: now},
			InfoBefore: &schema.QueryStatsInfo{StatsReset: now.Add(-time.Hour)},
		},
	}
	for name, s := range cases {
		got := queryStatsCaveats([]schema.QueryStatsSnapshot{s}, nil)
		if len(matching(got, "unknown for")) != 1 {
			t.Errorf("%s: want an explicit unknown-epoch caveat, got %#v", name, got)
		}
	}
}

// A capture inconsistent with itself cannot be reasoned about at all.
func TestStraddledCaptureIsBlocking(t *testing.T) {
	now := time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)

	reset := snap("primary", now.Add(-100*time.Hour), now)
	reset.InfoAfter = &schema.QueryStatsInfo{StatsReset: now.Add(-time.Minute)}
	if got := queryStatsCaveats([]schema.QueryStatsSnapshot{reset}, nil); len(matching(got, "was reset while")) != 1 {
		t.Errorf("a reset mid-capture must be reported, got %#v", got)
	}

	evict := snap("primary", now.Add(-100*time.Hour), now)
	evict.InfoBefore.Dealloc, evict.InfoAfter.Dealloc = 1, 4
	got := queryStatsCaveats([]schema.QueryStatsSnapshot{evict}, nil)
	if len(matching(got, "evicted entries while")) != 1 {
		t.Errorf("eviction mid-capture makes the rows inconsistent, got %#v", got)
	}
	if !hasBand(got, "BLOCKING:") {
		t.Errorf("want BLOCKING band, got %#v", got)
	}
}

func TestBandOrderingPutsBlockingFirst(t *testing.T) {
	got := band([]string{"b"}, []string{"c"}, []string{"s"})
	want := []string{"BLOCKING: b", "COMPARABILITY: c", "SCOPE: s"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("band order = %#v, want %#v", got, want)
		}
	}
}

// The cap must not push a BLOCKING item out in favour of a SCOPE one, and the
// omission note counts against the ceiling rather than exceeding it.
func TestBandCapKeepsCeilingAndSeverity(t *testing.T) {
	var blocking, scope []string
	for i := 0; i < maxCaveats; i++ {
		blocking = append(blocking, "b")
		scope = append(scope, "s")
	}

	got := band(blocking, nil, scope)
	if len(got) > maxCaveats {
		t.Errorf("emitted %d lines, want at most %d", len(got), maxCaveats)
	}
	if !strings.Contains(got[len(got)-1], "omitted") {
		t.Errorf("truncation must be disclosed, got %#v", got)
	}
	for _, c := range got[:len(got)-1] {
		if !strings.HasPrefix(c, "BLOCKING:") {
			t.Errorf("severity order must survive truncation, got %#v", got)
		}
	}
	if got := band(nil, nil, []string{"s"}); len(got) != 1 || strings.Contains(got[0], "omitted") {
		t.Errorf("under the cap nothing is dropped, got %#v", got)
	}
}

func TestHumanDurationBuckets(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{90 * time.Minute, "90m"},
		{5 * time.Hour, "5h"},
		{47 * time.Hour, "47h"},
		{27 * 24 * time.Hour, "27d"},
	}
	for _, c := range cases {
		if got := humanDuration(c.in); got != c.want {
			t.Errorf("humanDuration(%v) = %s, want %s", c.in, got, c.want)
		}
	}
}

// wrapText prefixes only the first line, so continuations need their own
// marker or the second caveat onwards renders unquoted.
func TestJoinCaveatsMarksContinuationLines(t *testing.T) {
	got := joinCaveats([]string{"BLOCKING: a", "SCOPE: b"})
	if got != "BLOCKING: a\n> SCOPE: b" {
		t.Errorf("joinCaveats = %q", got)
	}
	if joinCaveats(nil) != "" {
		t.Error("no caveats must join to the empty hint, which callers treat as absent")
	}
}
