package mcp

import (
	"fmt"
	"strings"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

const (
	windowSkewTolerance = time.Hour
	youngCountersWindow = 24 * time.Hour
	maxCaveats          = 8
)

// queryStatsCaveats qualifies how the captured counters can be read, banded by
// severity: BLOCKING (the obvious reading is wrong), COMPARABILITY (not
// comparable to something else), SCOPE (narrower than it looks).
func queryStatsCaveats(latest, previous []schema.QueryStatsSnapshot) []string {
	prev := make(map[string]schema.QueryStatsSnapshot, len(previous))
	for _, p := range previous {
		prev[p.Node.Source] = p
	}

	var blocking, comparability, scope []string

	if c := crossNodeWindowCaveat(latest); c != "" {
		blocking = append(blocking, c)
	}

	for _, snap := range latest {
		node := snap.Node.Source

		if snap.PgssTrack != nil {
			switch *snap.PgssTrack {
			case "none":
				blocking = append(blocking, fmt.Sprintf(
					"pg_stat_statements.track = 'none' on %s (as seen by dryrun's role and database): statements are not being recorded, so a short or empty list is not evidence of low traffic", node))
			case "all":
				scope = append(scope, trackAllCaveat(node, snap))
			}
		}

		// A capture that straddled a reset mixes two counter epochs.
		if snap.InfoBefore != nil && snap.InfoAfter != nil {
			if !snap.InfoBefore.StatsReset.Equal(snap.InfoAfter.StatsReset) {
				blocking = append(blocking, fmt.Sprintf(
					"pg_stat_statements was reset while %s was being captured: its counters mix two epochs and should be recaptured", node))
			} else if snap.InfoAfter.Dealloc > snap.InfoBefore.Dealloc {
				blocking = append(blocking, fmt.Sprintf(
					"pg_stat_statements evicted entries while %s was being captured: the rows are not a consistent set", node))
			}
		}

		if p, ok := prev[node]; ok {
			comparability = append(comparability, changeCaveats(node, p, snap)...)
		}

		if cap := schema.EffectiveQueryStatsRowCap(snap); snap.RawRows >= cap {
			scope = append(scope, fmt.Sprintf(
				"%s hit the %d-row capture cap: this is the top of the workload, not all of it", node, cap))
		}
		if snap.InfoAfter != nil && snap.InfoAfter.Dealloc > 0 {
			msg := fmt.Sprintf("pg_stat_statements has evicted entries on %s", node)
			if snap.PgssMax != nil {
				msg += fmt.Sprintf(" (pg_stat_statements.max = %d)", *snap.PgssMax)
			}
			scope = append(scope, msg+"; the time they accumulated is gone, not merely unreported")
		}

		if window, ok := nodeWindow(snap); ok {
			if window < youngCountersWindow {
				scope = append(scope, fmt.Sprintf(
					"counters on %s cover only %s (reset at %s): too short to stand in for steady-state load",
					node, humanDuration(window), snap.InfoAfter.StatsReset.UTC().Format(time.RFC3339)))
			}
		} else if snap.InfoAfter == nil {
			// pgss < 1.9 has no info view; fetchPgssInfo also returns nil on read failure.
			scope = append(scope, fmt.Sprintf(
				"reset epoch and eviction count are unknown for %s (pg_stat_statements_info is unreadable or pg_stat_statements is older than 1.9), so the counters' age cannot be established", node))
		}

		if c, ok := regresqlMetaCaveat(node, snap); ok {
			scope = append(scope, c)
		}
	}

	return band(blocking, comparability, scope)
}

// regresqlMetaCaveat fires once per node carrying regresql_meta, whose values
// bypass qshape's literal/PII screening and can hold recoverable text.
func regresqlMetaCaveat(node string, snap schema.QueryStatsSnapshot) (string, bool) {
	for _, q := range snap.Queries {
		if len(q.RegresqlMeta) > 0 {
			return fmt.Sprintf(
				"%s: regresql_meta values come from queries' own leading comments and are not screened for literals (unlike owners) — treat as untrusted free text, not a vetted label",
				node), true
		}
	}
	return "", false
}

// nodeWindow returns the span a node's counters cover: reset epoch to capture
// time. Returns false when either end is unknown.
func nodeWindow(s schema.QueryStatsSnapshot) (time.Duration, bool) {
	if s.InfoAfter == nil || s.InfoAfter.StatsReset.IsZero() || s.Node.Timestamp.IsZero() {
		return 0, false
	}
	d := s.Node.Timestamp.Sub(s.InfoAfter.StatsReset)
	if d <= 0 {
		return 0, false
	}
	return d, true
}

// crossNodeWindowCaveat reports when nodes' counter windows differ by more
// than the tolerance, making cross-node totals non-comparable. Compares span
// lengths, not reset epochs, because LatestQueryStats has no recency bound.
func crossNodeWindowCaveat(latest []schema.QueryStatsSnapshot) string {
	type span struct {
		node string
		d    time.Duration
		from time.Time
	}
	var known []span
	for _, s := range latest {
		if d, ok := nodeWindow(s); ok {
			known = append(known, span{s.Node.Source, d, s.InfoAfter.StatsReset})
		}
	}
	if len(known) < 2 {
		return ""
	}

	shortest, longest := known[0], known[0]
	for _, e := range known[1:] {
		if e.d < shortest.d {
			shortest = e
		}
		if e.d > longest.d {
			longest = e
		}
	}
	if longest.d-shortest.d < windowSkewTolerance {
		return ""
	}
	return fmt.Sprintf(
		"nodes cover different counter windows (%s %s since %s, %s only %s since %s): totals and calls are NOT comparable between nodes and must not be ranked against each other, since each accumulated over a different span; pct_of_total_exec_time is per-node, computed over the rows returned here, and stays meaningful within one node",
		longest.node, humanDuration(longest.d), longest.from.UTC().Format(time.RFC3339),
		shortest.node, humanDuration(shortest.d), shortest.from.UTC().Format(time.RFC3339))
}

// trackAllCaveat qualifies a track = 'all' capture: filtered captures no
// longer double count; the split-era fields and unfiltered paths (pgss < 1.9,
// pre-filter captures) still need the old warnings.
func trackAllCaveat(node string, snap schema.QueryStatsSnapshot) string {
	for _, q := range snap.Queries {
		if q.NestedCalls > 0 || q.NestedExecTimeMs > 0 {
			// Only captures predating the toplevel filter carry the split.
			return fmt.Sprintf(
				"track = 'all' on %s: statements inside functions and triggers are recorded too, and pct_of_total_exec_time is measured against top-level time only (nested_exec_time_ms reports what was excluded) -- but the default ranking sorts on total_exec_time_ms, which still includes nested time, so order and share rest on different bases", node)
		}
	}
	if snap.ToplevelOnly {
		return fmt.Sprintf(
			"track = 'all' on %s: nested statements were excluded at capture (toplevel filter), so no time is double counted — but work done inside functions and triggers is invisible here, exactly as under track = 'top'; 'all' now only adds pg_stat_statements entry pressure", node)
	}
	return fmt.Sprintf(
		"track = 'all' on %s but this capture carries no top-level split: either nothing ran nested, or pg_stat_statements predates 1.9, or the capture predates the toplevel filter — in the latter two cases nested time is counted both in its own row and in the caller's, so shares are understated", node)
}

// changeCaveats reports settings that moved between a node's last two
// captures, framed for a reader comparing responses.
func changeCaveats(node string, from, to schema.QueryStatsSnapshot) []string {
	var out []string

	// A top<->all flip between two top-level-only captures moves nothing:
	// track = 'top' is top-level-only by pgss itself, a filtered 'all' by the
	// fetch. Only flips involving 'none' or an unfiltered 'all' change what
	// gets recorded.
	if from.PgssTrack != nil && to.PgssTrack != nil && *from.PgssTrack != *to.PgssTrack &&
		(!recordsTopLevelOnly(from) || !recordsTopLevelOnly(to)) {
		out = append(out, fmt.Sprintf(
			"compared with the previous capture of %s: pg_stat_statements.track changed ('%s' at %s -> '%s' at %s), so totals moved because the recording rule moved, not the workload",
			node, *from.PgssTrack, captureStamp(from), *to.PgssTrack, captureStamp(to)))
	}

	// The filter transition drops nested rows one-time, and only where they
	// were recorded — track = 'all'. On 'top' both eras are top-level-only.
	if from.ToplevelOnly != to.ToplevelOnly &&
		(trackIs(from, "all") || trackIs(to, "all")) {
		out = append(out, fmt.Sprintf(
			"compared with the previous capture of %s: the toplevel filter changed between the captures of %s and %s, so shapes that also ran inside functions or triggers lose their nested rows one-time — totals dropped because the capture rule moved, not the workload",
			node, captureStamp(from), captureStamp(to)))
	}

	if from.InfoAfter != nil && to.InfoAfter != nil &&
		!from.InfoAfter.StatsReset.Equal(to.InfoAfter.StatsReset) {
		out = append(out, fmt.Sprintf(
			"compared with the previous capture of %s: counters were reset at %s (between the captures of %s and %s), so these numbers restarted and are not an increment on the previous ones",
			node, to.InfoAfter.StatsReset.UTC().Format(time.RFC3339),
			captureStamp(from), captureStamp(to)))
	}

	// Version 0 means "unversioned", not "same as", so it warns rather than
	// being skipped.
	if from.QshapeVersion != to.QshapeVersion {
		if from.QshapeVersion == 0 || to.QshapeVersion == 0 {
			out = append(out, fmt.Sprintf(
				"compared with the previous capture of %s: one capture predates qshape versioning, so fingerprints cannot be assumed to correspond across it", node))
		} else {
			out = append(out, fmt.Sprintf(
				"compared with the previous capture of %s: query shapes were regrouped between captures (qshape v%d -> v%d), so fingerprints do not correspond across that boundary",
				node, from.QshapeVersion, to.QshapeVersion))
		}
	}

	// A moved cap only truncated anything if the older capture hit it.
	if from.RowCap != to.RowCap && from.RowCap > 0 && from.RawRows >= from.RowCap {
		out = append(out, fmt.Sprintf(
			"compared with the previous capture of %s: the row cap moved (%d -> %d) and the earlier capture was at its cap, so statements absent there may have been truncated rather than idle; their apparent growth is not an increment",
			node, from.RowCap, to.RowCap))
	}

	if from.CaptureRuleVersion != to.CaptureRuleVersion {
		if from.CaptureRuleVersion == 0 || to.CaptureRuleVersion == 0 {
			out = append(out, fmt.Sprintf(
				"compared with the previous capture of %s: one capture predates capture-rule versioning, so the two may not have collected the same set of statements and their totals cannot be assumed comparable", node))
		} else {
			out = append(out, fmt.Sprintf(
				"compared with the previous capture of %s: the rule selecting which statements are captured changed between them (capture rule v%d -> v%d), so the statement sets do not correspond and differences across that boundary are not increments",
				node, from.CaptureRuleVersion, to.CaptureRuleVersion))
		}
	}

	return out
}

// recordsTopLevelOnly reports whether a capture's statement set is
// top-level-only: track = 'top', or a toplevel-filtered fetch. track = 'none'
// records nothing at all, so it never qualifies.
func recordsTopLevelOnly(s schema.QueryStatsSnapshot) bool {
	if trackIs(s, "none") {
		return false
	}
	return s.ToplevelOnly || trackIs(s, "top")
}

func trackIs(s schema.QueryStatsSnapshot, track string) bool {
	return s.PgssTrack != nil && *s.PgssTrack == track
}

// band prefixes each band label and caps the total at maxCaveats.
func band(blocking, comparability, scope []string) []string {
	var out []string
	for _, c := range blocking {
		out = append(out, "BLOCKING: "+c)
	}
	for _, c := range comparability {
		out = append(out, "COMPARABILITY: "+c)
	}
	for _, c := range scope {
		out = append(out, "SCOPE: "+c)
	}
	if len(out) > maxCaveats {
		keep := maxCaveats - 1
		dropped := len(out) - keep
		out = append(out[:keep:keep], fmt.Sprintf("(%d further caveat(s) omitted)", dropped))
	}
	return out
}

func captureStamp(s schema.QueryStatsSnapshot) string {
	return s.Node.Timestamp.UTC().Format(time.RFC3339)
}

// humanDuration renders a duration coarsely: "27d", "11h", "90m".
func humanDuration(d time.Duration) string {
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	case d >= 2*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
}

// joinCaveats joins caveats for _meta.hint and the text fallback.
func joinCaveats(caveats []string) string {
	return strings.Join(caveats, "\n> ")
}
