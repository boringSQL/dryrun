package main

import (
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
)

// `--since` has to mean the same thing on `list` as it does on `pull`, which
// is why this goes through parseSince rather than a second parser.
func TestParseSince_ForList(t *testing.T) {
	t.Run("durations, days and weeks", func(t *testing.T) {
		for in, want := range map[string]time.Duration{
			"24h": 24 * time.Hour,
			"7d":  7 * 24 * time.Hour,
			"2w":  14 * 24 * time.Hour,
			"90m": 90 * time.Minute,
		} {
			from, err := parseSince(in)
			if err != nil {
				t.Fatalf("parseSince(%q): %v", in, err)
			}
			if got := time.Since(from).Round(time.Minute); got != want {
				t.Errorf("parseSince(%q) is %s ago, want %s", in, got, want)
			}
		}
	})

	// stored timestamps are UTC and compared as strings, so a local-zone bound
	// silently drops rows by the offset
	t.Run("bound is UTC", func(t *testing.T) {
		from, err := parseSince("24h")
		if err != nil {
			t.Fatal(err)
		}
		if from.Location() != time.UTC {
			t.Errorf("parseSince returned %s, want UTC", from.Location())
		}
		abs, err := parseSince("2026-08-01")
		if err != nil {
			t.Fatal(err)
		}
		if abs.Location() != time.UTC {
			t.Errorf("absolute date is %s, want UTC", abs.Location())
		}
	})

	t.Run("negative and nonsense are refused", func(t *testing.T) {
		for _, in := range []string{"-3d", "yesterday", "7dd", ""} {
			if _, err := parseSince(in); err == nil {
				t.Errorf("parseSince(%q) was accepted", in)
			}
		}
	})
}

func TestKindMatches(t *testing.T) {
	activity := history.ActivityKind("replica-1")
	query := history.QueryKind("replica-1")
	schemaKind := history.SchemaKind()

	cases := []struct {
		name       string
		kind       history.SnapshotKind
		kindFilter string
		nodeFilter string
		want       bool
	}{
		{"no filters", activity, "", "", true},
		{"kind matches", query, "query", "", true},
		{"kind excludes", activity, "query", "", false},
		{"kind is case-insensitive", query, "QUERY", "", true},
		{"node matches", query, "", "replica-1", true},
		{"node excludes", query, "", "replica-2", false},
		{"both", query, "query", "replica-1", true},
		// schema and planner carry no node, so a node filter excludes them
		{"node filter excludes nodeless kinds", schemaKind, "", "replica-1", false},
		{"schema passes its own kind filter", schemaKind, "schema", "", true},
		// a label may contain the separator the kind string uses
		{"label containing a colon", history.QueryKind("db:5432"), "query", "db:5432", true},
		{"unknown kind matches nothing", query, "bogus", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := kindMatches(tc.kind, tc.kindFilter, tc.nodeFilter); got != tc.want {
				t.Errorf("kindMatches(%s, %q, %q) = %t, want %t",
					tc.kind, tc.kindFilter, tc.nodeFilter, got, tc.want)
			}
		})
	}
}

// `capture --all` writes a whole fleet inside one second, and timestamps are
// second-resolution. Without a tiebreak, --limit returns a different slice on
// each run over identical data.
func TestSortAndLimit(t *testing.T) {
	at := time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)
	newer := at.Add(time.Hour)
	mk := func(k history.SnapshotKind, hash string, ts time.Time) history.SnapshotSummary {
		return history.SnapshotSummary{Kind: k, ContentHash: hash, Timestamp: ts}
	}

	t.Run("newest first", func(t *testing.T) {
		got, total := sortAndLimit([]history.SnapshotSummary{
			mk(history.SchemaKind(), "old", at),
			mk(history.SchemaKind(), "new", newer),
		}, 0)
		if total != 2 || got[0].ContentHash != "new" {
			t.Errorf("got %+v total=%d", got, total)
		}
	})

	t.Run("same-second ties are deterministic", func(t *testing.T) {
		in := []history.SnapshotSummary{
			mk(history.QueryKind("b"), "h2", at),
			mk(history.ActivityKind("a"), "h1", at),
			mk(history.QueryKind("a"), "h3", at),
		}
		first, _ := sortAndLimit(append([]history.SnapshotSummary(nil), in...), 0)
		// same input in another order must produce the same output order
		second, _ := sortAndLimit([]history.SnapshotSummary{in[2], in[0], in[1]}, 0)
		for i := range first {
			if first[i].ContentHash != second[i].ContentHash {
				t.Fatalf("order depends on input order: %v vs %v", hashes(first), hashes(second))
			}
		}
	})

	t.Run("limit is applied after sorting and reports the total", func(t *testing.T) {
		got, total := sortAndLimit([]history.SnapshotSummary{
			mk(history.SchemaKind(), "old", at),
			mk(history.SchemaKind(), "new", newer),
		}, 1)
		if total != 2 || len(got) != 1 || got[0].ContentHash != "new" {
			t.Errorf("got %v total=%d, want the newest of 2", hashes(got), total)
		}
	})
}

func hashes(in []history.SnapshotSummary) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = s.ContentHash
	}
	return out
}

// "nothing captured yet" and "your filter matched nothing" send the user to
// different places.
func TestEmptyListMessage(t *testing.T) {
	if got := emptyListMessage("", "", ""); !strings.Contains(got, "No snapshots found") {
		t.Errorf("unfiltered message is %q", got)
	}
	got := emptyListMessage("replica-1", "query", "7d")
	for _, want := range []string{"replica-1", "query", "7d"} {
		if !strings.Contains(got, want) {
			t.Errorf("filtered message %q omits %q", got, want)
		}
	}
}

// A single sighting cannot show rotation -- that needs two rows to compare --
// so it must not render as a confident "1".
func TestMembersCell(t *testing.T) {
	cases := []struct {
		name string
		in   history.NodeSummary
		want string
	}{
		{"no evidence", history.NodeSummary{Fingerprinted: 0}, "unknown"},
		{"one sighting", history.NodeSummary{Fingerprinted: 1, Members: 1}, "1?"},
		{"settled on one", history.NodeSummary{Fingerprinted: 9, Members: 1}, "1"},
		{"two, one after another", history.NodeSummary{Fingerprinted: 9, Members: 2}, "2"},
		{"rotating", history.NodeSummary{Fingerprinted: 9, Members: 2, Oscillating: true}, "2 (osc.)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := membersCell(tc.in); got != tc.want {
				t.Errorf("membersCell(%+v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
