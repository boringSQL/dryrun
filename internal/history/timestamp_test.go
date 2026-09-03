package history

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Every ordering and cutoff in this package compares `timestamp` lexically as
// TEXT, and '+' (0x2B) sorts before 'Z' — so one row stored with a non-UTC
// offset reorders the whole series and can hide the newest row from
// "latest reads" and retention alike. Local captures normalise upstream, but a
// pulled snapshot carries the remote host's offset all the way to the writer,
// which is the path these pin down.

func storedTimestamps(t *testing.T, s *Store, table string, k SnapshotKey) []string {
	t.Helper()
	rows, err := s.db.Query(
		"SELECT timestamp FROM "+table+" WHERE project_id = ? AND database_id = ?",
		string(k.ProjectID), string(k.DatabaseID))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

func TestPut_NormalisesTimestampToUTC(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")

	// the offset a pull from a CEST host carries in
	cest := time.FixedZone("CEST", 2*60*60)
	ts := time.Date(2026, 9, 3, 22, 25, 37, 0, cest)
	wantUTC := ts.UTC().Format(time.RFC3339)

	cases := []struct {
		name  string
		table string
		put   func(*Store)
	}{
		{"schema", "snapshots", func(s *Store) {
			snap := testSnapshot("h-offset", "acme")
			snap.Timestamp = ts
			if _, err := s.PutSchema(ctx, k, snap); err != nil {
				t.Fatal(err)
			}
		}},
		{"planner", "planner_stats", func(s *Store) {
			p := plannerFixture("sref-A", "ch-offset", "acme")
			p.Timestamp = ts
			if _, err := s.PutPlanner(ctx, k, p); err != nil {
				t.Fatal(err)
			}
		}},
		{"activity", "activity_stats", func(s *Store) {
			a := activityFixture("sref-A", "ch-offset", "node-a", false)
			a.Node.Timestamp = ts
			if _, err := s.PutActivity(ctx, k, a); err != nil {
				t.Fatal(err)
			}
		}},
		{"query", "query_stats", func(s *Store) {
			q := queryStatsFixture("sref-A", "ch-offset", "node-a")
			q.Node.Timestamp = ts
			if _, err := s.PutQueryStats(ctx, k, q); err != nil {
				t.Fatal(err)
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := testStore(t)
			tc.put(store)
			got := storedTimestamps(t, store, tc.table, k)
			if len(got) != 1 {
				t.Fatalf("%s rows = %d, want 1", tc.table, len(got))
			}
			if !strings.HasSuffix(got[0], "Z") {
				t.Errorf("%s stored %q, want a UTC 'Z' suffix", tc.table, got[0])
			}
			if got[0] != wantUTC {
				t.Errorf("%s stored %q, want %q", tc.table, got[0], wantUTC)
			}
		})
	}
}

// The failure this prevents: a 'Z' row and a '+02:00' row for the same instant
// sort in the wrong order, so "newest" reads pick the older one.
func TestPut_OffsetRowDoesNotOutsortUTCRow(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	k := key("acme", "primary")

	cest := time.FixedZone("CEST", 2*60*60)
	earlier := time.Date(2026, 9, 3, 20, 25, 37, 0, cest) // 18:25:37Z
	later := time.Date(2026, 9, 3, 19, 0, 0, 0, time.UTC) // 19:00:00Z

	older := testSnapshot("h-earlier", "acme")
	older.Timestamp = earlier
	newer := testSnapshot("h-later", "acme")
	newer.Timestamp = later
	if _, err := store.PutSchema(ctx, k, older); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSchema(ctx, k, newer); err != nil {
		t.Fatal(err)
	}

	got, err := store.Latest(ctx, k, SchemaKind())
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ContentHash != "h-later" {
		t.Fatalf("Latest = %v, want h-later — an unnormalised offset outsorted a UTC row", got)
	}
}
