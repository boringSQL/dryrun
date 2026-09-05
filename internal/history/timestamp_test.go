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

// The bind side of the offset hazard: a RefAt/--since/--until value arrives
// in the caller's zone, and an un-normalised render shifts the lexical TEXT
// comparison by the offset — the 19:30Z row then sorts at-or-before a 19:00Z
// cutoff and wins the read.
func TestBind_OffsetCutoffComparesAsUTC(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")

	cest := time.FixedZone("CEST", 2*60*60)
	at := func(hour, minute int) time.Time {
		return time.Date(2026, 9, 3, hour, minute, 0, 0, time.UTC)
	}
	// the cutoff instant 19:00Z, supplied with a +02:00 offset
	cutoff := time.Date(2026, 9, 3, 21, 0, 0, 0, cest)

	cases := []struct {
		name          string
		kind          SnapshotKind
		before, after string
		seed          func(t *testing.T, s *Store, hash string, ts time.Time)
	}{
		{"schema", SchemaKind(), "s-before", "s-after", func(t *testing.T, s *Store, hash string, ts time.Time) {
			snap := testSnapshot(hash, "acme")
			snap.Timestamp = ts
			if _, err := s.PutSchema(ctx, k, snap); err != nil {
				t.Fatal(err)
			}
		}},
		{"planner", PlannerKind(), "p-before", "p-after", func(t *testing.T, s *Store, hash string, ts time.Time) {
			p := plannerFixture("sref-A", hash, "acme")
			p.Timestamp = ts
			if _, err := s.PutPlanner(ctx, k, p); err != nil {
				t.Fatal(err)
			}
		}},
		{"activity", ActivityKind("node-a"), "a-before", "a-after", func(t *testing.T, s *Store, hash string, ts time.Time) {
			a := activityFixture("sref-A", hash, "node-a", false)
			a.Node.Timestamp = ts
			if _, err := s.PutActivity(ctx, k, a); err != nil {
				t.Fatal(err)
			}
		}},
		{"query", QueryKind("node-a"), "q-before", "q-after", func(t *testing.T, s *Store, hash string, ts time.Time) {
			q := queryStatsFixture("sref-A", hash, "node-a")
			q.Node.Timestamp = ts
			if _, err := s.PutQueryStats(ctx, k, q); err != nil {
				t.Fatal(err)
			}
		}},
	}

	listHashes := func(list []SnapshotSummary) string {
		out := make([]string, len(list))
		for i, ss := range list {
			out[i] = ss.ContentHash
		}
		return strings.Join(out, ",")
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := testStore(t)
			tc.seed(t, store, tc.before, at(18, 0)) // before the cutoff
			tc.seed(t, store, tc.after, at(19, 30)) // past the cutoff

			got, err := store.Get(ctx, k, tc.kind, SnapshotRef{Kind: RefAt, At: cutoff})
			if err != nil {
				t.Fatalf("RefAt: %v", err)
			}
			if got.ContentHash() != tc.before {
				t.Errorf("RefAt = %s, want %s — an offset bind let a row past the cutoff win",
					got.ContentHash(), tc.before)
			}

			since := cutoff
			list, err := store.List(ctx, k, tc.kind, TimeRange{From: &since})
			if err != nil {
				t.Fatalf("since: %v", err)
			}
			if listHashes(list) != tc.after {
				t.Errorf("since = [%s], want [%s]", listHashes(list), tc.after)
			}

			until := cutoff
			list, err = store.List(ctx, k, tc.kind, TimeRange{To: &until})
			if err != nil {
				t.Fatalf("until: %v", err)
			}
			if listHashes(list) != tc.before {
				t.Errorf("until = [%s], want [%s]", listHashes(list), tc.before)
			}
		})
	}
}

// The destructive binds are the dangerous half of the site list: an
// un-normalised offset cutoff makes the lexical `timestamp < ?` eat rows
// younger than the cutoff instant. Seed a row on each side of the cutoff
// instant, delete with that instant supplied at +02:00, and only the older
// row may go.
func TestDelete_OffsetCutoffDeletesAsUTC(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")

	cest := time.FixedZone("CEST", 2*60*60)
	at := func(hour, minute int) time.Time {
		return time.Date(2026, 9, 3, hour, minute, 0, 0, time.UTC)
	}
	// the cutoff instant 19:00Z, supplied with a +02:00 offset
	cutoff := time.Date(2026, 9, 3, 21, 0, 0, 0, cest)

	cases := []struct {
		name          string
		kind          SnapshotKind
		before, after string
		seed          func(t *testing.T, s *Store, hash string, ts time.Time)
	}{
		{"schema", SchemaKind(), "s-before", "s-after", func(t *testing.T, s *Store, hash string, ts time.Time) {
			snap := testSnapshot(hash, "acme")
			snap.Timestamp = ts
			if _, err := s.PutSchema(ctx, k, snap); err != nil {
				t.Fatal(err)
			}
		}},
		{"planner", PlannerKind(), "p-before", "p-after", func(t *testing.T, s *Store, hash string, ts time.Time) {
			p := plannerFixture("sref-A", hash, "acme")
			p.Timestamp = ts
			if _, err := s.PutPlanner(ctx, k, p); err != nil {
				t.Fatal(err)
			}
		}},
		{"activity", ActivityKind("node-a"), "a-before", "a-after", func(t *testing.T, s *Store, hash string, ts time.Time) {
			a := activityFixture("sref-A", hash, "node-a", false)
			a.Node.Timestamp = ts
			if _, err := s.PutActivity(ctx, k, a); err != nil {
				t.Fatal(err)
			}
		}},
		{"query", QueryKind("node-a"), "q-before", "q-after", func(t *testing.T, s *Store, hash string, ts time.Time) {
			q := queryStatsFixture("sref-A", hash, "node-a")
			q.Node.Timestamp = ts
			if _, err := s.PutQueryStats(ctx, k, q); err != nil {
				t.Fatal(err)
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := testStore(t)
			tc.seed(t, store, tc.before, at(18, 0)) // before the cutoff
			tc.seed(t, store, tc.after, at(19, 30)) // past the cutoff

			n, err := store.DeleteBefore(ctx, k, tc.kind, cutoff)
			if err != nil {
				t.Fatalf("DeleteBefore: %v", err)
			}
			if n != 1 {
				t.Errorf("deleted = %d, want 1 — an offset cutoff ate a row younger than 19:00Z", n)
			}

			list, err := store.List(ctx, k, tc.kind, TimeRange{})
			if err != nil {
				t.Fatal(err)
			}
			if len(list) != 1 || list[0].ContentHash != tc.after {
				t.Errorf("survivors = %v, want only %s", list, tc.after)
			}
		})
	}
}

// Prune's keep-newest guard would mask a bad bind on a two-row seed (the
// young row survives regardless), so seed three and watch the middle one —
// younger than the cutoff instant but not the newest — which an offset
// cutoff rendered at +02:00 would wrongly prune.
func TestPrune_OffsetCutoffPrunesAsUTC(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")

	cest := time.FixedZone("CEST", 2*60*60)
	at := func(hour, minute int) time.Time {
		return time.Date(2026, 9, 3, hour, minute, 0, 0, time.UTC)
	}
	// the cutoff instant 19:00Z, supplied with a +02:00 offset
	cutoff := time.Date(2026, 9, 3, 21, 0, 0, 0, cest)

	store := testStore(t)
	for _, age := range []struct {
		hash string
		ts   time.Time
	}{
		{"old", at(18, 0)},  // pruned: older than the cutoff
		{"mid", at(19, 30)}, // kept: younger than the cutoff, must survive
		{"new", at(20, 0)},  // kept: newest per kind
	} {
		snap := testSnapshot("sch-"+age.hash, "acme")
		snap.Timestamp = age.ts
		if _, err := store.PutSchema(ctx, k, snap); err != nil {
			t.Fatal(err)
		}
		p := plannerFixture("sref-A", "p-"+age.hash, "acme")
		p.Timestamp = age.ts
		if _, err := store.PutPlanner(ctx, k, p); err != nil {
			t.Fatal(err)
		}
		a := activityFixture("sref-A", "a-"+age.hash, "node-a", false)
		a.Node.Timestamp = age.ts
		if _, err := store.PutActivity(ctx, k, a); err != nil {
			t.Fatal(err)
		}
		q := queryStatsFixture("sref-A", "q-"+age.hash, "node-a")
		q.Node.Timestamp = age.ts
		if _, err := store.PutQueryStats(ctx, k, q); err != nil {
			t.Fatal(err)
		}
	}

	res, err := store.Prune(ctx, k, PruneOptions{Cutoff: cutoff, KeepSchemas: 1, KeepPlanner: 1})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if res.Activity != 1 || res.Query != 1 || res.Planner != 1 || res.Schema != 1 {
		t.Errorf("pruned = %+v, want 1 per kind — an offset cutoff ate the 19:30Z rows", res)
	}
	if res.SchemaPinned != 0 {
		t.Errorf("pinned = %d, want 0 — no stats row references a schema hash here", res.SchemaPinned)
	}
}

// The upsert's WHERE compares attempted_at lexically inside SQL: an offset
// write of an *earlier* instant renders "20:00:00+02:00", which outsorts the
// stored "19:00:00Z" and shoves the clock backwards — holding the stream
// back past its interval, the exact failure the column exists to prevent.
func TestMarkCaptureAttempt_OffsetWriteKeepsNewestInstant(t *testing.T) {
	ctx := context.Background()
	k := key("acme", "primary")
	store := testStore(t)

	cest := time.FixedZone("CEST", 2*60*60)
	later := time.Date(2026, 9, 3, 19, 0, 0, 0, time.UTC) // 19:00Z
	earlier := time.Date(2026, 9, 3, 20, 0, 0, 0, cest)   // 18:00Z

	if err := store.MarkCaptureAttempt(ctx, k, "node-a", "activity", later); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkCaptureAttempt(ctx, k, "node-a", "activity", earlier); err != nil {
		t.Fatal(err)
	}

	got, ok, err := store.LastCaptureAttemptAt(ctx, k, "node-a", "activity")
	if err != nil || !ok {
		t.Fatalf("LastCaptureAttemptAt = %v, %v, %v", got, ok, err)
	}
	if !got.Equal(later) {
		t.Errorf("attempted_at = %v, want %v — an offset render outsorted a later instant", got, later)
	}

	var raw string
	if err := store.db.QueryRow(
		`SELECT attempted_at FROM capture_attempts
		  WHERE project_id = ? AND database_id = ? AND node_label = ? AND stream = ?`,
		string(k.ProjectID), string(k.DatabaseID), "node-a", "activity").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(raw, "Z") {
		t.Errorf("stored %q, want a UTC 'Z' suffix", raw)
	}
}
