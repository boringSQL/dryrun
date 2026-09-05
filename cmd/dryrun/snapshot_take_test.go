package main

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// snapshot take is primary-only: it must refuse a standby outright (no
// --allow-replica analogue) and on a primary it must write all three streams.
// Mirrors TestRunInitCapture_Branches but for the takeCmd path, since take
// and init share runPrimaryCapture but diverge in their standby gate.
func TestRunSnapshotTake_Branches(t *testing.T) {
	cases := []struct {
		name          string
		standby       bool
		wantErrKind   *dryrun.ErrorKind
		wantSchemaN   int
		wantPlannerN  int
		wantActivityN int
	}{
		{
			name:          "primary writes all three streams",
			standby:       false,
			wantSchemaN:   1,
			wantPlannerN:  1,
			wantActivityN: 1,
		},
		{
			name:        "standby is refused with no replica fallback",
			standby:     true,
			wantErrKind: ptrKind(dryrun.ErrReplicaCapture),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &stubCapturer{Standby: tc.standby}
			w := &stubWriter{}
			_, _, _, _, err := runSnapshotTake(context.Background(), cap, w,
				history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, nil, false, false)

			if tc.wantErrKind != nil {
				var derr *dryrun.Error
				if !errors.As(err, &derr) || derr.Kind != *tc.wantErrKind {
					t.Fatalf("want ErrorKind=%v, got err=%v", *tc.wantErrKind, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if w.SchemaN != tc.wantSchemaN {
				t.Errorf("schema puts=%d want=%d", w.SchemaN, tc.wantSchemaN)
			}
			if w.PlannerN != tc.wantPlannerN {
				t.Errorf("planner puts=%d want=%d", w.PlannerN, tc.wantPlannerN)
			}
			if w.ActivityN != tc.wantActivityN {
				t.Errorf("activity puts=%d want=%d", w.ActivityN, tc.wantActivityN)
			}
		})
	}
}

// The regression this test exists for: snapshot take grew to capture
// schema + planner + activity (commit 0577c01) but kept passing a nil
// policy into runPrimaryCapture, so planner stats landed in history.db
// unmasked. Asserts on the PlannerStatsSnapshot reaching PutPlanner that
// (a) a non-nil policy NULLs the matching column's value-bearing stats,
// (b) a nil policy leaves them intact (so masking stays strictly opt-in),
// (c) content_hash is recomputed after masking.
func TestRunSnapshotTake_Masking(t *testing.T) {
	cases := []struct {
		name       string
		policy     *masking.Policy
		wantMasked map[string]bool
	}{
		{
			name:       "nil policy leaves every column intact",
			policy:     nil,
			wantMasked: map[string]bool{"email": false, "id": false},
		},
		{
			name:       "policy nulls the matching column only",
			policy:     loadTestPolicy(t, "users.email"),
			wantMasked: map[string]bool{"email": true, "id": false},
		},
		{
			name:       "policy matching nothing leaves every column intact",
			policy:     loadTestPolicy(t, "orders.total"),
			wantMasked: map[string]bool{"email": false, "id": false},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &stubCapturer{
				PlannerColumns: []schema.ColumnStatsEntry{
					colWithStats("users", "email"),
					colWithStats("users", "id"),
				},
			}
			w := &stubWriter{}

			_, _, _, masked, err := runSnapshotTake(context.Background(), cap, w,
				history.SnapshotKey{ProjectID: "p", DatabaseID: "testdb"}, tc.policy, false, false)
			if err != nil {
				t.Fatalf("runSnapshotTake: %v", err)
			}
			if w.LastPlanner == nil {
				t.Fatal("PutPlanner never received a planner snapshot")
			}

			gotMasked := 0
			for _, c := range w.LastPlanner.Columns {
				m := c.Stats.MostCommonVals == nil &&
					c.Stats.MostCommonFreqs == nil &&
					c.Stats.HistogramBounds == nil
				want, known := tc.wantMasked[c.Column]
				if !known {
					t.Fatalf("unexpected column %q in snapshot", c.Column)
				}
				if m != want {
					t.Errorf("column %q: masked=%v, want %v "+
						"(mcv=%v freqs=%v hist=%v)",
						c.Column, m, want,
						c.Stats.MostCommonVals, c.Stats.MostCommonFreqs,
						c.Stats.HistogramBounds)
				}
				if m {
					gotMasked++
				}
			}
			if masked != gotMasked {
				t.Errorf("returned masked count=%d, observed=%d", masked, gotMasked)
			}

			if got, want := w.LastPlanner.ContentHash,
				schema.ComputePlannerContentHash(w.LastPlanner); got != want {
				t.Errorf("content_hash %q does not match the masked payload "+
					"hash %q — runSnapshotTake did not recompute it after masking",
					got, want)
			}
		})
	}
}

// take routes through captureStreams now, so it must inherit what that path
// provides and `take` never had: schema-first ordering (planner annotates
// against the hash this run wrote, not a stale one) and the local attempt
// clock, so a `capture --due` right after a take does not re-introspect.
func TestRunSnapshotTake_IsACaptureAlias(t *testing.T) {
	cap := &stubCapturer{}
	w := &stubWriter{}

	snap, planner, activity, _, err := runSnapshotTake(context.Background(), cap, w,
		history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, nil, false, false)
	if err != nil {
		t.Fatalf("runSnapshotTake: %v", err)
	}
	if snap == nil || planner == nil || activity == nil {
		t.Fatalf("take must return all three documents for its stdout summary")
	}
	if w.LastActivityRef != snap.ContentHash {
		t.Errorf("activity bound to %q, want the hash this run wrote (%q)",
			w.LastActivityRef, snap.ContentHash)
	}
	want := []string{"primary/schema", "primary/planner", "primary/activity"}
	if !slices.Equal(w.Attempts, want) {
		t.Errorf("attempt markers = %v, want %v", w.Attempts, want)
	}
}

// The point of guarding identity from cap.Identity() rather than from the
// introspected document: a stray DATABASE_URL is refused before take pays for
// a full introspection, which is the expensive half of the capture.
func TestRunSnapshotTake_IdentityGuard_RefusesBeforeIntrospect(t *testing.T) {
	cap := &stubCapturer{SystemID: "999", Database: "app"}
	w := &stubWriter{Stored: &schema.SchemaSnapshot{SystemIdentifier: "111", Database: "app"}}

	_, _, _, _, err := runSnapshotTake(context.Background(), cap, w,
		history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, nil, false, false)

	var de *dryrun.Error
	if !errors.As(err, &de) || de.Kind != dryrun.ErrIdentityMismatch {
		t.Fatalf("want ErrIdentityMismatch, got %v", err)
	}
	if cap.IntrospectN != 0 {
		t.Errorf("introspected %d times on a refused capture, want 0", cap.IntrospectN)
	}
}

// An unchanged schema is the common case, and it is the one the alias could
// have regressed: PutSchema dedups, captureStream turns that into
// errStreamUnchanged, and take must still report the stored snapshot rather
// than return a nil it would then dereference when printing.
func TestRunSnapshotTake_DedupedSchemaStillReports(t *testing.T) {
	cap := &stubCapturer{}
	w := &stubWriter{SchemaDedups: true}

	snap, planner, activity, _, err := runSnapshotTake(context.Background(), cap, w,
		history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, nil, false, false)
	if err != nil {
		t.Fatalf("runSnapshotTake: %v", err)
	}
	if snap == nil || snap.ContentHash == "" {
		t.Fatal("a deduped schema must still be reported: the content is in the store")
	}
	if planner == nil || activity == nil {
		t.Fatal("a deduped schema must not stop planner/activity")
	}
	if planner.SchemaRefHash != snap.ContentHash {
		t.Errorf("planner bound to %q, want the deduped hash %q", planner.SchemaRefHash, snap.ContentHash)
	}
}

// Writes used to warn and continue, so take printed "Snapshot saved" for a
// snapshot it had not saved. Through captureStreams a failed put fails the run.
func TestRunSnapshotTake_PutErrorFailsTheRun(t *testing.T) {
	cap := &stubCapturer{}
	w := &stubWriter{PutQueryStatsErr: nil, PutSchemaErr: errors.New("disk full")}

	_, _, _, _, err := runSnapshotTake(context.Background(), cap, w,
		history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, nil, false, false)
	if err == nil {
		t.Fatal("a failed PutSchema must fail take, not be warned about")
	}
	if cap.PlannerN != 0 {
		t.Errorf("planner ran after the schema write failed (%d calls)", cap.PlannerN)
	}
}
