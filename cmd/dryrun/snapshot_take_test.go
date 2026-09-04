package main

import (
	"context"
	"errors"
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
