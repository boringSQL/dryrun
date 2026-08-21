package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// Pins the contracts `snapshot activity` promises as of v0.16.
//
// Before v0.16 the command refused to run against a primary at all. That gate
// is gone: pg_stat_user_tables is per-node and just as meaningful on a primary,
// and refusing meant the only way to refresh a primary's activity counters was
// to re-run `snapshot take` and rewrite the whole schema snapshot with it.
//
// Removing the gate widened the entry point, so a role guard landed in the same
// change and is exercised here alongside the older orphan-binding contracts:
//
//   - either role captures, and the row binds to the newest schema snapshot's
//     content_hash so the stats-apply join is satisfied;
//   - a node with no prior schema snapshot is refused unless --allow-orphan,
//     in which case the row is written unbound;
//   - a label whose newest stored row recorded the OTHER role is refused with
//     ErrNodeRoleChanged, because that usually means --label is aimed at a
//     rotating endpoint and two physical nodes' cumulative counters are about
//     to append into one series;
//   - --allow-role-change accepts it, so a genuine promotion is never a dead
//     end;
//   - an UNKNOWN prior role (no rows yet, or a legacy row with no recorded
//     role) stays silent rather than guessing "primary" and refusing a valid
//     capture.
func TestRunSnapshotActivity_Branches(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	cases := []struct {
		name        string
		standby     bool
		stored      *schema.SchemaSnapshot
		allowOrphan bool
		// prevRole is what the store reports for this label's newest row;
		// NodeRoleUnknown ("") stands for "no rows yet".
		prevRole    string
		prevRoleErr error
		allowRole   bool

		wantErrKind     *dryrun.ErrorKind
		wantErrContains string
		wantActivityN   int
		wantBoundRef    string
	}{
		{
			// the v0.16 change: this case would have failed with
			// ErrReplicaCapture before the gate came out
			name:          "primary captures activity, bound like a standby",
			standby:       false,
			stored:        &schema.SchemaSnapshot{ContentHash: "primary-schema-xyz"},
			wantActivityN: 1,
			wantBoundRef:  "primary-schema-xyz",
		},
		{
			name:          "standby whose label was already a standby is unaffected",
			standby:       true,
			prevRole:      history.NodeRoleStandby,
			stored:        &schema.SchemaSnapshot{ContentHash: "h"},
			wantActivityN: 1,
			wantBoundRef:  "h",
		},
		{
			// the failure the guard exists for: one label, two physical nodes
			name:        "label recorded as standby, now a primary, is refused",
			standby:     false,
			prevRole:    history.NodeRoleStandby,
			stored:      &schema.SchemaSnapshot{ContentHash: "h"},
			wantErrKind: ptrKind(dryrun.ErrNodeRoleChanged),
		},
		{
			name:        "label recorded as primary, now a standby, is refused",
			standby:     true,
			prevRole:    history.NodeRolePrimary,
			stored:      &schema.SchemaSnapshot{ContentHash: "h"},
			wantErrKind: ptrKind(dryrun.ErrNodeRoleChanged),
		},
		{
			// failover must stay recoverable, or a promoted node can never be
			// captured under its own label again
			name:          "--allow-role-change accepts a promotion",
			standby:       false,
			prevRole:      history.NodeRoleStandby,
			allowRole:     true,
			stored:        &schema.SchemaSnapshot{ContentHash: "h"},
			wantActivityN: 1,
			wantBoundRef:  "h",
		},
		{
			// unknown must not be read as primary: doing so would refuse the
			// first capture of every standby under a fresh label
			name:          "unknown prior role does not trip the guard",
			standby:       true,
			prevRole:      history.NodeRoleUnknown,
			stored:        &schema.SchemaSnapshot{ContentHash: "h"},
			wantActivityN: 1,
			wantBoundRef:  "h",
		},
		{
			// a store failure must not read as "no prior role" and wave the
			// capture through unguarded
			name:            "store error while reading the prior role is surfaced",
			standby:         false,
			prevRoleErr:     errors.New("database is locked"),
			stored:          &schema.SchemaSnapshot{ContentHash: "h"},
			wantErrContains: "check recorded node role",
		},
		{
			name:            "standby without prior snapshot refused",
			standby:         true,
			stored:          nil,
			wantErrContains: "no schema snapshot to bind to",
		},
		{
			name:          "standby with prior snapshot writes activity bound to its hash",
			standby:       true,
			stored:        &schema.SchemaSnapshot{ContentHash: "primary-schema-xyz"},
			wantActivityN: 1,
			wantBoundRef:  "primary-schema-xyz",
		},
		{
			name:          "standby without snapshot but --allow-orphan writes unbound row",
			standby:       true,
			stored:        nil,
			allowOrphan:   true,
			wantActivityN: 1,
			wantBoundRef:  "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &stubCapturer{Standby: tc.standby}
			w := &stubWriter{Stored: tc.stored, PrevRole: tc.prevRole, PrevRoleErr: tc.prevRoleErr}

			err := runSnapshotActivity(context.Background(), cap, w, key, captureOptions{
				Label:           "node-1",
				AllowOrphan:     tc.allowOrphan,
				AllowRoleChange: tc.allowRole,
			})

			if tc.wantErrKind != nil {
				var derr *dryrun.Error
				if !errors.As(err, &derr) || derr.Kind != *tc.wantErrKind {
					t.Fatalf("want ErrorKind=%v, got err=%v", *tc.wantErrKind, err)
				}
				return
			}
			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("want error containing %q, got %v", tc.wantErrContains, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if w.ActivityN != tc.wantActivityN {
				t.Errorf("activity puts=%d want=%d", w.ActivityN, tc.wantActivityN)
			}
			if w.LastActivityRef != tc.wantBoundRef {
				t.Errorf("activity bound to ref=%q want=%q", w.LastActivityRef, tc.wantBoundRef)
			}
			// activity capture never writes schema or planner rows, on either
			// role -- those stay exclusive to take/init
			if w.SchemaN != 0 || w.PlannerN != 0 {
				t.Errorf("activity path wrote primary streams: schema=%d planner=%d", w.SchemaN, w.PlannerN)
			}
		})
	}
}
