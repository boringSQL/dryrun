package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// runSnapshotQueryStats is the standalone `dryrun snapshot query-stats`
// command, and it deliberately does NOT share runSnapshotActivity's
// primary/standby restriction — pg_stat_statements is just as meaningful on a
// primary as on a replica, so there's no IsStandby check at all here. It also
// deliberately does NOT share captureQueryStatsBestEffort's swallow-and-warn
// behavior (used when query stats are captured as a side effect of `init`,
// `take`, or `snapshot activity`): as a command the user invoked directly to
// get query stats, a failure to capture them must be a loud, actionable
// error, not a silently-skipped no-op. These two contrasts are the entire
// point of this test file.
func TestRunSnapshotQueryStats_Branches(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	cases := []struct {
		name        string
		standby     bool
		stored      *schema.SchemaSnapshot
		allowOrphan bool
		captureErr  error
		putErr      error

		wantErrContains string
		wantQueryStatsN int
		wantBoundRef    string
	}{
		{
			// The single most load-bearing contrast with runSnapshotActivity:
			// a primary is refused there via ErrReplicaCapture, but here a
			// primary with a prior schema snapshot must succeed exactly like
			// a standby would. If someone "fixes" this by copying activity's
			// IsStandby guard over, this case starts failing.
			name:            "primary allowed, binds to prior schema",
			standby:         false,
			stored:          &schema.SchemaSnapshot{ContentHash: "primary-schema-abc"},
			wantQueryStatsN: 1,
			wantBoundRef:    "primary-schema-abc",
		},
		{
			name:            "standby with prior snapshot writes query stats bound to its hash",
			standby:         true,
			stored:          &schema.SchemaSnapshot{ContentHash: "standby-schema-xyz"},
			wantQueryStatsN: 1,
			wantBoundRef:    "standby-schema-xyz",
		},
		{
			name:            "no prior snapshot and no --allow-orphan is refused before capture",
			standby:         true,
			stored:          nil,
			wantErrContains: "no prior schema snapshot",
			wantQueryStatsN: 0,
		},
		{
			name:            "no prior snapshot but --allow-orphan writes an unbound row",
			standby:         true,
			stored:          nil,
			allowOrphan:     true,
			wantQueryStatsN: 1,
			wantBoundRef:    "",
		},
		{
			// This is the case that distinguishes runSnapshotQueryStats from
			// captureQueryStatsBestEffort: the latter swallows exactly this
			// error and moves on quietly. runSnapshotQueryStats must NOT
			// swallow it — the user ran this command specifically to get
			// query stats, so "pg_stat_statements isn't set up" needs to
			// reach them as a real, actionable failure with setup
			// instructions, not vanish.
			name:            "ErrQueryStatsUnavailable is surfaced, not swallowed",
			standby:         true,
			stored:          &schema.SchemaSnapshot{ContentHash: "some-schema"},
			captureErr:      schema.ErrQueryStatsUnavailable,
			wantErrContains: "pg_stat_statements is not available",
		},
		{
			name:            "a generic capture error is wrapped and propagated",
			standby:         true,
			stored:          &schema.SchemaSnapshot{ContentHash: "some-schema"},
			captureErr:      errors.New("connection reset by peer"),
			wantErrContains: "capture query stats",
		},
		{
			name:            "a store failure on Put is wrapped and propagated",
			standby:         true,
			stored:          &schema.SchemaSnapshot{ContentHash: "some-schema"},
			putErr:          errors.New("disk full"),
			wantErrContains: "save query stats",
			wantQueryStatsN: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &stubCapturer{Standby: tc.standby, QueryStatsErr: tc.captureErr}
			w := &stubWriter{Stored: tc.stored, PutQueryStatsErr: tc.putErr}

			err := runSnapshotQueryStats(context.Background(), cap, w, key, captureOptions{
				Label:       "node-1",
				AllowOrphan: tc.allowOrphan,
			})

			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("want error containing %q, got %v", tc.wantErrContains, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if w.QueryStatsN != tc.wantQueryStatsN {
				t.Errorf("query stats puts=%d want=%d", w.QueryStatsN, tc.wantQueryStatsN)
			}
			if tc.wantErrContains == "" && w.LastQueryRef != tc.wantBoundRef {
				t.Errorf("query stats bound to ref=%q want=%q", w.LastQueryRef, tc.wantBoundRef)
			}
			// This command only ever touches the query-stats stream — a
			// regression that starts also writing schema/planner/activity
			// rows would be a silent behavior change worth catching here.
			if w.SchemaN != 0 || w.PlannerN != 0 || w.ActivityN != 0 {
				t.Errorf("query-stats path wrote other streams: schema=%d planner=%d activity=%d", w.SchemaN, w.PlannerN, w.ActivityN)
			}
		})
	}
}
