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

func TestRunSnapshotActivity_Branches(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	cases := []struct {
		name        string
		standby     bool
		stored      *schema.SchemaSnapshot
		allowOrphan bool

		wantErrKind     *dryrun.ErrorKind
		wantErrContains string
		wantActivityN   int
		wantBoundRef    string
	}{
		{
			name:            "standby without prior snapshot refused",
			standby:         true,
			stored:          nil,
			wantErrContains: "no prior schema snapshot",
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
			w := &stubWriter{Stored: tc.stored}

			err := runSnapshotActivity(context.Background(), cap, w, key, captureOptions{
				Label:       "standby-1",
				AllowOrphan: tc.allowOrphan,
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
			// Primary-only streams must stay untouched on the standby path.
			if w.SchemaN != 0 || w.PlannerN != 0 {
				t.Errorf("standby path wrote primary streams: schema=%d planner=%d", w.SchemaN, w.PlannerN)
			}
		})
	}
}
