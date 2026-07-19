package main

import (
	"context"
	"errors"
	"testing"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// The reported footgun: a stray DATABASE_URL captures a foreign cluster/database
// into an existing project. runSnapshotTake must refuse it unless --force.
func TestRunSnapshotTake_IdentityGuard(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	cases := []struct {
		name       string
		stored     *schema.SchemaSnapshot // prior snapshot on this key (nil = fresh project)
		capSystem  string
		capDB      string
		force      bool
		wantRefuse bool
	}{
		{
			name:       "first capture on an empty key establishes the baseline",
			stored:     nil,
			capSystem:  "111",
			capDB:      "app",
			wantRefuse: false,
		},
		{
			name:       "same cluster and database is recorded",
			stored:     &schema.SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			capSystem:  "111",
			capDB:      "app",
			wantRefuse: false,
		},
		{
			name:       "foreign cluster is refused",
			stored:     &schema.SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			capSystem:  "999",
			capDB:      "app",
			wantRefuse: true,
		},
		{
			name:       "wrong database on the same cluster is refused",
			stored:     &schema.SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			capSystem:  "111",
			capDB:      "billing",
			wantRefuse: true,
		},
		{
			name:       "--force records a mismatch anyway",
			stored:     &schema.SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			capSystem:  "999",
			capDB:      "app",
			force:      true,
			wantRefuse: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &stubCapturer{SystemID: tc.capSystem, Database: tc.capDB}
			w := &stubWriter{Stored: tc.stored}

			_, _, _, _, err := runSnapshotTake(context.Background(), cap, w, key, nil, tc.force)

			if tc.wantRefuse {
				var de *dryrun.Error
				if !errors.As(err, &de) || de.Kind != dryrun.ErrIdentityMismatch {
					t.Fatalf("want ErrIdentityMismatch, got %v", err)
				}
				if w.SchemaN != 0 {
					t.Fatalf("refused capture still wrote %d schema rows", w.SchemaN)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if w.SchemaN != 1 {
				t.Fatalf("want 1 schema row written, got %d", w.SchemaN)
			}
		})
	}
}

// An unreadable prior (anything but the not-found sentinel) must fail closed:
// the guard cannot prove a match, so it refuses rather than record blind.
func TestRunSnapshotTake_IdentityGuard_FailsClosedOnReadError(t *testing.T) {
	cap := &stubCapturer{SystemID: "111", Database: "app"}
	w := &stubWriter{GetErr: errors.New("history.db read failed")}

	_, _, _, _, err := runSnapshotTake(context.Background(), cap, w,
		history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, nil, false)

	var de *dryrun.Error
	if !errors.As(err, &de) || de.Kind != dryrun.ErrHistory {
		t.Fatalf("want ErrHistory, got %v", err)
	}
	if w.SchemaN != 0 {
		t.Fatalf("guard failed open: wrote %d schema rows on a read error", w.SchemaN)
	}
}
