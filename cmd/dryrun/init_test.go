package main

import (
	"context"
	"errors"
	"testing"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// stubCapturer records which capture methods were called and returns canned
// values. Standby controls the primary/replica branch; IntrospectErr lets
// tests force an Introspect failure to verify error propagation.
type stubCapturer struct {
	Standby      bool
	IntrospectN  int
	PlannerN     int
	ActivityN    int
	StandbyErr   error
	IntrospectErr error
}

func (s *stubCapturer) IsStandby(_ context.Context) (bool, error) {
	return s.Standby, s.StandbyErr
}

func (s *stubCapturer) Introspect(_ context.Context) (*schema.SchemaSnapshot, error) {
	s.IntrospectN++
	if s.IntrospectErr != nil {
		return nil, s.IntrospectErr
	}
	return &schema.SchemaSnapshot{ContentHash: "schema-hash-1"}, nil
}

func (s *stubCapturer) CapturePlanner(_ context.Context, ref string) (*schema.PlannerStatsSnapshot, error) {
	s.PlannerN++
	return &schema.PlannerStatsSnapshot{SchemaRefHash: ref, ContentHash: "planner-hash-1"}, nil
}

func (s *stubCapturer) CaptureActivity(_ context.Context, ref, src string) (*schema.ActivityStatsSnapshot, error) {
	s.ActivityN++
	a := &schema.ActivityStatsSnapshot{SchemaRefHash: ref, ContentHash: "activity-hash-1"}
	a.Node.Source = src
	return a, nil
}

// stubWriter counts Put* calls and optionally hands back a stored schema
// from Get so the replica path can resolve a schema_ref_hash.
type stubWriter struct {
	SchemaN, PlannerN, ActivityN int
	Stored                       *schema.SchemaSnapshot
	LastActivityRef              string
}

func (s *stubWriter) Get(_ context.Context, _ history.SnapshotKey, _ history.SnapshotRef) (*schema.SchemaSnapshot, error) {
	if s.Stored == nil {
		return nil, history.ErrSnapshotNotFound
	}
	return s.Stored, nil
}

func (s *stubWriter) Put(_ context.Context, _ history.SnapshotKey, _ *schema.SchemaSnapshot) (history.PutOutcome, error) {
	s.SchemaN++
	return history.PutInserted, nil
}

func (s *stubWriter) PutPlanner(_ context.Context, _ history.SnapshotKey, _ *schema.PlannerStatsSnapshot) (history.PutOutcome, error) {
	s.PlannerN++
	return history.PutInserted, nil
}

func (s *stubWriter) PutActivity(_ context.Context, _ history.SnapshotKey, a *schema.ActivityStatsSnapshot) (history.PutOutcome, error) {
	s.ActivityN++
	s.LastActivityRef = a.SchemaRefHash
	return history.PutInserted, nil
}

// Drives runInitCapture across the three v0.6 branches: primary, replica
// (refused), and replica with --allow-replica. Each case pins exactly which
// streams land in the store, which is the contract the rest of dryrun
// (stats apply, reload_schema) depends on.
func TestRunInitCapture_Branches(t *testing.T) {
	cases := []struct {
		name          string
		standby       bool
		allowReplica  bool
		wantErrKind   *dryrun.ErrorKind
		wantSchemaN   int
		wantPlannerN  int
		wantActivityN int
	}{
		{
			name:          "primary writes all three streams",
			standby:       false,
			allowReplica:  false,
			wantSchemaN:   1,
			wantPlannerN:  1,
			wantActivityN: 1,
		},
		{
			name:        "replica without flag refuses",
			standby:     true,
			wantErrKind: ptrKind(dryrun.ErrReplicaCapture),
		},
		{
			name:          "replica with --allow-replica writes activity only",
			standby:       true,
			allowReplica:  true,
			wantActivityN: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &stubCapturer{Standby: tc.standby}
			w := &stubWriter{}
			err := runInitCapture(context.Background(), cap, w, history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, t.TempDir(), initOptions{
				AllowReplica: tc.allowReplica,
				Source:       "test-node",
			})

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

// Confirms that when the standby has a previously-captured schema in the
// store, the activity row is bound to that schema's content_hash rather
// than the empty string; the stats apply path joins on schema_ref_hash so
// an empty ref would orphan the row.
func TestRunInitCapture_ReplicaBindsToStoredSchemaRef(t *testing.T) {
	cap := &stubCapturer{Standby: true}
	w := &stubWriter{Stored: &schema.SchemaSnapshot{ContentHash: "primary-schema-abc"}}

	if err := runInitCapture(context.Background(), cap, w, history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, t.TempDir(), initOptions{
		AllowReplica: true,
		Source:       "replica-1",
	}); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if w.ActivityN != 1 {
		t.Fatalf("activity puts=%d want=1", w.ActivityN)
	}
	if w.LastActivityRef != "primary-schema-abc" {
		t.Errorf("activity bound to ref=%q, want=primary-schema-abc", w.LastActivityRef)
	}
}

func ptrKind(k dryrun.ErrorKind) *dryrun.ErrorKind { return &k }
