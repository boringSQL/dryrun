package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// stubCapturer records which capture methods were called and returns canned
// values. Standby controls the primary/replica branch; IntrospectErr lets
// tests force an Introspect failure to verify error propagation.
// PlannerColumns, when set, is the column list CapturePlanner hands back — the
// masking tests need real columns with real stats to mask, where the original
// branch tests were happy with an empty planner snapshot.
type stubCapturer struct {
	Standby        bool
	IntrospectN    int
	PlannerN       int
	ActivityN      int
	QueryStatsN    int
	StandbyErr     error
	IntrospectErr  error
	QueryStatsErr  error
	PlannerColumns []schema.ColumnStatsEntry
	SystemID       string
	Database       string
	IdentityErr    error
	LastRowCap     int
}

func (s *stubCapturer) IsStandby(_ context.Context) (bool, error) {
	return s.Standby, s.StandbyErr
}

func (s *stubCapturer) Identity(_ context.Context) (string, string, error) {
	return s.SystemID, s.Database, s.IdentityErr
}

func (s *stubCapturer) Introspect(_ context.Context) (*schema.SchemaSnapshot, error) {
	s.IntrospectN++
	if s.IntrospectErr != nil {
		return nil, s.IntrospectErr
	}
	return &schema.SchemaSnapshot{
		ContentHash:      "schema-hash-1",
		SystemIdentifier: s.SystemID,
		Database:         s.Database,
	}, nil
}

func (s *stubCapturer) CapturePlanner(_ context.Context, ref string) (*schema.PlannerStatsSnapshot, error) {
	s.PlannerN++
	snap := &schema.PlannerStatsSnapshot{SchemaRefHash: ref, Columns: s.PlannerColumns}
	// mirror the real CapturePlannerStats: the content hash is computed over
	// the captured payload, so a later masking pass genuinely changes it.
	snap.ContentHash = schema.ComputePlannerContentHash(snap)
	return snap, nil
}

func (s *stubCapturer) CaptureActivity(_ context.Context, ref, src string) (*schema.ActivityStatsSnapshot, error) {
	s.ActivityN++
	a := &schema.ActivityStatsSnapshot{SchemaRefHash: ref, ContentHash: "activity-hash-1"}
	a.Node.Source = src
	return a, nil
}

func (s *stubCapturer) CaptureQueryStats(_ context.Context, ref, src string, rowCap int) (*schema.QueryStatsSnapshot, error) {
	s.QueryStatsN++
	s.LastRowCap = rowCap
	if s.QueryStatsErr != nil {
		return nil, s.QueryStatsErr
	}
	q := &schema.QueryStatsSnapshot{SchemaRefHash: ref, ContentHash: "query-stats-hash-1"}
	q.Node.Source = src
	return q, nil
}

// stubWriter counts Put* calls and optionally hands back a stored schema
// from Get so the replica path can resolve a schema_ref_hash.
type stubWriter struct {
	SchemaN, PlannerN, ActivityN, QueryStatsN int
	Stored                                    *schema.SchemaSnapshot
	GetErr                                    error
	LastActivityRef                           string
	LastPlanner                               *schema.PlannerStatsSnapshot
	LastQueryRef                              string
	PutQueryStatsErr                          error
	PrevRole                                  string
	PrevRoleErr                               error
	PrevSeen                                  []history.NodeFingerprint
	PrevFpErr                                 error
}

func (s *stubWriter) LatestNodeRole(_ context.Context, _ history.SnapshotKey, _ string) (string, error) {
	return s.PrevRole, s.PrevRoleErr
}

func (s *stubWriter) RecentNodeFingerprints(_ context.Context, _ history.SnapshotKey, _ string) ([]history.NodeFingerprint, error) {
	return s.PrevSeen, s.PrevFpErr
}

func (s *stubWriter) GetSchema(_ context.Context, _ history.SnapshotKey, _ history.SnapshotRef) (*schema.SchemaSnapshot, error) {
	if s.GetErr != nil {
		return nil, s.GetErr
	}
	if s.Stored == nil {
		return nil, history.ErrSnapshotNotFound
	}
	return s.Stored, nil
}

func (s *stubWriter) PutSchema(_ context.Context, _ history.SnapshotKey, _ *schema.SchemaSnapshot) (history.PutOutcome, error) {
	s.SchemaN++
	return history.PutInserted, nil
}

func (s *stubWriter) PutPlanner(_ context.Context, _ history.SnapshotKey, p *schema.PlannerStatsSnapshot) (history.PutOutcome, error) {
	s.PlannerN++
	// capture the snapshot exactly as runInitCapture handed it over — this is
	// the post-masking payload the masking tests assert against.
	s.LastPlanner = p
	return history.PutInserted, nil
}

func (s *stubWriter) PutActivity(_ context.Context, _ history.SnapshotKey, a *schema.ActivityStatsSnapshot) (history.PutOutcome, error) {
	s.ActivityN++
	s.LastActivityRef = a.SchemaRefHash
	return history.PutInserted, nil
}

func (s *stubWriter) PutQueryStats(_ context.Context, _ history.SnapshotKey, q *schema.QueryStatsSnapshot) (history.PutOutcome, error) {
	s.QueryStatsN++
	if s.PutQueryStatsErr != nil {
		return history.PutInserted, s.PutQueryStatsErr
	}
	s.LastQueryRef = q.SchemaRefHash
	return history.PutInserted, nil
}

// Drives runInitCapture across the three v0.6 branches: primary, replica
// (refused), and replica with --allow-replica. Each case pins exactly which
// streams land in the store, which is the contract the rest of dryrun
// (stats apply, MCP snapshot pickup) depends on.
//
// This table also doubles as coverage for the newer query-stats capture path:
// captureQueryStatsBestEffort is invoked on both non-refused branches (primary,
// and standby with --allow-replica), and the whole point of that helper is that
// it is best-effort — neither the documented ErrQueryStatsUnavailable sentinel
// (pg_stat_statements missing or not preloaded, which is an entirely normal,
// expected condition on plenty of real databases) nor any other arbitrary error
// bubbling up from the capture call should ever cause runInitCapture itself to
// fail. The last two cases below exist specifically to pin that "capture
// query stats is allowed to fail quietly" contract, since it would be very
// easy for a future refactor to accidentally start treating it like a hard
// dependency the way schema/planner/activity capture already are.
func TestRunInitCapture_Branches(t *testing.T) {
	cases := []struct {
		name            string
		standby         bool
		allowReplica    bool
		queryStatsErr   error
		wantErrKind     *dryrun.ErrorKind
		wantSchemaN     int
		wantPlannerN    int
		wantActivityN   int
		wantQueryStatsN int
	}{
		{
			name:            "primary writes all three streams",
			standby:         false,
			allowReplica:    false,
			wantSchemaN:     1,
			wantPlannerN:    1,
			wantActivityN:   1,
			wantQueryStatsN: 1,
		},
		{
			name:        "replica without flag refuses",
			standby:     true,
			wantErrKind: ptrKind(dryrun.ErrReplicaCapture),
		},
		{
			name:            "replica with --allow-replica writes activity only",
			standby:         true,
			allowReplica:    true,
			wantActivityN:   1,
			wantQueryStatsN: 1,
		},
		{
			// The sentinel is the expected outcome on any database that hasn't
			// preloaded pg_stat_statements — this must be a complete non-event
			// for the rest of the primary capture, not something callers need
			// to special-case or even notice.
			name:            "missing pg_stat_statements does not fail primary capture",
			standby:         false,
			queryStatsErr:   schema.ErrQueryStatsUnavailable,
			wantSchemaN:     1,
			wantPlannerN:    1,
			wantActivityN:   1,
			wantQueryStatsN: 1,
		},
		{
			// Deliberately a generic, unrecognized error (not the sentinel) to
			// prove the best-effort contract isn't just "know about this one
			// specific error" but genuinely "any capture failure here is
			// swallowed and logged, never propagated."
			name:            "arbitrary query-stats capture error does not fail primary capture",
			standby:         false,
			queryStatsErr:   errors.New("boom: connection reset mid-query"),
			wantSchemaN:     1,
			wantPlannerN:    1,
			wantActivityN:   1,
			wantQueryStatsN: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &stubCapturer{Standby: tc.standby, QueryStatsErr: tc.queryStatsErr}
			w := &stubWriter{}
			err := runInitCapture(context.Background(), cap, w, history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, initOptions{
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
			if cap.QueryStatsN != tc.wantQueryStatsN {
				t.Errorf("query stats capture attempts=%d want=%d", cap.QueryStatsN, tc.wantQueryStatsN)
			}
		})
	}
}

func TestRunInitCapture_ForwardsRowCap(t *testing.T) {
	cap := &stubCapturer{}
	w := &stubWriter{}
	err := runInitCapture(context.Background(), cap, w, history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, initOptions{
		Source: "test-node",
		RowCap: 1000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cap.LastRowCap != 1000 {
		t.Errorf("LastRowCap=%d want=1000", cap.LastRowCap)
	}
}

// captureQueryStatsBestEffort has an unusual contract for a capture helper in
// this file: it swallows almost everything, but not quite everything. This
// test drives it directly (rather than through the higher-level
// runInitCapture/runSnapshotTake plumbing) so each branch of that contract is
// pinned in isolation, independent of whatever the surrounding capture flow
// happens to be doing at the time:
//
//   - a clean capture returns nil and the caller can treat the call as a
//     no-op from an error-handling perspective;
//   - schema.ErrQueryStatsUnavailable (pg_stat_statements missing, or present
//     but not preloaded via shared_preload_libraries) is an expected, common
//     outcome on real databases and must never surface as an error;
//   - some other, unrelated capture failure (a network blip, a permissions
//     problem, whatever) is logged as a warning but likewise must not
//     surface — the whole point of "best effort" is that query-stats capture
//     is a nice-to-have layered on top of the snapshot, not a dependency of it;
//   - but context cancellation/deadline exceeded is different in kind from a
//     capture-specific failure: it means the *caller* wants to stop, not that
//     this particular capture step had a problem, so it is the one thing this
//     helper does NOT swallow and must propagate faithfully.
func TestCaptureQueryStatsBestEffort(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	t.Run("successful capture is persisted, returns nil", func(t *testing.T) {
		cap := &stubCapturer{}
		w := &stubWriter{}
		if err := captureQueryStatsBestEffort(context.Background(), cap, w, key, "schema-hash", "primary", 0); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if cap.QueryStatsN != 1 {
			t.Errorf("query stats capture attempts=%d want=1", cap.QueryStatsN)
		}
		if w.QueryStatsN != 1 {
			t.Errorf("query stats puts=%d want=1", w.QueryStatsN)
		}
	})

	t.Run("ErrQueryStatsUnavailable is swallowed, not surfaced", func(t *testing.T) {
		cap := &stubCapturer{QueryStatsErr: schema.ErrQueryStatsUnavailable}
		w := &stubWriter{}
		if err := captureQueryStatsBestEffort(context.Background(), cap, w, key, "schema-hash", "primary", 0); err != nil {
			t.Errorf("expected the sentinel to be swallowed, got: %v", err)
		}
		if w.QueryStatsN != 0 {
			t.Errorf("nothing to put when capture failed; puts=%d want=0", w.QueryStatsN)
		}
	})

	t.Run("an arbitrary capture error is logged and swallowed, not surfaced", func(t *testing.T) {
		cap := &stubCapturer{QueryStatsErr: errors.New("connection reset")}
		w := &stubWriter{}
		if err := captureQueryStatsBestEffort(context.Background(), cap, w, key, "schema-hash", "primary", 0); err != nil {
			t.Errorf("expected an ordinary capture error to be swallowed, got: %v", err)
		}
	})

	t.Run("a save failure is logged and swallowed, not surfaced", func(t *testing.T) {
		cap := &stubCapturer{}
		w := &stubWriter{PutQueryStatsErr: errors.New("disk full")}
		if err := captureQueryStatsBestEffort(context.Background(), cap, w, key, "schema-hash", "primary", 0); err != nil {
			t.Errorf("expected a save failure to be swallowed, got: %v", err)
		}
	})

	t.Run("context cancellation is the one error that propagates", func(t *testing.T) {
		cap := &stubCapturer{QueryStatsErr: context.Canceled}
		w := &stubWriter{}
		err := captureQueryStatsBestEffort(context.Background(), cap, w, key, "schema-hash", "primary", 0)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled to propagate, got: %v", err)
		}
	})

	t.Run("deadline exceeded also propagates", func(t *testing.T) {
		cap := &stubCapturer{QueryStatsErr: context.DeadlineExceeded}
		w := &stubWriter{}
		err := captureQueryStatsBestEffort(context.Background(), cap, w, key, "schema-hash", "primary", 0)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want context.DeadlineExceeded to propagate, got: %v", err)
		}
	})
}

// Confirms that when the standby has a previously-captured schema in the
// store, the activity row is bound to that schema's content_hash rather
// than the empty string; the stats apply path joins on schema_ref_hash so
// an empty ref would orphan the row.
func TestRunInitCapture_ReplicaBindsToStoredSchemaRef(t *testing.T) {
	cap := &stubCapturer{Standby: true}
	w := &stubWriter{Stored: &schema.SchemaSnapshot{ContentHash: "primary-schema-abc"}}

	if err := runInitCapture(context.Background(), cap, w, history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, initOptions{
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

func strPtr(s string) *string { return &s }

// colWithStats builds one ColumnStatsEntry in the public schema whose three
// value-bearing statistics fields — most_common_vals, most_common_freqs and
// histogram_bounds — are all populated with non-nil sample data. Starting from
// a fully-populated column is what makes a masking assertion trustworthy: a
// column that began life with nil stats would "look masked" even if
// runInitCapture did nothing at all, so the test would pass for the wrong
// reason. By seeding real values we guarantee that a nil afterwards can only
// be the result of ApplyPlanner having run.
func colWithStats(table, column string) schema.ColumnStatsEntry {
	return schema.ColumnStatsEntry{
		Table:  schema.QualifiedName{Schema: "public", Name: table},
		Column: column,
		Stats: schema.ColumnStats{
			MostCommonVals:  strPtr("{alice@example.com,bob@example.com}"),
			MostCommonFreqs: strPtr("{0.5,0.5}"),
			HistogramBounds: strPtr("{a,b,c}"),
		},
	}
}

// loadTestPolicy writes a throwaway data-masking-policy.yml that lists the
// given table.column keys under a single database block ("testdb"), then loads
// it into a real *masking.Policy via masking.Load. Round-tripping through a
// real YAML file and the real loader exercises the genuine parsing path
// rather than a hand-mocked policy, so these tests would catch a regression
// in Load just as readily as one in runInitCapture.
func loadTestPolicy(t *testing.T, columns ...string) *masking.Policy {
	t.Helper()

	var b strings.Builder
	b.WriteString("version: 1\ndatabases:\n  testdb:\n    columns:\n")
	for _, c := range columns {
		// expr is required by the shared schema but ignored by dryrun, which
		// only consumes the set of column keys — any placeholder will do.
		fmt.Fprintf(&b, "      %s: { expr: \"redacted\", tags: [pii] }\n", c)
	}
	b.WriteString("    policies:\n      pii: { include_tags: [pii] }\n")

	path := filepath.Join(t.TempDir(), "data-masking-policy.yml")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write test masks file: %v", err)
	}
	pol, err := masking.Load(path, "testdb", nil)
	if err != nil {
		t.Fatalf("load test policy: %v", err)
	}
	return pol
}

// TestRunInitCapture_Masking is the C6 integration check for the C5 wiring.
// It drives runInitCapture down the primary-capture path with three different
// initOptions.MaskPolicy values and asserts, on the exact PlannerStatsSnapshot
// that reaches PutPlanner, that:
//
//   - a nil policy leaves every column's statistics untouched (the regression
//     guard — masking must be strictly opt-in, never accidental);
//   - a policy whose key matches a captured column NULLs that column's three
//     value-bearing stat fields, and ONLY that column's;
//   - a policy whose keys match nothing captured leaves every column intact.
//
// On top of those per-case expectations, every case re-checks one invariant:
// the snapshot's stored content_hash must equal ComputePlannerContentHash of
// the snapshot as handed to PutPlanner. That single assertion is what proves
// the C5 "recompute the hash after masking" step actually happened — in the
// matching-column case the payload genuinely changes, so a runInitCapture that
// masked the columns but forgot to recompute would leave a stale hash and this
// check would fail.
func TestRunInitCapture_Masking(t *testing.T) {
	cases := []struct {
		name string
		// policy is the masking policy fed in via initOptions.MaskPolicy.
		policy *masking.Policy
		// wantMasked maps a captured column name to whether its stats should
		// have been NULLed by the time PutPlanner saw the snapshot.
		wantMasked map[string]bool
	}{
		{
			// No policy at all: capture must behave exactly as it did before
			// the masking feature existed. Nothing is touched.
			name:       "nil policy leaves every column intact",
			policy:     nil,
			wantMasked: map[string]bool{"email": false, "id": false},
		},
		{
			// The policy lists users.email; the capture contains users.email
			// and users.id. Only email may be masked — id must survive so
			// that planner estimates on non-sensitive columns stay realistic.
			name:       "policy nulls the matching column only",
			policy:     loadTestPolicy(t, "users.email"),
			wantMasked: map[string]bool{"email": true, "id": false},
		},
		{
			// The policy is non-nil but its only key (orders.total) matches
			// nothing in the captured customers/users columns. ApplyPlanner
			// runs, masks zero columns, and the payload is unchanged.
			name:       "policy matching nothing leaves every column intact",
			policy:     loadTestPolicy(t, "orders.total"),
			wantMasked: map[string]bool{"email": false, "id": false},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh capturer per case: it always reports the same two
			// columns, both fully populated, so the only variable across
			// cases is the policy.
			cap := &stubCapturer{
				PlannerColumns: []schema.ColumnStatsEntry{
					colWithStats("users", "email"),
					colWithStats("users", "id"),
				},
			}
			w := &stubWriter{}

			err := runInitCapture(
				context.Background(), cap, w,
				history.SnapshotKey{ProjectID: "p", DatabaseID: "testdb"},
				initOptions{Source: "test-node", Policy: tc.policy},
			)
			if err != nil {
				t.Fatalf("runInitCapture: %v", err)
			}
			if w.LastPlanner == nil {
				t.Fatal("PutPlanner never received a planner snapshot")
			}

			// Per-column expectation: a column counts as masked only when all
			// three value-bearing fields are nil together. Checking all three
			// also catches a partial mask (e.g. MCV cleared but histogram
			// left behind), which would still leak sample values.
			for _, c := range w.LastPlanner.Columns {
				masked := c.Stats.MostCommonVals == nil &&
					c.Stats.MostCommonFreqs == nil &&
					c.Stats.HistogramBounds == nil
				want, known := tc.wantMasked[c.Column]
				if !known {
					t.Fatalf("unexpected column %q in snapshot", c.Column)
				}
				if masked != want {
					t.Errorf("column %q: masked=%v, want %v "+
						"(mcv=%v freqs=%v hist=%v)",
						c.Column, masked, want,
						c.Stats.MostCommonVals, c.Stats.MostCommonFreqs,
						c.Stats.HistogramBounds)
				}
			}

			// Invariant: the stored content_hash must describe the stored
			// (post-masking) payload. This is the recompute check from C5.
			if got, want := w.LastPlanner.ContentHash,
				schema.ComputePlannerContentHash(w.LastPlanner); got != want {
				t.Errorf("content_hash %q does not match the masked payload "+
					"hash %q — runInitCapture did not recompute it after masking",
					got, want)
			}
		})
	}
}
