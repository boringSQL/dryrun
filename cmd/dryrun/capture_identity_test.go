package main

import (
	"context"
	"errors"
	"testing"

	"github.com/spf13/cobra"

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

			_, _, _, _, err := runSnapshotTake(context.Background(), cap, w, key, nil, tc.force, false)

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
		history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}, nil, false, false)

	var de *dryrun.Error
	if !errors.As(err, &de) || de.Kind != dryrun.ErrHistory {
		t.Fatalf("want ErrHistory, got %v", err)
	}
	if w.SchemaN != 0 {
		t.Fatalf("guard failed open: wrote %d schema rows on a read error", w.SchemaN)
	}
}

// A node capture never introspects, so it proves identity from the pair alone.
// Before this, node-scoped captures ran no identity guard at all and a stray
// node URL could record a foreign cluster into the project.
func TestGuardCaptureIdentity_OnTheIdentityPair(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}
	prior := &schema.SchemaSnapshot{SystemIdentifier: "111", Database: "app"}

	for _, tc := range []struct {
		name       string
		stored     *schema.SchemaSnapshot
		systemID   string
		database   string
		force      bool
		wantRefuse bool
	}{
		{"no baseline yet accepts anything", nil, "999", "billing", false, false},
		{"matching pair is accepted", prior, "111", "app", false, false},
		{"foreign cluster is refused", prior, "999", "app", false, true},
		{"wrong database on the same cluster is refused", prior, "111", "billing", false, true},
		{"--force records the mismatch anyway", prior, "999", "billing", true, false},
		// a capture that could not read system_identifier (permissions) still
		// proves the database axis; an absent axis must not false-positive
		{"an empty axis does not conflict", prior, "", "app", false, false},
		{"an empty cluster still catches the database", prior, "", "billing", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &stubWriter{Stored: tc.stored}
			err := guardCaptureIdentity(context.Background(), w, key, tc.systemID, tc.database, tc.force)
			if !tc.wantRefuse {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			var de *dryrun.Error
			if !errors.As(err, &de) || de.Kind != dryrun.ErrIdentityMismatch {
				t.Fatalf("want ErrIdentityMismatch, got %v", err)
			}
		})
	}
}

// take checked the standby gate but never the recorded role, so it could
// re-point a label that `capture` would have refused.
func TestRunSnapshotTake_GuardsTheRecordedRole(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	for _, tc := range []struct {
		name            string
		prevRole        string
		allowRoleChange bool
		wantRefuse      bool
	}{
		{"a label last seen as a standby is refused", history.NodeRoleStandby, false, true},
		{"--allow-role-change accepts the flip", history.NodeRoleStandby, true, false},
		{"an unchanged role is accepted", history.NodeRolePrimary, false, false},
		{"a label with no recorded role is accepted", history.NodeRoleUnknown, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cap := &stubCapturer{}
			w := &stubWriter{PrevRole: tc.prevRole}

			_, _, _, _, err := runSnapshotTake(context.Background(), cap, w, key, nil, false, tc.allowRoleChange)

			if !tc.wantRefuse {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			var de *dryrun.Error
			if !errors.As(err, &de) || de.Kind != dryrun.ErrNodeRoleChanged {
				t.Fatalf("want ErrNodeRoleChanged, got %v", err)
			}
			if w.SchemaN != 0 {
				t.Fatalf("refused capture still wrote %d schema rows", w.SchemaN)
			}
		})
	}
}

// capture had no --force at all, so an identity mismatch was unrecoverable
// there, and take never checked the recorded role that capture has always
// checked. The guards are unit-tested above -- this pins that both commands
// expose the same two escape hatches.
func TestCaptureAndTake_ExposeTheSameGuardFlags(t *testing.T) {
	db := "unused.db"
	cmds := map[string]*cobra.Command{"capture": snapshotCaptureCmd(&db)}
	for _, c := range snapshotCmd().Commands() {
		if c.Name() == "take" {
			cmds["take"] = c
		}
	}
	if cmds["take"] == nil {
		t.Fatal("could not find the take subcommand")
	}

	for _, name := range []string{"force", "allow-role-change"} {
		for cmdName, c := range cmds {
			f := c.Flags().Lookup(name)
			if f == nil {
				t.Errorf("snapshot %s is missing --%s, which its guard tells users to pass", cmdName, name)
				continue
			}
			if f.DefValue != "false" {
				t.Errorf("snapshot %s --%s defaults to %q, want the guard on by default", cmdName, name, f.DefValue)
			}
		}
	}
}
