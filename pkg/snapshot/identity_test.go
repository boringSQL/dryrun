package snapshot

import "testing"

func TestIdentityConflictWith(t *testing.T) {
	cases := []struct {
		name         string
		prior        *SchemaSnapshot
		systemID     string
		database     string
		wantConflict bool
	}{
		{
			name:         "nil prior never conflicts",
			prior:        nil,
			systemID:     "111",
			database:     "app",
			wantConflict: false,
		},
		{
			// a capture that proved neither axis (no permission for either)
			name:         "an unproven capture never conflicts",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			systemID:     "",
			database:     "",
			wantConflict: false,
		},
		{
			name:         "same cluster and database is clean",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			systemID:     "111",
			database:     "app",
			wantConflict: false,
		},
		{
			name:         "different system_identifier conflicts even when db name matches",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			systemID:     "222",
			database:     "app",
			wantConflict: true,
		},
		{
			name:         "different database name conflicts on the same cluster",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			systemID:     "111",
			database:     "billing",
			wantConflict: true,
		},
		{
			name:         "a matching system_identifier does not clear a db-name difference",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			systemID:     "111",
			database:     "other",
			wantConflict: true,
		},
		{
			name:         "missing system_identifier on one side falls back to db name (match, clean)",
			prior:        &SchemaSnapshot{SystemIdentifier: "", Database: "app"},
			systemID:     "111",
			database:     "app",
			wantConflict: false,
		},
		{
			name:         "missing system_identifier on one side falls back to db name (differ, conflict)",
			prior:        &SchemaSnapshot{SystemIdentifier: "", Database: "app"},
			systemID:     "111",
			database:     "billing",
			wantConflict: true,
		},
		{
			name:         "both axes empty stays lenient",
			prior:        &SchemaSnapshot{},
			systemID:     "",
			database:     "",
			wantConflict: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, got := IdentityConflictWith(tc.prior, tc.systemID, tc.database)
			if got != tc.wantConflict {
				t.Fatalf("IdentityConflictWith = %v (%q), want %v", got, reason, tc.wantConflict)
			}
			if got && reason == "" {
				t.Fatal("conflict reported with an empty reason")
			}
			if !got && reason != "" {
				t.Fatalf("no conflict but reason set: %q", reason)
			}
		})
	}
}
