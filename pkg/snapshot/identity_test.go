package snapshot

import "testing"

func TestIdentityConflict(t *testing.T) {
	cases := []struct {
		name         string
		prior        *SchemaSnapshot
		incoming     *SchemaSnapshot
		wantConflict bool
	}{
		{
			name:         "nil prior never conflicts",
			prior:        nil,
			incoming:     &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			wantConflict: false,
		},
		{
			name:         "nil incoming never conflicts",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			incoming:     nil,
			wantConflict: false,
		},
		{
			name:         "same cluster and database is clean",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			incoming:     &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			wantConflict: false,
		},
		{
			name:         "different system_identifier conflicts even when db name matches",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			incoming:     &SchemaSnapshot{SystemIdentifier: "222", Database: "app"},
			wantConflict: true,
		},
		{
			name:         "different database name conflicts on the same cluster",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			incoming:     &SchemaSnapshot{SystemIdentifier: "111", Database: "billing"},
			wantConflict: true,
		},
		{
			name:         "system_identifier wins so a matching one clears a db-name difference? no, db still checked",
			prior:        &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			incoming:     &SchemaSnapshot{SystemIdentifier: "111", Database: "other"},
			wantConflict: true,
		},
		{
			name:         "missing system_identifier on one side falls back to db name (match, clean)",
			prior:        &SchemaSnapshot{SystemIdentifier: "", Database: "app"},
			incoming:     &SchemaSnapshot{SystemIdentifier: "111", Database: "app"},
			wantConflict: false,
		},
		{
			name:         "missing system_identifier on one side falls back to db name (differ, conflict)",
			prior:        &SchemaSnapshot{SystemIdentifier: "", Database: "app"},
			incoming:     &SchemaSnapshot{SystemIdentifier: "111", Database: "billing"},
			wantConflict: true,
		},
		{
			name:         "both axes empty stays lenient",
			prior:        &SchemaSnapshot{},
			incoming:     &SchemaSnapshot{},
			wantConflict: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, got := IdentityConflict(tc.prior, tc.incoming)
			if got != tc.wantConflict {
				t.Fatalf("IdentityConflict = %v (%q), want %v", got, reason, tc.wantConflict)
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
