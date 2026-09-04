package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// A store failure must never be read as "no schema yet" -- that would let
// --allow-orphan wave an unbound capture through on a corrupt or locked db.
func TestResolveSchemaRef(t *testing.T) {
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "d"}

	tests := []struct {
		name            string
		writer          *stubWriter
		allowOrphan     bool
		wantRef         string
		wantErrContains string
	}{
		{
			name:    "existing schema resolves to its hash",
			writer:  &stubWriter{Stored: &schema.SchemaSnapshot{ContentHash: "abc123"}},
			wantRef: "abc123",
		},
		{
			name:            "absent schema without --allow-orphan is refused",
			writer:          &stubWriter{},
			wantErrContains: "no schema snapshot to bind to",
		},
		{
			name:        "absent schema with --allow-orphan binds to nothing",
			writer:      &stubWriter{},
			allowOrphan: true,
			wantRef:     "",
		},
		{
			name:            "store error is surfaced even with --allow-orphan",
			writer:          &stubWriter{GetErr: errors.New("database is locked")},
			allowOrphan:     true,
			wantErrContains: "read latest schema snapshot",
		},
		{
			name:            "store error without --allow-orphan is not the orphan message",
			writer:          &stubWriter{GetErr: errors.New("corrupt snapshot JSON")},
			wantErrContains: "read latest schema snapshot",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := resolveSchemaRef(context.Background(), tc.writer, key, tc.allowOrphan)
			if tc.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("got err %v, want containing %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ref != tc.wantRef {
				t.Errorf("ref = %q, want %q", ref, tc.wantRef)
			}
		})
	}
}
