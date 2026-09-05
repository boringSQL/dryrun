package mcp

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

func minimalSnapshot() *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion: "PostgreSQL 17.2 on x86_64",
		Database:  "appdb",
		Timestamp: time.Now().UTC(),
	}
}

// TestInstructionsCleanWhenNoHistory: the warning is conditional. A server
// with no history store at all (offline mode) must NOT carry a history
// warning — a false alarm would train users to ignore the real one.
func TestInstructionsCleanWhenNoHistory(t *testing.T) {
	srv := NewOfflineServer(minimalSnapshot(), lint.DefaultConfig())

	if got := srv.Instructions(); strings.Contains(got, "newer dryrun") {
		t.Errorf("offline server must not warn about history.db, got:\n%s", got)
	}
}

// TestHistoryNoteEmptyWhenStoreOK: a healthy, current store produces no note,
// so neither Instructions nor any tool response nags about it.
func TestHistoryNoteEmptyWhenStoreOK(t *testing.T) {
	okStore, err := history.Open(filepath.Join(t.TempDir(), "fresh.db"))
	if err != nil {
		t.Fatalf("history.Open: %v", err)
	}
	t.Cleanup(func() { okStore.Close() })

	srv := NewServer(nil, "", minimalSnapshot(), okStore, lint.DefaultConfig(), "")
	if note := srv.historyNote(); note != nil {
		t.Errorf("a healthy store must yield no note, got %q", *note)
	}
}
