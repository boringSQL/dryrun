package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/boringsql/dryrun/internal/history"
)

// examples/demo is the README's 30-second demo (`cd examples/demo && dryrun
// lint`). Since v0.15 that reads .dryrun/history.db, so the committed fixture
// has to be readable by the current binary AND carry the key the demo's
// dryrun.toml resolves to. A legacy-format fixture broke the demo silently
// once; this fails loudly instead.
func TestDemoFixtureIsReadable(t *testing.T) {
	dir, err := filepath.Abs("../../examples/demo")
	if err != nil {
		t.Fatal(err)
	}

	// resolve exactly as the CLI does, from inside the demo directory, so a
	// divergence in key resolution fails here too
	resetFlags(t)
	withCWD(t, dir)
	key := resolveSnapshotKey()

	store, err := history.Open(filepath.Join(dir, ".dryrun", "history.db"))
	if err != nil {
		t.Fatalf("open demo history.db: %v", err)
	}
	defer store.Close()

	if c := store.Compat(); c != history.CompatOK {
		t.Fatalf("demo history.db compat = %v, want CompatOK — regenerate the fixture", c)
	}

	snap, err := store.GetSchema(context.Background(), key, history.NewRefLatest())
	if err != nil {
		t.Fatalf("demo history.db has no schema under project=%s database=%s: %v", key.ProjectID, key.DatabaseID, err)
	}
	// the README prints "(13 tables checked)"
	if len(snap.Tables) != 13 {
		t.Errorf("demo fixture has %d tables, README advertises 13", len(snap.Tables))
	}
}
