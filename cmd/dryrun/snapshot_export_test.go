package main

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// TestSnapshotExportRoundTrip seeds a temp history.db with two distinct
// (project, database) streams totalling three snapshots, runs the export,
// then walks the output tree and asserts every .json.zst decompresses to a
// SchemaSnapshot whose ContentHash matches the filename. This is the
// end-to-end contract: bytes written are bytes that come back.
func TestSnapshotExportRoundTrip(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	store, err := history.Open(filepath.Join(dbDir, "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Second)
	seed := []struct {
		project, database, hash string
		offset                  time.Duration
	}{
		{"acme", "primary", "hash-a1", -2 * time.Hour},
		{"acme", "primary", "hash-a2", -1 * time.Hour},
		{"zeta", "replica", "hash-z1", -30 * time.Minute},
	}
	for _, s := range seed {
		snap := &schema.SchemaSnapshot{
			PgVersion:   "PostgreSQL 17.0",
			Database:    s.database,
			Timestamp:   now.Add(s.offset),
			ContentHash: s.hash,
			Tables:      []schema.Table{{Schema: "public", Name: "users"}},
		}
		k := history.SnapshotKey{
			ProjectID:  history.ProjectId(s.project),
			DatabaseID: history.DatabaseId(s.database),
		}
		if _, err := store.PutSchema(ctx, k, snap); err != nil {
			t.Fatalf("seed %s/%s: %v", s.project, s.database, err)
		}
	}

	outRoot := filepath.Join(t.TempDir(), "snapshots")
	written, streams, err := runSnapshotExport(ctx, store, outRoot)
	if err != nil {
		t.Fatal(err)
	}
	if written != len(seed) || streams != 2 {
		t.Errorf("written=%d streams=%d, want 3 / 2", written, streams)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()

	// expected layout: <out>/<project>/<database>/<ts>-<hash>.json.zst
	found := map[string]bool{}
	err = filepath.WalkDir(outRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".json.zst") {
			return err
		}
		rel, _ := filepath.Rel(outRoot, path)
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 3 {
			t.Errorf("unexpected path depth: %s", rel)
			return nil
		}
		project, database, file := parts[0], parts[1], parts[2]

		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		decoded, err := dec.DecodeAll(raw, nil)
		if err != nil {
			t.Errorf("%s: decompress: %v", rel, err)
			return nil
		}

		var snap schema.SchemaSnapshot
		if err := json.Unmarshal(decoded, &snap); err != nil {
			t.Errorf("%s: unmarshal: %v", rel, err)
			return nil
		}
		// filename embeds the hash; the persisted hash must match
		if !strings.Contains(file, snap.ContentHash) {
			t.Errorf("%s: filename %q does not embed ContentHash %q", rel, file, snap.ContentHash)
		}
		if snap.Database != database {
			t.Errorf("%s: snap.Database=%q, want %q", rel, snap.Database, database)
		}
		found[project+"/"+database+"/"+snap.ContentHash] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, s := range seed {
		k := s.project + "/" + s.database + "/" + s.hash
		if !found[k] {
			t.Errorf("missing exported snapshot: %s", k)
		}
	}
}

// TestSnapshotExportEmptyStore exercises the no-keys path: an export against
// an empty history.db must succeed silently, write nothing, and not even
// create the output root (we never made it to MkdirAll).
func TestSnapshotExportEmptyStore(t *testing.T) {
	ctx := context.Background()
	store, err := history.Open(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	outRoot := filepath.Join(t.TempDir(), "snapshots")
	written, streams, err := runSnapshotExport(ctx, store, outRoot)
	if err != nil {
		t.Fatal(err)
	}
	if written != 0 || streams != 0 {
		t.Errorf("written=%d streams=%d, want 0 / 0", written, streams)
	}
	if _, err := os.Stat(outRoot); !os.IsNotExist(err) {
		t.Errorf("outRoot exists after empty export: err=%v", err)
	}
}
