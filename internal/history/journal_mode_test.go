package history

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// rawJournalMode opens the file without any DSN pragma, so it reports what is
// persisted in the database header rather than what this connection asked for.
func rawJournalMode(t *testing.T, path string) string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer db.Close()
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	return mode
}

// A fresh history.db must come up in WAL: capture --due on a cron and a
// long-lived mcp-serve share this file, and WAL is what keeps the readers off
// SQLITE_BUSY.
func TestOpenUsesWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	var mode string
	if err := s.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
}

// journal_mode lives in the file header, not the connection, so an existing
// rollback-journal database written by an older dryrun is converted on open.
func TestOpenConvertsExistingRollbackJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")

	pre, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(DELETE)")
	if err != nil {
		t.Fatalf("seed db: %v", err)
	}
	if _, err := pre.Exec(`CREATE TABLE seed (x INTEGER)`); err != nil {
		t.Fatalf("seed db: %v", err)
	}
	if err := pre.Close(); err != nil {
		t.Fatalf("close seed db: %v", err)
	}
	if mode := rawJournalMode(t, path); mode == "wal" {
		t.Fatalf("seed db already in wal, test proves nothing")
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if mode := rawJournalMode(t, path); mode != "wal" {
		t.Fatalf("journal_mode = %q after open, want wal", mode)
	}
}

// WAL leaves -wal/-shm sidecars next to history.db while a connection is open;
// a clean Close must checkpoint and remove them, so .dryrun/ does not collect
// stale files after every CLI run.
func TestCloseRemovesWALSidecars(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Errorf("%s survived Close (stat err = %v)", filepath.Base(path+suffix), err)
		}
	}
}

// A rust-era db is refused, and Open must not have converted its journal on the
// way to refusing it: the file has to come out exactly as it went in.
func TestOpenLeavesForeignStoreJournalAlone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rust.db")

	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("create raw db: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE snapshots (
		id            INTEGER PRIMARY KEY,
		kind          TEXT NOT NULL,
		content_hash  TEXT NOT NULL,
		snapshot_json TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("seed foreign schema: %v", err)
	}
	raw.Close()

	before := rawJournalMode(t, path)
	if before == "wal" {
		t.Fatalf("seed db already in wal, test proves nothing")
	}

	if s, err := Open(path); err == nil {
		s.Close()
		t.Fatal("Open must refuse a foreign store")
	}

	if after := rawJournalMode(t, path); after != before {
		t.Errorf("journal_mode changed from %q to %q on a store Open refused", before, after)
	}
}

// The point of WAL: a writer commits while a reader holds an open snapshot.
// Under the rollback journal that commit needs an exclusive lock, so it waits
// out busy_timeout and fails — which is capture-on-a-cron losing a snapshot
// because mcp-serve happened to be mid-query.
func TestWriterCommitsWhileReaderHoldsSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")

	writer, err := Open(path)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer writer.Close()

	reader, err := Open(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	ctx := context.Background()

	// an open read transaction that has actually touched the db, so the lock
	// (or the WAL snapshot) is really held
	rtx, err := reader.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}
	defer rtx.Rollback()
	var n int
	if err := rtx.QueryRowContext(ctx, `SELECT count(*) FROM snapshots`).Scan(&n); err != nil {
		t.Fatalf("read inside tx: %v", err)
	}

	// busy_timeout is 5s, so a blocked writer stalls well past this budget
	done := make(chan error, 1)
	go func() {
		_, err := writer.db.ExecContext(ctx,
			`INSERT INTO snapshots (db_url_hash, timestamp, content_hash, database_name, snapshot_json)
			 VALUES ('h', ?, 'c', 'db', '{}')`, time.Now().UTC().Format(time.RFC3339))
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write while a reader holds a snapshot: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write blocked behind an open read transaction; WAL is not in effect")
	}
}
