package history

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// capture_attempts records what this host tried: a capture that dedups writes
// no stats row, so row timestamps alone would keep a quiet stream due forever.
// Local-only -- nothing on the sync or pull path writes it.

// MarkCaptureAttempt records that this host just attempted stream for a node.
// Callers must not record a real capture error, and the clock only moves
// forward -- a backwards write would hold a stream back past its interval.
func (s *Store) MarkCaptureAttempt(ctx context.Context, key SnapshotKey, nodeLabel, stream string, at time.Time) error {
	label, err := attemptLabel(nodeLabel, stream)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO capture_attempts (project_id, database_id, node_label, stream, attempted_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(project_id, database_id, node_label, stream)
		 DO UPDATE SET attempted_at = excluded.attempted_at
		 WHERE excluded.attempted_at > capture_attempts.attempted_at`,
		string(key.ProjectID), string(key.DatabaseID), label, stream,
		formatHistoryTS(at))
	if err != nil {
		return fmt.Errorf("record capture attempt: %w", err)
	}
	return nil
}

// LastCaptureAttemptAt is the newest attempt this host made, whether or not
// it produced a row.
func (s *Store) LastCaptureAttemptAt(ctx context.Context, key SnapshotKey, nodeLabel, stream string) (time.Time, bool, error) {
	label, err := attemptLabel(nodeLabel, stream)
	if err != nil {
		return time.Time{}, false, err
	}
	var ts string
	err = s.db.QueryRowContext(ctx,
		`SELECT attempted_at FROM capture_attempts
		  WHERE project_id = ? AND database_id = ? AND node_label = ? AND stream = ?`,
		string(key.ProjectID), string(key.DatabaseID), label, stream).Scan(&ts)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	at, perr := time.Parse(time.RFC3339, ts)
	if perr != nil {
		return time.Time{}, false, nil
	}
	return at, true, nil
}

// StreamKnown reports whether a stream has a source table here.
func StreamKnown(stream string) bool {
	_, ok := streamSources[stream]
	return ok
}

// A project-scoped stream's clock covers the whole project, so one node's
// result satisfies it for every node.
func StreamIsProjectScoped(stream string) bool {
	src, ok := streamSources[stream]
	return ok && !src.perNode
}

// A project-scoped stream is keyed with an empty label, matching its rows
// being read with no node filter.
func attemptLabel(nodeLabel, stream string) (string, error) {
	src, ok := streamSources[stream]
	if !ok {
		return "", fmt.Errorf("unknown stream %q", stream)
	}
	if !src.perNode {
		return "", nil
	}
	return nodeLabel, nil
}
