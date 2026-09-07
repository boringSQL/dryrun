package history

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type (
	// What the store holds for one stream, so a caller can tell whether a
	// question about change over time can be answered from here at all.
	//
	// Rows, deliberately, not "captures": three of the four streams dedup on
	// content (PutSchema skips an unchanged schema; planner and query stats
	// carry a unique content index and keep the FIRST row), so a stream
	// captured hourly on a quiet database stores one row. Activity does not
	// dedup and counts every row, a re-imported one included.
	//
	// Newest is therefore when the content last CHANGED, and LastAttempt is
	// when this host last tried, whether or not a row came of it. The gap
	// between them is a quiet stream, not a stale one — the distinction
	// capture_attempts exists for.
	StreamSpan struct {
		Stream string `json:"stream"`
		Rows   int    `json:"rows"`
		// nil where no stored row carries a readable timestamp
		Oldest *time.Time `json:"oldest,omitempty"`
		Newest *time.Time `json:"newest,omitempty"`
		// The last SUCCESSFUL attempt: MarkCaptureAttempt is not called on a
		// capture error, so a week of failures shows a week-old clock.
		// Local-only, and nil on a store built by `snapshot pull` or written
		// before capture_attempts existed — its absence says nothing about
		// capture cadence either way.
		LastAttempt *time.Time `json:"last_attempt,omitempty"`
		// distinct node labels ever seen, not labels still pushing. nil on the
		// streams whose rows are not node-scoped, where it would be a count of
		// a dimension they do not have -- which is not the statement a
		// node-scoped stream with no rows left makes.
		Nodes *int `json:"nodes,omitempty"`
		// rows whose timestamp will not parse: excluded from the span above,
		// counted here rather than dropped silently, as ListNodes does
		CorruptRows int `json:"corrupt_rows,omitempty"`
	}
)

// capture order, so the list reads the way the pipeline runs
var inventoryStreams = []string{"schema", "planner", "activity", "query"}

// A timestamp SQLite's lexical MIN/MAX may be trusted with. One unparseable row
// sorts above every real one, so an unfiltered MAX returns the garbage and the
// span ends at an unknown instead of at the newest row.
// Deliberately looser than time.Parse -- an RFC3339 value always opens with a
// date, so this admits everything Go can read and a little it cannot. The
// overlap is settled on the Go side, where a bound that will not parse is
// counted corrupt rather than reported as an absent span with no corruption.
const readableTS = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*"

// Inventory reports one span per stream that holds rows for this key, or that
// holds none but was captured (see the retention note below). A stream with
// neither is OMITTED rather than reported as zero: "never captured" and
// "captured nothing" are the same row otherwise, and the absence is the answer.
//
// Aggregates only, no payload read, but each is a range scan over the key's
// rows rather than a seek — cheap against a local file, not free per call.
func (s *Store) Inventory(ctx context.Context, key SnapshotKey) ([]StreamSpan, error) {
	attempts, err := s.lastAttempts(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("inventory attempts: %w", err)
	}

	out := make([]StreamSpan, 0, len(inventoryStreams))
	for _, stream := range inventoryStreams {
		src, ok := streamSources[stream]
		if !ok {
			return nil, fmt.Errorf("stream %q has no source table", stream)
		}
		span, err := s.streamSpan(ctx, key, stream, src.table, src.perNode)
		if err != nil {
			return nil, fmt.Errorf("inventory %s: %w", stream, err)
		}
		span.LastAttempt = attempts[stream]
		// A stream pruned back to nothing that was captured this morning is
		// not an uncaptured stream: retention deletes rows and leaves the
		// attempt clock, so the clock alone keeps the entry.
		if span.Rows == 0 && span.LastAttempt == nil {
			continue
		}
		out = append(out, span)
	}
	return out, nil
}

// table is a caller-side literal, never user input.
func (s *Store) streamSpan(ctx context.Context, key SnapshotKey, stream, table string, perNode bool) (StreamSpan, error) {
	nodes := "0"
	if perNode {
		nodes = "COUNT(DISTINCT node_source)"
	}
	q := `SELECT COUNT(*),
	             SUM(CASE WHEN timestamp GLOB ? THEN 0 ELSE 1 END),
	             MIN(CASE WHEN timestamp GLOB ? THEN timestamp END),
	             MAX(CASE WHEN timestamp GLOB ? THEN timestamp END),
	             ` + nodes + `
	        FROM ` + table + `
	       WHERE project_id = ? AND database_id = ?`

	var (
		span           = StreamSpan{Stream: stream}
		corrupt        sql.NullInt64
		oldest, newest sql.NullString
		nodeCount      int
	)
	err := s.db.QueryRowContext(ctx, q,
		readableTS, readableTS, readableTS,
		string(key.ProjectID), string(key.DatabaseID),
	).Scan(&span.Rows, &corrupt, &oldest, &newest, &nodeCount)
	if err != nil {
		return StreamSpan{}, err
	}
	span.CorruptRows = int(corrupt.Int64)
	if perNode {
		span.Nodes = &nodeCount
	}

	// A bound the SQL admitted and Go cannot read is a corrupt row the count
	// missed: report it, or the span goes absent beside corrupt_rows: 0, which
	// asserts the store is clean.
	unreadable := map[string]bool{}
	span.Oldest = spanBound(oldest, unreadable)
	span.Newest = spanBound(newest, unreadable)
	span.CorruptRows += len(unreadable)
	return span, nil
}

// The newest attempt per stream, folded across node labels: one node still
// capturing is what says the stream is live. It cannot express PARTIAL
// liveness — two of three nodes gone reads as fresh — and Nodes counts labels
// ever seen, so a caller wanting "replica stopped pushing" needs
// LastCaptureAttemptAt per label rather than this.
func (s *Store) lastAttempts(ctx context.Context, key SnapshotKey) (map[string]*time.Time, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT stream, MAX(CASE WHEN attempted_at GLOB ? THEN attempted_at END)
		   FROM capture_attempts
		  WHERE project_id = ? AND database_id = ?
		  GROUP BY stream`,
		readableTS, string(key.ProjectID), string(key.DatabaseID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]*time.Time{}
	for rows.Next() {
		var (
			stream string
			at     sql.NullString
		)
		if err := rows.Scan(&stream, &at); err != nil {
			return nil, err
		}
		if t := spanBound(at, nil); t != nil {
			out[stream] = t
		}
	}
	return out, rows.Err()
}

// unreadable, when given, collects the values that would not parse, so the
// caller can report them rather than letting an absent bound read as clean.
func spanBound(v sql.NullString, unreadable map[string]bool) *time.Time {
	if !v.Valid {
		return nil
	}
	t, ok := parseHistoryTS(v.String)
	if !ok {
		if unreadable != nil {
			unreadable[v.String] = true
		}
		return nil
	}
	return &t
}
