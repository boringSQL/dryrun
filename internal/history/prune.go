package history

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	DefaultKeepSchemas = 3
	DefaultKeepPlanner = 3
)

type (
	PruneOptions struct {
		Cutoff time.Time
		// <= 0 skips pruning that kind
		KeepSchemas int
		KeepPlanner int
	}

	PruneResult struct {
		Activity int64
		Query    int64
		Planner  int64
		Schema   int64
		// kept because surviving stats rows still reference the content hash
		SchemaPinned int
		BytesFreed   int64
	}
)

func (r PruneResult) Total() int64 {
	return r.Activity + r.Query + r.Planner + r.Schema
}

// Prune drops history older than opts.Cutoff for one key. Planner runs before
// schema: a surviving planner row pins the schema hash it annotates.
func (s *Store) Prune(ctx context.Context, key SnapshotKey, opts PruneOptions) (PruneResult, error) {
	var out PruneResult

	for _, t := range []struct {
		table string
		n     *int64
	}{
		{"activity_stats", &out.Activity},
		{"query_stats", &out.Query},
	} {
		n, bytes, err := s.pruneNodeSeries(ctx, key, t.table, opts.Cutoff)
		if err != nil {
			return out, fmt.Errorf("prune %s: %w", t.table, err)
		}
		*t.n = n
		out.BytesFreed += bytes
	}

	if opts.KeepPlanner > 0 {
		n, bytes, err := s.prunePlanner(ctx, key, opts.Cutoff, opts.KeepPlanner)
		if err != nil {
			return out, fmt.Errorf("prune planner_stats: %w", err)
		}
		out.Planner = n
		out.BytesFreed += bytes
	}

	if opts.KeepSchemas > 0 {
		n, pinned, bytes, err := s.pruneSchemas(ctx, key, opts.Cutoff, opts.KeepSchemas)
		if err != nil {
			return out, fmt.Errorf("prune snapshots: %w", err)
		}
		out.Schema, out.SchemaPinned = n, pinned
		out.BytesFreed += bytes
	}

	return out, nil
}

// newest row per node survives; timestamps are second-resolution, id breaks ties
func (s *Store) pruneNodeSeries(ctx context.Context, key SnapshotKey, table string, cutoff time.Time) (rows, bytes int64, err error) {
	where := " WHERE project_id = ? AND database_id = ? AND timestamp < ?" +
		" AND id NOT IN (SELECT id FROM " + table + " AS keep" +
		" WHERE project_id = ? AND database_id = ?" +
		" AND id = (SELECT id FROM " + table +
		" WHERE project_id = keep.project_id AND database_id = keep.database_id" +
		" AND node_source = keep.node_source" +
		" ORDER BY timestamp DESC, id DESC LIMIT 1))"
	args := []any{
		string(key.ProjectID), string(key.DatabaseID), cutoff.Format(time.RFC3339),
		string(key.ProjectID), string(key.DatabaseID),
	}
	return s.deleteMeasured(ctx, table, "payload_json", where, args)
}

func (s *Store) prunePlanner(ctx context.Context, key SnapshotKey, cutoff time.Time, keep int) (rows, bytes int64, err error) {
	where := " WHERE project_id = ? AND database_id = ? AND timestamp < ?" +
		" AND id NOT IN (SELECT id FROM planner_stats" +
		" WHERE project_id = ? AND database_id = ?" +
		" ORDER BY timestamp DESC, id DESC LIMIT ?)"
	args := []any{
		string(key.ProjectID), string(key.DatabaseID), cutoff.Format(time.RFC3339),
		string(key.ProjectID), string(key.DatabaseID), keep,
	}
	return s.deleteMeasured(ctx, "planner_stats", "payload_json", where, args)
}

// bytes is the summed payload size of the deleted rows, not reclaimed file size
func (s *Store) deleteMeasured(ctx context.Context, table, payloadCol, where string, args []any) (rows, bytes int64, err error) {
	var size sql.NullInt64
	if err = s.db.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(LENGTH("+payloadCol+")), 0) FROM "+table+where, args...,
	).Scan(&size); err != nil {
		return 0, 0, err
	}
	res, err := s.db.ExecContext(ctx, "DELETE FROM "+table+where, args...)
	if err != nil {
		return 0, 0, err
	}
	rows, _ = res.RowsAffected()
	return rows, size.Int64, nil
}

// Rows still referenced by surviving stats are pinned, not deleted:
// DeleteSchemaSnapshot would cascade that history away.
func (s *Store) pruneSchemas(ctx context.Context, key SnapshotKey, cutoff time.Time, keep int) (rows int64, pinned int, bytes int64, err error) {
	pid, did := string(key.ProjectID), string(key.DatabaseID)

	kept, err := s.scanStrings(ctx,
		`SELECT content_hash FROM snapshots
		  WHERE project_id = ? AND database_id = ?
		    AND (timestamp >= ? OR id IN (SELECT id FROM snapshots
		         WHERE project_id = ? AND database_id = ?
		         ORDER BY timestamp DESC, id DESC LIMIT ?))`,
		pid, did, cutoff.Format(time.RFC3339), pid, did, keep)
	if err != nil {
		return 0, 0, 0, err
	}

	referenced := map[string]bool{}
	for _, table := range []string{"planner_stats", "activity_stats", "query_stats"} {
		hashes, err := s.scanStrings(ctx,
			"SELECT DISTINCT schema_ref_hash FROM "+table+
				" WHERE project_id = ? AND database_id = ?", pid, did)
		if err != nil {
			return 0, 0, 0, err
		}
		for _, h := range hashes {
			referenced[h] = true
		}
	}

	type candidate struct {
		id   int64
		hash string
		size int64
	}
	cand, err := func() ([]candidate, error) {
		r, err := s.db.QueryContext(ctx,
			`SELECT id, content_hash, LENGTH(snapshot_json) FROM snapshots
			  WHERE project_id = ? AND database_id = ? AND timestamp < ?
			    AND id NOT IN (SELECT id FROM snapshots
			         WHERE project_id = ? AND database_id = ?
			         ORDER BY timestamp DESC, id DESC LIMIT ?)
			  ORDER BY timestamp ASC, id ASC`,
			pid, did, cutoff.Format(time.RFC3339), pid, did, keep)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		var out []candidate
		for r.Next() {
			var c candidate
			if err := r.Scan(&c.id, &c.hash, &c.size); err != nil {
				return nil, err
			}
			out = append(out, c)
		}
		return out, r.Err()
	}()
	if err != nil {
		return 0, 0, 0, err
	}

	survivingTwin := map[string]bool{}
	for _, h := range kept {
		survivingTwin[h] = true
	}

	for _, c := range cand {
		if referenced[c.hash] && !survivingTwin[c.hash] {
			pinned++
			continue
		}
		out, err := s.DeleteSchemaSnapshot(ctx, key, SnapshotSummary{ID: c.id})
		if err != nil {
			return rows, pinned, bytes, err
		}
		// guard: any cascade here means the selection above was wrong
		if lost := out.PlannerRemoved + out.ActivityRemoved + out.QueryStatsRemoved; lost > 0 {
			return rows, pinned, bytes, fmt.Errorf(
				"prune cascaded stats for schema %s (planner=%d activity=%d query=%d); this is a bug",
				c.hash, out.PlannerRemoved, out.ActivityRemoved, out.QueryStatsRemoved)
		}
		rows++
		bytes += c.size
	}
	return rows, pinned, bytes, nil
}

func (s *Store) scanStrings(ctx context.Context, query string, args ...any) ([]string, error) {
	r, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var out []string
	for r.Next() {
		var v string
		if err := r.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, r.Err()
}
