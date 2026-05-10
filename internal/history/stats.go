package history

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

// idempotent on (schema_ref_hash, content_hash); re-puts collapse to a no-op
func (s *Store) PutPlanner(ctx context.Context, key SnapshotKey, p *schema.PlannerStatsSnapshot) (PutOutcome, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot serialize planner stats: %w", err)
	}

	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO planner_stats
		   (project_id, database_id, schema_ref_hash, content_hash, timestamp, payload_json)
		   VALUES (?, ?, ?, ?, ?, ?)`,
		string(key.ProjectID), string(key.DatabaseID),
		p.SchemaRefHash, p.ContentHash, p.Timestamp.Format(time.RFC3339), string(data),
	)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot save planner stats: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		slog.Debug("planner stats unchanged, skipping put", "hash", p.ContentHash)
		return PutDeduped, nil
	}
	slog.Info("planner stats put", "hash", p.ContentHash, "schema_ref", p.SchemaRefHash)
	return PutInserted, nil
}

// activity is per-node and append-only; replicas write one row per probe cycle
func (s *Store) PutActivity(ctx context.Context, key SnapshotKey, a *schema.ActivityStatsSnapshot) (PutOutcome, error) {
	data, err := json.Marshal(a)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot serialize activity stats: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO activity_stats
		   (project_id, database_id, schema_ref_hash, content_hash, node_source, timestamp, payload_json)
		   VALUES (?, ?, ?, ?, ?, ?, ?)`,
		string(key.ProjectID), string(key.DatabaseID),
		a.SchemaRefHash, a.ContentHash, a.Node.Source,
		a.Node.Timestamp.Format(time.RFC3339), string(data),
	)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot save activity stats: %w", err)
	}
	slog.Info("activity stats put", "hash", a.ContentHash, "node", a.Node.Source)
	return PutInserted, nil
}

func (s *Store) GetPlanner(ctx context.Context, key SnapshotKey, schemaRefHash string) (*schema.PlannerStatsSnapshot, error) {
	var jsonStr string
	err := s.db.QueryRowContext(ctx,
		`SELECT payload_json FROM planner_stats
		  WHERE project_id = ? AND database_id = ? AND schema_ref_hash = ?
		  ORDER BY timestamp DESC LIMIT 1`,
		string(key.ProjectID), string(key.DatabaseID), schemaRefHash,
	).Scan(&jsonStr)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w (planner schema_ref=%s)", ErrSnapshotNotFound, schemaRefHash)
	}
	if err != nil {
		return nil, err
	}

	var p schema.PlannerStatsSnapshot
	if err := json.Unmarshal([]byte(jsonStr), &p); err != nil {
		return nil, fmt.Errorf("corrupt planner stats JSON: %w", err)
	}
	return &p, nil
}

// latest row per node, joined under the requested schema_ref_hash
func (s *Store) GetActivity(ctx context.Context, key SnapshotKey, schemaRefHash string) ([]schema.ActivityStatsSnapshot, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT payload_json FROM activity_stats AS a
		  WHERE project_id = ? AND database_id = ? AND schema_ref_hash = ?
		    AND timestamp = (
		      SELECT MAX(timestamp) FROM activity_stats
		       WHERE project_id = a.project_id
		         AND database_id = a.database_id
		         AND schema_ref_hash = a.schema_ref_hash
		         AND node_source = a.node_source
		    )
		  ORDER BY node_source`,
		string(key.ProjectID), string(key.DatabaseID), schemaRefHash,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []schema.ActivityStatsSnapshot
	for rows.Next() {
		var jsonStr string
		if err := rows.Scan(&jsonStr); err != nil {
			return nil, err
		}
		var a schema.ActivityStatsSnapshot
		if err := json.Unmarshal([]byte(jsonStr), &a); err != nil {
			return nil, fmt.Errorf("corrupt activity stats JSON: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) LatestPlanner(ctx context.Context, key SnapshotKey) (*schema.PlannerStatsSnapshot, error) {
	var jsonStr string
	err := s.db.QueryRowContext(ctx,
		`SELECT payload_json FROM planner_stats
		  WHERE project_id = ? AND database_id = ?
		  ORDER BY timestamp DESC LIMIT 1`,
		string(key.ProjectID), string(key.DatabaseID),
	).Scan(&jsonStr)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w (latest planner)", ErrSnapshotNotFound)
	}
	if err != nil {
		return nil, err
	}

	var p schema.PlannerStatsSnapshot
	if err := json.Unmarshal([]byte(jsonStr), &p); err != nil {
		return nil, fmt.Errorf("corrupt planner stats JSON: %w", err)
	}
	return &p, nil
}

// one row per node, taken at the most recent timestamp per node_source
func (s *Store) LatestActivity(ctx context.Context, key SnapshotKey) ([]schema.ActivityStatsSnapshot, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT payload_json FROM activity_stats AS a
		  WHERE project_id = ? AND database_id = ?
		    AND timestamp = (
		      SELECT MAX(timestamp) FROM activity_stats
		       WHERE project_id = a.project_id
		         AND database_id = a.database_id
		         AND node_source = a.node_source
		    )
		  ORDER BY node_source`,
		string(key.ProjectID), string(key.DatabaseID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []schema.ActivityStatsSnapshot
	for rows.Next() {
		var jsonStr string
		if err := rows.Scan(&jsonStr); err != nil {
			return nil, err
		}
		var a schema.ActivityStatsSnapshot
		if err := json.Unmarshal([]byte(jsonStr), &a); err != nil {
			return nil, fmt.Errorf("corrupt activity stats JSON: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
