package history

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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
		a.Node.Timestamp.UTC().Format(time.RFC3339), string(data),
	)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot save activity stats: %w", err)
	}
	slog.Info("activity stats put", "hash", a.ContentHash, "node", a.Node.Source)
	return PutInserted, nil
}

// idempotent on (project_id, database_id, content_hash); an idle node
// re-captured byte-identically collapses to a no-op
func hasQueryMembers(q *schema.QueryStatsSnapshot) bool {
	for _, e := range q.Queries {
		if len(e.Members) > 0 {
			return true
		}
	}
	return false
}

func (s *Store) PutQueryStats(ctx context.Context, key SnapshotKey, q *schema.QueryStatsSnapshot) (PutOutcome, error) {
	// Pre-Members payloads all digest alike; reject them (pull path only,
	// the migration clears local ones).
	if len(q.Queries) > 0 && !hasQueryMembers(q) {
		return PutInserted, fmt.Errorf(
			"query stats payload predates the raw-row digest (no members) and cannot be hashed; recapture it")
	}
	// Derive a missing hash; keep a mismatched one (sync compares it against
	// the remote digest) and just warn.
	switch recomputed := schema.ComputeQueryStatsContentHash(q); {
	case q.ContentHash == "":
		q.ContentHash = recomputed
	case q.ContentHash != recomputed:
		slog.Warn("query stats content hash does not match its payload",
			"stored", q.ContentHash, "recomputed", recomputed, "node", q.Node.Source)
	}
	data, err := json.Marshal(q)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot serialize query stats: %w", err)
	}

	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO query_stats
		   (project_id, database_id, schema_ref_hash, content_hash, node_source, timestamp, payload_json)
		   VALUES (?, ?, ?, ?, ?, ?, ?)`,
		string(key.ProjectID), string(key.DatabaseID),
		q.SchemaRefHash, q.ContentHash, q.Node.Source,
		q.Node.Timestamp.UTC().Format(time.RFC3339), string(data),
	)
	if err != nil {
		return PutInserted, fmt.Errorf("cannot save query stats: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		slog.Debug("query stats unchanged, skipping put", "hash", q.ContentHash)
		return PutDeduped, nil
	}
	slog.Info("query stats put", "hash", q.ContentHash, "node", q.Node.Source)
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

// latest row per node, joined under the requested schema_ref_hash; id
// tiebreak keeps a node to one row on same-second captures
func (s *Store) GetQueryStats(ctx context.Context, key SnapshotKey, schemaRefHash string) ([]schema.QueryStatsSnapshot, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT payload_json FROM query_stats AS q
		  WHERE project_id = ? AND database_id = ? AND schema_ref_hash = ?
		    AND id = (
		      SELECT id FROM query_stats
		       WHERE project_id = q.project_id
		         AND database_id = q.database_id
		         AND schema_ref_hash = q.schema_ref_hash
		         AND node_source = q.node_source
		       ORDER BY timestamp DESC, id DESC LIMIT 1
		    )
		  ORDER BY node_source`,
		string(key.ProjectID), string(key.DatabaseID), schemaRefHash,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []schema.QueryStatsSnapshot
	for rows.Next() {
		var jsonStr string
		if err := rows.Scan(&jsonStr); err != nil {
			return nil, err
		}
		var q schema.QueryStatsSnapshot
		if err := json.Unmarshal([]byte(jsonStr), &q); err != nil {
			return nil, fmt.Errorf("corrupt query stats JSON: %w", err)
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

// LatestQueryStats returns the newest snapshot per node_source. Unlike
// GetQueryStats it is not schema_ref_hash-scoped, so callers still get the
// latest stats after DDL has moved on. Ties on timestamp (two pushes for the
// same node within the same RFC3339 second) are broken by id so a node never
// contributes more than one row.
func (s *Store) LatestQueryStats(ctx context.Context, key SnapshotKey) ([]schema.QueryStatsSnapshot, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT payload_json FROM query_stats AS q
		  WHERE project_id = ? AND database_id = ?
		    AND id = (
		      SELECT id FROM query_stats
		       WHERE project_id = q.project_id
		         AND database_id = q.database_id
		         AND node_source = q.node_source
		       ORDER BY timestamp DESC, id DESC LIMIT 1
		    )
		  ORDER BY node_source`,
		string(key.ProjectID), string(key.DatabaseID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []schema.QueryStatsSnapshot
	for rows.Next() {
		var jsonStr string
		if err := rows.Scan(&jsonStr); err != nil {
			return nil, err
		}
		var q schema.QueryStatsSnapshot
		if err := json.Unmarshal([]byte(jsonStr), &q); err != nil {
			return nil, fmt.Errorf("corrupt query stats JSON: %w", err)
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

// PreviousQueryStats returns the second-newest snapshot per node_source, the
// capture LatestQueryStats supersedes. Nodes with only one capture are absent.
func (s *Store) PreviousQueryStats(ctx context.Context, key SnapshotKey) ([]schema.QueryStatsSnapshot, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT payload_json FROM query_stats AS q
		  WHERE project_id = ? AND database_id = ?
		    AND id = (
		      SELECT id FROM query_stats
		       WHERE project_id = q.project_id
		         AND database_id = q.database_id
		         AND node_source = q.node_source
		       ORDER BY timestamp DESC, id DESC LIMIT 1 OFFSET 1
		    )
		  ORDER BY node_source`,
		string(key.ProjectID), string(key.DatabaseID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []schema.QueryStatsSnapshot
	for rows.Next() {
		var jsonStr string
		if err := rows.Scan(&jsonStr); err != nil {
			return nil, err
		}
		var q schema.QueryStatsSnapshot
		if err := json.Unmarshal([]byte(jsonStr), &q); err != nil {
			return nil, fmt.Errorf("corrupt query stats JSON: %w", err)
		}
		out = append(out, q)
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

// ErrSnapshotNotFound only when schema is missing; planner/activity can be absent
func (s *Store) GetAnnotated(ctx context.Context, key SnapshotKey, at SnapshotRef) (*schema.AnnotatedSchema, error) {
	snap, err := s.GetSchema(ctx, key, at)
	if err != nil {
		return nil, err
	}
	out := &schema.AnnotatedSchema{Schema: snap}

	if planner, err := s.GetPlanner(ctx, key, snap.ContentHash); err == nil {
		out.Planner = schema.RollUpPartitionSizing(snap, planner)
	} else if !errors.Is(err, ErrSnapshotNotFound) {
		return nil, err
	}

	acts, err := s.GetActivity(ctx, key, snap.ContentHash)
	if err != nil {
		return nil, err
	}
	if len(acts) > 0 {
		nodes := make([]schema.NodeActivity, len(acts))
		for i := range acts {
			tables, indexes := schema.RollUpPartitionActivity(snap, acts[i].Tables, acts[i].Indexes)
			nodes[i] = schema.NodeActivity{
				Node:    acts[i].Node,
				Tables:  tables,
				Indexes: indexes,
			}
		}
		out.Merged = &schema.MergedActivity{Nodes: nodes}
	}

	qs, err := s.GetQueryStats(ctx, key, snap.ContentHash)
	if err != nil {
		return nil, err
	}
	out.QueryStats = qs
	return out, nil
}

// scanHashPrefix runs a git like LIMIT 2 prefix query, returning the single
// match's payload or sql.ErrNoRows; an ambiguous prefix is rejected
func scanHashPrefix(ctx context.Context, db *sql.DB, query string, args ...any) (string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var (
		jsonStr string
		matches int
	)
	for rows.Next() {
		matches++
		if matches == 1 {
			if err := rows.Scan(&jsonStr); err != nil {
				return "", err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	switch matches {
	case 0:
		return "", sql.ErrNoRows
	case 1:
		return jsonStr, nil
	default:
		return "", fmt.Errorf("ambiguous snapshot hash prefix (matches multiple)")
	}
}

// getPlannerRef resolves a SnapshotRef against the planner_stats table.
func (s *Store) getPlannerRef(ctx context.Context, key SnapshotKey, at SnapshotRef) (*schema.PlannerStatsSnapshot, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	var (
		jsonStr string
		err     error
		detail  string
	)
	switch at.Kind {
	case RefLatest:
		detail = "latest planner"
		err = s.db.QueryRowContext(ctx,
			`SELECT payload_json FROM planner_stats
			  WHERE project_id = ? AND database_id = ?
			  ORDER BY timestamp DESC LIMIT 1`,
			pid, did,
		).Scan(&jsonStr)
	case RefAt:
		detail = fmt.Sprintf("planner at-or-before %s", at.At.Format(time.RFC3339))
		err = s.db.QueryRowContext(ctx,
			`SELECT payload_json FROM planner_stats
			  WHERE project_id = ? AND database_id = ? AND timestamp <= ?
			  ORDER BY timestamp DESC LIMIT 1`,
			pid, did, at.At.Format(time.RFC3339),
		).Scan(&jsonStr)
	case RefHash:
		detail = "planner hash " + at.Hash
		// git-style prefix match; ambiguous prefixes are rejected
		jsonStr, err = scanHashPrefix(ctx, s.db,
			`SELECT payload_json FROM planner_stats
			  WHERE project_id = ? AND database_id = ? AND content_hash LIKE ?
			  LIMIT 2`,
			pid, did, at.Hash+"%")
	default:
		return nil, fmt.Errorf("unknown SnapshotRef kind: %d", at.Kind)
	}

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w (%s)", ErrSnapshotNotFound, detail)
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

// getActivityRef resolves a SnapshotRef against activity_stats, optionally
// filtered to a single node_source.
func (s *Store) getActivityRef(ctx context.Context, key SnapshotKey, nodeLabel string, at SnapshotRef) (*schema.ActivityStatsSnapshot, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	base := `SELECT payload_json FROM activity_stats
	          WHERE project_id = ? AND database_id = ?`
	args := []any{pid, did}
	if nodeLabel != "" {
		base += " AND node_source = ?"
		args = append(args, nodeLabel)
	}

	var (
		jsonStr string
		err     error
		detail  string
	)
	switch at.Kind {
	case RefLatest:
		detail = "latest activity"
		err = s.db.QueryRowContext(ctx, base+" ORDER BY timestamp DESC LIMIT 1", args...).Scan(&jsonStr)
	case RefAt:
		detail = fmt.Sprintf("activity at-or-before %s", at.At.Format(time.RFC3339))
		args = append(args, at.At.Format(time.RFC3339))
		err = s.db.QueryRowContext(ctx,
			base+" AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
			args...).Scan(&jsonStr)
	case RefHash:
		detail = "activity hash " + at.Hash
		// git-style prefix match; ambiguous prefixes are rejected
		args = append(args, at.Hash+"%")
		jsonStr, err = scanHashPrefix(ctx, s.db, base+" AND content_hash LIKE ? LIMIT 2", args...)
	default:
		return nil, fmt.Errorf("unknown SnapshotRef kind: %d", at.Kind)
	}

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w (%s)", ErrSnapshotNotFound, detail)
	}
	if err != nil {
		return nil, err
	}
	var a schema.ActivityStatsSnapshot
	if err := json.Unmarshal([]byte(jsonStr), &a); err != nil {
		return nil, fmt.Errorf("corrupt activity stats JSON: %w", err)
	}
	return &a, nil
}

func (s *Store) listPlanner(ctx context.Context, key SnapshotKey, rng TimeRange) ([]SnapshotSummary, error) {
	var (
		sb   strings.Builder
		args []any
	)
	sb.WriteString(`SELECT id, schema_ref_hash, content_hash, timestamp, project_id, database_id
	                  FROM planner_stats WHERE project_id = ? AND database_id = ?`)
	args = append(args, string(key.ProjectID), string(key.DatabaseID))
	if rng.From != nil {
		sb.WriteString(" AND timestamp >= ?")
		args = append(args, rng.From.Format(time.RFC3339))
	}
	if rng.To != nil {
		sb.WriteString(" AND timestamp < ?")
		args = append(args, rng.To.Format(time.RFC3339))
	}
	sb.WriteString(" ORDER BY timestamp DESC")

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SnapshotSummary
	for rows.Next() {
		var (
			ss    SnapshotSummary
			tsStr string
			pid   sql.NullString
			did   sql.NullString
		)
		if err := rows.Scan(&ss.ID, &ss.SchemaRefHash, &ss.ContentHash, &tsStr, &pid, &did); err != nil {
			return nil, err
		}
		ss.Kind = PlannerKind()
		ss.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
		if pid.Valid {
			v := pid.String
			ss.ProjectID = &v
		}
		if did.Valid {
			v := did.String
			ss.DatabaseID = &v
		}
		out = append(out, ss)
	}
	return out, rows.Err()
}

func (s *Store) listActivity(ctx context.Context, key SnapshotKey, nodeLabel string, rng TimeRange) ([]SnapshotSummary, error) {
	var (
		sb   strings.Builder
		args []any
	)
	sb.WriteString(`SELECT id, schema_ref_hash, content_hash, node_source, timestamp, project_id, database_id
	                  FROM activity_stats WHERE project_id = ? AND database_id = ?`)
	args = append(args, string(key.ProjectID), string(key.DatabaseID))
	if nodeLabel != "" {
		sb.WriteString(" AND node_source = ?")
		args = append(args, nodeLabel)
	}
	if rng.From != nil {
		sb.WriteString(" AND timestamp >= ?")
		args = append(args, rng.From.Format(time.RFC3339))
	}
	if rng.To != nil {
		sb.WriteString(" AND timestamp < ?")
		args = append(args, rng.To.Format(time.RFC3339))
	}
	sb.WriteString(" ORDER BY timestamp DESC")

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SnapshotSummary
	for rows.Next() {
		var (
			ss    SnapshotSummary
			tsStr string
			label string
			pid   sql.NullString
			did   sql.NullString
		)
		if err := rows.Scan(&ss.ID, &ss.SchemaRefHash, &ss.ContentHash, &label, &tsStr, &pid, &did); err != nil {
			return nil, err
		}
		ss.Kind = ActivityKind(label)
		ss.NodeLabel = label
		ss.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
		if pid.Valid {
			v := pid.String
			ss.ProjectID = &v
		}
		if did.Valid {
			v := did.String
			ss.DatabaseID = &v
		}
		out = append(out, ss)
	}
	return out, rows.Err()
}

// getQueryStatsRef resolves a SnapshotRef against query_stats, optionally
// filtered to a single node_source.
func (s *Store) getQueryStatsRef(ctx context.Context, key SnapshotKey, nodeLabel string, at SnapshotRef) (*schema.QueryStatsSnapshot, error) {
	pid := string(key.ProjectID)
	did := string(key.DatabaseID)

	base := `SELECT payload_json FROM query_stats
	          WHERE project_id = ? AND database_id = ?`
	args := []any{pid, did}
	if nodeLabel != "" {
		base += " AND node_source = ?"
		args = append(args, nodeLabel)
	}

	var (
		jsonStr string
		err     error
		detail  string
	)
	switch at.Kind {
	case RefLatest:
		detail = "latest query stats"
		// id tiebreak: same-second captures collide at RFC3339 granularity
		err = s.db.QueryRowContext(ctx, base+" ORDER BY timestamp DESC, id DESC LIMIT 1", args...).Scan(&jsonStr)
	case RefAt:
		detail = fmt.Sprintf("query stats at-or-before %s", at.At.Format(time.RFC3339))
		args = append(args, at.At.Format(time.RFC3339))
		err = s.db.QueryRowContext(ctx,
			base+" AND timestamp <= ? ORDER BY timestamp DESC, id DESC LIMIT 1",
			args...).Scan(&jsonStr)
	case RefHash:
		detail = "query stats hash " + at.Hash
		args = append(args, at.Hash+"%")
		jsonStr, err = scanHashPrefix(ctx, s.db, base+" AND content_hash LIKE ? LIMIT 2", args...)
	default:
		return nil, fmt.Errorf("unknown SnapshotRef kind: %d", at.Kind)
	}

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w (%s)", ErrSnapshotNotFound, detail)
	}
	if err != nil {
		return nil, err
	}
	var q schema.QueryStatsSnapshot
	if err := json.Unmarshal([]byte(jsonStr), &q); err != nil {
		return nil, fmt.Errorf("corrupt query stats JSON: %w", err)
	}
	return &q, nil
}

func (s *Store) listQueryStats(ctx context.Context, key SnapshotKey, nodeLabel string, rng TimeRange) ([]SnapshotSummary, error) {
	var (
		sb   strings.Builder
		args []any
	)
	sb.WriteString(`SELECT id, schema_ref_hash, content_hash, node_source, timestamp, project_id, database_id
	                  FROM query_stats WHERE project_id = ? AND database_id = ?`)
	args = append(args, string(key.ProjectID), string(key.DatabaseID))
	if nodeLabel != "" {
		sb.WriteString(" AND node_source = ?")
		args = append(args, nodeLabel)
	}
	if rng.From != nil {
		sb.WriteString(" AND timestamp >= ?")
		args = append(args, rng.From.Format(time.RFC3339))
	}
	if rng.To != nil {
		sb.WriteString(" AND timestamp < ?")
		args = append(args, rng.To.Format(time.RFC3339))
	}
	sb.WriteString(" ORDER BY timestamp DESC")

	rows, err := s.db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SnapshotSummary
	for rows.Next() {
		var (
			ss    SnapshotSummary
			tsStr string
			label string
			pid   sql.NullString
			did   sql.NullString
		)
		if err := rows.Scan(&ss.ID, &ss.SchemaRefHash, &ss.ContentHash, &label, &tsStr, &pid, &did); err != nil {
			return nil, err
		}
		ss.Kind = QueryKind(label)
		ss.NodeLabel = label
		ss.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
		if pid.Valid {
			v := pid.String
			ss.ProjectID = &v
		}
		if did.Valid {
			v := did.String
			ss.DatabaseID = &v
		}
		out = append(out, ss)
	}
	return out, rows.Err()
}

const (
	NodeRoleUnknown = ""
	NodeRolePrimary = "primary"
	NodeRoleStandby = "standby"
)

func (s *Store) LatestNodeRole(ctx context.Context, key SnapshotKey, nodeLabel string) (string, error) {
	pid, did := string(key.ProjectID), string(key.DatabaseID)
	var standby sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT standby FROM (
		   SELECT timestamp AS ts, id, 0 AS stream, `+nodeStandbyExpr("")+` AS standby
		     FROM activity_stats WHERE project_id = ? AND database_id = ? AND node_source = ?
		   UNION ALL
		   SELECT timestamp, id, 1, `+nodeStandbyExpr("")+`
		     FROM query_stats WHERE project_id = ? AND database_id = ? AND node_source = ?
		 ) ORDER BY ts DESC, stream ASC, id DESC LIMIT 1`,
		pid, did, nodeLabel, pid, did, nodeLabel,
	).Scan(&standby)
	if errors.Is(err, sql.ErrNoRows) {
		return NodeRoleUnknown, nil
	}
	if err != nil {
		return NodeRoleUnknown, err
	}
	// legacy rows lack the field: unknown, never primary
	return roleFromExtract(standby), nil
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
