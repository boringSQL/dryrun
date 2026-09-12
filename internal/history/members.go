package history

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

const (
	// a miss parses every payload scanned; unfingerprinted rows count, filtering
	// them in SQL costs the same json_extract
	memberBaselineScan = 100
)

// IsPool: the label's fingerprinted captures up to upTo alternate between
// servers (2.5.1's rule). A one-way change is a restart, not a pool.
func (s *Store) IsPool(ctx context.Context, key SnapshotKey, label string, upTo time.Time) (bool, error) {
	fps, err := s.fingerprintWindow(ctx, key, label, upTo)
	if err != nil {
		return false, err
	}
	_, oscillating := summariseMembers(fps)
	return oscillating, nil
}

// MemberBaseline is the newest capture of kind's label from the server started
// at `started`, before `before` and no later than notAfter. Timestamps are
// whole seconds, so a same-second capture never qualifies.
func (s *Store) MemberBaseline(ctx context.Context, key SnapshotKey, kind SnapshotKind, started, before, notAfter time.Time) (StoredSnapshot, bool, error) {
	var table string
	switch kind.Tag {
	case KindActivity:
		table = "activity_stats"
	case KindQuery:
		table = "query_stats"
	default:
		return StoredSnapshot{}, false, fmt.Errorf("%s is not a per-node stream", kind)
	}

	id, found, err := s.memberBaselineID(ctx, key, table, kind.NodeLabel, started, before, notAfter)
	if err != nil || !found {
		return StoredSnapshot{}, false, err
	}

	var payload string
	err = s.db.QueryRowContext(ctx, "SELECT payload_json FROM "+table+" WHERE id = ?", id).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredSnapshot{}, false, nil
	}
	if err != nil {
		return StoredSnapshot{}, false, err
	}

	if kind.Tag == KindActivity {
		var a schema.ActivityStatsSnapshot
		if err := json.Unmarshal([]byte(payload), &a); err != nil {
			return StoredSnapshot{}, false, fmt.Errorf("corrupt activity stats JSON: %w", err)
		}
		return WrapActivity(&a), true, nil
	}
	var q schema.QueryStatsSnapshot
	if err := json.Unmarshal([]byte(payload), &q); err != nil {
		return StoredSnapshot{}, false, fmt.Errorf("corrupt query stats JSON: %w", err)
	}
	return WrapQueryStats(&q), true, nil
}

// Start times are compared parsed, not as text: the payload keeps the zone
// offset the driver returned, so one instant can be spelled several ways.
func (s *Store) memberBaselineID(ctx context.Context, key SnapshotKey, table, label string, started, before, notAfter time.Time) (int64, bool, error) {
	// whole-second timestamps: "before `before`" is "at or before the previous
	// second", keeping one indexable upper bound
	bound := before.Truncate(time.Second).Add(-time.Second)
	if notAfter.Before(bound) {
		bound = notAfter
	}
	rows, err := s.db.QueryContext(ctx, memberBaselineSQL(table),
		string(key.ProjectID), string(key.DatabaseID), label, formatHistoryTS(bound), memberBaselineScan)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id int64
			st sql.NullString
		)
		if err := rows.Scan(&id, &st); err != nil {
			return 0, false, err
		}
		if !st.Valid {
			continue
		}
		if at, perr := time.Parse(time.RFC3339Nano, st.String); perr == nil && at.Equal(started) {
			return id, true, nil
		}
	}
	return 0, false, rows.Err()
}

func memberBaselineSQL(table string) string {
	return `SELECT id, ` + nodeJSONExpr("", "$.node.postmaster_start_time") + `
	          FROM ` + table + `
	         WHERE project_id = ? AND database_id = ? AND node_source = ? AND timestamp <= ?
	         ORDER BY timestamp DESC, id DESC LIMIT ?`
}
