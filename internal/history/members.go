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

// A rotating label's second-newest capture is usually another server's; pair the newest with its own server's earlier one, or none.
func (s *Store) previousWithinMember(ctx context.Context, key SnapshotKey, prev []schema.QueryStatsSnapshot) ([]schema.QueryStatsSnapshot, error) {
	if len(prev) == 0 {
		return prev, nil
	}
	latest, err := s.LatestQueryStats(ctx, key)
	if err != nil {
		return nil, err
	}
	newest := make(map[string]schema.QueryStatsSnapshot, len(latest))
	for _, l := range latest {
		newest[l.Node.Source] = l
	}

	out := make([]schema.QueryStatsSnapshot, 0, len(prev))
	for _, p := range prev {
		l, ok := newest[p.Node.Source]
		started := l.Node.PostmasterStartTime
		// same server already, or nothing to pair on
		if !ok || started == nil || (p.Node.PostmasterStartTime != nil && p.Node.PostmasterStartTime.Equal(*started)) {
			out = append(out, p)
			continue
		}
		// leave out on failure: keeping p is the cross-server comparison this avoids
		pool, err := s.IsPool(ctx, key, l.Node.Source, l.Node.Timestamp)
		if err != nil {
			if ctxErr(err) {
				return nil, err
			}
			slog.Debug("pool check failed; previous capture left out", "node", l.Node.Source, "error", err)
			continue
		}
		if !pool {
			out = append(out, p)
			continue
		}
		base, found, err := s.MemberBaseline(ctx, key, QueryKind(l.Node.Source), *started, l.Node.Timestamp, l.Node.Timestamp)
		if err != nil {
			if ctxErr(err) {
				return nil, err
			}
			slog.Debug("member baseline failed; previous capture left out", "node", l.Node.Source, "error", err)
			continue
		}
		if found {
			out = append(out, *base.AsQueryStats())
		}
	}
	return out, nil
}

func ctxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// adds the other servers' newest rows for a rotating label; a restart inside
// the window counts twice until it ages out: inflates, never hides use
func (s *Store) activityMembers(ctx context.Context, key SnapshotKey, schemaRef string, newest schema.ActivityStatsSnapshot) ([]schema.ActivityStatsSnapshot, error) {
	started := newest.Node.PostmasterStartTime
	if started == nil {
		return nil, nil
	}
	fps, err := s.fingerprintWindow(ctx, key, newest.Node.Source, newest.Node.Timestamp)
	if err != nil {
		return nil, err
	}
	if _, oscillating := summariseMembers(fps); !oscillating {
		return nil, nil
	}
	var others []time.Time
	for _, f := range fps {
		if f.StartedAt.Equal(*started) || containsTime(others, f.StartedAt) {
			continue
		}
		others = append(others, f.StartedAt)
	}

	// lower bound matches the fingerprint window: a hash returning months
	// later must not pull that server's old counters in
	rows, err := s.db.QueryContext(ctx, activityMembersSQL(),
		string(key.ProjectID), string(key.DatabaseID), newest.Node.Source, schemaRef,
		formatHistoryTS(newest.Node.Timestamp.Add(-nodeFingerprintAge)), formatHistoryTS(newest.Node.Timestamp),
		memberBaselineScan)
	if err != nil {
		return nil, err
	}
	var (
		ids   []int64
		found []time.Time
	)
	for rows.Next() {
		var (
			id int64
			st sql.NullString
		)
		if err := rows.Scan(&id, &st); err != nil {
			rows.Close()
			return nil, err
		}
		if !st.Valid {
			continue
		}
		at, perr := time.Parse(time.RFC3339Nano, st.String)
		if perr != nil || !containsTime(others, at) || containsTime(found, at) {
			continue
		}
		ids = append(ids, id)
		found = append(found, at)
		// every later row parses a full payload for nothing
		if len(found) == len(others) {
			break
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	out := make([]schema.ActivityStatsSnapshot, 0, len(ids))
	for _, id := range ids {
		var payload string
		err := s.db.QueryRowContext(ctx, "SELECT payload_json FROM activity_stats WHERE id = ?", id).Scan(&payload)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var a schema.ActivityStatsSnapshot
		// one unreadable row loses that server, not the others
		if err := json.Unmarshal([]byte(payload), &a); err != nil {
			slog.Debug("corrupt pool server activity skipped", "id", id, "error", err)
			continue
		}
		out = append(out, a)
	}
	return out, nil
}

func activityMembersSQL() string {
	return `SELECT id, ` + nodeJSONExpr("", "$.node.postmaster_start_time") + `
	          FROM activity_stats
	         WHERE project_id = ? AND database_id = ? AND node_source = ? AND schema_ref_hash = ?
	           AND timestamp >= ? AND timestamp <= ?
	         ORDER BY timestamp DESC, id DESC LIMIT ?`
}

func containsTime(ts []time.Time, t time.Time) bool {
	for _, x := range ts {
		if x.Equal(t) {
			return true
		}
	}
	return false
}
