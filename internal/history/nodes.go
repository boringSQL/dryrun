package history

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"
)

type (
	// One node label as it appears in local history. Schema and planner rows
	// carry no node_source, so they are not attributed to a node here.
	NodeSummary struct {
		Label        string    `json:"label"`
		Role         string    `json:"role,omitempty"`
		PgVersion    string    `json:"pg_version,omitempty"`
		Streams      []string  `json:"streams"`
		LastCapture  time.Time `json:"last_capture"`
		ActivityRows int       `json:"activity_rows"`
		QueryRows    int       `json:"query_rows"`
		OrphanRows   int       `json:"orphan_rows,omitempty"`
		// more than one role observed under this label, across both streams:
		// a promotion, or two machines sharing a label
		RoleFlipped bool `json:"role_flipped,omitempty"`
		// a timestamp that would not parse; LastCapture is zero
		CorruptRows int `json:"corrupt_rows,omitempty"`
		// distinct servers seen under this label in the recent window, and
		// whether they alternate (a rotating endpoint) rather than succeed
		// one another (a restart or a replacement)
		Members     int  `json:"members,omitempty"`
		Oscillating bool `json:"oscillating,omitempty"`
		// rows in the window that carry a fingerprint at all: without it
		// "no rotation" is indistinguishable from "no evidence"
		Fingerprinted int `json:"fingerprinted_rows"`
	}

	// which server a capture came from: boot time identifies the member,
	// address only colours the message
	NodeFingerprint struct {
		StartedAt  time.Time
		ServerAddr string
	}

	nodeAgg struct {
		rows        int
		last        time.Time
		orphans     int
		pgVersion   sql.NullString
		sawPrimary  bool
		sawStandby  bool
		corruptRows int
	}
)

var nodeTables = []struct {
	table  string
	stream string
}{
	{"activity_stats", "activity"},
	{"query_stats", "query"},
}

func (s *Store) ListNodes(ctx context.Context, key SnapshotKey) ([]NodeSummary, error) {
	byLabel := map[string]*NodeSummary{}
	// roles observed per label, accumulated across both tables: a flip that
	// splits one role per stream shows in neither table's own counts
	seen := map[string]*struct{ primary, standby bool }{}

	for _, t := range nodeTables {
		aggs, err := s.aggregateNodes(ctx, key, t.table)
		if err != nil {
			return nil, err
		}
		for label, a := range aggs {
			n, ok := byLabel[label]
			if !ok {
				n = &NodeSummary{Label: label}
				byLabel[label] = n
			}
			roles, ok := seen[label]
			if !ok {
				roles = &struct{ primary, standby bool }{}
				seen[label] = roles
			}
			roles.primary = roles.primary || a.sawPrimary
			roles.standby = roles.standby || a.sawStandby

			n.Streams = append(n.Streams, t.stream)
			n.OrphanRows += a.orphans
			n.CorruptRows += a.corruptRows
			if t.stream == "activity" {
				n.ActivityRows = a.rows
			} else {
				n.QueryRows = a.rows
			}
			if a.last.After(n.LastCapture) {
				n.LastCapture = a.last
				n.PgVersion = a.pgVersion.String
			}
		}
	}

	out := make([]NodeSummary, 0, len(byLabel))
	for _, n := range byLabel {
		role, err := s.LatestNodeRole(ctx, key, n.Label)
		if err != nil {
			return nil, err
		}
		n.Role = role
		if r := seen[n.Label]; r != nil {
			n.RoleFlipped = r.primary && r.standby
		}
		fps, err := s.RecentNodeFingerprints(ctx, key, n.Label)
		if err != nil {
			return nil, err
		}
		n.Fingerprinted = len(fps)
		n.Members, n.Oscillating = summariseMembers(fps)
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Label < out[j].Label })
	return out, nil
}

// 2.5.1: oscillation is judged over a window, not against one prior row.
const (
	// exported so the CLI can say how far MEMBERS looks back
	NodeFingerprintWindow = nodeFingerprintRows

	nodeFingerprintRows = 20
	nodeFingerprintAge  = 30 * 24 * time.Hour
)

// Newest first, fingerprinted rows only. Unparseable rows are skipped:
// corruption is not evidence the node moved.
func (s *Store) RecentNodeFingerprints(ctx context.Context, key SnapshotKey, nodeLabel string) ([]NodeFingerprint, error) {
	pid, did := string(key.ProjectID), string(key.DatabaseID)
	since := time.Now().UTC().Add(-nodeFingerprintAge).Format(time.RFC3339)
	// each arm orders and limits on its own so the index supplies the order;
	// sorting the whole window to return 20 rows runs on every capture
	arm := func(table string) string {
		return `SELECT timestamp AS ts, id, ` + nodeJSONExpr("", "$.node.postmaster_start_time") + ` AS started,
		               ` + nodeJSONExpr("", "$.node.server_addr") + ` AS addr
		          FROM ` + table + `
		         WHERE project_id = ? AND database_id = ? AND node_source = ? AND timestamp >= ?
		           AND ` + nodeJSONExpr("", "$.node.postmaster_start_time") + ` IS NOT NULL
		         ORDER BY timestamp DESC, id DESC LIMIT ?`
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT started, addr FROM (
		   SELECT ts, id, 0 AS stream, started, addr FROM (`+arm("activity_stats")+`)
		   UNION ALL
		   SELECT ts, id, 1, started, addr FROM (`+arm("query_stats")+`)
		 ) WHERE started IS NOT NULL
		 ORDER BY ts DESC, stream ASC, id DESC LIMIT ?`,
		pid, did, nodeLabel, since, nodeFingerprintRows,
		pid, did, nodeLabel, since, nodeFingerprintRows,
		nodeFingerprintRows,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []NodeFingerprint
	for rows.Next() {
		var started string
		var addr sql.NullString
		if err := rows.Scan(&started, &addr); err != nil {
			return nil, err
		}
		at, perr := time.Parse(time.RFC3339Nano, started)
		if perr != nil {
			continue
		}
		out = append(out, NodeFingerprint{StartedAt: at, ServerAddr: addr.String})
	}
	return out, rows.Err()
}

// Newest capture for one node's stream, for cadence decisions. Pulled rows
// land in the same tables, so a pull can make a node look recently captured;
// v0.17's captured_locally column is what fixes that.
func (s *Store) LastCaptureAt(ctx context.Context, key SnapshotKey, nodeLabel, stream string) (time.Time, bool, error) {
	table, ok := map[string]string{
		"activity": "activity_stats",
		"query":    "query_stats",
		"planner":  "planner_stats",
		"schema":   "snapshots",
	}[stream]
	if !ok {
		return time.Time{}, false, fmt.Errorf("unknown stream %q", stream)
	}
	q := `SELECT timestamp FROM ` + table + ` WHERE project_id = ? AND database_id = ?`
	args := []any{string(key.ProjectID), string(key.DatabaseID)}
	// only activity and query are per node; schema and planner are project-wide
	if table == "activity_stats" || table == "query_stats" {
		q += " AND node_source = ?"
		args = append(args, nodeLabel)
	}
	q += " ORDER BY timestamp DESC, id DESC LIMIT 1"

	var ts string
	err := s.db.QueryRowContext(ctx, q, args...).Scan(&ts)
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

// Members counts distinct servers; oscillation is one of them recurring after
// another intervened, which is what separates a rotating endpoint from a
// restart.
func summariseMembers(fps []NodeFingerprint) (members int, oscillating bool) {
	var distinct []time.Time
	for i, f := range fps {
		known := -1
		for j, d := range distinct {
			if d.Equal(f.StartedAt) {
				known = j
				break
			}
		}
		if known < 0 {
			distinct = append(distinct, f.StartedAt)
			continue
		}
		// seen before, and something different sat between the two
		if !fps[i-1].StartedAt.Equal(f.StartedAt) {
			oscillating = true
		}
	}
	return len(distinct), oscillating
}

func roleFromExtract(standby sql.NullInt64) string {
	if !standby.Valid {
		return NodeRoleUnknown
	}
	if standby.Int64 == 1 {
		return NodeRoleStandby
	}
	return NodeRolePrimary
}

func (s *Store) aggregateNodes(ctx context.Context, key SnapshotKey, table string) (map[string]*nodeAgg, error) {
	pid, did := string(key.ProjectID), string(key.DatabaseID)
	rows, err := s.db.QueryContext(ctx,
		`SELECT g.node_source, g.n, g.last, g.orphans, g.primaries, g.standbys,
		        `+nodeJSONExpr("newest.", "$.node.pg_version")+`
		   FROM (SELECT node_source,
		                COUNT(*) AS n,
		                MAX(timestamp) AS last,
		                SUM(CASE WHEN schema_ref_hash = '' THEN 1 ELSE 0 END) AS orphans,
		                SUM(CASE WHEN `+nodeStandbyExpr("")+` = 0 THEN 1 ELSE 0 END) AS primaries,
		                SUM(CASE WHEN `+nodeStandbyExpr("")+` = 1 THEN 1 ELSE 0 END) AS standbys
		           FROM `+table+`
		          WHERE project_id = ? AND database_id = ?
		          GROUP BY node_source) AS g
		   JOIN `+table+` AS newest
		     ON newest.id = (SELECT id FROM `+table+`
		                      WHERE project_id = ? AND database_id = ?
		                        AND node_source = g.node_source
		                      ORDER BY timestamp DESC, id DESC LIMIT 1)`,
		pid, did, pid, did)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]*nodeAgg{}
	for rows.Next() {
		var (
			label               string
			a                   nodeAgg
			lastTS              string
			primaries, standbys int
		)
		if err := rows.Scan(&label, &a.rows, &lastTS, &a.orphans,
			&primaries, &standbys, &a.pgVersion); err != nil {
			return nil, err
		}
		a.sawPrimary, a.sawStandby = primaries > 0, standbys > 0
		if ts, err := time.Parse(time.RFC3339, lastTS); err == nil {
			a.last = ts
		} else {
			a.corruptRows++
		}
		out[label] = &a
	}
	return out, rows.Err()
}

func nodeJSONExpr(prefix, path string) string {
	col := prefix + "payload_json"
	return "CASE WHEN json_valid(" + col + ") THEN json_extract(" + col + ", '" + path + "') END"
}

func nodeStandbyExpr(prefix string) string {
	return nodeJSONExpr(prefix, "$.node.is_standby")
}
