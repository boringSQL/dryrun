package history

import (
	"context"
	"database/sql"
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
			n.Streams = append(n.Streams, t.stream)
			n.OrphanRows += a.orphans
			n.CorruptRows += a.corruptRows
			if t.stream == "activity" {
				n.ActivityRows = a.rows
			} else {
				n.QueryRows = a.rows
			}
			if a.sawPrimary && a.sawStandby {
				n.RoleFlipped = true
			}
			if (a.sawPrimary && n.Role == NodeRoleStandby) || (a.sawStandby && n.Role == NodeRolePrimary) {
				n.RoleFlipped = true
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
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Label < out[j].Label })
	return out, nil
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
