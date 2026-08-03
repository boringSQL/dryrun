package schema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/boringsql/qshape"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	// not installed, not preloaded, or off search_path; callers should skip, not fail the snapshot
	ErrQueryStatsUnavailable = errors.New("pg_stat_statements extension not available")
)

// Sizing + per-column pg_stats; schema_ref ties it back to a DDL snapshot
func CapturePlannerStats(ctx context.Context, pool Querier, schemaRefHash string) (*PlannerStatsSnapshot, error) {
	var database string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil {
		return nil, fmt.Errorf("query current_database: %w", err)
	}

	// Reference counters for ageing relfrozenxid/relminmxid offline.
	// xmax = next xid (pg_current_snapshot doesn't consume one, safe in our read tx).
	// mxid_age('1')+1 = next_multixact, avoiding superuser-gated pg_control_checkpoint;
	// cast before +1 since mxid_age is int4 and nears INT_MAX at wraparound.
	var databaseXid, databaseMxid int64
	if err := pool.QueryRow(ctx,
		"SELECT pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())::text::int8, "+
			"(pg_catalog.mxid_age('1'::xid)::int8 + 1)",
	).Scan(&databaseXid, &databaseMxid); err != nil {
		return nil, fmt.Errorf("query reference counters: %w", err)
	}

	tables, err := fetchPlannerTableSizing(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch table sizing: %w", err)
	}
	indexes, err := fetchPlannerIndexSizing(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch index sizing: %w", err)
	}
	columns, err := fetchPlannerColumnStats(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch column stats: %w", err)
	}
	gucs, err := fetchGUCs(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch gucs: %w", err)
	}

	snap := &PlannerStatsSnapshot{
		FormatVersion: FormatVersion,
		SchemaRefHash: schemaRefHash,
		Database:      database,
		Timestamp:     time.Now().UTC(),
		DatabaseXid:   databaseXid,
		DatabaseMxid:  databaseMxid,
		Tables:        tables,
		Indexes:       indexes,
		Columns:       columns,
		GUCs:          gucs,
	}
	snap.ContentHash = ComputePlannerContentHash(snap)
	return snap, nil
}

// Per-node activity counters; source identifies the producing node
func CaptureActivityStats(ctx context.Context, pool Querier, schemaRefHash, source string) (*ActivityStatsSnapshot, error) {
	node, err := CaptureNodeIdentity(ctx, pool, source)
	if err != nil {
		return nil, err
	}
	tables, err := fetchActivityTables(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch activity tables: %w", err)
	}
	indexes, err := fetchActivityIndexes(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetch activity indexes: %w", err)
	}

	// Unlike query stats (an optional extension), these are core catalogs — always
	// present, PUBLIC-readable — so they run inline. A real error still aborts the
	// shared tx; see the caveat at the CaptureQueryStats call site (cmd/dryrun/init.go).
	slots, slotsOK := fetchReplicationSlots(ctx, pool)
	snap := &ActivityStatsSnapshot{
		FormatVersion:          FormatVersion,
		SchemaRefHash:          schemaRefHash,
		Node:                   *node,
		Tables:                 tables,
		Indexes:                indexes,
		Database:               fetchDatabaseActivity(ctx, pool),
		ReplicationSlots:       slots,
		ReplicationSlotsReadOK: &slotsOK,
		Checkpointer:           fetchCheckpointerActivity(ctx, pool),
	}
	snap.ContentHash = ComputeActivityContentHash(snap)
	return snap, nil
}

// Per-node pg_stat_statements rollup, fingerprinted via qshape; ErrQueryStatsUnavailable if the extension isn't installed
func CaptureQueryStats(ctx context.Context, pool Querier, schemaRefHash, source string) (*QueryStatsSnapshot, error) {
	var installed, hasInfoView, hasToplevel, renamedBlkTime bool
	if err := pool.QueryRow(ctx, q("fetch-pg-stat-statements-installed")).Scan(&installed, &hasInfoView, &hasToplevel, &renamedBlkTime); err != nil {
		return nil, fmt.Errorf("check pg_stat_statements: %w", err)
	}
	if !installed {
		return nil, ErrQueryStatsUnavailable
	}

	node, err := CaptureNodeIdentity(ctx, pool, source)
	if err != nil {
		return nil, err
	}
	// bracket the top-500 fetch: pgss isn't MVCC-consistent with its info view,
	// so only differing before/after values prove a reset mid-capture. Gated on
	// the probe above: capture runs in one tx, and reading a missing view aborts it.
	var infoBefore *QueryStatsInfo
	if hasInfoView {
		infoBefore = fetchPgssInfo(ctx, pool)
	}
	ioTiming := fetchTrackIOTiming(ctx, pool)
	queries, err := fetchQueryStats(ctx, pool, hasToplevel, renamedBlkTime, ioTiming)
	if err != nil {
		// role/session search_path can differ from the earlier to_regclass check.
		// 42703: pgss left at 1.7 by pg_upgrade has no total_exec_time, so the
		// projection itself fails — skip query stats rather than kill the snapshot.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "0A000" || pgErr.Code == "42P01" || pgErr.Code == "42703") {
			return nil, ErrQueryStatsUnavailable
		}
		return nil, fmt.Errorf("fetch query stats: %w", err)
	}
	// The whitelist admits COPY; keep only its literal-free forms.
	clusters, err := qshape.Group(dropUnsafeCopy(queries))
	if err != nil {
		return nil, fmt.Errorf("group query stats: %w", err)
	}

	entries := make([]QueryStatsEntry, len(clusters))
	for i, c := range clusters {
		members := make([]QueryStatsMember, len(c.Members))
		for j, m := range c.Members {
			stddev := m.StddevExecTimeMs
			members[j] = QueryStatsMember{
				QueryID:              m.QueryID,
				Calls:                m.Calls,
				TotalExecTimeMs:      m.TotalExecTimeMs,
				StddevExecTimeMs:     &stddev,
				Rows:                 m.Rows,
				TempBlksRead:         m.TempBlksRead,
				TempBlksWritten:      m.TempBlksWritten,
				SharedBlksHit:        m.SharedBlksHit,
				SharedBlksRead:       m.SharedBlksRead,
				SharedBlksDirtied:    m.SharedBlksDirtied,
				SharedBlksWritten:    m.SharedBlksWritten,
				SharedBlkReadTimeMs:  m.SharedBlkReadTimeMs,
				SharedBlkWriteTimeMs: m.SharedBlkWriteTimeMs,
			}
		}
		entries[i] = QueryStatsEntry{
			Fingerprint:     c.Fingerprint,
			Canonical:       c.Canonical,
			Members:         members,
			Calls:           c.TotalCalls,
			TotalExecTimeMs: c.TotalExecTimeMs,
			Rows:            c.Rows,
			TempBlksRead:    c.TempBlksRead,
			TempBlksWritten: c.TempBlksWritten,
		}
	}

	var infoAfter *QueryStatsInfo
	if hasInfoView {
		infoAfter = fetchPgssInfo(ctx, pool)
	}
	snap := &QueryStatsSnapshot{
		FormatVersion: FormatVersion,
		SchemaRefHash: schemaRefHash,
		QshapeVersion: qshape.GroupingVersion,
		RawRows:       len(queries),
		PgssMax:       fetchPgssMax(ctx, pool),
		PgssTrack:     fetchPgssTrack(ctx, pool),
		TrackIOTiming: ioTiming,
		BlockSize:     fetchBlockSize(ctx, pool),
		InfoBefore:    infoBefore,
		InfoAfter:     infoAfter,
		ToplevelOnly:  hasToplevel,
		Node:          *node,
		Queries:       entries,
	}
	snap.ContentHash = ComputeQueryStatsContentHash(snap)
	return snap, nil
}

func CaptureNodeIdentity(ctx context.Context, pool Querier, source string) (*NodeIdentity, error) {
	var (
		isStandby bool
		pgVersion string
	)
	if err := pool.QueryRow(ctx, q("fetch-node-identity")).Scan(&isStandby, &pgVersion); err != nil {
		return nil, fmt.Errorf("fetch node identity: %w", err)
	}
	return &NodeIdentity{
		Source:    source,
		IsStandby: isStandby,
		PgVersion: pgVersion,
		Timestamp: time.Now().UTC(),
	}, nil
}

func fetchPlannerTableSizing(ctx context.Context, pool Querier) ([]TableSizingEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-planner-table-sizing"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (TableSizingEntry, error) {
		var e TableSizingEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name,
			&e.Sizing.Reltuples, &e.Sizing.Relpages,
			&e.Sizing.TableSize, &e.Sizing.TotalRelationSize,
			&e.Sizing.IndexesSize, &e.Sizing.ToastSize,
			&e.Sizing.RelfrozenXid, &e.Sizing.RelminMxid,
		)
		return e, err
	})
}

func fetchPlannerIndexSizing(ctx context.Context, pool Querier) ([]IndexSizingEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-planner-index-sizing"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (IndexSizingEntry, error) {
		var e IndexSizingEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name, &e.Index,
			&e.Sizing.Relpages, &e.Sizing.Reltuples, &e.Sizing.Size,
		)
		return e, err
	})
}

func fetchPlannerColumnStats(ctx context.Context, pool Querier) ([]ColumnStatsEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-planner-column-stats"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (ColumnStatsEntry, error) {
		var e ColumnStatsEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name, &e.Column,
			&e.Stats.NullFrac, &e.Stats.NDistinct,
			&e.Stats.MostCommonVals, &e.Stats.MostCommonFreqs,
			&e.Stats.HistogramBounds, &e.Stats.Correlation,
			&e.Stats.AvgWidth,
		)
		return e, err
	})
}

// pg_stat_statements_info, PG14+. nil on PG13 or any read failure: absent is
// not zero, or every PG13 row would claim a reset epoch it never had.
func fetchPgssInfo(ctx context.Context, pool Querier) *QueryStatsInfo {
	var info QueryStatsInfo
	if err := pool.QueryRow(ctx, q("fetch-pgss-info")).Scan(&info.StatsReset, &info.Dealloc); err != nil {
		return nil
	}
	return &info
}

// nil when the GUC has no row or the role can't read it
func fetchPgssMax(ctx context.Context, pool Querier) *int {
	var max int
	if err := pool.QueryRow(ctx, q("fetch-pgss-max")).Scan(&max); err != nil {
		return nil
	}
	return &max
}

// block_size is a preset GUC every role can read, so nil here means the query itself
// failed — effectively never. A consumer treats nil as unknown and renders temp blocks as
// blocks rather than assuming 8192 and being silently 2x out on a cluster built otherwise.
func fetchBlockSize(ctx context.Context, pool Querier) *int {
	var size int
	if err := pool.QueryRow(ctx, q("fetch-block-size")).Scan(&size); err != nil {
		return nil
	}
	return &size
}

func fetchPgssTrack(ctx context.Context, pool Querier) *string {
	var track string
	if err := pool.QueryRow(ctx, q("fetch-pgss-track")).Scan(&track); err != nil {
		return nil
	}
	return &track
}

// The toplevel variant filters nested rows in SQL (pgss 1.9+); the plain one
// is for a pgss without the column (PG13, or < 1.9 after pg_upgrade).
func fetchQueryStats(ctx context.Context, pool Querier, hasToplevel, renamedBlkTime bool, ioTiming *bool) ([]qshape.Query, error) {
	name := "fetch-query-stats"
	if hasToplevel {
		name = "fetch-query-stats-toplevel"
	}
	readCol, writeCol := "blk_read_time", "blk_write_time"
	if renamedBlkTime {
		readCol, writeCol = "shared_blk_read_time", "shared_blk_write_time"
	}
	sql := strings.NewReplacer("__READ_TIME__", readCol, "__WRITE_TIME__", writeCol).Replace(q(name))
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	timed := ioTiming == nil || *ioTiming
	return scanAll(rows, func(r pgx.Rows) (qshape.Query, error) {
		var e qshape.Query
		err := r.Scan(&e.QueryID, &e.Calls, &e.Raw, &e.TotalExecTimeMs,
			&e.StddevExecTimeMs, &e.Rows,
			&e.TempBlksRead, &e.TempBlksWritten,
			&e.SharedBlksHit, &e.SharedBlksRead, &e.SharedBlksDirtied, &e.SharedBlksWritten,
			&e.SharedBlkReadTimeMs, &e.SharedBlkWriteTimeMs)
		return stripUntimed(e, timed), err
	})
}

func stripUntimed(e qshape.Query, timed bool) qshape.Query {
	if !timed {
		e.SharedBlkReadTimeMs, e.SharedBlkWriteTimeMs = nil, nil
	}
	return e
}

func fetchTrackIOTiming(ctx context.Context, pool Querier) *bool {
	var on bool
	if err := pool.QueryRow(ctx, q("fetch-track-io-timing")).Scan(&on); err != nil {
		return nil
	}
	return &on
}

func fetchActivityTables(ctx context.Context, pool Querier) ([]TableActivityEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-activity-tables"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (TableActivityEntry, error) {
		var e TableActivityEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name,
			&e.Activity.SeqScan, &e.Activity.SeqTupRead,
			&e.Activity.IdxScan, &e.Activity.IdxTupFetch,
			&e.Activity.NTupIns, &e.Activity.NTupUpd, &e.Activity.NTupDel, &e.Activity.NTupHotUpd,
			&e.Activity.NLiveTup, &e.Activity.NDeadTup, &e.Activity.NModSinceAnalyze,
			&e.Activity.LastVacuum, &e.Activity.LastAutovacuum,
			&e.Activity.LastAnalyze, &e.Activity.LastAutoanalyze,
			&e.Activity.VacuumCount, &e.Activity.AutovacuumCount,
			&e.Activity.AnalyzeCount, &e.Activity.AutoanalyzeCount,
		)
		return e, err
	})
}

func fetchActivityIndexes(ctx context.Context, pool Querier) ([]IndexActivityEntry, error) {
	rows, err := pool.Query(ctx, q("fetch-activity-indexes"))
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(r pgx.Rows) (IndexActivityEntry, error) {
		var e IndexActivityEntry
		err := r.Scan(
			&e.Table.Schema, &e.Table.Name, &e.Index,
			&e.Activity.IdxScan, &e.Activity.IdxTupRead, &e.Activity.IdxTupFetch,
		)
		return e, err
	})
}

// Best-effort like the two below: nil on any error rather than failing the snapshot.
// Debug, not Warn — an unprivileged role would otherwise spam every capture.
func fetchDatabaseActivity(ctx context.Context, pool Querier) *DatabaseActivity {
	var d DatabaseActivity
	if err := pool.QueryRow(ctx, q("fetch-database-stats")).Scan(
		&d.Deadlocks, &d.TempFiles, &d.TempBytes, &d.XactCommit, &d.XactRollback,
		&d.BlksHit, &d.BlksRead, &d.Conflicts, &d.ChecksumFailures, &d.StatsReset,
	); err != nil {
		slog.Debug("pg_stat_database unavailable; capturing without it", "error", err)
		return nil
	}
	return &d
}

// wal_status/safe_wal_size (PG13+) are probed once, the same probe-then-branch shape
// fetchQueryStats uses. ok=false only on read failure — a genuine "no slots" result is
// ok=true with a nil slice, distinct from "never checked".
func fetchReplicationSlots(ctx context.Context, pool Querier) ([]ReplicationSlotActivity, bool) {
	var hasWalStatus bool
	if err := pool.QueryRow(ctx, q("fetch-has-wal-status")).Scan(&hasWalStatus); err != nil {
		slog.Debug("replication slots unavailable; capturing without them", "error", err)
		return nil, false
	}

	name := "fetch-replication-slots-no-wal-status"
	if hasWalStatus {
		name = "fetch-replication-slots"
	}
	rows, err := pool.Query(ctx, q(name))
	if err != nil {
		slog.Debug("replication slots unavailable; capturing without them", "error", err)
		return nil, false
	}

	slots, err := scanAll(rows, func(r pgx.Rows) (ReplicationSlotActivity, error) {
		var s ReplicationSlotActivity
		if hasWalStatus {
			return s, r.Scan(&s.SlotName, &s.SlotType, &s.Active, &s.WalStatus, &s.SafeWalSize)
		}
		return s, r.Scan(&s.SlotName, &s.SlotType, &s.Active)
	})
	if err != nil {
		slog.Debug("replication slots unavailable; capturing without them", "error", err)
		return nil, false
	}
	return slots, true
}

// pg_stat_checkpointer (PG17+) with pg_stat_bgwriter fallback; the renamed counters
// are normalized to one shape, View records which answered.
func fetchCheckpointerActivity(ctx context.Context, pool Querier) *CheckpointerActivity {
	var hasCheckpointer bool
	if err := pool.QueryRow(ctx, q("fetch-has-pg-stat-checkpointer")).Scan(&hasCheckpointer); err != nil {
		slog.Debug("checkpointer stats unavailable; capturing without them", "error", err)
		return nil
	}
	c := &CheckpointerActivity{View: "pg_stat_bgwriter"}
	name := "fetch-checkpointer-stats-legacy"
	if hasCheckpointer {
		c.View = "pg_stat_checkpointer"
		name = "fetch-checkpointer-stats-pg17"
	}
	if err := pool.QueryRow(ctx, q(name)).Scan(&c.CheckpointsTimed, &c.CheckpointsReq, &c.StatsReset); err != nil {
		slog.Debug("checkpointer stats unavailable; capturing without them", "error", err)
		return nil
	}
	return c
}

func FetchIsStandby(ctx context.Context, pool Querier) (bool, error) {
	var b bool
	err := pool.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&b)
	return b, err
}

func FetchCurrentDatabase(ctx context.Context, pool Querier) (string, error) {
	var db string
	err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&db)
	return db, err
}

// superuser-gated; callers treat failure as unknown. text keeps the uint64 opaque.
func FetchSystemIdentifier(ctx context.Context, pool Querier) (string, error) {
	var id string
	err := pool.QueryRow(ctx, "SELECT system_identifier::text FROM pg_catalog.pg_control_system()").Scan(&id)
	return id, err
}
