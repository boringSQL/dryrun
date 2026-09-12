package snapdiff

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/diff"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// exactly one field set, matching the pair's kind
type NodeDelta struct {
	Activity *diff.ActivityDelta
	Query    *diff.QueryDelta
}

// schemas are caller-resolved: CLI uses each row's own schema_ref, correlate the moment's
func DiffNodePair(from, to history.StoredSnapshot, fromSchema, toSchema *snapshot.SchemaSnapshot) (NodeDelta, error) {
	fk, tk := from.Kind(), to.Kind()
	// tag only: a label mismatch is DiffQueryStats' to judge (Incomparable)
	if fk.Tag != tk.Tag {
		return NodeDelta{}, fmt.Errorf("not comparable: %s and %s", fk, tk)
	}
	switch fk.Tag {
	case history.KindActivity:
		fromA, toA := from.AsActivity(), to.AsActivity()
		// all-or-nothing: rolling up one side makes its partitioned parents
		// read as zero on the other, fabricating huge deltas
		if fromSchema != nil && toSchema != nil {
			fromA, toA = snapshot.RollUpActivitySnapshot(fromA, fromSchema), snapshot.RollUpActivitySnapshot(toA, toSchema)
		}
		d, err := diff.DiffActivity(fromA, toA)
		return NodeDelta{Activity: d}, err
	case history.KindQuery:
		d, err := diff.DiffQueryStats(from.AsQueryStats(), to.AsQueryStats())
		return NodeDelta{Query: d}, err
	default:
		return NodeDelta{}, fmt.Errorf("%s is not a per-node stream", fk)
	}
}

// nil when the row carries no schema_ref or the schema is not in the store
func RowSchema(ctx context.Context, store *history.Store, key history.SnapshotKey, a *snapshot.ActivityStatsSnapshot) *snapshot.SchemaSnapshot {
	if a == nil || a.SchemaRefHash == "" {
		return nil
	}
	snap, err := store.GetSchemaByExactHash(ctx, key, a.SchemaRefHash)
	if err != nil {
		slog.Debug("activity rollup skipped: schema not resolved", "schema_ref_hash", a.SchemaRefHash, "err", err)
		return nil
	}
	return snap
}
