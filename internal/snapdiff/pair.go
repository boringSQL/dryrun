package snapdiff

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/diff"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// exactly one of Activity/Query set, matching the pair's kind
type NodeDelta struct {
	Activity *diff.ActivityDelta
	Query    *diff.QueryDelta
	// the capture actually differenced: a rotating label swaps in the to-server's
	// own earlier capture
	From history.StoredSnapshot
	// why From is not the capture asked for; empty when it is
	Note string
}

// schemas are caller-resolved: CLI uses each row's own schema_ref, correlate the
// moment's. A nil store skips pool pairing.
func DiffNodePair(ctx context.Context, store *history.Store, key history.SnapshotKey, from, to history.StoredSnapshot, fromSchema, toSchema *snapshot.SchemaSnapshot) (NodeDelta, error) {
	fk, tk := from.Kind(), to.Kind()
	// tag only: a label mismatch is DiffQueryStats' to judge (Incomparable)
	if fk.Tag != tk.Tag {
		return NodeDelta{}, fmt.Errorf("not comparable: %s and %s", fk, tk)
	}
	if fk.Tag != history.KindActivity && fk.Tag != history.KindQuery {
		return NodeDelta{}, fmt.Errorf("%s is not a per-node stream", fk)
	}

	base, note, refusal, err := poolBaseline(ctx, store, key, from, to)
	if err != nil {
		return NodeDelta{}, err
	}
	if refusal != "" {
		if fk.Tag == history.KindActivity {
			return NodeDelta{From: from, Activity: &diff.ActivityDelta{
				FromHash: from.ContentHash(), ToHash: to.ContentHash(), Incomparable: refusal,
			}}, nil
		}
		return NodeDelta{From: from, Query: &diff.QueryDelta{
			Node: tk.NodeLabel, Window: absDur(to.Timestamp().Sub(from.Timestamp())), Incomparable: refusal,
		}}, nil
	}
	if base != nil {
		from = *base
		// the anchor's schema describes a different row now
		if fromSchema != nil {
			fromSchema = RowSchema(ctx, store, key, from.AsActivity())
		}
	}

	nd := NodeDelta{From: from, Note: note}
	if fk.Tag == history.KindActivity {
		fromA, toA := from.AsActivity(), to.AsActivity()
		// all-or-nothing: rolling up one side makes its partitioned parents
		// read as zero on the other, fabricating huge deltas
		if fromSchema != nil && toSchema != nil {
			fromA, toA = snapshot.RollUpActivitySnapshot(fromA, fromSchema), snapshot.RollUpActivitySnapshot(toA, toSchema)
		}
		d, err := diff.DiffActivity(fromA, toA)
		if note != "" && d != nil && !d.Refused() {
			d.Caveats = append(d.Caveats, note)
		}
		nd.Activity = d
		return nd, err
	}
	d, err := diff.DiffQueryStats(from.AsQueryStats(), to.AsQueryStats())
	if note != "" && d != nil && d.Incomparable == "" {
		d.Caveats = append(d.Caveats, note)
	}
	nd.Query = d
	return nd, err
}

// A rotating label's previous capture is usually another server's. Only an
// oscillating label pairs: a one-way boot change is a restart and keeps today's
// diff with its caveat. Returns a baseline and its note, or a refusal.
func poolBaseline(ctx context.Context, store *history.Store, key history.SnapshotKey, from, to history.StoredSnapshot) (*history.StoredSnapshot, string, string, error) {
	label := to.Kind().NodeLabel
	// pairing is within one label; a mismatch stays DiffQueryStats' refusal
	if store == nil || from.Kind().NodeLabel != label {
		return nil, "", "", nil
	}
	ts := to.Node().PostmasterStartTime
	if ts == nil {
		return nil, "", "", nil
	}
	// an unfingerprinted from still pairs: it is the one side we cannot trust
	if fs := from.Node().PostmasterStartTime; fs != nil && fs.Equal(*ts) {
		return nil, "", "", nil
	}

	upTo := to.Timestamp()
	if from.Timestamp().After(upTo) {
		upTo = from.Timestamp()
	}
	pool, err := store.IsPool(ctx, key, label, upTo)
	if err != nil {
		return lookupFailed(label, err)
	}
	if !pool {
		return nil, "", "", nil
	}
	// pairing looks backwards from to; a reversed pair would subtract servers
	if !from.Timestamp().Before(to.Timestamp()) {
		return nil, "", fmt.Sprintf("label %q rotates between servers: pass the older capture first so both sides are read from one server", label), nil
	}

	base, found, err := store.MemberBaseline(ctx, key, to.Kind(), *ts, to.Timestamp(), from.Timestamp())
	if err != nil {
		return lookupFailed(label, err)
	}
	if !found {
		return nil, "", fmt.Sprintf("label %q rotates between servers: the server that answered at %s (started %s) has no capture of its own among the label's newest captures at or before %s",
			label, utc(to.Timestamp()), utc(*ts), utc(from.Timestamp())), nil
	}
	return &base, fmt.Sprintf("label %q rotates between servers: compared the server that answered at %s (started %s) with its own capture at %s instead of the %s capture",
		label, utc(to.Timestamp()), utc(*ts), utc(base.Timestamp()), utc(from.Timestamp())), "", nil
}

// today's path is known wrong for a rotating label, so an unanswerable check
// refuses; a cancelled request is still an error
func lookupFailed(label string, err error) (*history.StoredSnapshot, string, string, error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, "", "", err
	}
	return nil, "", fmt.Sprintf("could not check whether label %q rotates between servers: %v", label, err), nil
}

func utc(t time.Time) string { return t.UTC().Format(time.RFC3339) }

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
