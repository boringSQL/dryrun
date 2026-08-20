package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/diff"
)

// default is store-to-store, no DB connection. --live is the one exception
// (schema only) and never captures behind the user's back.
func newDiffCmd(historyDB *string) *cobra.Command {
	var (
		fromHash, toHash     string
		kindFlag, nodeFlag   string
		latest, live         bool
		prettyDiff, jsonDiff bool
		minPct               float64
	)

	c := &cobra.Command{
		Use:   "diff [<from> <to>]",
		Short: "Diff two snapshots of the same kind",
		Long: `Diff two snapshots of the same kind, resolved from history.db (no connection).

  dryrun snapshot diff <from> <to>      diff two snapshots by hash prefix
  dryrun snapshot diff --latest         diff the previous capture against the latest
  dryrun snapshot diff latest~1 latest --kind planner
                                        same, for a specific kind
  dryrun snapshot diff <from> --live    diff a stored snapshot against the database now (schema only)

latest / latest~N name a snapshot of the kind given by --kind (default schema);
hash-prefix operands carry their own kind. Mixing kinds is rejected.`,
		Args: cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openHistoryStore(*historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			ctx := cmd.Context()
			key := resolveSnapshotKey()

			fromTok, toTok, liveTo, err := diffOperands(args, fromHash, toHash, latest, live)
			if err != nil {
				return err
			}

			fromKind, fromRef, err := store.ResolveToken(ctx, key, fromTok, kindFlag, nodeFlag)
			if err != nil {
				return err
			}
			fromSnap, err := store.Get(ctx, key, fromKind, fromRef)
			if err != nil {
				return err
			}

			if liveTo {
				if fromKind.Tag != history.KindSchema {
					return fmt.Errorf("--live is schema-only (it reads the catalog); %s is a %s snapshot\n"+
						"       take a snapshot first and diff store-to-store",
						short(fromTok), fromKind)
				}
				lctx, conn, cerr := connectDBProd()
				if cerr != nil {
					return cerr
				}
				defer conn.Close()
				live, lerr := conn.Introspect(lctx)
				if lerr != nil {
					return lerr
				}
				env, berr := buildSnapshotDiff(ctx, store, key, fromKind, fromSnap, history.WrapSchema(live))
				if berr != nil {
					return berr
				}
				return emitDiff(env, jsonDiff, prettyDiff, minPct)
			}

			toKind, toRef, err := store.ResolveToken(ctx, key, toTok, kindFlag, nodeFlag)
			if err != nil {
				return err
			}
			if fromKind.Tag != toKind.Tag {
				return fmt.Errorf("not comparable: %s is a %s snapshot, %s is a %s snapshot\n"+
					"       diff snapshots of the same kind",
					short(fromTok), fromKind, short(toTok), toKind)
			}
			toSnap, err := store.Get(ctx, key, toKind, toRef)
			if err != nil {
				return err
			}

			env, err := buildSnapshotDiff(ctx, store, key, fromKind, fromSnap, toSnap)
			if err != nil {
				return err
			}
			return emitDiff(env, jsonDiff, prettyDiff, minPct)
		},
	}

	c.Flags().StringVar(&fromHash, "from", "", "source snapshot (compat; prefer positional)")
	c.Flags().StringVar(&toHash, "to", "", "target snapshot (compat; prefer positional)")
	c.Flags().BoolVar(&latest, "latest", false, "diff the previous capture against the latest (latest~1..latest)")
	c.Flags().StringVar(&kindFlag, "kind", "schema", "kind for latest/latest~N operands: schema|planner|activity|query")
	c.Flags().StringVar(&nodeFlag, "node", "", "activity/query node label (when multiple nodes)")
	c.Flags().BoolVar(&live, "live", false, "diff a stored snapshot against the live database (schema only)")
	c.Flags().BoolVar(&jsonDiff, "json", false, "output the SnapshotDiff as JSON")
	c.Flags().BoolVar(&prettyDiff, "pretty", false, "pretty-print JSON")
	c.Flags().Float64Var(&minPct, "min-pct", diff.DefaultMinPct, "console: hide planner/activity rows whose |Δ| is below this percent")
	return c
}

// store-to-store needs two operands; --live needs exactly one.
func diffOperands(args []string, fromHash, toHash string, latest, live bool) (from, to string, liveTo bool, err error) {
	var ops []string
	if len(args) > 0 {
		ops = append(ops, args...)
	} else {
		if fromHash != "" {
			ops = append(ops, fromHash)
		}
		if toHash != "" {
			ops = append(ops, toHash)
		}
		if latest {
			if len(ops) == 0 {
				ops = []string{"latest~1", "latest"}
			} else {
				ops = append(ops, "latest")
			}
		}
	}

	if live {
		if len(ops) != 1 {
			return "", "", false, fmt.Errorf("--live diffs one stored snapshot against the database now; give exactly one snapshot")
		}
		return ops[0], "", true, nil
	}

	// legacy: bare `--from <hash>` (no positional, no --to) meant diff against live
	if len(args) == 0 && fromHash != "" && toHash == "" && !latest {
		return fromHash, "", true, nil
	}

	if len(ops) != 2 {
		return "", "", false, fmt.Errorf("specify two snapshots: `diff <from> <to>`, `--latest`, or `--from/--to` (add `--live` to diff against the database)")
	}
	return ops[0], ops[1], false, nil
}

func buildSnapshotDiff(ctx context.Context, store *history.Store, key history.SnapshotKey, kind history.SnapshotKind, from, to history.StoredSnapshot) (*diff.SnapshotDiff, error) {
	env := &diff.SnapshotDiff{
		FromHash:    from.ContentHash(),
		ToHash:      to.ContentHash(),
		FromTakenAt: from.Timestamp(),
		ToTakenAt:   to.Timestamp(),
	}
	switch kind.Tag {
	case history.KindSchema:
		d, err := diff.DiffSchema(from.AsSchema(), to.AsSchema())
		if err != nil {
			return nil, err
		}
		env.Kind, env.Schema = "schema", d
	case history.KindPlanner:
		d, err := diff.DiffPlanner(from.AsPlanner(), to.AsPlanner())
		if err != nil {
			return nil, err
		}
		env.Kind, env.Planner = "planner", d
	case history.KindActivity:
		fromActivity, toActivity := rollUpActivityPairForDiff(ctx, store, key, from.AsActivity(), to.AsActivity())
		d, err := diff.DiffActivity(fromActivity, toActivity)
		if err != nil {
			return nil, err
		}
		env.Kind, env.Activity = "activity", d
	case history.KindQuery:
		d, err := diff.DiffQueryStats(from.AsQueryStats(), to.AsQueryStats())
		if err != nil {
			return nil, err
		}
		env.Kind, env.Query = "query", d
	default:
		return nil, fmt.Errorf("unsupported diff kind %s", kind)
	}
	return env, nil
}

// All-or-nothing: rolling up only one side would make DiffActivity read the
// other's absent partitioned parents as zero, fabricating huge deltas.
func rollUpActivityPairForDiff(ctx context.Context, store *history.Store, key history.SnapshotKey, from, to *schema.ActivityStatsSnapshot) (*schema.ActivityStatsSnapshot, *schema.ActivityStatsSnapshot) {
	fromSchema, fromOK := resolveActivitySchema(ctx, store, key, from)
	toSchema, toOK := resolveActivitySchema(ctx, store, key, to)
	if !fromOK || !toOK {
		return from, to
	}
	return schema.RollUpActivitySnapshot(from, fromSchema), schema.RollUpActivitySnapshot(to, toSchema)
}

func resolveActivitySchema(ctx context.Context, store *history.Store, key history.SnapshotKey, a *schema.ActivityStatsSnapshot) (*schema.SchemaSnapshot, bool) {
	if a == nil || a.SchemaRefHash == "" {
		return nil, false
	}
	snap, err := store.GetSchemaByExactHash(ctx, key, a.SchemaRefHash)
	if err != nil {
		slog.Debug("activity rollup skipped: schema not resolved", "schema_ref_hash", a.SchemaRefHash, "err", err)
		return nil, false
	}
	return snap, true
}

func emitDiff(env *diff.SnapshotDiff, jsonDiff, prettyDiff bool, minPct float64) error {
	if jsonDiff {
		fmt.Println(string(marshalJSON(env, prettyDiff)))
		return nil
	}
	diff.RenderConsoleMinPct(os.Stdout, env, minPct)
	return nil
}
