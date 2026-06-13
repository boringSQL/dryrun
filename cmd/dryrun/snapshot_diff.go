package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/diff"
)

var latestRefRe = regexp.MustCompile(`^latest(?:~(\d+))?$`)

// Store-to-store is the default (no connection); --live is the lone schema-only
// exception and never silently captures.
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

			fromKind, fromRef, err := resolveDiffToken(ctx, store, key, fromTok, kindFlag, nodeFlag)
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
				lctx, conn, cerr := connectDB()
				if cerr != nil {
					return cerr
				}
				defer conn.Close()
				live, lerr := conn.Introspect(lctx)
				if lerr != nil {
					return lerr
				}
				env, berr := buildSnapshotDiff(fromKind, fromSnap, history.WrapSchema(live))
				if berr != nil {
					return berr
				}
				return emitDiff(env, jsonDiff, prettyDiff, minPct)
			}

			toKind, toRef, err := resolveDiffToken(ctx, store, key, toTok, kindFlag, nodeFlag)
			if err != nil {
				return err
			}
			if fromKind.Tag != toKind.Tag {
				return fmt.Errorf("not comparable: %s is a %s snapshot, %s is a %s snapshot\n"+
					"       diff snapshots of the same kind (schema↔schema, planner↔planner, activity↔activity)",
					short(fromTok), fromKind, short(toTok), toKind)
			}
			toSnap, err := store.Get(ctx, key, toKind, toRef)
			if err != nil {
				return err
			}

			env, err := buildSnapshotDiff(fromKind, fromSnap, toSnap)
			if err != nil {
				return err
			}
			return emitDiff(env, jsonDiff, prettyDiff, minPct)
		},
	}

	c.Flags().StringVar(&fromHash, "from", "", "source snapshot (compat; prefer positional)")
	c.Flags().StringVar(&toHash, "to", "", "target snapshot (compat; prefer positional)")
	c.Flags().BoolVar(&latest, "latest", false, "diff the previous capture against the latest (latest~1..latest)")
	c.Flags().StringVar(&kindFlag, "kind", "schema", "kind for latest/latest~N operands: schema|planner|activity")
	c.Flags().StringVar(&nodeFlag, "node", "", "activity node label (when --kind activity has multiple nodes)")
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

// latest/latest~N takes its kind from --kind; a hash prefix carries its own.
func resolveDiffToken(ctx context.Context, store *history.Store, key history.SnapshotKey, token, kindFlag, nodeFlag string) (history.SnapshotKind, history.SnapshotRef, error) {
	if m := latestRefRe.FindStringSubmatch(token); m != nil {
		kind, err := parseKindFlag(ctx, store, key, kindFlag, nodeFlag)
		if err != nil {
			return history.SnapshotKind{}, history.SnapshotRef{}, err
		}
		if m[1] == "" {
			return kind, history.NewRefLatest(), nil
		}
		n, _ := strconv.Atoi(m[1])
		list, err := store.List(ctx, key, kind, history.TimeRange{})
		if err != nil {
			return history.SnapshotKind{}, history.SnapshotRef{}, err
		}
		if n >= len(list) {
			return history.SnapshotKind{}, history.SnapshotRef{},
				fmt.Errorf("latest~%d: only %d %s snapshot(s) in history", n, len(list), kind)
		}
		return kind, history.NewRefHash(list[n].ContentHash), nil
	}
	kind, err := store.ResolveKind(ctx, key, token)
	if err != nil {
		return history.SnapshotKind{}, history.SnapshotRef{}, err
	}
	return kind, history.NewRefHash(token), nil
}

func parseKindFlag(ctx context.Context, store *history.Store, key history.SnapshotKey, kindFlag, nodeFlag string) (history.SnapshotKind, error) {
	switch strings.ToLower(kindFlag) {
	case "", "schema":
		return history.SchemaKind(), nil
	case "planner":
		return history.PlannerKind(), nil
	case "activity":
		return resolveActivityKind(ctx, store, key, nodeFlag)
	default:
		return history.SnapshotKind{}, fmt.Errorf("unknown --kind %q (use schema|planner|activity)", kindFlag)
	}
}

// --node, else the sole activity node, else error listing the choices
func resolveActivityKind(ctx context.Context, store *history.Store, key history.SnapshotKey, nodeFlag string) (history.SnapshotKind, error) {
	if nodeFlag != "" {
		return history.ActivityKind(nodeFlag), nil
	}
	kinds, err := store.ListKinds(ctx, key)
	if err != nil {
		return history.SnapshotKind{}, err
	}
	var nodes []string
	for _, k := range kinds {
		if k.Tag == history.KindActivity {
			nodes = append(nodes, k.NodeLabel)
		}
	}
	switch len(nodes) {
	case 0:
		return history.SnapshotKind{}, fmt.Errorf("no activity snapshots in history")
	case 1:
		return history.ActivityKind(nodes[0]), nil
	default:
		return history.SnapshotKind{}, fmt.Errorf("multiple activity nodes (%s); pass --node to pick one", strings.Join(nodes, ", "))
	}
}

func buildSnapshotDiff(kind history.SnapshotKind, from, to history.StoredSnapshot) (*diff.SnapshotDiff, error) {
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
		d, err := diff.DiffActivity(from.AsActivity(), to.AsActivity())
		if err != nil {
			return nil, err
		}
		env.Kind, env.Activity = "activity", d
	default:
		return nil, fmt.Errorf("unsupported diff kind %s", kind)
	}
	return env, nil
}

func emitDiff(env *diff.SnapshotDiff, jsonDiff, prettyDiff bool, minPct float64) error {
	if jsonDiff {
		fmt.Println(string(marshalJSON(env, prettyDiff)))
		return nil
	}
	diff.RenderConsoleMinPct(os.Stdout, env, minPct)
	return nil
}
