package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/datamask"
	"github.com/boringsql/dryrun/internal/history"
)

// maskResolver loads the masking policy for a key; nil means no masking
type maskResolver func(history.SnapshotKey) (*datamask.Policy, error)

// KindCounts splits a per-kind sync result into work done vs work skipped.
type KindCounts struct {
	Copied   int
	UpToDate int
}

type SyncOutcome struct {
	Key      history.SnapshotKey
	Schema   KindCounts
	Planner  KindCounts
	Activity KindCounts
}

func snapshotPushCmd() *cobra.Command {
	var (
		toPath    string
		all       bool
		historyDB string
	)
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Push snapshots from history.db to a filesystem store",
		RunE: func(cmd *cobra.Command, args []string) error {
			if toPath == "" {
				return fmt.Errorf("--to-path is required")
			}
			src, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer src.Close()
			dst, err := history.NewFilesystemStore(toPath)
			if err != nil {
				return err
			}
			// push masks planner stats on the way out; --no-masks opts out
			return runSync(cmd.Context(), src, dst, all, resolveMaskPolicyForKey, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&toPath, "to-path", "", "destination directory (required)")
	cmd.Flags().BoolVar(&all, "all", false, "sync all keys from the source")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	return cmd
}

func snapshotPullCmd() *cobra.Command {
	var (
		fromPath  string
		all       bool
		historyDB string
	)
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "Pull snapshots from a filesystem store into history.db",
		RunE: func(cmd *cobra.Command, args []string) error {
			if fromPath == "" {
				return fmt.Errorf("--from-path is required")
			}
			src, err := history.NewFilesystemStore(fromPath)
			if err != nil {
				return err
			}
			dst, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer dst.Close()
			// pull imports into the trusted history.db; no masking on the way in
			return runSync(cmd.Context(), src, dst, all, nil, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&fromPath, "from-path", "", "source directory (required)")
	cmd.Flags().BoolVar(&all, "all", false, "sync all keys from the source")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	return cmd
}

// runSync resolves the key set and drives syncKeys; --all takes src.ListKeys,
// otherwise scope is the resolved profile key. resolveMask nil means no masking
func runSync(ctx context.Context, src, dst history.SnapshotStore, all bool, resolveMask maskResolver, w io.Writer) error {
	var keys []history.SnapshotKey
	if all {
		ks, err := src.ListKeys(ctx)
		if err != nil {
			return err
		}
		keys = ks
	} else {
		keys = []history.SnapshotKey{resolveSnapshotKey()}
	}

	outs, err := syncKeys(ctx, src, dst, keys, resolveMask)
	if err != nil {
		return err
	}
	printSyncOutcomes(w, outs)
	return nil
}

// syncKeys diffs src vs dst by content_hash per kind and copies the gap
// goes from schema, planner, activity to FilesystemStore
func syncKeys(ctx context.Context, src, dst history.SnapshotStore, keys []history.SnapshotKey, resolveMask maskResolver) ([]SyncOutcome, error) {
	out := make([]SyncOutcome, 0, len(keys))
	for _, key := range keys {
		// per-key so multi-database picks the right masks block; broken file fails fast
		var policy *datamask.Policy
		if resolveMask != nil {
			p, err := resolveMask(key)
			if err != nil {
				return out, fmt.Errorf("load masks for %s/%s: %w", key.ProjectID, key.DatabaseID, err)
			}
			policy = p
		}
		o := SyncOutcome{Key: key}
		for _, kind := range kindOrder() {
			c, err := syncKind(ctx, src, dst, key, kind, policy)
			if err != nil {
				return out, fmt.Errorf("sync %s/%s %s: %w",
					key.ProjectID, key.DatabaseID, kind, err)
			}
			switch kind.Tag {
			case history.KindSchema:
				o.Schema = c
			case history.KindPlanner:
				o.Planner = c
			case history.KindActivity:
				o.Activity = c
			}
		}
		out = append(out, o)
	}
	return out, nil
}

// kindOrder pins the schema -> planner -> activity sequence; activity uses
// an empty NodeLabel so List returns every node's row in one pass.
func kindOrder() []history.SnapshotKind {
	return []history.SnapshotKind{
		history.SchemaKind(),
		history.PlannerKind(),
		history.ActivityKind(""),
	}
}

// syncKind copies one stream; non-nil policy masks planner snapshots in place
func syncKind(ctx context.Context, src, dst history.SnapshotStore, key history.SnapshotKey, kind history.SnapshotKind, policy *datamask.Policy) (KindCounts, error) {
	var counts KindCounts

	srcList, err := src.List(ctx, key, kind, history.TimeRange{})
	if err != nil {
		return counts, err
	}
	if len(srcList) == 0 {
		return counts, nil
	}

	dstList, err := dst.List(ctx, key, kind, history.TimeRange{})
	if err != nil {
		return counts, err
	}
	// dedup gate; content_hash is stable across stores, so set membership
	// is enough to decide whether to copy.
	have := make(map[string]struct{}, len(dstList))
	for _, s := range dstList {
		have[s.ContentHash] = struct{}{}
	}

	maskingPlanner := kind.Tag == history.KindPlanner && policy != nil

	for _, s := range srcList {
		if !maskingPlanner {
			if _, ok := have[s.ContentHash]; ok {
				counts.UpToDate++
				continue
			}
		}
		// summary carries the resolved kind (with NodeLabel for activity)
		// so Get works the same for all three streams.
		stored, err := src.Get(ctx, key, s.Kind, history.NewRefHash(s.ContentHash))
		if err != nil {
			return counts, err
		}
		if maskingPlanner {
			if pl := stored.AsPlanner(); pl != nil {
				policy.MaskPlanner(pl)
				if _, ok := have[pl.ContentHash]; ok {
					counts.UpToDate++
					continue
				}
			}
		}
		if _, err := dst.Put(ctx, key, stored); err != nil {
			return counts, err
		}
		counts.Copied++
	}
	return counts, nil
}

func printSyncOutcomes(w io.Writer, outs []SyncOutcome) {
	if len(outs) == 0 {
		fmt.Fprintln(w, "No keys to sync.")
		return
	}
	for _, o := range outs {
		fmt.Fprintf(w, "Sync %s/%s:\n", o.Key.ProjectID, o.Key.DatabaseID)
		fmt.Fprintf(w, "  schema:   %d copied, %d up-to-date\n", o.Schema.Copied, o.Schema.UpToDate)
		fmt.Fprintf(w, "  planner:  %d copied, %d up-to-date\n", o.Planner.Copied, o.Planner.UpToDate)
		fmt.Fprintf(w, "  activity: %d copied, %d up-to-date\n", o.Activity.Copied, o.Activity.UpToDate)
	}
}
