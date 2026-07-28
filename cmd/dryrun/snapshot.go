package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

func snapshotActivityCmd() *cobra.Command {
	var (
		from        string
		label       string
		allowOrphan bool
		historyDB   string
		pushAfter   bool
		pushRemote  string
	)

	cmd := &cobra.Command{
		Use:   "activity",
		Short: "Capture activity stats from a standby into history",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if label == "" {
				return fmt.Errorf("--label is required")
			}
			url := from
			if url == "" {
				url = flagDB
			}
			if url == "" {
				return fmt.Errorf("--from <replica-url> or --db is required")
			}

			ctx, conn, err := connectDBProdFor(url)
			if err != nil {
				return err
			}
			defer conn.Close()

			cap, err := newPgxCapturer(ctx, conn.Pool())
			if err != nil {
				return err
			}
			defer cap.Close(ctx)

			store, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			if err := runSnapshotActivity(ctx, cap, store, resolveSnapshotKey(), captureOptions{
				Label:       label,
				AllowOrphan: allowOrphan,
			}); err != nil {
				return err
			}

			if pushAfter {
				dst, err := resolveSyncStore("", "", pushRemote)
				if err != nil {
					return err
				}
				return runSync(ctx, store, dst, false, fullScope(), os.Stdout)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "standby connection URL (default: --db)")
	cmd.Flags().StringVar(&label, "label", "", "node label for the activity row (required)")
	cmd.Flags().BoolVar(&allowOrphan, "allow-orphan", false, "permit capture without a bound schema snapshot")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	cmd.Flags().BoolVar(&pushAfter, "push", false, "push the snapshot to a remote after capture")
	cmd.Flags().StringVar(&pushRemote, "remote", "", "configured [[remote]] name (with --push)")
	return cmd
}

// Label/AllowOrphan are shared by every node-scoped capture command (activity, query-stats).
type captureOptions struct {
	Label       string
	AllowOrphan bool
}

func snapshotQueryStatsCmd() *cobra.Command {
	var (
		from        string
		label       string
		allowOrphan bool
		historyDB   string
		pushAfter   bool
		pushRemote  string
	)

	cmd := &cobra.Command{
		Use:   "query-stats",
		Short: "Capture pg_stat_statements into history (primary or replica)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if label == "" {
				return fmt.Errorf("--label is required")
			}
			url := from
			if url == "" {
				url = flagDB
			}
			if url == "" {
				return fmt.Errorf("--from <connection-url> or --db is required")
			}

			ctx, conn, err := connectDBProdFor(url)
			if err != nil {
				return err
			}
			defer conn.Close()

			cap, err := newPgxCapturer(ctx, conn.Pool())
			if err != nil {
				return err
			}
			defer cap.Close(ctx)

			store, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			if err := runSnapshotQueryStats(ctx, cap, store, resolveSnapshotKey(), captureOptions{
				Label:       label,
				AllowOrphan: allowOrphan,
			}); err != nil {
				return err
			}

			if pushAfter {
				dst, err := resolveSyncStore("", "", pushRemote)
				if err != nil {
					return err
				}
				return runSync(ctx, store, dst, false, fullScope(), os.Stdout)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "connection URL, primary or standby (default: --db)")
	cmd.Flags().StringVar(&label, "label", "", "node label for the query-stats row (required)")
	cmd.Flags().BoolVar(&allowOrphan, "allow-orphan", false, "permit capture without a bound schema snapshot")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	cmd.Flags().BoolVar(&pushAfter, "push", false, "push the snapshot to a remote after capture")
	cmd.Flags().StringVar(&pushRemote, "remote", "", "configured [[remote]] name (with --push)")
	return cmd
}

// query stats capture: no primary/standby restriction, unlike activity - pg_stat_statements
// is meaningful on either. Binds to the latest schema hash unless --allow-orphan.
func runSnapshotQueryStats(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, opts captureOptions) error {
	schemaRef := ""
	if snap, err := store.GetSchema(ctx, key, history.NewRefLatest()); err == nil && snap != nil {
		schemaRef = snap.ContentHash
	}
	if schemaRef == "" && !opts.AllowOrphan {
		return fmt.Errorf("no prior schema snapshot to bind to; take one first or pass --allow-orphan")
	}

	qs, err := cap.CaptureQueryStats(ctx, schemaRef, opts.Label)
	if err != nil {
		if errors.Is(err, schema.ErrQueryStatsUnavailable) {
			return fmt.Errorf("pg_stat_statements is not available on this node; " +
				"add it to shared_preload_libraries and restart, then CREATE EXTENSION pg_stat_statements")
		}
		return fmt.Errorf("capture query stats: %w", err)
	}
	if _, err := store.PutQueryStats(ctx, key, qs); err != nil {
		return fmt.Errorf("save query stats: %w", err)
	}

	bound := schemaRef
	if bound == "" {
		bound = "(orphan)"
	}
	fmt.Fprintf(os.Stderr, "Query stats captured: label=%s, %d shapes (schema=%s)\n",
		opts.Label, len(qs.Queries), bound)
	if len(qs.Queries) == 0 {
		fmt.Fprintln(os.Stderr, "  (0 shapes: role may lack pg_read_all_stats, or pg_stat_statements was recently reset)")
	}
	return nil
}

// activity capture: standby-only, binds to latest schema hash unless --allow-orphan.
func runSnapshotActivity(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, opts captureOptions) error {
	standby, err := cap.IsStandby(ctx)
	if err != nil {
		return fmt.Errorf("check standby status: %w", err)
	}
	if !standby {
		return dryrun.NewError(dryrun.ErrReplicaCapture,
			"snapshot activity is for standbys; this node is a primary. Re-run on a standby (pg_is_in_recovery() must be true)")
	}

	schemaRef := ""
	if snap, err := store.GetSchema(ctx, key, history.NewRefLatest()); err == nil && snap != nil {
		schemaRef = snap.ContentHash
	}
	if schemaRef == "" && !opts.AllowOrphan {
		return fmt.Errorf("no prior schema snapshot to bind to; take one on the primary first or pass --allow-orphan")
	}

	activity, err := cap.CaptureActivity(ctx, schemaRef, opts.Label)
	if err != nil {
		return fmt.Errorf("capture activity stats: %w", err)
	}
	if _, err := store.PutActivity(ctx, key, activity); err != nil {
		return fmt.Errorf("save activity stats: %w", err)
	}
	if err := captureQueryStatsBestEffort(ctx, cap, store, key, schemaRef, opts.Label); err != nil {
		return err
	}

	bound := schemaRef
	if bound == "" {
		bound = "(orphan)"
	}
	fmt.Fprintf(os.Stderr, "Activity stats captured: label=%s, %d tables, %d indexes (schema=%s)\n",
		opts.Label, len(activity.Tables), len(activity.Indexes), bound)
	return nil
}
