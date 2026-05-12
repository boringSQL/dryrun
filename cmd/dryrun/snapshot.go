package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

func snapshotExportCmd() *cobra.Command {
	var (
		out       string
		historyDB string
	)

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export all snapshot streams as zstd-compressed JSON files",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			outRoot := out
			if outRoot == "" {
				dataDir, err := history.DefaultDataDir()
				if err != nil {
					return err
				}
				outRoot = filepath.Join(dataDir, "snapshots")
			}

			written, streams, err := runSnapshotExport(cmd.Context(), store, outRoot)
			if err != nil {
				return err
			}
			fmt.Printf("Exported %d snapshot(s) from %d stream(s) to %s\n", written, streams, outRoot)
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "output directory (default: .dryrun/snapshots)")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	return cmd
}

func snapshotActivityCmd() *cobra.Command {
	var (
		from        string
		label       string
		allowOrphan bool
		historyDB   string
	)

	cmd := &cobra.Command{
		Use:   "activity",
		Short: "Capture activity stats from a standby into history",
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

			ctx := context.Background()
			conn, err := schema.Connect(ctx, url)
			if err != nil {
				return err
			}
			defer conn.Close()

			store, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			return runSnapshotActivity(ctx, pgxCapturer{pool: conn.Pool()}, store, resolveSnapshotKey(), activityOptions{
				Label:       label,
				AllowOrphan: allowOrphan,
			})
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "standby connection URL (default: --db)")
	cmd.Flags().StringVar(&label, "label", "", "node label for the activity row (required)")
	cmd.Flags().BoolVar(&allowOrphan, "allow-orphan", false, "permit capture without a bound schema snapshot")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	return cmd
}

type activityOptions struct {
	Label       string
	AllowOrphan bool
}

// activity capture: standby-only, binds to latest schema hash unless --allow-orphan.
func runSnapshotActivity(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, opts activityOptions) error {
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

	bound := schemaRef
	if bound == "" {
		bound = "(orphan)"
	}
	fmt.Fprintf(os.Stderr, "Activity stats captured: label=%s, %d tables, %d indexes (schema=%s)\n",
		opts.Label, len(activity.Tables), len(activity.Indexes), bound)
	return nil
}

// runSnapshotExport drives the export loop against any SnapshotStore so tests
// can seed an in-memory store without going through cobra/flags.
func runSnapshotExport(ctx context.Context, store history.SnapshotStore, outRoot string) (written, streams int, err error) {
	keys, err := store.ListKeys(ctx)
	if err != nil {
		return 0, 0, err
	}

	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return 0, 0, err
	}
	defer enc.Close()

	for _, key := range keys {
		summaries, err := store.List(ctx, key, history.SchemaKind(), history.TimeRange{})
		if err != nil {
			return written, len(keys), err
		}
		for _, s := range summaries {
			stored, err := store.Get(ctx, key, history.SchemaKind(), history.NewRefHash(s.ContentHash))
			if err != nil {
				return written, len(keys), err
			}
			dir := history.BundleDir(outRoot, key)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return written, len(keys), err
			}
			raw, err := json.Marshal(stored.AsSchema())
			if err != nil {
				return written, len(keys), err
			}
			if err := os.WriteFile(history.BundlePath(outRoot, key, s.Timestamp, s.ContentHash), enc.EncodeAll(raw, nil), 0o644); err != nil {
				return written, len(keys), err
			}
			written++
		}
	}
	return written, len(keys), nil
}
