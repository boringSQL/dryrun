package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
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

			ctx := cmd.Context()
			keys, err := store.ListKeys(ctx)
			if err != nil {
				return err
			}

			enc, err := zstd.NewWriter(nil)
			if err != nil {
				return err
			}
			defer enc.Close()

			var written int
			for _, key := range keys {
				summaries, err := store.List(ctx, key, history.TimeRange{})
				if err != nil {
					return err
				}
				for _, s := range summaries {
					snap, err := store.Get(ctx, key, history.NewRefHash(s.ContentHash))
					if err != nil {
						return err
					}
					dir := filepath.Join(outRoot, string(key.ProjectID), string(key.DatabaseID))
					if err := os.MkdirAll(dir, 0o755); err != nil {
						return err
					}
					name := fmt.Sprintf("%s-%s.json.zst",
						s.Timestamp.UTC().Format("20060102T150405Z"), s.ContentHash)
					raw, err := json.Marshal(snap)
					if err != nil {
						return err
					}
					compressed := enc.EncodeAll(raw, nil)
					if err := os.WriteFile(filepath.Join(dir, name), compressed, 0o644); err != nil {
						return err
					}
					written++
				}
			}

			fmt.Printf("Exported %d snapshot(s) from %d stream(s) to %s\n",
				written, len(keys), outRoot)
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "output directory (default: .dryrun/snapshots)")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	return cmd
}
