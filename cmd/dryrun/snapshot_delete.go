package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
)

func snapshotDeleteCmd(historyDB *string) *cobra.Command {
	var (
		latest bool
		yes    bool
	)

	c := &cobra.Command{
		Use:   "delete [<hash-prefix>]",
		Short: "Delete one snapshot from the local history",
		Long: `Delete a single snapshot from the local history.db, identified by any
content-hash prefix shown in "snapshot list" (schema, planner, activity, or query).
Deleting a schema snapshot also removes the planner/activity/query stats bound to it.
Targets the local store only, never a remote.

  dryrun snapshot delete <hash-prefix>   delete the snapshot matching the prefix
  dryrun snapshot delete --latest        delete the most recent schema snapshot`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if latest == (len(args) == 1) {
				return fmt.Errorf("pass exactly one of <hash-prefix> or --latest")
			}

			store, err := openHistoryStore(*historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			key := resolveSnapshotKey()

			var target history.SnapshotSummary
			if latest {
				got, err := store.LatestSchema(cmd.Context(), key)
				if err != nil {
					return err
				}
				if got == nil {
					fmt.Println("No snapshots found for this database.")
					return nil
				}
				target = *got
			} else {
				target, err = store.ResolveSnapshot(cmd.Context(), key, args[0])
				if err != nil {
					return err
				}
			}

			hash := target.ContentHash
			if len(hash) > 16 {
				hash = hash[:16]
			}
			fmt.Printf("%s  %s  %s  %s\n",
				target.Timestamp.Format("2006-01-02 15:04:05"),
				target.Kind.String(), hash, target.Database)
			if target.Kind.Tag == history.KindSchema {
				fmt.Println("Planner/activity/query stats captured with it will be removed too, unless kept by an identical snapshot.")
			}

			if !yes && !confirm("Delete this snapshot?") {
				fmt.Println("Aborted.")
				return nil
			}

			res, err := store.DeleteSnapshot(cmd.Context(), key, target)
			if err != nil {
				return err
			}
			fmt.Printf("Deleted %s snapshot %s", target.Kind.String(), hash)
			if res.Cascaded {
				fmt.Printf(" (+%d planner, +%d activity, +%d query)", res.PlannerRemoved, res.ActivityRemoved, res.QueryStatsRemoved)
			}
			fmt.Println()
			return nil
		},
	}
	c.Flags().BoolVar(&latest, "latest", false, "delete the most recent schema snapshot")
	c.Flags().BoolVarP(&yes, "yes", "y", false, "skip confirmation prompt")
	return c
}

func confirm(prompt string) bool {
	fmt.Fprintf(os.Stderr, "%s [y/N] ", prompt)
	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	}
	return false
}
