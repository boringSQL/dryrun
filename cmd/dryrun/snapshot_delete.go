package main

import (
	"bufio"
	"fmt"
	"log/slog"
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
			// twins are one snapshot to resolve but N rows on disk; delete removes one row only
			twins, err := store.CountContentTwins(cmd.Context(), key, target)
			if err != nil {
				return err
			}
			if twins > 1 {
				fmt.Printf("%d rows carry this content hash (a node captured while unchanged); this deletes the newest one only.\n", twins)
			}
			if target.Kind.Tag == history.KindSchema {
				// advisory: a count we cannot read must not block a delete that would work
				if cascade, err := store.CountCascade(cmd.Context(), key, target); err != nil {
					slog.Debug("cannot count bound stats", "err", err)
				} else {
					fmt.Println(describeCascade(cascade))
				}
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
			if twins > 1 {
				fmt.Printf(" (1 of %d identical rows)", twins)
			}
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

// what the cascade does to the stats bound to a schema snapshot, in one line
func describeCascade(c history.CascadeCounts) string {
	total := c.Total()
	if total == 0 {
		return "No planner/activity/query stats are bound to it."
	}
	var parts []string
	for _, p := range []struct {
		n    int
		name string
	}{
		{c.Planner, "planner"},
		{c.Activity, "activity"},
		{c.QueryStats, "query"},
	} {
		if p.n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", p.n, p.name))
		}
	}
	rows, stays, goes := "rows", "stay", "go"
	if total == 1 {
		rows, stays, goes = "row", "stays", "goes"
	}
	if c.Blocked {
		return fmt.Sprintf("%d bound stats %s %s, kept by the identical snapshot: %s.",
			total, rows, stays, strings.Join(parts, ", "))
	}
	return fmt.Sprintf("%d bound stats %s %s with it: %s.", total, rows, goes, strings.Join(parts, ", "))
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
