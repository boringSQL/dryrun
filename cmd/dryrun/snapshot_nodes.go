package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
)

const (
	nodeStaleAfter = 7 * 24 * time.Hour
	nodeNoCapture  = "-"
)

func snapshotNodesCmd(historyDB *string) *cobra.Command {
	var jsonOutput, pretty bool

	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "List nodes seen in local history",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openHistoryStore(*historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			// a legacy db never ran migrate(), so the node tables may not exist
			switch store.Compat() {
			case history.CompatLegacy:
				return fmt.Errorf(".dryrun/history.db was created by an older dryrun and cannot be read; " +
					"re-run 'dryrun init' or 'dryrun snapshot pull' to recapture its snapshots")
			case history.CompatNewer:
				return fmt.Errorf(".dryrun/history.db was written by a newer dryrun; upgrade dryrun")
			}

			key := resolveSnapshotKey()
			nodes, err := store.ListNodes(cmd.Context(), key)
			if err != nil {
				return err
			}

			if jsonOutput {
				fmt.Println(string(marshalJSON(nodes, pretty)))
				return nil
			}
			if len(nodes) == 0 {
				fmt.Printf("No nodes in history for %s/%s.\n", key.ProjectID, key.DatabaseID)
				fmt.Println("Capture one with `dryrun snapshot activity --from <url> --label <name>`.")
				return nil
			}
			printNodeTable(nodes)
			fmt.Fprintf(os.Stderr, "\nROWS is activity/query. Schema and planner rows are not node-scoped.\n")
			printNodeWarnings(nodes)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "output as JSON")
	cmd.Flags().BoolVar(&pretty, "pretty", false, "pretty-print JSON")
	return cmd
}

func printNodeTable(nodes []history.NodeSummary) {
	labelW := len("NODE")
	for _, n := range nodes {
		if len(n.Label) > labelW {
			labelW = len(n.Label)
		}
	}

	fmt.Printf("%-*s  %-8s  %-8s  %-16s  %-19s  %s\n",
		labelW, "NODE", "ROLE", "PG", "STREAMS", "LAST CAPTURE", "ROWS")
	for _, n := range nodes {
		role := n.Role
		if role == history.NodeRoleUnknown {
			role = "unknown"
		}
		pg := shortPgVersion(n.PgVersion)
		if pg == "" {
			pg = nodeNoCapture
		}
		last := nodeNoCapture
		if !n.LastCapture.IsZero() {
			last = n.LastCapture.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%-*s  %-8s  %-8s  %-16s  %-19s  %d/%d\n",
			labelW, n.Label, role, pg,
			strings.Join(n.Streams, ","),
			last, n.ActivityRows, n.QueryRows)
	}
}

func printNodeWarnings(nodes []history.NodeSummary) {
	var warnings []string
	for _, n := range nodes {
		if n.RoleFlipped {
			warnings = append(warnings, fmt.Sprintf(
				"%s: captured under more than one role. A promotion, or two nodes sharing a label.", n.Label))
		}
		if n.Role == history.NodeRoleUnknown {
			warnings = append(warnings, fmt.Sprintf(
				"%s: no role recorded; the role guard cannot check this label.", n.Label))
		}
		if n.OrphanRows > 0 {
			warnings = append(warnings, fmt.Sprintf(
				"%s: %d row(s) bound to no schema snapshot.", n.Label, n.OrphanRows))
		}
		if n.CorruptRows > 0 {
			warnings = append(warnings, fmt.Sprintf(
				"%s: unreadable capture timestamp; last capture unknown.", n.Label))
		}
		if age := time.Since(n.LastCapture); !n.LastCapture.IsZero() && age > nodeStaleAfter {
			warnings = append(warnings, fmt.Sprintf(
				"%s: nothing captured in %d days.", n.Label, int(age.Hours()/24)))
		}
	}
	if len(warnings) == 0 {
		return
	}
	fmt.Fprintln(os.Stderr)
	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "warning: %s\n", w)
	}
}

// "PostgreSQL 17.0 on x86_64..." -> "17.0"
func shortPgVersion(v string) string {
	f := strings.Fields(strings.TrimPrefix(v, "PostgreSQL "))
	if len(f) == 0 {
		return ""
	}
	return f[0]
}
