package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
)

func snapshotListCmd(historyDB *string) *cobra.Command {
	var (
		node, kind, since  string
		limit              int
		jsonOut, prettyOut bool
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List saved snapshots",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openHistoryStore(*historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			var rng history.TimeRange
			if since != "" {
				from, err := parseSince(since)
				if err != nil {
					return err
				}
				rng.From = &from
			}
			if kind != "" {
				if _, err := history.ParseKindTag(kind); err != nil {
					return err
				}
			}

			key := resolveSnapshotKey()
			kinds, err := store.ListKinds(cmd.Context(), key)
			if err != nil {
				return err
			}
			var summaries []history.SnapshotSummary
			for _, k := range kinds {
				if !kindMatches(k, kind, node) {
					continue
				}
				list, err := store.List(cmd.Context(), key, k, rng)
				if err != nil {
					return err
				}
				for i := range list {
					list[i].Kind = k
				}
				summaries = append(summaries, list...)
			}

			shown, total := sortAndLimit(summaries, limit)
			if jsonOut {
				fmt.Println(string(marshalJSON(shown, prettyOut)))
				return nil
			}
			if total == 0 {
				fmt.Println(emptyListMessage(node, kind, since))
				return nil
			}

			typeW := len("TYPE")
			for _, s := range shown {
				if n := len(s.Kind.String()); n > typeW {
					typeW = n
				}
			}
			for _, s := range shown {
				hash := s.ContentHash
				if len(hash) > 16 {
					hash = hash[:16]
				}
				fmt.Printf("%s  %-*s  %s  %s\n",
					s.Timestamp.Format("2006-01-02 15:04:05"),
					typeW, s.Kind.String(), hash, s.Database)
			}
			if len(shown) < total {
				fmt.Printf("\n%d of %d snapshot(s) (--limit %d)\n", len(shown), total, limit)
			} else {
				fmt.Printf("\n%d snapshot(s) total\n", total)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&node, "node", "", "only this node label")
	cmd.Flags().StringVar(&kind, "kind", "", "schema|planner|activity|query")
	cmd.Flags().StringVar(&since, "since", "", "only captures newer than this (7d, 2w, 24h, or 2006-01-02)")
	cmd.Flags().IntVar(&limit, "limit", 0, "show at most N (newest first)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().BoolVar(&prettyOut, "pretty", false, "pretty-print JSON")
	return cmd
}

// Newest first. Timestamps are second-resolution and `capture --all` writes a
// whole fleet inside one second, so ties need a deterministic order or
// --limit returns a different slice each run.
func sortAndLimit(summaries []history.SnapshotSummary, limit int) (shown []history.SnapshotSummary, total int) {
	sort.SliceStable(summaries, func(i, j int) bool {
		a, b := summaries[i], summaries[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.After(b.Timestamp)
		}
		if ka, kb := a.Kind.String(), b.Kind.String(); ka != kb {
			return ka < kb
		}
		return a.ContentHash < b.ContentHash
	})
	total = len(summaries)
	if limit > 0 && total > limit {
		return summaries[:limit], total
	}
	return summaries, total
}

// "nothing here" and "your filter matched nothing" are different problems.
func emptyListMessage(node, kind, since string) string {
	var filters []string
	if node != "" {
		filters = append(filters, "--node "+node)
	}
	if kind != "" {
		filters = append(filters, "--kind "+kind)
	}
	if since != "" {
		filters = append(filters, "--since "+since)
	}
	if len(filters) == 0 {
		return "No snapshots found for this database."
	}
	return fmt.Sprintf("No snapshots match %s.", strings.Join(filters, " "))
}

// Kinds are (tag, node label) pairs, so a node filter only applies to the
// per-node kinds; schema and planner are project-wide and drop out.
func kindMatches(k history.SnapshotKind, kind, node string) bool {
	if kind != "" {
		want, err := history.ParseKindTag(kind)
		if err != nil || k.Tag != want {
			return false
		}
	}
	if node != "" && k.NodeLabel != node {
		return false
	}
	return true
}
