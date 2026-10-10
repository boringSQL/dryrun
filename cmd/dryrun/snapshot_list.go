package main

import (
	"context"
	"fmt"
	"os"
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
		oneline            bool
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List schema versions and the captures taken under each",
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
				tag, err := history.ParseKindTag(kind)
				if err != nil {
					return err
				}
				kind = tag.String()
			}

			key := resolveSnapshotKey()
			if oneline && (jsonOut || node != "" || kind != "") {
				return fmt.Errorf("--oneline lists schema versions only; it does not combine with --json, --node or --kind")
			}
			// schema rows carry no node, so this pair can match nothing (as in --json)
			if kind == "schema" && node != "" {
				fmt.Println(emptyListMessage(node, kind, since))
				return nil
			}
			if jsonOut {
				return listJSON(cmd.Context(), store, key, node, kind, rng, limit, prettyOut)
			}

			if !cmd.Flags().Changed("limit") {
				limit = defaultLogLimit
			}
			lg, err := buildLog(cmd.Context(), store, key, logFilter{Since: rng.From, Kind: kind, Node: node, Limit: limit})
			if err != nil {
				return err
			}
			if len(lg.Versions) == 0 && len(lg.Orphans) == 0 {
				fmt.Println(emptyListMessage(node, kind, since))
				return nil
			}
			if oneline {
				renderOneline(os.Stdout, lg)
				return nil
			}
			renderLog(os.Stdout, lg, kind != "schema", node != "" || kind != "")
			return nil
		},
	}
	cmd.Flags().StringVar(&node, "node", "", "only this node label")
	cmd.Flags().StringVar(&kind, "kind", "", "schema|planner|activity|query")
	cmd.Flags().StringVar(&since, "since", "", "schema versions and captures since (7d, 2w, 24h, or 2006-01-02)")
	cmd.Flags().IntVar(&limit, "limit", 0, "show at most N schema versions (default 20, 0 for all); with --json, N rows (default all)")
	cmd.Flags().BoolVar(&oneline, "oneline", false, "one line per schema version")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output one row per stored snapshot as JSON (not grouped)")
	cmd.Flags().BoolVar(&prettyOut, "pretty", false, "pretty-print JSON")
	return cmd
}

func listJSON(ctx context.Context, store *history.Store, key history.SnapshotKey, node, kind string, rng history.TimeRange, limit int, pretty bool) error {
	kinds, err := store.ListKinds(ctx, key)
	if err != nil {
		return err
	}
	var summaries []history.SnapshotSummary
	for _, k := range kinds {
		if !kindMatches(k, kind, node) {
			continue
		}
		list, err := store.List(ctx, key, k, rng)
		if err != nil {
			return err
		}
		for i := range list {
			list[i].Kind = k
		}
		summaries = append(summaries, list...)
	}
	shown, _ := sortAndLimit(summaries, limit)
	fmt.Println(string(marshalJSON(shown, pretty)))
	return nil
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
		return "No snapshots found for this database. Capture one with `dryrun snapshot capture`."
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
