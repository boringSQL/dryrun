package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
)

// latest keeps only the newest take (per node for activity); rng is the --since window.
type pullScope struct {
	latest bool
	rng    history.TimeRange
}

func fullScope() pullScope { return pullScope{} }

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
		toPath, ociRef, remoteName string
		all                        bool
		historyDB                  string
	)
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Push snapshots from history.db to a filesystem store or OCI registry",
		RunE: func(cmd *cobra.Command, args []string) error {
			src, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer src.Close()
			dst, err := resolveSyncStore(toPath, ociRef, remoteName)
			if err != nil {
				return err
			}
			return runSync(cmd.Context(), src, dst, all, fullScope(), os.Stdout)
		},
	}
	cmd.Flags().StringVar(&toPath, "to-path", "", "destination directory")
	cmd.Flags().StringVar(&ociRef, "oci", "", "OCI registry base ref (e.g. ghcr.io/org/dryrun)")
	cmd.Flags().StringVar(&remoteName, "remote", "", "configured [[remote]] name")
	cmd.Flags().BoolVar(&all, "all", false, "sync all keys from the source")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	return cmd
}

func snapshotPullCmd() *cobra.Command {
	var (
		fromPath, ociRef, remoteName string
		all, full                    bool
		since                        string
		historyDB                    string
	)
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "Pull the latest snapshot from a filesystem store or OCI registry into history.db",
		RunE: func(cmd *cobra.Command, args []string) error {
			scope := pullScope{latest: !full}
			if since != "" {
				from, err := parseSince(since)
				if err != nil {
					return err
				}
				scope.rng = history.TimeRange{From: &from}
			}
			src, err := resolveSyncStore(fromPath, ociRef, remoteName)
			if err != nil {
				return err
			}
			dst, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer dst.Close()
			return runSync(cmd.Context(), src, dst, all, scope, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&fromPath, "from-path", "", "source directory")
	cmd.Flags().StringVar(&ociRef, "oci", "", "OCI registry base ref (e.g. ghcr.io/org/dryrun)")
	cmd.Flags().StringVar(&remoteName, "remote", "", "configured [[remote]] name")
	cmd.Flags().BoolVar(&all, "all", false, "sync all keys from the source")
	cmd.Flags().BoolVar(&full, "full", false, "pull the entire history, not just the latest take")
	cmd.Flags().StringVar(&since, "since", "", "only pull snapshots newer than a duration (7d, 2w, 24h) or UTC date (2006-01-02)")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	return cmd
}

// parseSince accepts a relative duration (7d, 2w, 24h) or an absolute date.
func parseSince(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty --since")
	}
	if d, err := parseRelative(s); err == nil {
		return time.Now().Add(-d), nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid --since %q: want duration (7d, 2w, 24h) or date (2006-01-02)", s)
}

// parseRelative extends time.ParseDuration with day/week suffixes it lacks.
func parseRelative(s string) (time.Duration, error) {
	var d time.Duration
	if n := len(s); n >= 2 {
		switch unit := s[n-1]; unit {
		case 'd', 'w':
			val, err := strconv.ParseFloat(s[:n-1], 64)
			if err != nil {
				return 0, err
			}
			hours := val * 24
			if unit == 'w' {
				hours *= 7
			}
			d = time.Duration(hours * float64(time.Hour))
		}
	}
	if d == 0 {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return 0, err
		}
	}
	if d < 0 {
		return 0, fmt.Errorf("negative duration: %s", s)
	}
	return d, nil
}

// explicit flags win; with none, fall back to the profile's configured remote.
func resolveSyncStore(path, ociRef, remoteName string) (history.SnapshotStore, error) {
	switch {
	case ociRef != "" || remoteName != "":
		return buildRemoteStore(ociRef, remoteName)
	case path != "":
		return history.NewFilesystemStore(path)
	default:
		if r := profileRemote(); r != "" {
			return buildRemoteStore("", r)
		}
		return nil, fmt.Errorf("specify --to-path/--from-path, --oci, or --remote")
	}
}

// buildRemoteStore dispatches a configured remote to its backend. A direct --oci
// ref is always OCI; a named [[remote]] selects by its type ("http" is the
// Hindsight registry, "" or "oci" the OCI one).
func buildRemoteStore(ociRef, remoteName string) (history.SnapshotStore, error) {
	if ociRef == "" && remoteName != "" {
		_, cfg, err := loadProjectConfig()
		if err != nil {
			return nil, err
		}
		r, err := cfg.ResolveRemote(remoteName)
		if err != nil {
			return nil, err
		}
		if r.Type == "http" {
			return buildHTTPStore(r.Name, r.Ref, r.TokenEnv)
		}
	}
	return buildOCIStore(ociRef, remoteName)
}

// buildHTTPStore builds a Hindsight (predict) /v1 store. The token comes from
// token_env (default DRYRUN_TOKEN), never from disk.
func buildHTTPStore(name, ref, tokenEnv string) (history.SnapshotStore, error) {
	if ref == "" {
		return nil, fmt.Errorf("remote %q: http remote requires a url in ref", name)
	}
	if tokenEnv == "" {
		tokenEnv = "DRYRUN_TOKEN"
	}
	token := os.Getenv(tokenEnv)
	if token == "" {
		return nil, fmt.Errorf("remote %q: %s is not set", name, tokenEnv)
	}
	return history.NewHTTPStore(history.HTTPConfig{BaseURL: ref, Token: token})
}

// --oci is a direct ref; --remote resolves base+token_env from [[remote]].
func buildOCIStore(ociRef, remoteName string) (history.SnapshotStore, error) {
	base := ociRef
	var tokenEnv string
	if base == "" {
		_, cfg, err := loadProjectConfig()
		if err != nil {
			return nil, err
		}
		r, err := cfg.ResolveRemote(remoteName)
		if err != nil {
			return nil, err
		}
		if r.Type != "" && r.Type != "oci" {
			return nil, fmt.Errorf("remote %q has unsupported type %q", r.Name, r.Type)
		}
		base, tokenEnv = r.Ref, r.TokenEnv
	}
	client, err := history.NewAuthClient(history.AuthConfig{TokenEnv: tokenEnv})
	if err != nil {
		return nil, err
	}
	return history.NewOCIStore(history.OCIConfig{
		Base:      base,
		Client:    client,
		PlainHTTP: isLocalRegistry(base),
		StreamFor: streamMapper(),
	})
}

// profileRemote is the [[remote]] name a profile pins, "" if none/unresolvable.
func profileRemote() string {
	cwd, _ := os.Getwd()
	_, cfg, err := loadProjectConfig()
	if err != nil {
		return ""
	}
	rp, err := cfg.ResolveProfile(nil, nil, nilIfEmpty(flagProfile), cwd)
	if err != nil || rp.Remote == nil {
		return ""
	}
	return *rp.Remote
}

// streamMapper maps each key to its profile's stream override, else StreamSuffix.
func streamMapper() func(history.SnapshotKey) string {
	cwd, _ := os.Getwd()
	_, cfg, err := loadProjectConfig()
	if err != nil {
		return history.StreamSuffix
	}
	overrides := map[history.SnapshotKey]string{}
	for name := range cfg.Profiles {
		n := name
		rp, err := cfg.ResolveProfile(nil, nil, &n, cwd)
		if err != nil {
			continue
		}
		key := rp.SnapshotKey()
		if s := rp.Stream(); s != history.StreamSuffix(key) {
			overrides[key] = s
		}
	}
	return func(k history.SnapshotKey) string {
		if s, ok := overrides[k]; ok {
			return s
		}
		return history.StreamSuffix(k)
	}
}

// local registries (registry:2/zot) speak http, not https
func isLocalRegistry(ref string) bool {
	return strings.HasPrefix(ref, "localhost") ||
		strings.HasPrefix(ref, "127.0.0.1") ||
		strings.HasPrefix(ref, "::1")
}

// --all takes src.ListKeys, otherwise scope is the resolved profile key.
func runSync(ctx context.Context, src, dst history.SnapshotStore, all bool, scope pullScope, w io.Writer) error {
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

	outs, err := syncKeys(ctx, src, dst, keys, scope)
	if err != nil {
		return err
	}
	printSyncOutcomes(w, outs)
	return nil
}

// syncKeys diffs src vs dst by content_hash per kind and copies the gap
// in schema -> planner -> activity order.
func syncKeys(ctx context.Context, src, dst history.SnapshotStore, keys []history.SnapshotKey, scope pullScope) ([]SyncOutcome, error) {
	out := make([]SyncOutcome, 0, len(keys))
	for _, key := range keys {
		o := SyncOutcome{Key: key}
		sel, err := selectSnapshots(ctx, src, key, scope)
		if err != nil {
			return out, fmt.Errorf("select %s/%s: %w", key.ProjectID, key.DatabaseID, err)
		}
		for _, kind := range kindOrder() {
			c, err := syncKindList(ctx, src, dst, key, kind, sel[kind.Tag])
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

// schema dedups, so a recent take can reference a schema older than --since;
// resolve schema by ref over the full list, not by the time window.
func selectSnapshots(ctx context.Context, src history.SnapshotStore, key history.SnapshotKey, scope pullScope) (map[history.SnapshotKindTag][]history.SnapshotSummary, error) {
	planner, err := src.List(ctx, key, history.PlannerKind(), scope.rng)
	if err != nil {
		return nil, err
	}
	activity, err := src.List(ctx, key, history.ActivityKind(""), scope.rng)
	if err != nil {
		return nil, err
	}
	allSchema, err := src.List(ctx, key, history.SchemaKind(), history.TimeRange{})
	if err != nil {
		return nil, err
	}

	if scope.latest {
		planner = newestPerNode(planner)
		activity = newestPerNode(activity)
	}

	need := make(map[string]struct{}, len(planner)+len(activity))
	for _, s := range planner {
		need[s.SchemaRefHash] = struct{}{}
	}
	for _, s := range activity {
		need[s.SchemaRefHash] = struct{}{}
	}
	schema := filterByContentHash(allSchema, need)

	switch {
	case scope.latest && len(schema) == 0:
		// schema-only stream: nothing references a schema, pull the newest one.
		schema = newestPerNode(allSchema)
	case !scope.latest:
		// full mode also keeps schemas captured within the window itself
		// (e.g. a schema-only take), unioned with the referenced ones.
		schema = unionByContentHash(schema, windowed(allSchema, scope.rng))
	}

	return map[history.SnapshotKindTag][]history.SnapshotSummary{
		history.KindSchema:   schema,
		history.KindPlanner:  planner,
		history.KindActivity: activity,
	}, nil
}

// newest summary per node label; equal-second ties break on hash for determinism.
func newestPerNode(list []history.SnapshotSummary) []history.SnapshotSummary {
	best := map[string]history.SnapshotSummary{}
	for _, s := range list {
		cur, ok := best[s.Kind.NodeLabel]
		if !ok || s.Timestamp.After(cur.Timestamp) ||
			(s.Timestamp.Equal(cur.Timestamp) && s.ContentHash > cur.ContentHash) {
			best[s.Kind.NodeLabel] = s
		}
	}
	out := make([]history.SnapshotSummary, 0, len(best))
	for _, s := range best {
		out = append(out, s)
	}
	return out
}

func filterByContentHash(list []history.SnapshotSummary, want map[string]struct{}) []history.SnapshotSummary {
	var out []history.SnapshotSummary
	for _, s := range list {
		if _, ok := want[s.ContentHash]; ok {
			out = append(out, s)
		}
	}
	return out
}

// windowed mirrors the store's range semantics: From inclusive, To exclusive.
func windowed(list []history.SnapshotSummary, rng history.TimeRange) []history.SnapshotSummary {
	if rng.From == nil && rng.To == nil {
		return list
	}
	var out []history.SnapshotSummary
	for _, s := range list {
		if rng.From != nil && s.Timestamp.Before(*rng.From) {
			continue
		}
		if rng.To != nil && !s.Timestamp.Before(*rng.To) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func unionByContentHash(a, b []history.SnapshotSummary) []history.SnapshotSummary {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]history.SnapshotSummary, 0, len(a)+len(b))
	for _, list := range [][]history.SnapshotSummary{a, b} {
		for _, s := range list {
			if _, ok := seen[s.ContentHash]; ok {
				continue
			}
			seen[s.ContentHash] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// activity uses an empty NodeLabel so List returns every node's row in one pass.
func kindOrder() []history.SnapshotKind {
	return []history.SnapshotKind{
		history.SchemaKind(),
		history.PlannerKind(),
		history.ActivityKind(""),
	}
}

// dst is listed unwindowed so rows already present outside scope.rng count as up-to-date.
func syncKindList(ctx context.Context, src, dst history.SnapshotStore, key history.SnapshotKey, kind history.SnapshotKind, srcList []history.SnapshotSummary) (KindCounts, error) {
	var counts KindCounts
	if len(srcList) == 0 {
		return counts, nil
	}

	dstList, err := dst.List(ctx, key, kind, history.TimeRange{})
	if err != nil {
		return counts, err
	}
	// content_hash is stable across stores; set membership decides copy vs skip.
	have := make(map[string]struct{}, len(dstList))
	for _, s := range dstList {
		have[s.ContentHash] = struct{}{}
	}

	for _, s := range srcList {
		if _, ok := have[s.ContentHash]; ok {
			counts.UpToDate++
			continue
		}
		// summary carries the resolved kind (with NodeLabel for activity)
		// so Get works the same for all three streams.
		stored, err := src.Get(ctx, key, s.Kind, history.NewRefHash(s.ContentHash))
		if err != nil {
			return counts, err
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
