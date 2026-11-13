package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
)

const (
	// long enough that a daily capture job doesn't warn
	pullStaleAfter = 48 * time.Hour
)

// latest keeps only the newest take (per node for activity, per server on a pool label); rng is the --since window.
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
	Query    KindCounts
	// newest capture selected across all four streams; zero when the source has nothing
	SourceNewest time.Time
}

func snapshotPushCmd() *cobra.Command {
	var (
		toPath, ociRef, remoteName string
		all, full                  bool
		since                      string
		historyDB                  string
	)
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Push the latest snapshot from history.db to a filesystem store or OCI registry",
		RunE: func(cmd *cobra.Command, args []string) error {
			scope := pullScope{latest: !full}
			if since != "" {
				from, err := parseSince(since)
				if err != nil {
					return err
				}
				scope.rng = history.TimeRange{From: &from}
			}
			src, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer src.Close()
			dst, err := resolveSyncStore(cmd.Context(), toPath, ociRef, remoteName, true)
			if err != nil {
				return err
			}
			_, err = runSync(cmd.Context(), src, dst, all, scope, os.Stdout)
			return err
		},
	}
	cmd.Flags().StringVar(&toPath, "to-path", "", "destination directory")
	cmd.Flags().StringVar(&ociRef, "oci", "", "OCI registry base ref (e.g. ghcr.io/org/dryrun)")
	cmd.Flags().StringVar(&remoteName, "remote", "", "configured [[remote]] name")
	cmd.Flags().BoolVar(&all, "all", false, "sync all keys from the source")
	cmd.Flags().BoolVar(&full, "full", false, "push the entire history, not just the latest take")
	cmd.Flags().StringVar(&since, "since", "", "only push snapshots newer than a duration (7d, 2w, 24h) or UTC date (2006-01-02)")
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
			src, err := resolveSyncStore(cmd.Context(), fromPath, ociRef, remoteName, false)
			if err != nil {
				return err
			}
			dst, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer dst.Close()
			// pull writes into history.db; refuse a newer-dryrun db, as capture does
			if err := historyUsable(dst); err != nil {
				return err
			}
			outs, err := runSync(cmd.Context(), src, dst, all, scope, os.Stdout)
			if err != nil {
				return err
			}
			// a window says nothing about the source as a whole
			if since == "" {
				warnPullSource(os.Stderr, outs, all, time.Now())
			}
			// --full asks for the whole history; don't prune it away in the same run
			if !full {
				for _, o := range outs {
					maybeAutoPrune(cmd.Context(), dst, o.Key)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&fromPath, "from-path", "", "source directory, gs://bucket[/prefix] or s3://bucket[/prefix]")
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
	// stored timestamps are UTC and compared as strings, so a local-zone
	// bound would be off by the offset
	if d, err := parseRelative(s); err == nil {
		return time.Now().UTC().Add(-d), nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
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
func resolveSyncStore(ctx context.Context, path, ociRef, remoteName string, push bool) (history.SnapshotStore, error) {
	switch {
	case ociRef != "":
		return buildOCIStore(ociRef, "", "")
	case remoteName != "":
		return buildRemoteStore(ctx, remoteName, push)
	case strings.Contains(path, "://"):
		// else s3://b becomes the local directory ./s3:/b
		u, err := history.ParseObjectURL(path)
		if err != nil {
			return nil, err
		}
		return openBucketStore(ctx, u, history.S3Options{}, push)
	case path != "":
		return history.NewFilesystemStore(path)
	default:
		if r := profileRemote(); r != "" {
			return buildRemoteStore(ctx, r, push)
		}
		return nil, fmt.Errorf("specify --to-path/--from-path, --oci, or --remote")
	}
}

func buildRemoteStore(ctx context.Context, name string, push bool) (history.SnapshotStore, error) {
	_, cfg, err := loadProjectConfig()
	if err != nil {
		return nil, err
	}
	r, err := cfg.ResolveRemote(name)
	if err != nil {
		return nil, err
	}
	if err := r.Check(); err != nil {
		return nil, err
	}
	if r.Type == "gcs" || r.Type == "s3" {
		u, err := r.BucketURL()
		if err != nil {
			return nil, err
		}
		return openBucketStore(ctx, u, history.S3Options{Region: r.Region, Endpoint: r.Endpoint, PathStyle: r.PathStyle}, push)
	}
	return buildOCIStore(r.Ref, r.TokenEnv, r.Auth)
}

// Refused here, not at Put: a deduped Put would report success.
func openBucketStore(ctx context.Context, u history.ObjectURL, opts history.S3Options, push bool) (history.SnapshotStore, error) {
	if push {
		return nil, fmt.Errorf("push to %s:// is not supported yet: %s", u.Scheme, history.BucketUploadHint)
	}
	if u.Scheme == "gs" {
		return history.NewGCSStore(ctx, u, streamMapper())
	}
	return history.NewS3Store(ctx, u, opts, streamMapper())
}

func buildOCIStore(base, tokenEnv, authMode string) (history.SnapshotStore, error) {
	client, err := history.NewAuthClient(history.AuthConfig{TokenEnv: tokenEnv, Mode: authMode})
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
	rp, err := cfg.ResolveProfile(nil, nilIfEmpty(flagProfile), cwd)
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
		rp, err := cfg.ResolveProfile(nil, &n, cwd)
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
func runSync(ctx context.Context, src, dst history.SnapshotStore, all bool, scope pullScope, w io.Writer) ([]SyncOutcome, error) {
	var keys []history.SnapshotKey
	if all {
		ks, err := src.ListKeys(ctx)
		if err != nil {
			return nil, err
		}
		keys = ks
	} else {
		keys = []history.SnapshotKey{resolveSnapshotKey()}
	}

	outs, err := syncKeys(ctx, src, dst, keys, scope)
	if err != nil {
		return nil, err
	}
	printSyncOutcomes(w, outs)
	return outs, nil
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
		for _, list := range sel {
			for _, s := range list {
				if s.Timestamp.After(o.SourceNewest) {
					o.SourceNewest = s.Timestamp
				}
			}
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
			case history.KindQuery:
				o.Query = c
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
	query, err := src.List(ctx, key, history.QueryKind(""), scope.rng)
	if err != nil {
		return nil, err
	}
	allSchema, err := src.List(ctx, key, history.SchemaKind(), history.TimeRange{})
	if err != nil {
		return nil, err
	}

	if scope.latest {
		planner = newestPerNode(planner)
		if activity, err = withPoolMembers(ctx, src, key, activity, scope.rng); err != nil {
			return nil, err
		}
		if query, err = withPoolMembers(ctx, src, key, query, scope.rng); err != nil {
			return nil, err
		}
	}

	need := make(map[string]struct{}, len(planner)+len(activity)+len(query))
	for _, s := range planner {
		need[s.SchemaRefHash] = struct{}{}
	}
	for _, s := range activity {
		need[s.SchemaRefHash] = struct{}{}
	}
	for _, s := range query {
		need[s.SchemaRefHash] = struct{}{}
	}
	schema := filterByContentHash(allSchema, need)

	switch {
	case scope.latest:
		schema = withNewestSchema(schema, allSchema, planner, activity, query)
	default:
		// full mode also keeps schemas captured within the window itself
		// (e.g. a schema-only take), unioned with the referenced ones.
		schema = unionSummaries(schema, windowed(allSchema, scope.rng))
	}

	return map[history.SnapshotKindTag][]history.SnapshotSummary{
		history.KindSchema:   schema,
		history.KindPlanner:  planner,
		history.KindActivity: activity,
		history.KindQuery:    query,
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

// Stats pick schemas, so a newer schema with no stats yet needs adding. Unless
// stats newer than it are bound to another schema the source still holds: a
// long-lived remote dedups a reverted schema, leaving the superseded one as its
// newest row. A dead or clock-skewed node reads the same; the log note is the
// only signal.
func withNewestSchema(selected, allSchema []history.SnapshotSummary, stats ...[]history.SnapshotSummary) []history.SnapshotSummary {
	newest := newestPerNode(allSchema)
	if len(newest) == 0 {
		return selected
	}
	held := make(map[string]struct{}, len(allSchema))
	for _, s := range allSchema {
		held[s.ContentHash] = struct{}{}
	}
	for _, list := range stats {
		for _, s := range list {
			_, known := held[s.SchemaRefHash]
			if known && s.SchemaRefHash != newest[0].ContentHash && s.Timestamp.After(newest[0].Timestamp) {
				slog.Info("newest schema not selected: newer stats are bound to another schema",
					"newest_schema", newest[0].ContentHash, "stats_schema", s.SchemaRefHash)
				return selected
			}
		}
	}
	return unionSummaries(selected, newest)
}

// A pool label's newest row is one server's; the others travel with it so a
// fresh clone can add them up. Same schema only: an older schema's rows would
// pull that schema along and the read side would ignore them anyway. A members
// error fails the pull: skipping them silently would recreate the one-server
// clone this exists to fix.
func withPoolMembers(ctx context.Context, src history.SnapshotStore, key history.SnapshotKey, all []history.SnapshotSummary, rng history.TimeRange) ([]history.SnapshotSummary, error) {
	rowsUnder := make(map[[2]string]int, len(all))
	for _, s := range all {
		rowsUnder[[2]string{s.Kind.NodeLabel, s.SchemaRefHash}]++
	}
	newest := newestPerNode(all)
	out := append([]history.SnapshotSummary(nil), newest...)
	for _, n := range newest {
		// a lone row has no members; asking would fetch a whole bundle from a registry
		if rowsUnder[[2]string{n.Kind.NodeLabel, n.SchemaRefHash}] < 2 {
			continue
		}
		members, err := src.ListPoolMembers(ctx, key, n.Kind, n.SchemaRefHash)
		if err != nil {
			return nil, err
		}
		out = append(out, windowed(members, rng)...)
	}
	return out, nil
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

// Keyed by hash and time, not hash alone: content twins are separate captures,
// and history.db dates a version by the first one.
func unionSummaries(a, b []history.SnapshotSummary) []history.SnapshotSummary {
	type at struct {
		hash string
		ts   time.Time
	}
	seen := make(map[at]struct{}, len(a)+len(b))
	out := make([]history.SnapshotSummary, 0, len(a)+len(b))
	for _, list := range [][]history.SnapshotSummary{a, b} {
		for _, s := range list {
			k := at{s.ContentHash, s.Timestamp.UTC()}
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// activity/query use an empty NodeLabel so List returns every node's row in one pass.
func kindOrder() []history.SnapshotKind {
	return []history.SnapshotKind{
		history.SchemaKind(),
		history.PlannerKind(),
		history.ActivityKind(""),
		history.QueryKind(""),
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
	// content_hash is stable across stores; a known hash skips unless it refreshes.
	have := make(map[string]struct{}, len(dstList))
	for _, s := range dstList {
		have[s.ContentHash] = struct{}{}
	}

	// stores list newest-first; push oldest-first so remote history fills in chronological order.
	// Equal-second ties break on hash so the last row is the one newestPerNode would pick.
	ordered := make([]history.SnapshotSummary, len(srcList))
	copy(ordered, srcList)
	sort.SliceStable(ordered, func(i, j int) bool {
		if !ordered[i].Timestamp.Equal(ordered[j].Timestamp) {
			return ordered[i].Timestamp.Before(ordered[j].Timestamp)
		}
		return ordered[i].ContentHash < ordered[j].ContentHash
	})

	for i, s := range ordered {
		_, known := have[s.ContentHash]
		// A known hash can still be newer: a reverted migration is old history
		// that is current again, and the digest omits partition children. The
		// column has seconds and the summary may not, so compare in seconds. The
		// column stays behind after a refresh, so each sync of that row costs
		// one Get.
		refresh := kind.Tag == history.KindSchema && i == len(ordered)-1 && len(dstList) > 0 &&
			s.Timestamp.Truncate(time.Second).After(dstList[0].Timestamp)
		if known && !refresh {
			counts.UpToDate++
			continue
		}
		// summary carries the resolved kind (with NodeLabel for activity/query)
		// so Get works the same for all four streams.
		stored, err := getForSync(ctx, src, dst, key, s)
		if err != nil {
			return counts, err
		}
		outcome, err := dst.Put(ctx, key, stored)
		if err != nil {
			return counts, err
		}
		// only an insert is a copy; dedup isn't
		if outcome == history.PutDeduped {
			counts.UpToDate++
			continue
		}
		counts.Copied++
	}
	return counts, nil
}

// A hash resolves to its newest content twin. history.db dates a version by the
// twin it first sees and refreshes the payload from later ones, so it gets each
// twin in turn; a bundle store keeps one bundle per hash and wants the newest.
func getForSync(ctx context.Context, src, dst history.SnapshotStore, key history.SnapshotKey, s history.SnapshotSummary) (history.StoredSnapshot, error) {
	if _, toHistoryDB := dst.(*history.Store); toHistoryDB && s.Kind.Tag == history.KindSchema {
		stored, err := src.Get(ctx, key, s.Kind, history.SnapshotRef{Kind: history.RefAt, At: s.Timestamp})
		if err != nil && !errors.Is(err, history.ErrSnapshotNotFound) {
			return history.StoredSnapshot{}, err
		}
		// at-or-before can land on another hash captured in the same second
		if err == nil && stored.AsSchema().ContentHash == s.ContentHash {
			return stored, nil
		}
	}
	return src.Get(ctx, key, s.Kind, history.NewRefHash(s.ContentHash))
}

// Exit stays 0: a source with no data yet isn't a failed pull, and an absent
// registry repository reads the same as an empty one.
func warnPullSource(w io.Writer, outs []SyncOutcome, all bool, now time.Time) {
	if all && len(outs) == 0 {
		fmt.Fprintln(w, "warning: source has no snapshots")
		return
	}
	for _, o := range outs {
		name := fmt.Sprintf("%s/%s", o.Key.ProjectID, o.Key.DatabaseID)
		switch {
		case o.SourceNewest.IsZero():
			// --all took this key from the source, so its config isn't the problem
			if !all {
				fmt.Fprintf(w, "warning: source has no snapshots for %s; check project, database_id and stream in dryrun.toml\n", name)
			}
		case now.Sub(o.SourceNewest) > pullStaleAfter:
			fmt.Fprintf(w, "warning: newest snapshot of %s was captured %s ago; the capture job may have stopped\n", name, fmtLived(now.Sub(o.SourceNewest)))
		}
	}
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
		fmt.Fprintf(w, "  query:    %d copied, %d up-to-date\n", o.Query.Copied, o.Query.UpToDate)
	}
}
