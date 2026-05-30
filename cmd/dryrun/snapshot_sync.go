package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
)

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
			return runSync(cmd.Context(), src, dst, all, os.Stdout)
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
		all                          bool
		historyDB                    string
	)
	cmd := &cobra.Command{
		Use:   "pull",
		Short: "Pull snapshots from a filesystem store or OCI registry into history.db",
		RunE: func(cmd *cobra.Command, args []string) error {
			src, err := resolveSyncStore(fromPath, ociRef, remoteName)
			if err != nil {
				return err
			}
			dst, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer dst.Close()
			return runSync(cmd.Context(), src, dst, all, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&fromPath, "from-path", "", "source directory")
	cmd.Flags().StringVar(&ociRef, "oci", "", "OCI registry base ref (e.g. ghcr.io/org/dryrun)")
	cmd.Flags().StringVar(&remoteName, "remote", "", "configured [[remote]] name")
	cmd.Flags().BoolVar(&all, "all", false, "sync all keys from the source")
	cmd.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	return cmd
}

// explicit flags win; with none, fall back to the profile's configured remote.
func resolveSyncStore(path, ociRef, remoteName string) (history.SnapshotStore, error) {
	switch {
	case ociRef != "" || remoteName != "":
		return buildOCIStore(ociRef, remoteName)
	case path != "":
		return history.NewFilesystemStore(path)
	default:
		if r := profileRemote(); r != "" {
			return buildOCIStore("", r)
		}
		return nil, fmt.Errorf("specify --to-path/--from-path, --oci, or --remote")
	}
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
func runSync(ctx context.Context, src, dst history.SnapshotStore, all bool, w io.Writer) error {
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

	outs, err := syncKeys(ctx, src, dst, keys)
	if err != nil {
		return err
	}
	printSyncOutcomes(w, outs)
	return nil
}

// syncKeys diffs src vs dst by content_hash per kind and copies the gap
// in schema -> planner -> activity order.
func syncKeys(ctx context.Context, src, dst history.SnapshotStore, keys []history.SnapshotKey) ([]SyncOutcome, error) {
	out := make([]SyncOutcome, 0, len(keys))
	for _, key := range keys {
		o := SyncOutcome{Key: key}
		for _, kind := range kindOrder() {
			c, err := syncKind(ctx, src, dst, key, kind)
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

// activity uses an empty NodeLabel so List returns every node's row in one pass.
func kindOrder() []history.SnapshotKind {
	return []history.SnapshotKind{
		history.SchemaKind(),
		history.PlannerKind(),
		history.ActivityKind(""),
	}
}

func syncKind(ctx context.Context, src, dst history.SnapshotStore, key history.SnapshotKey, kind history.SnapshotKind) (KindCounts, error) {
	var counts KindCounts

	srcList, err := src.List(ctx, key, kind, history.TimeRange{})
	if err != nil {
		return counts, err
	}
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
