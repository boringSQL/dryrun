package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

// nil, nil when the key has no schema yet
func latestSchema(ctx context.Context, store initWriter, key history.SnapshotKey) (*schema.SchemaSnapshot, error) {
	snap, err := store.GetSchema(ctx, key, history.NewRefLatest())
	// only an absent snapshot falls through to --allow-orphan
	if err != nil && !errors.Is(err, history.ErrSnapshotNotFound) {
		return nil, fmt.Errorf("read latest schema snapshot: %w", err)
	}
	if err != nil {
		return nil, nil
	}
	return snap, nil
}

func schemaRefFrom(prior *schema.SchemaSnapshot, allowOrphan bool) (string, error) {
	if prior != nil {
		return prior.ContentHash, nil
	}
	if allowOrphan {
		return "", nil
	}
	return "", fmt.Errorf("no schema snapshot to bind to; run `dryrun snapshot capture --streams schema` on the primary first, or pass --allow-orphan")
}

// pg_stat_statements is a server configuration, so the remedy is the same
// wherever the capture was attempted from.
const pgssUnavailable = "pg_stat_statements is not available on this node; " +
	"add it to shared_preload_libraries and restart, then CREATE EXTENSION pg_stat_statements"

type captureOptions struct {
	Label           string
	AllowOrphan     bool
	RowCap          int
	AllowRoleChange bool
	AllowRotation   bool
}

func guardNodeRole(ctx context.Context, store initWriter, key history.SnapshotKey, opts captureOptions, role string) error {
	if opts.AllowRoleChange {
		return nil
	}
	prev, err := store.LatestNodeRole(ctx, key, opts.Label)
	if err != nil {
		return fmt.Errorf("check recorded node role: %w", err)
	}
	if prev == history.NodeRoleUnknown || prev == role {
		return nil
	}
	return dryrun.NewError(dryrun.ErrNodeRoleChanged, fmt.Sprintf(
		"label %q was last captured as a %s, this node is a %s.\n"+
			"After a promotion or failover, re-run with --allow-role-change.\n"+
			"During a PITR restore or an in-progress promotion, wait and retry.\n"+
			"Otherwise --label is pointed at a different node than before.",
		opts.Label, prev, role))
}

// 2.5.1: oscillation (A->B->A) is a rotating endpoint and warns; a one-way
// change is a restart or a replacement and only notices. Never fails: a label
// may legitimately name a pool, which is what --allow-rotation declares.
func warnNodeIdentityDrift(ctx context.Context, store initWriter, key history.SnapshotKey, label string, node schema.NodeIdentity, allowRotation bool) {
	if node.PostmasterStartTime == nil {
		return
	}
	seen, err := store.RecentNodeFingerprints(ctx, key, label)
	if err != nil {
		slog.Debug("node fingerprint check skipped", "node", label, "error", err)
		return
	}
	if len(seen) == 0 {
		return
	}
	prev := seen[0]
	if prev.StartedAt.Equal(*node.PostmasterStartTime) {
		return
	}
	for _, f := range seen[1:] {
		if f.StartedAt.Equal(*node.PostmasterStartTime) {
			if allowRotation {
				return
			}
			fmt.Fprintf(os.Stderr,
				"warning: label %q is alternating between servers (%d distinct in the last %d rows).\n"+
					"  Their counters are interleaving under one label and deltas will be wrong.\n"+
					"  If it names a pool, pass --allow-rotation; otherwise --label is aimed at a rotating endpoint.\n",
				label, countDistinctServers(seen, *node.PostmasterStartTime), len(seen))
			return
		}
	}
	if prev.ServerAddr != "" && prev.ServerAddr == node.ServerAddr {
		fmt.Fprintf(os.Stderr, "notice: %s restarted since the last capture under label %q (counters reset).\n",
			node.ServerAddr, label)
		return
	}
	fmt.Fprintf(os.Stderr,
		"notice: label %q now reports a different server (was started %s addr %s; now %s addr %s).\n"+
			"  Expected after a restart or a node replacement. Watch for it alternating back.\n",
		label, prev.StartedAt.Format(time.RFC3339Nano), addrOrUnknown(prev.ServerAddr),
		node.PostmasterStartTime.Format(time.RFC3339Nano), addrOrUnknown(node.ServerAddr))
}

func countDistinctServers(seen []history.NodeFingerprint, current time.Time) int {
	distinct := []time.Time{current}
	for _, f := range seen {
		known := false
		for _, d := range distinct {
			if d.Equal(f.StartedAt) {
				known = true
				break
			}
		}
		if !known {
			distinct = append(distinct, f.StartedAt)
		}
	}
	return len(distinct)
}

func addrOrUnknown(addr string) string {
	if addr == "" {
		return "unknown"
	}
	return addr
}

// maybeAutoPrune prunes history after a successful capture, per [history] in dryrun.toml.
func maybeAutoPrune(ctx context.Context, store *history.Store, key history.SnapshotKey) {
	_, cfg, err := loadProjectConfig()
	if err != nil || cfg.History == nil || !cfg.History.AutoPrune {
		return
	}
	if cfg.History.MaxAge == "" {
		fmt.Fprintln(os.Stderr, "warning: [history] auto_prune is set but max_age is empty; skipping prune")
		return
	}
	maxAge, err := parseRelative(cfg.History.MaxAge)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: invalid [history] max_age %q: %v\n", cfg.History.MaxAge, err)
		return
	}
	if maxAge == 0 {
		fmt.Fprintf(os.Stderr, "warning: [history] max_age %q is zero; skipping prune\n", cfg.History.MaxAge)
		return
	}
	res, err := store.Prune(ctx, key, history.PruneOptions{
		Cutoff:      time.Now().UTC().Add(-maxAge),
		KeepSchemas: keepOrDefault(cfg.History.KeepSchemas, history.DefaultKeepSchemas),
		KeepPlanner: keepOrDefault(cfg.History.KeepPlanner, history.DefaultKeepPlanner),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: auto-prune failed: %v\n", err)
		return
	}
	if n := res.Total(); n > 0 {
		fmt.Fprintf(os.Stderr, "Auto-pruned %d history rows older than %s (%s reclaimed)\n",
			n, cfg.History.MaxAge, humanBytes(res.BytesFreed))
	}
	if res.SchemaPinned > 0 {
		fmt.Fprintf(os.Stderr,
			"  %d schema snapshot(s) kept past %s: stats rows still reference them\n",
			res.SchemaPinned, cfg.History.MaxAge)
	}
}

func keepOrDefault(v *int, def int) int {
	if v == nil {
		return def
	}
	return *v
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}
