package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

type captureTarget struct {
	Label   string
	URL     string
	Role    string // primary | standby | auto
	Streams []string
	// this label names a read pool, so members rotate by design
	Pool     bool
	Interval time.Duration
}

func snapshotCaptureCmd(historyDB *string) *cobra.Command {
	var (
		nodeName, from, label string
		streams               []string
		all, due              bool
		allowOrphan           bool
		allowRotation         bool
		allowRoleChange       bool
		pushAfter             bool
		pushRemote            string
	)

	cmd := &cobra.Command{
		Use:   "capture",
		Short: "Capture stats from one node or the whole fleet",
		Long: `Capture planner, activity and query stats from a node.

Nodes come from [[node]] blocks in dryrun.toml, or from --from/--label for a
one-off. With --all every configured node is captured in turn; --due skips the
ones whose interval has not elapsed, so a single cron line implements every
node's cadence.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := captureTargets(nodeName, from, label, streams, all)
			if err != nil {
				return err
			}
			store, err := openHistoryStore(*historyDB)
			if err != nil {
				return err
			}
			defer store.Close()
			if err := historyUsable(store); err != nil {
				return err
			}

			// one capture at a time per project: an overlapping cron tick on a
			// slow node would otherwise stack connections on production
			unlock, err := lockCaptures(*historyDB)
			if err != nil {
				return err
			}
			defer unlock()

			key := resolveSnapshotKey()
			opts := captureRunOptions{
				AllowOrphan:     allowOrphan,
				AllowRotation:   allowRotation,
				AllowRoleChange: allowRoleChange,
				Due:             due,
			}

			var failed []string
			for _, t := range targets {
				if err := captureOneNode(cmd.Context(), store, key, t, opts); err != nil {
					if len(targets) == 1 {
						return err
					}
					// one bad node must not strand the rest of the fleet
					fmt.Fprintf(os.Stderr, "error: %s: %s\n", t.Label, redactURLPasswords(err.Error()))
					failed = append(failed, t.Label)
				}
			}

			if pushAfter && len(failed) < len(targets) {
				dst, err := resolveSyncStore("", "", pushRemote)
				if err != nil {
					return err
				}
				if err := runSync(cmd.Context(), store, dst, false, fullScope(), os.Stdout); err != nil {
					return err
				}
			}
			maybeAutoPrune(cmd.Context(), store, key)

			if len(failed) > 0 {
				return fmt.Errorf("%d of %d node(s) failed: %s",
					len(failed), len(targets), strings.Join(failed, ", "))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&nodeName, "node", "", "capture the named [[node]] from dryrun.toml")
	cmd.Flags().StringVar(&from, "from", "", "connection URL for a one-off capture (with --label)")
	cmd.Flags().StringVar(&label, "label", "", "node label for a one-off capture")
	cmd.Flags().StringSliceVar(&streams, "streams", nil, "planner,activity,query (default: by detected role)")
	cmd.Flags().BoolVar(&all, "all", false, "capture every [[node]] in dryrun.toml")
	cmd.Flags().BoolVar(&due, "due", false, "skip streams whose interval has not elapsed")
	cmd.Flags().BoolVar(&allowOrphan, "allow-orphan", false, "permit capture without a bound schema snapshot")
	cmd.Flags().BoolVar(&allowRotation, "allow-rotation", false, "this label names a pool; do not warn when it alternates between servers")
	cmd.Flags().BoolVar(&allowRoleChange, "allow-role-change", false, "accept a label whose role flipped (promotion/failover)")
	cmd.Flags().BoolVar(&pushAfter, "push", false, "push to a remote after capturing")
	cmd.Flags().StringVar(&pushRemote, "remote", "", "configured [[remote]] name (with --push)")
	return cmd
}

type captureRunOptions struct {
	AllowOrphan     bool
	AllowRotation   bool
	AllowRoleChange bool
	Due             bool
}

// --all reads the fleet from config; otherwise one node, named or ad hoc.
func captureTargets(nodeName, from, label string, streams []string, all bool) ([]captureTarget, error) {
	if all && (nodeName != "" || from != "") {
		return nil, fmt.Errorf("--all captures every configured node; drop --node/--from")
	}
	if from != "" && nodeName != "" {
		return nil, fmt.Errorf("--from is a one-off connection; it does not combine with --node")
	}

	if all || nodeName != "" {
		_, cfg, err := loadProjectConfig()
		if err != nil {
			return nil, err
		}
		if all {
			nodes, err := cfg.ResolveNodes()
			if err != nil {
				return nil, err
			}
			if len(nodes) == 0 {
				return nil, fmt.Errorf("no [[node]] blocks in dryrun.toml; add one, or capture a single node with --from/--label")
			}
			out := make([]captureTarget, 0, len(nodes))
			for _, n := range nodes {
				out = append(out, targetFromNode(n, streams))
			}
			return out, nil
		}
		n, err := cfg.ResolveNode(nodeName)
		if err != nil {
			return nil, err
		}
		return []captureTarget{targetFromNode(*n, streams)}, nil
	}

	if from == "" {
		from = flagDB
	}
	if from == "" {
		return nil, fmt.Errorf("give --node NAME, --from <url> --label <name>, or --all")
	}
	if label == "" {
		return nil, fmt.Errorf("--label is required with --from; it names the series every counter is differenced against")
	}
	return []captureTarget{{Label: label, URL: from, Role: "auto", Streams: streams}}, nil
}

func targetFromNode(n config.ResolvedNode, cliStreams []string) captureTarget {
	t := captureTarget{
		Label:    n.Name,
		URL:      n.URL,
		Role:     n.Role,
		Streams:  n.Streams,
		Pool:     n.Pool,
		Interval: n.Interval,
	}
	// an explicit --streams is the operator overriding config for this run
	if len(cliStreams) > 0 {
		t.Streams = cliStreams
	}
	return t
}

func captureOneNode(ctx context.Context, store *history.Store, key history.SnapshotKey, t captureTarget, opts captureRunOptions) error {
	// Decide cadence before connecting. On a fleet cron most ticks have
	// nothing to do, and opening a production connection to find that out is
	// the cost that makes a short interval expensive.
	if opts.Due && t.Interval > 0 {
		due, _, err := dueStreams(ctx, store, key, t, candidateStreams(t), true)
		if err != nil {
			return err
		}
		if len(due) == 0 {
			fmt.Fprintf(os.Stderr, "%s: nothing due\n", t.Label)
			return nil
		}
	}

	ctx, conn, err := connectDBProdFor(t.URL)
	if err != nil {
		return err
	}
	defer conn.Close()

	cap, err := newPgxCapturer(ctx, conn.Pool())
	if err != nil {
		return err
	}
	defer cap.Close(ctx)

	standby, err := cap.IsStandby(ctx)
	if err != nil {
		return fmt.Errorf("check standby status: %w", err)
	}
	role := history.NodeRolePrimary
	if standby {
		role = history.NodeRoleStandby
	}
	if t.Role != "" && t.Role != "auto" && t.Role != role {
		return dryrun.NewError(dryrun.ErrReplicaCapture,
			fmt.Sprintf("configured role %s, but this node is a %s", t.Role, role))
	}
	if err := guardNodeRole(ctx, store, key, captureOptions{
		Label: t.Label, AllowRoleChange: opts.AllowRoleChange,
	}, role); err != nil {
		return err
	}

	wanted := t.Streams
	if len(wanted) == 0 {
		wanted = config.DefaultStreamsFor(role)
	}
	wanted, skipped, err := dueStreams(ctx, store, key, t, wanted, opts.Due)
	if err != nil {
		return err
	}
	if len(wanted) == 0 {
		fmt.Fprintf(os.Stderr, "%s: nothing due\n", t.Label)
		return nil
	}

	schemaRef := ""
	if snap, err := store.GetSchema(ctx, key, history.NewRefLatest()); err == nil && snap != nil {
		schemaRef = snap.ContentHash
	}
	if schemaRef == "" && !opts.AllowOrphan {
		return fmt.Errorf("no schema snapshot to bind to; run `dryrun snapshot take` on the primary first, or pass --allow-orphan")
	}

	rowCap, err := resolveQueryStatsRowCap()
	if err != nil {
		return err
	}

	var done []string
	for _, s := range wanted {
		n, err := captureStream(ctx, cap, store, key, t, s, schemaRef, rowCap, opts)
		if err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
		done = append(done, fmt.Sprintf("%s=%d", s, n))
	}
	fmt.Fprintf(os.Stderr, "%s (%s): %s\n", t.Label, role, strings.Join(done, " "))
	if len(skipped) > 0 {
		fmt.Fprintf(os.Stderr, "  not due: %s\n", strings.Join(skipped, ", "))
	}
	return nil
}

// Returns the item count so the caller can say what it actually wrote rather
// than just that it ran.
func captureStream(ctx context.Context, cap initCapturer, store *history.Store, key history.SnapshotKey, t captureTarget, stream, schemaRef string, rowCap int, opts captureRunOptions) (int, error) {
	switch stream {
	case "planner":
		p, err := cap.CapturePlanner(ctx, schemaRef)
		if err != nil {
			return 0, err
		}
		if _, err := store.PutPlanner(ctx, key, p); err != nil {
			return 0, err
		}
		return len(p.Tables), nil

	case "activity":
		a, err := cap.CaptureActivity(ctx, schemaRef, t.Label)
		if err != nil {
			return 0, err
		}
		warnNodeIdentityDrift(ctx, store, key, a.Node.Source, a.Node, opts.AllowRotation || t.Pool)
		if _, err := store.PutActivity(ctx, key, a); err != nil {
			return 0, err
		}
		return len(a.Tables), nil

	case "query":
		q, err := cap.CaptureQueryStats(ctx, schemaRef, t.Label, rowCap)
		if err != nil {
			if errors.Is(err, schema.ErrQueryStatsUnavailable) {
				return 0, fmt.Errorf("pg_stat_statements is not available on this node; " +
					"add it to shared_preload_libraries and restart, then CREATE EXTENSION pg_stat_statements")
			}
			return 0, err
		}
		warnNodeIdentityDrift(ctx, store, key, q.Node.Source, q.Node, opts.AllowRotation || t.Pool)
		if _, err := store.PutQueryStats(ctx, key, q); err != nil {
			return 0, err
		}
		return len(q.Queries), nil

	case "schema":
		return 0, fmt.Errorf("schema is captured by `dryrun snapshot take`, which guards that it runs on a primary")
	}
	return 0, fmt.Errorf("unknown stream %q", stream)
}

// What the node could capture, without knowing its role yet: the union is
// safe here because a stream that turns out not to apply is dropped later.
func candidateStreams(t captureTarget) []string {
	if len(t.Streams) > 0 {
		return t.Streams
	}
	return []string{"planner", "activity", "query"}
}

// --due keys off the newest stored row. Pulled rows land in the same tables,
// so a pull can make a node look freshly captured; worst case one stream skips
// one interval and self-heals on the next tick.
func dueStreams(ctx context.Context, store *history.Store, key history.SnapshotKey, t captureTarget, wanted []string, due bool) (run, skipped []string, err error) {
	if !due || t.Interval <= 0 {
		return wanted, nil, nil
	}
	now := time.Now().UTC()
	for _, s := range wanted {
		last, ok, err := store.LastCaptureAt(ctx, key, t.Label, s)
		if err != nil {
			return nil, nil, err
		}
		// A row dated in the future came from a host with a skewed clock, most
		// likely via pull. It is not evidence that we captured recently, so it
		// is ignored rather than clamped to now -- clamping would still hold
		// the stream back for a full interval on garbage input.
		if ok && last.After(now) {
			ok = false
		}
		if ok && now.Sub(last) < t.Interval {
			skipped = append(skipped, fmt.Sprintf("%s (%s ago)", s, now.Sub(last).Round(time.Minute)))
			continue
		}
		run = append(run, s)
	}
	return run, skipped, nil
}

// Connection errors quote the URL they failed on, and --all puts that in a
// cron log for every unreachable node.
func redactURLPasswords(msg string) string {
	for {
		i := strings.Index(msg, "://")
		if i < 0 {
			return msg
		}
		rest := msg[i+3:]
		at := strings.IndexAny(rest, "@ \t\n")
		if at < 0 || rest[at] != '@' {
			return msg[:i+3] + redactURLPasswords(rest)
		}
		cred := rest[:at]
		colon := strings.Index(cred, ":")
		if colon < 0 {
			return msg[:i+3] + cred + "@" + redactURLPasswords(rest[at+1:])
		}
		return msg[:i+3] + cred[:colon] + ":***@" + redactURLPasswords(rest[at+1:])
	}
}

// A legacy database never ran migrate(), so the node tables may not exist.
func historyUsable(store *history.Store) error {
	switch store.Compat() {
	case history.CompatLegacy:
		return fmt.Errorf(".dryrun/history.db was created by an older dryrun and cannot be read; " +
			"re-run 'dryrun init' or 'dryrun snapshot pull' to recapture its snapshots")
	case history.CompatNewer:
		return fmt.Errorf(".dryrun/history.db was written by a newer dryrun; upgrade dryrun")
	}
	return nil
}

// Single-flight across processes: a cron tick that overlaps a slow fleet
// capture would otherwise open a second set of production connections. The
// lock is advisory and self-healing -- a crashed run leaves a file, and the
// next run past the timeout takes it over.
const captureLockStale = 2 * time.Hour

func lockCaptures(historyDB string) (func(), error) {
	path := captureLockPath(historyDB)
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err == nil {
		fmt.Fprintf(f, "%d %s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
		f.Close()
		return func() { os.Remove(path) }, nil
	}
	if !os.IsExist(err) {
		return nil, err
	}
	info, statErr := os.Stat(path)
	if statErr == nil && time.Since(info.ModTime()) > captureLockStale {
		slog.Warn("taking over a stale capture lock", "path", path, "age", time.Since(info.ModTime()).Round(time.Minute))
		os.Remove(path)
		return lockCaptures(historyDB)
	}
	return nil, fmt.Errorf("another capture is running (%s); wait for it, or remove the file if it is stale", path)
}

func captureLockPath(historyDB string) string {
	if historyDB != "" {
		return historyDB + ".capture.lock"
	}
	return filepath.Join(".dryrun", "capture.lock")
}

func filepathDir(p string) string { return filepath.Dir(p) }
