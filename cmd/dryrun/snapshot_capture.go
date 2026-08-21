package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/boringsql/fixturize/masking"
	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/datamask"
	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/bloat"
)

type captureTarget struct {
	Label string
	// nil for an ad-hoc --from; otherwise resolved per node so one missing
	// environment variable costs one node, not the fleet
	node    *config.ResolvedNode
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
		check                 bool
		checkTimeout          time.Duration
	)

	cmd := &cobra.Command{
		Use:   "capture",
		Short: "Capture stats from one node or the whole fleet",
		Long: `Capture planner, activity and query stats from a node.

Nodes come from [[node]] blocks in dryrun.toml, or from --from/--label for a
one-off. With --all every configured node is captured in turn; --due skips the
ones whose interval has not elapsed, so a single cron line implements every
node's cadence.

--check is the preflight: it connects to every target, runs SELECT 1, and
reports what capture would do -- role, database, server, streams -- without
capturing anything. Nodes are checked independently, so one unreachable node
still leaves a full report, and the exit status is non-zero if any node would
fail.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if check && pushAfter {
				return fmt.Errorf("--check writes nothing, so there is nothing to --push")
			}
			if check && checkTimeout <= 0 {
				return fmt.Errorf("--check-timeout must be positive")
			}
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

			key := resolveSnapshotKey()
			opts := captureRunOptions{
				AllowOrphan:     allowOrphan,
				AllowRotation:   allowRotation,
				AllowRoleChange: allowRoleChange,
				Due:             due,
			}

			// the preflight takes no lock: it has to work while the cron capture
			// it's diagnosing is still running
			if check {
				// a failed preflight isn't a usage error; the report is the output
				cmd.SilenceUsage = true
				// resolve masking before touching any node: require_masks fails
				// the whole capture, so find out now
				if _, err := buildMasker(key); err != nil {
					return err
				}
				// same for a bad row cap: fails every node
				if _, err := resolveQueryStatsRowCap(); err != nil {
					return err
				}
				return runCaptureCheck(cmd.Context(), store, key, targets, opts, checkTimeout, cmd.OutOrStdout())
			}

			// one capture at a time per project: an overlapping cron tick on a
			// slow node would otherwise stack connections on production
			unlock, err := lockCaptures(*historyDB)
			if err != nil {
				return err
			}
			defer unlock()

			// same policy resolution `snapshot take` uses, including
			// require_masks; planner rows are pushed, so this is not optional
			policy, err := buildMasker(key)
			if err != nil {
				return err
			}
			opts.MaskPolicy = policy

			var failed []string
			for _, t := range targets {
				err := captureOneNode(cmd.Context(), store, key, t, opts)
				if err == nil {
					continue
				}
				if len(targets) == 1 {
					return err
				}
				// one bad node must not strand the rest of the fleet
				fmt.Fprintf(os.Stderr, "error: %s: %s\n", t.Label, redactSecrets(err.Error()))
				failed = append(failed, t.Label)
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
	cmd.Flags().BoolVar(&check, "check", false, "preflight only: connect, SELECT 1, report what capture would do; capture nothing")
	cmd.Flags().DurationVar(&checkTimeout, "check-timeout", checkTimeoutDefault, "per-node time budget for --check")
	cmd.Flags().BoolVar(&pushAfter, "push", false, "push to a remote after capturing")
	cmd.Flags().StringVar(&pushRemote, "remote", "", "configured [[remote]] name (with --push)")
	return cmd
}

// a stream this node cannot provide: skipped with a notice, never a failure
var (
	errStreamUnavailable = errors.New("stream unavailable on this node")
	// the put collapsed into an existing row: nothing was written, so the
	// summary must not claim a count
	errStreamUnchanged = errors.New("unchanged since the last capture")
)

type captureRunOptions struct {
	MaskPolicy      *masking.Policy
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
	// unknown --streams must fail before anything connects; config streams
	// are checked when the node resolves
	if err := config.ValidateStreams(streams); err != nil {
		return nil, err
	}
	if from != "" && nodeName != "" {
		return nil, fmt.Errorf("--from is a one-off connection; it does not combine with --node")
	}
	// a label from config would be silently overridden, and silently is the
	// problem: the label decides which series the counters land in
	if label != "" && (all || nodeName != "") {
		return nil, fmt.Errorf("--label names an ad-hoc node; a configured node's label is its [[node]] name")
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
		node:     &n,
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
	url := t.URL
	if t.node != nil {
		var err error
		if url, err = t.node.URL(); err != nil {
			return err
		}
	}
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

	ctx, conn, err := connectDBProdFor(url)
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
		return dryrun.NewError(dryrun.ErrNodeRoleChanged, fmt.Sprintf(
			"[[node]] %s declares role %s, but this node is a %s.\n"+
				"  After a failover, swap the roles in dryrun.toml or set role = auto.",
			t.Label, t.Role, role))
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

	schemaRef, err := resolveSchemaRef(ctx, store, key, opts.AllowOrphan)
	if err != nil {
		return err
	}

	rowCap, err := resolveQueryStatsRowCap()
	if err != nil {
		return err
	}

	var done []string
	for _, s := range wanted {
		n, err := captureStream(ctx, cap, store, key, t, s, schemaRef, rowCap, opts)
		switch {
		case errors.Is(err, errStreamUnavailable):
			done = append(done, s+"=n/a")
		case errors.Is(err, errStreamUnchanged):
			done = append(done, s+"=unchanged")
		case err != nil:
			// the capture shares one transaction, so a failed read can leave
			// it unusable for the streams after it
			return fmt.Errorf("%s: %w", s, err)
		default:
			done = append(done, fmt.Sprintf("%s=%d", s, n))
		}
	}
	fmt.Fprintf(os.Stderr, "%s (%s): %s\n", t.Label, role, strings.Join(done, " "))
	if len(skipped) > 0 {
		fmt.Fprintf(os.Stderr, "  not due: %s\n", strings.Join(skipped, ", "))
	}
	return nil
}

func captureStream(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, t captureTarget, stream, schemaRef string, rowCap int, opts captureRunOptions) (int, error) {
	switch stream {
	case "planner":
		// planner rows carry pg_statistic MCVs and histogram bounds, so they
		// go through the same masking `snapshot take` applies -- push ships
		// whatever lands in history.db
		snap, err := store.GetSchema(ctx, key, history.NewRefLatest())
		if err != nil || snap == nil {
			return 0, fmt.Errorf("planner stats need a schema snapshot to annotate against; run `dryrun snapshot take` first")
		}
		p, err := cap.CapturePlanner(ctx, schemaRef)
		if err != nil {
			return 0, err
		}
		bloat.Annotate(p, snap)
		masked := datamask.MaskPlanner(opts.MaskPolicy, p)
		p.Masking = &schema.MaskingInfo{
			Applied:       opts.MaskPolicy != nil,
			ColumnsMasked: masked,
			JSONBStripped: true,
		}
		out, err := store.PutPlanner(ctx, key, p)
		if err != nil {
			return 0, err
		}
		if out == history.PutDeduped {
			return 0, errStreamUnchanged
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
			// a replica without the extension must not fail a fleet run every
			// tick; every other capture path treats this as best-effort
			if errors.Is(err, schema.ErrQueryStatsUnavailable) {
				return 0, errStreamUnavailable
			}
			return 0, err
		}
		warnNodeIdentityDrift(ctx, store, key, q.Node.Source, q.Node, opts.AllowRotation || t.Pool)
		out, err := store.PutQueryStats(ctx, key, q)
		if err != nil {
			return 0, err
		}
		if out == history.PutDeduped {
			return 0, errStreamUnchanged
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
	if t.Role == "standby" {
		return config.DefaultStreamsFor("standby")
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

// Redaction lives with the code that builds connection errors, so every
// command benefits; --all is only the loudest consumer.
func redactSecrets(msg string) string { return schema.RedactSecrets(msg) }

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
// capture would otherwise open a second set of production connections.
//
// A crashed run leaves the file behind and it is NOT taken over automatically.
// Reclaiming a lock by path cannot be made safe -- rename and remove are both
// unconditional, so two processes that agree a lock is stale will both take it
// and both run -- and the alternative, a kernel lock, is not portable to every
// platform this ships on. So a stale lock is reported with its age and cleared
// by hand.
func lockCaptures(historyDB string) (func(), error) {
	path := captureLockPath(historyDB)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	unlock, ok, err := tryLock(path)
	if err != nil {
		return nil, err
	}
	if ok {
		return unlock, nil
	}

	held := "unknown age"
	if info, serr := os.Stat(path); serr == nil {
		held = fmt.Sprintf("held for %s", time.Since(info.ModTime()).Round(time.Second))
	}
	return nil, fmt.Errorf("another capture is running (%s, %s).\n"+
		"  If no capture is running, that run crashed: rm %s", path, held, path)
}

// The token is what makes unlock safe: only the holder removes the file, so a
// lock cleared by hand and retaken by another run is never deleted by the one
// that lost it.
func tryLock(path string) (func(), bool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if os.IsExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	token := fmt.Sprintf("%d %s", os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano))
	if _, err := fmt.Fprintln(f, token); err != nil {
		f.Close()
		os.Remove(path)
		return nil, false, err
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, false, err
	}
	return func() {
		if got, rerr := os.ReadFile(path); rerr == nil && strings.TrimSpace(string(got)) == token {
			os.Remove(path)
		}
	}, true, nil
}

func captureLockPath(historyDB string) string {
	if historyDB != "" {
		return historyDB + ".capture.lock"
	}
	return filepath.Join(".dryrun", "capture.lock")
}
