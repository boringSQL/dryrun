package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/datamask"
	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/bloat"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// init capture surface; kept narrow so tests can stub it.
type initCapturer interface {
	IsStandby(ctx context.Context) (bool, error)
	Introspect(ctx context.Context) (*schema.SchemaSnapshot, error)
	CapturePlanner(ctx context.Context, schemaRefHash string) (*schema.PlannerStatsSnapshot, error)
	CaptureActivity(ctx context.Context, schemaRefHash, source string) (*schema.ActivityStatsSnapshot, error)
	CaptureQueryStats(ctx context.Context, schemaRefHash, source string, rowCap int) (*schema.QueryStatsSnapshot, error)
}

type initWriter interface {
	GetSchema(ctx context.Context, key history.SnapshotKey, at history.SnapshotRef) (*schema.SchemaSnapshot, error)
	PutSchema(ctx context.Context, key history.SnapshotKey, snap *schema.SchemaSnapshot) (history.PutOutcome, error)
	PutPlanner(ctx context.Context, key history.SnapshotKey, p *schema.PlannerStatsSnapshot) (history.PutOutcome, error)
	PutActivity(ctx context.Context, key history.SnapshotKey, a *schema.ActivityStatsSnapshot) (history.PutOutcome, error)
	PutQueryStats(ctx context.Context, key history.SnapshotKey, q *schema.QueryStatsSnapshot) (history.PutOutcome, error)
	LatestNodeRole(ctx context.Context, key history.SnapshotKey, nodeLabel string) (string, error)
	RecentNodeFingerprints(ctx context.Context, key history.SnapshotKey, nodeLabel string) ([]history.NodeFingerprint, error)
}

// one REPEATABLE READ, READ ONLY tx (as pg_dump uses) for the whole capture:
// consistent snapshot, no writes, one connection
type pgxCapturer struct {
	tx       pgx.Tx
	systemID string
}

func newPgxCapturer(ctx context.Context, pool *pgxpool.Pool) (pgxCapturer, error) {
	// probe on the pool before the tx: a permission error here must not poison capture
	systemID, err := schema.FetchSystemIdentifier(ctx, pool)
	if err != nil {
		slog.Debug("system_identifier unavailable; capturing without cluster id", "error", err)
		systemID = ""
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return pgxCapturer{}, fmt.Errorf("begin read-only transaction: %w", err)
	}
	return pgxCapturer{tx: tx, systemID: systemID}, nil
}

// read-only, so there is nothing to commit; rollback releases the snapshot
func (c pgxCapturer) Close(ctx context.Context) {
	if err := c.tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		slog.Warn("rollback capture transaction", "error", err)
	}
}

func (c pgxCapturer) IsStandby(ctx context.Context) (bool, error) {
	return schema.FetchIsStandby(ctx, c.tx)
}

func (c pgxCapturer) CurrentDatabase(ctx context.Context) (string, error) {
	return schema.FetchCurrentDatabase(ctx, c.tx)
}

func (c pgxCapturer) Introspect(ctx context.Context) (*schema.SchemaSnapshot, error) {
	snap, err := schema.IntrospectSchema(ctx, c.tx)
	if err != nil {
		return nil, err
	}
	snap.SystemIdentifier = c.systemID
	return snap, nil
}

func (c pgxCapturer) CapturePlanner(ctx context.Context, schemaRefHash string) (*schema.PlannerStatsSnapshot, error) {
	return schema.CapturePlannerStats(ctx, c.tx, schemaRefHash)
}

func (c pgxCapturer) CaptureActivity(ctx context.Context, schemaRefHash, source string) (*schema.ActivityStatsSnapshot, error) {
	return schema.CaptureActivityStats(ctx, c.tx, schemaRefHash, source)
}

func (c pgxCapturer) CaptureQueryStats(ctx context.Context, schemaRefHash, source string, rowCap int) (*schema.QueryStatsSnapshot, error) {
	return schema.CaptureQueryStats(ctx, c.tx, schemaRefHash, source, rowCap)
}

// best-effort, call last: a real capture error aborts the shared REPEATABLE READ tx,
// so nothing after this call can use cap. Only ctx cancellation/deadline propagates.
func captureQueryStatsBestEffort(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, schemaRefHash, source string, rowCap int) error {
	qs, err := cap.CaptureQueryStats(ctx, schemaRefHash, source, rowCap)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if err != nil {
		if !errors.Is(err, schema.ErrQueryStatsUnavailable) {
			slog.Warn("capture query stats", "source", source, "error", err)
		}
		return nil
	}
	if qs == nil {
		return nil
	}
	if _, err := store.PutQueryStats(ctx, key, qs); err != nil {
		slog.Warn("save query stats", "source", source, "error", err)
		return nil
	}
	fmt.Fprintf(os.Stderr, "  Query stats: %d shapes (node=%s)\n", len(qs.Queries), source)
	return nil
}

// init owns the masking flag surface; other subcommands don't mask anything.
var (
	flagMasksFile  string
	flagMaskPolicy []string
	flagNoMasks    bool
)

func initCmd() *cobra.Command {
	var (
		allowReplica bool
		source       string
		force        bool
	)

	cmd := &cobra.Command{
		Use:   "init [config-file]",
		Short: "Scaffold dryrun.toml and .dryrun/; with --db, capture full snapshot (primary only)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			configPath := "dryrun.toml"
			if len(args) > 0 {
				configPath = args[0]
			}

			dataDir, err := history.DefaultDataDir()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dataDir, 0o755); err != nil {
				return err
			}

			if flagDB == "" {
				if err := scaffoldConfig(configPath, ""); err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "Run 'dryrun --db <url> init' to capture a schema snapshot\n")
				return nil
			}
			rowCap, err := resolveQueryStatsRowCap()
			if err != nil {
				return err
			}

			ctx, conn, err := connectDBProd()
			if err != nil {
				return err
			}
			defer conn.Close()

			cap, err := newPgxCapturer(ctx, conn.Pool())
			if err != nil {
				return err
			}
			defer cap.Close(ctx)

			// bake the real db name so the first capture's key matches later ones
			dbName, err := cap.CurrentDatabase(ctx)
			if err != nil {
				return fmt.Errorf("query current database: %w", err)
			}
			if err := scaffoldConfig(configPath, dbName); err != nil {
				return err
			}

			store, err := openHistoryStore("")
			if err != nil {
				return fmt.Errorf("open history store: %w", err)
			}
			defer store.Close()

			key := resolveSnapshotKey()
			policy, err := buildMasker(key)
			if err != nil {
				return err
			}
			if flagNoMasks {
				slog.Warn("masking disabled by --no-masks; raw planner stats will be written to history.db")
			}

			return runInitCapture(ctx, cap, store, key, initOptions{
				AllowReplica: allowReplica,
				Source:       source,
				Policy:       policy,
				Force:        force,
				RowCap:       rowCap,
			})
		},
	}
	cmd.Flags().BoolVar(&allowReplica, "allow-replica", false, "permit capture on a standby (activity stats only)")
	cmd.Flags().BoolVar(&force, "force", false, "capture even if the cluster/database identity differs from this project's history")
	cmd.Flags().StringVar(&source, "source", "", "node label for activity stats (default: the server's cluster_name, else its address)")
	cmd.Flags().StringVar(&flagMasksFile, "masks-file", "", "path to data-masking-policy.yml")
	cmd.Flags().StringSliceVar(&flagMaskPolicy, "mask-policy", nil, "masking policy name (repeatable, comma-separated)")
	cmd.Flags().BoolVar(&flagNoMasks, "no-masks", false, "disable planner-stats masking (raw stats land in history.db)")
	return cmd
}

// Profile resolved by name only; flagDB must not displace masks_file
func buildMasker(key history.SnapshotKey) (*masking.Policy, error) {
	cwd, err := os.Getwd()
	if err != nil {
		slog.Warn("masker: getwd failed, profile auto-discovery disabled", "error", err)
	}
	res := masking.Resolution{
		FlagFile:     flagMasksFile,
		FlagPolicies: flagMaskPolicy,
		DatabaseID:   string(key.DatabaseID),
		Cwd:          cwd,
		Disabled:     flagNoMasks,
	}
	var requireMasks bool
	if _, cfg, err := loadProjectConfig(); err == nil {
		if cfg.RequireMasks != nil {
			requireMasks = *cfg.RequireMasks
		}
		if rp, rerr := cfg.ResolveProfile(nil, nilIfEmpty(flagProfile), cwd); rerr == nil {
			if rp.MasksFile != nil {
				res.ProfileFile = *rp.MasksFile
			}
			res.ProfilePolicies = rp.MaskPolicies
		}
	}
	if requireMasks && flagNoMasks {
		return nil, fmt.Errorf("require_masks=true in dryrun.toml; --no-masks is not allowed")
	}
	p, err := res.Load()
	if errors.Is(err, masking.ErrNoMasksFile) {
		if requireMasks {
			return nil, fmt.Errorf("require_masks=true in dryrun.toml; data-masking-policy.yml must exist (pass --masks-file=PATH or set masks_file in the profile)")
		}
		slog.Warn("no data-masking-policy.yml resolved; capturing without masking (set masks_file in the profile, pass --masks-file=PATH, or set require_masks=true to enforce)")
		return nil, nil
	}
	return p, err
}

type initOptions struct {
	AllowReplica bool
	Source       string
	Policy       *masking.Policy
	Force        bool
	RowCap       int
}

// init flow: refuse standbys by default; primary writes all three streams,
// replica with --allow-replica writes activity only.
func runInitCapture(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, opts initOptions) error {
	standby, err := cap.IsStandby(ctx)
	if err != nil {
		return fmt.Errorf("check standby status: %w", err)
	}

	// Empty source is passed through: CaptureNodeIdentity derives a server-side
	// fallback; os.Hostname() here would name the machine running dryrun.
	source := opts.Source

	if standby {
		if !opts.AllowReplica {
			return dryrun.NewError(dryrun.ErrReplicaCapture,
				"init is for primaries; this node is a standby. Re-run on the primary, or pass --allow-replica to capture activity stats only")
		}
		// schema_ref_hash is unknown on a standby without a prior primary snapshot;
		// leave it empty so the row binds when a matching schema lands.
		schemaRef := ""
		if snap, err := store.GetSchema(ctx, key, history.NewRefLatest()); err == nil && snap != nil {
			schemaRef = snap.ContentHash
		}
		activity, err := cap.CaptureActivity(ctx, schemaRef, source)
		if err != nil {
			return fmt.Errorf("capture activity stats: %w", err)
		}
		warnNodeIdentityDrift(ctx, store, key, activity.Node.Source, activity.Node, false)
		if _, err := store.PutActivity(ctx, key, activity); err != nil {
			return fmt.Errorf("save activity stats: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Replica capture: activity stats only (node=%s)\n", source)
		return captureQueryStatsBestEffort(ctx, cap, store, key, schemaRef, source, opts.RowCap)
	}

	snap, planner, activity, masked, err := runPrimaryCapture(ctx, cap, store, key, source, opts.Policy, opts.Force)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Captured schema: %d tables, %d views, %d functions\n",
		len(snap.Tables), len(snap.Views), len(snap.Functions))
	fmt.Fprintf(os.Stderr, "  Schema:   %s\n", snap.ContentHash)
	fmt.Fprintf(os.Stderr, "  Planner:  %d tables, %d indexes, %d columns\n",
		len(planner.Tables), len(planner.Indexes), len(planner.Columns))
	if masked > 0 {
		fmt.Fprintf(os.Stderr, "  Masked:   %d planner-stats columns\n", masked)
	}
	fmt.Fprintf(os.Stderr, "  Activity: node=%s, %d tables, %d indexes\n",
		source, len(activity.Tables), len(activity.Indexes))
	return captureQueryStatsBestEffort(ctx, cap, store, key, snap.ContentHash, source, opts.RowCap)
}

// snapshot take wrapper: gate on standby (refuse, no replica fallback), then
// delegate to runPrimaryCapture. Kept here so the masking + standby contract
// lives next to init's; the snapshot take command only handles flag plumbing.
func runSnapshotTake(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, policy *masking.Policy, force bool) (*schema.SchemaSnapshot, *schema.PlannerStatsSnapshot, *schema.ActivityStatsSnapshot, int, error) {
	standby, err := cap.IsStandby(ctx)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("check standby status: %w", err)
	}
	if standby {
		return nil, nil, nil, 0, dryrun.NewError(dryrun.ErrReplicaCapture,
			"`dryrun snapshot take` must run against the primary; "+
				"use `dryrun snapshot activity --from <url> --label <name>` to capture activity from a replica")
	}
	return runPrimaryCapture(ctx, cap, store, key, "primary", policy, force)
}

// schema + planner + activity in one shot; caller is responsible for the standby gate
func runPrimaryCapture(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, source string, policy *masking.Policy, force bool) (*schema.SchemaSnapshot, *schema.PlannerStatsSnapshot, *schema.ActivityStatsSnapshot, int, error) {
	snap, err := cap.Introspect(ctx)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if err := guardCaptureIdentity(ctx, store, key, snap, force); err != nil {
		return snap, nil, nil, 0, err
	}
	if _, err := store.PutSchema(ctx, key, snap); err != nil {
		slog.Warn("could not save snapshot", "error", err)
	}

	planner, err := cap.CapturePlanner(ctx, snap.ContentHash)
	if err != nil {
		return snap, nil, nil, 0, fmt.Errorf("capture planner stats: %w", err)
	}
	bloat.Annotate(planner, snap)
	masked := datamask.MaskPlanner(policy, planner)
	planner.Masking = &schema.MaskingInfo{
		Applied:       policy != nil,
		ColumnsMasked: masked,
		JSONBStripped: true,
	}
	if _, err := store.PutPlanner(ctx, key, planner); err != nil {
		slog.Warn("could not save planner stats", "error", err)
	}

	activity, err := cap.CaptureActivity(ctx, snap.ContentHash, source)
	if err != nil {
		return snap, planner, nil, masked, fmt.Errorf("capture activity stats: %w", err)
	}
	warnNodeIdentityDrift(ctx, store, key, activity.Node.Source, activity.Node, false)
	if _, err := store.PutActivity(ctx, key, activity); err != nil {
		slog.Warn("could not save activity stats", "error", err)
	}

	return snap, planner, activity, masked, nil
}

// refuse a capture whose cluster/database contradicts this key's history: a stray
// DATABASE_URL recording a foreign db into the project.
func guardCaptureIdentity(ctx context.Context, store initWriter, key history.SnapshotKey, snap *schema.SchemaSnapshot, force bool) error {
	if force {
		return nil
	}
	prior, err := store.GetSchema(ctx, key, history.NewRefLatest())
	if errors.Is(err, history.ErrSnapshotNotFound) {
		return nil // no baseline yet
	}
	// unreadable prior: refuse rather than wave through unchecked
	if err != nil {
		return dryrun.WrapError(dryrun.ErrHistory, "read prior snapshot for identity check", err)
	}
	if prior == nil {
		return nil
	}
	reason, conflict := snapshot.IdentityConflict(prior, snap)
	if !conflict {
		return nil
	}
	return dryrun.NewError(dryrun.ErrIdentityMismatch,
		fmt.Sprintf("capture does not match this project's snapshot history: %s.\n"+
			"       The connection may point at the wrong database, or you may be in the wrong project directory.\n"+
			"       Re-check --db / DATABASE_URL, or pass --force to record it anyway.", reason))
}

// dbName, when set, is baked as the profile's database_id; empty leaves it commented
func scaffoldConfig(configPath, dbName string) error {
	if _, err := os.Stat(configPath); err == nil {
		fmt.Fprintf(os.Stderr, "%s already exists, skipping\n", configPath)
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	profileName := filepath.Base(cwd)

	databaseLine := `# database_id = "auth"   # the Postgres database name; defaults to the project id`
	if dbName != "" {
		databaseLine = fmt.Sprintf("database_id = %q   # the Postgres database name (from current_database())", dbName)
	}

	content := fmt.Sprintf(`[project]
id = %q

# require_masks = true   # fail init unless data-masking-policy.yml resolves; refuses --no-masks

[default]
profile = %q

[profiles.%s]
%s
# masks_file = "data-masking-policy.yml"   # PII policy shared with fixturize; auto-discovered if omitted
# mask_policies = ["pii"]                  # optional; default masks every column listed for this database

# [profiles.dev]
# db_url = "${DATABASE_URL}"

# Declare the fleet and "dryrun snapshot capture --all --due" becomes the whole
# crontab: each node keeps its own interval. url_env names an environment
# variable, so this file stays committable.
#
# [[node]]
# name     = "primary"
# role     = "primary"
# url_env  = "PRIMARY_URL"
# interval = "1h"
#
# [[node]]
# name     = "replica-eu"
# role     = "standby"
# url_env  = "REPLICA_EU_URL"
# streams  = ["activity", "query"]
# interval = "30m"
# pool     = true   # a read pool: members rotate, so do not warn about it

# [query_stats]
# row_cap = 500   # max pg_stat_statements rows to capture; overridable with --query-stats-limit

# [conventions]
# See: https://boringsql.com/dryrun/docs/dryrun-toml
`, profileName, profileName, profileName, databaseLine)
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Created %s (profile %q)\n", configPath, profileName)
	return nil
}
