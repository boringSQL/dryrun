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
)

// init capture surface; kept narrow so tests can stub it.
type initCapturer interface {
	IsStandby(ctx context.Context) (bool, error)
	Introspect(ctx context.Context) (*schema.SchemaSnapshot, error)
	CapturePlanner(ctx context.Context, schemaRefHash string) (*schema.PlannerStatsSnapshot, error)
	CaptureActivity(ctx context.Context, schemaRefHash, source string) (*schema.ActivityStatsSnapshot, error)
}

type initWriter interface {
	GetSchema(ctx context.Context, key history.SnapshotKey, at history.SnapshotRef) (*schema.SchemaSnapshot, error)
	PutSchema(ctx context.Context, key history.SnapshotKey, snap *schema.SchemaSnapshot) (history.PutOutcome, error)
	PutPlanner(ctx context.Context, key history.SnapshotKey, p *schema.PlannerStatsSnapshot) (history.PutOutcome, error)
	PutActivity(ctx context.Context, key history.SnapshotKey, a *schema.ActivityStatsSnapshot) (history.PutOutcome, error)
}

// one REPEATABLE READ, READ ONLY tx (as pg_dump uses) for the whole capture:
// consistent snapshot, no writes, one connection
type pgxCapturer struct{ tx pgx.Tx }

func newPgxCapturer(ctx context.Context, pool *pgxpool.Pool) (pgxCapturer, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return pgxCapturer{}, fmt.Errorf("begin read-only transaction: %w", err)
	}
	return pgxCapturer{tx: tx}, nil
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

func (c pgxCapturer) Introspect(ctx context.Context) (*schema.SchemaSnapshot, error) {
	return schema.IntrospectSchema(ctx, c.tx)
}

func (c pgxCapturer) CapturePlanner(ctx context.Context, schemaRefHash string) (*schema.PlannerStatsSnapshot, error) {
	return schema.CapturePlannerStats(ctx, c.tx, schemaRefHash)
}

func (c pgxCapturer) CaptureActivity(ctx context.Context, schemaRefHash, source string) (*schema.ActivityStatsSnapshot, error) {
	return schema.CaptureActivityStats(ctx, c.tx, schemaRefHash, source)
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

			if err := scaffoldConfig(configPath); err != nil {
				return err
			}

			dataDir, err := history.DefaultDataDir()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dataDir, 0o755); err != nil {
				return err
			}

			if flagDB == "" {
				fmt.Fprintf(os.Stderr, "Run 'dryrun --db <url> init' to capture a schema snapshot\n")
				return nil
			}

			ctx, conn, err := connectDB()
			if err != nil {
				return err
			}
			defer conn.Close()

			cap, err := newPgxCapturer(ctx, conn.Pool())
			if err != nil {
				return err
			}
			defer cap.Close(ctx)

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

			return runInitCapture(ctx, cap, store, key, dataDir, initOptions{
				AllowReplica: allowReplica,
				Source:       source,
				Policy:       policy,
			})
		},
	}
	cmd.Flags().BoolVar(&allowReplica, "allow-replica", false, "permit capture on a standby (activity stats only)")
	cmd.Flags().StringVar(&source, "source", "", "node label for activity stats (default: hostname)")
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
		if rp, rerr := cfg.ResolveProfile(nil, nil, nilIfEmpty(flagProfile), cwd); rerr == nil {
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
}

// init flow: refuse standbys by default; primary writes all three streams,
// replica with --allow-replica writes activity only.
func runInitCapture(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, dataDir string, opts initOptions) error {
	standby, err := cap.IsStandby(ctx)
	if err != nil {
		return fmt.Errorf("check standby status: %w", err)
	}

	source := opts.Source
	if source == "" {
		if h, err := os.Hostname(); err == nil {
			source = h
		} else {
			source = "unknown"
		}
	}

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
		if _, err := store.PutActivity(ctx, key, activity); err != nil {
			return fmt.Errorf("save activity stats: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Replica capture: activity stats only (node=%s)\n", source)
		return nil
	}

	snap, planner, activity, masked, err := runPrimaryCapture(ctx, cap, store, key, source, opts.Policy)
	if err != nil {
		return err
	}

	schemaPath := filepath.Join(dataDir, "schema.json")
	if err := writeJSONFile(schemaPath, snap, true); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Captured schema: %d tables, %d views, %d functions\n",
		len(snap.Tables), len(snap.Views), len(snap.Functions))
	fmt.Fprintf(os.Stderr, "  Schema:   %s\n", schemaPath)
	fmt.Fprintf(os.Stderr, "  Planner:  %d tables, %d indexes, %d columns\n",
		len(planner.Tables), len(planner.Indexes), len(planner.Columns))
	if masked > 0 {
		fmt.Fprintf(os.Stderr, "  Masked:   %d planner-stats columns\n", masked)
	}
	fmt.Fprintf(os.Stderr, "  Activity: node=%s, %d tables, %d indexes\n",
		source, len(activity.Tables), len(activity.Indexes))
	return nil
}

// snapshot take wrapper: gate on standby (refuse, no replica fallback), then
// delegate to runPrimaryCapture. Kept here so the masking + standby contract
// lives next to init's; the snapshot take command only handles flag plumbing.
func runSnapshotTake(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, policy *masking.Policy) (*schema.SchemaSnapshot, *schema.PlannerStatsSnapshot, *schema.ActivityStatsSnapshot, int, error) {
	standby, err := cap.IsStandby(ctx)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("check standby status: %w", err)
	}
	if standby {
		return nil, nil, nil, 0, dryrun.NewError(dryrun.ErrReplicaCapture,
			"`dryrun snapshot take` must run against the primary; "+
				"use `dryrun snapshot activity --from <url> --label <name>` to capture activity from a replica")
	}
	return runPrimaryCapture(ctx, cap, store, key, "primary", policy)
}

// schema + planner + activity in one shot; caller is responsible for the standby gate
func runPrimaryCapture(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, source string, policy *masking.Policy) (*schema.SchemaSnapshot, *schema.PlannerStatsSnapshot, *schema.ActivityStatsSnapshot, int, error) {
	snap, err := cap.Introspect(ctx)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if _, err := store.PutSchema(ctx, key, snap); err != nil {
		slog.Warn("could not save snapshot", "error", err)
	}

	planner, err := cap.CapturePlanner(ctx, snap.ContentHash)
	if err != nil {
		return snap, nil, nil, 0, fmt.Errorf("capture planner stats: %w", err)
	}
	masked := datamask.MaskPlanner(policy, planner)
	if _, err := store.PutPlanner(ctx, key, planner); err != nil {
		slog.Warn("could not save planner stats", "error", err)
	}

	activity, err := cap.CaptureActivity(ctx, snap.ContentHash, source)
	if err != nil {
		return snap, planner, nil, masked, fmt.Errorf("capture activity stats: %w", err)
	}
	if _, err := store.PutActivity(ctx, key, activity); err != nil {
		slog.Warn("could not save activity stats", "error", err)
	}
	return snap, planner, activity, masked, nil
}

func scaffoldConfig(configPath string) error {
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
	content := fmt.Sprintf(`[project]
id = %q

# require_masks = true   # fail init unless data-masking-policy.yml resolves; refuses --no-masks

[default]
profile = %q

[profiles.%s]
schema_file = ".dryrun/schema.json"
# database_id = %q   # defaults to profile name; override to e.g. "auth", "billing"
# masks_file = "data-masking-policy.yml"   # PII policy shared with fixturize; auto-discovered if omitted
# mask_policies = ["pii"]                  # optional; default masks every column listed for this database

# [profiles.dev]
# db_url = "${DATABASE_URL}"

# [conventions]
# See: https://boringsql.com/dryrun/docs/dryrun-toml
`, profileName, profileName, profileName, profileName)
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Created %s (profile %q)\n", configPath, profileName)
	return nil
}
