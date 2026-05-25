package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

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

type pgxCapturer struct{ pool *pgxpool.Pool }

func (c pgxCapturer) IsStandby(ctx context.Context) (bool, error) {
	return schema.FetchIsStandby(ctx, c.pool)
}

func (c pgxCapturer) Introspect(ctx context.Context) (*schema.SchemaSnapshot, error) {
	return schema.IntrospectSchema(ctx, c.pool)
}

func (c pgxCapturer) CapturePlanner(ctx context.Context, schemaRefHash string) (*schema.PlannerStatsSnapshot, error) {
	return schema.CapturePlannerStats(ctx, c.pool, schemaRefHash)
}

func (c pgxCapturer) CaptureActivity(ctx context.Context, schemaRefHash, source string) (*schema.ActivityStatsSnapshot, error) {
	return schema.CaptureActivityStats(ctx, c.pool, schemaRefHash, source)
}

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

			store, err := history.OpenDefault()
			if err != nil {
				return fmt.Errorf("open history store: %w", err)
			}
			defer store.Close()

			return runInitCapture(ctx, pgxCapturer{pool: conn.Pool()}, store, resolveSnapshotKey(), dataDir, initOptions{
				AllowReplica: allowReplica,
				Source:       source,
			})
		},
	}
	cmd.Flags().BoolVar(&allowReplica, "allow-replica", false, "permit capture on a standby (activity stats only)")
	cmd.Flags().StringVar(&source, "source", "", "node label for activity stats (default: hostname)")
	return cmd
}

type initOptions struct {
	AllowReplica bool
	Source       string
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

	snap, planner, activity, err := runPrimaryCapture(ctx, cap, store, key, source)
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
	fmt.Fprintf(os.Stderr, "  Activity: node=%s, %d tables, %d indexes\n",
		source, len(activity.Tables), len(activity.Indexes))
	return nil
}

// schema + planner + activity in one shot; caller is responsible for the standby gate
func runPrimaryCapture(ctx context.Context, cap initCapturer, store initWriter, key history.SnapshotKey, source string) (*schema.SchemaSnapshot, *schema.PlannerStatsSnapshot, *schema.ActivityStatsSnapshot, error) {
	snap, err := cap.Introspect(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if _, err := store.PutSchema(ctx, key, snap); err != nil {
		slog.Warn("could not save snapshot", "error", err)
	}

	planner, err := cap.CapturePlanner(ctx, snap.ContentHash)
	if err != nil {
		return snap, nil, nil, fmt.Errorf("capture planner stats: %w", err)
	}
	if _, err := store.PutPlanner(ctx, key, planner); err != nil {
		slog.Warn("could not save planner stats", "error", err)
	}

	activity, err := cap.CaptureActivity(ctx, snap.ContentHash, source)
	if err != nil {
		return snap, planner, nil, fmt.Errorf("capture activity stats: %w", err)
	}
	if _, err := store.PutActivity(ctx, key, activity); err != nil {
		slog.Warn("could not save activity stats", "error", err)
	}
	return snap, planner, activity, nil
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

[default]
profile = %q

[profiles.%s]
schema_file = ".dryrun/schema.json"
# database_id = %q   # defaults to profile name; override to e.g. "auth", "billing"

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
