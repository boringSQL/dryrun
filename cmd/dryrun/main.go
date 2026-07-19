package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/boringsql/dryrun/internal/buildinfo"
	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/history"
	drmcp "github.com/boringsql/dryrun/internal/mcp"
	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/diff"
	"github.com/boringsql/dryrun/pkg/lint"
)

var (
	flagDB              string
	flagProfile         string
	flagConfig          string
	flagSchemaFile      string
	flagAllowPrivileged bool
	flagStmtTimeout     time.Duration
	flagLockTimeout     time.Duration
	flagIdleTxTimeout   time.Duration
)

func main() {
	root := &cobra.Command{
		Use:     "dryrun",
		Short:   "PostgreSQL schema intelligence",
		Version: buildinfo.Get(),
	}

	pf := root.PersistentFlags()
	pf.StringVar(&flagDB, "db", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL [env: DATABASE_URL]")
	pf.StringVar(&flagProfile, "profile", "", "config profile name")
	pf.StringVar(&flagConfig, "config", "", "path to dryrun.toml")
	pf.StringVar(&flagSchemaFile, "schema-file", os.Getenv("SCHEMA_FILE"), "path to schema JSON file")
	pf.BoolVar(&flagAllowPrivileged, "allow-privileged", false, "permit superuser/replication/bypassrls roles on prod-reading commands (warns)")
	guards := schema.DefaultSessionGuards()
	pf.DurationVar(&flagStmtTimeout, "statement-timeout", guards.StatementTimeout, "session statement_timeout (0 disables)")
	pf.DurationVar(&flagLockTimeout, "lock-timeout", guards.LockTimeout, "session lock_timeout (0 disables)")
	pf.DurationVar(&flagIdleTxTimeout, "idle-tx-timeout", guards.IdleInTxTimeout, "session idle_in_transaction_session_timeout (0 disables)")

	root.AddCommand(
		probeCmd(), initCmd(), setupCmd(), importCmd(), dumpSchemaCmd(),
		lintCmd(), driftCmd(), snapshotCmd(), profileCmd(),
		remoteCmd(), mcpServeCmd(), statsCmd(), versionCmd(),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print dryrun and history schema versions",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("dryrun %s\n", buildinfo.Get())
			fmt.Printf("history schema: v%d\n", history.HistorySchemaVersion)
			return nil
		},
	}
}

func probeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "probe",
		Short: "Check PostgreSQL connectivity and privileges",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, conn, err := connectDBProd()
			if err != nil {
				return err
			}
			defer conn.Close()

			result, err := conn.Probe(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("%s %s\n", result.Flavor.Display(), result.Version.String())
			fmt.Printf("  %s\n", result.VersionString)
			if result.Flavor != schema.FlavorPostgres {
				fmt.Printf("  flavor: %s (catalog writable: %t, storage inspectable: %t, config tunable: %t)\n",
					result.Flavor, result.Capabilities.CatalogWritable,
					result.Capabilities.StorageInspectable, result.Capabilities.ConfigTunable)
			}

			report, err := conn.CheckPrivileges(ctx)
			if err != nil {
				return err
			}
			okDenied := func(ok bool) string {
				if ok {
					return "ok"
				}
				return "DENIED"
			}
			fmt.Println("Privileges:")
			fmt.Printf("  pg_catalog:           %s\n", okDenied(report.PgCatalog))
			fmt.Printf("  information_schema:   %s\n", okDenied(report.InformationSchema))
			fmt.Printf("  pg_stat_user_tables:  %s\n", okDenied(report.PgStatUserTables))
			return nil
		},
	}
}

func importCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import <schema-file>",
		Short: "Import a schema JSON file into .dryrun/",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			snap, err := schema.LoadSchemaFile(args[0])
			if err != nil {
				return fmt.Errorf("invalid schema file: %w", err)
			}

			if len(snap.Tables) == 0 && len(snap.Views) == 0 {
				return fmt.Errorf("schema file contains no tables or views")
			}

			snap.ContentHash = schema.DigestFor(snap)

			dataDir, err := history.DefaultDataDir()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dataDir, 0o755); err != nil {
				return err
			}

			outputPath := filepath.Join(dataDir, "schema.json")
			if err := writeJSONFile(outputPath, snap, true); err != nil {
				return err
			}

			fmt.Fprintf(os.Stderr, "Imported %d tables, %d views to %s\n",
				len(snap.Tables), len(snap.Views), outputPath)
			return nil
		},
	}
	return cmd
}

func dumpSchemaCmd() *cobra.Command {
	var pretty bool
	var output, name, source string

	cmd := &cobra.Command{
		Use:   "dump-schema",
		Short: "Export DDL schema from live database to JSON",
		RunE: func(cmd *cobra.Command, args []string) error {
			// --source wins over --db; env fallback is SOURCE_DATABASE_URL.
			url := source
			if url == "" {
				url = os.Getenv("SOURCE_DATABASE_URL")
			}
			ctx, conn, err := connectDBProdFor(url)
			if err != nil {
				return err
			}
			defer conn.Close()

			snap, err := conn.Introspect(ctx)
			if err != nil {
				return err
			}
			if name != "" {
				src := name
				snap.Source = &src
			}

			if output != "" {
				if err := writeJSONFile(output, snap, pretty); err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "Schema written to %s\n", output)
			} else {
				fmt.Println(string(marshalJSON(snap, pretty)))
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&pretty, "pretty", false, "pretty-print JSON")
	cmd.Flags().StringVarP(&output, "output", "o", "", "output file path")
	cmd.Flags().StringVar(&name, "name", "", "source name (sets snapshot.Source)")
	cmd.Flags().StringVar(&source, "source", "", "connection URL override [env: SOURCE_DATABASE_URL]")
	return cmd
}

func lintCmd() *cobra.Command {
	var schemaFilter string
	var pretty, jsonOutput bool

	cmd := &cobra.Command{
		Use:   "lint",
		Short: "Run lint rules against schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			snap, err := loadSchemaForLint()
			if err != nil {
				return err
			}

			if schemaFilter != "" {
				var filtered []schema.Table
				for _, t := range snap.Tables {
					if t.Schema == schemaFilter {
						filtered = append(filtered, t)
					}
				}
				snap.Tables = filtered
			}

			lintCfg := loadLintConfig()
			report := lint.LintSchema(snap, &lintCfg)

			if jsonOutput {
				fmt.Println(string(marshalJSON(report, pretty)))
				return nil
			}
			if len(report.Findings) == 0 {
				fmt.Printf("No lint findings (%d tables checked).\n", report.TablesChecked)
			} else {
				for _, f := range report.Findings {
					location := ""
					if len(f.Tables) > 0 {
						location = f.Tables[0]
					}
					if f.Column != nil {
						location += "." + *f.Column
					}
					severity := "INFO "
					switch f.Severity {
					case lint.SeverityError:
						severity = "ERROR"
					case lint.SeverityWarning:
						severity = "WARN "
					}
					fmt.Printf("[%s] %s: %s\n", severity, location, f.Message)
					fmt.Printf("       fix: %s\n", f.Recommendation)
				}
				fmt.Printf("\n%d finding(s): %d error, %d warning, %d info (%d tables checked)\n",
					len(report.Findings), report.Summary.Errors, report.Summary.Warnings, report.Summary.Info, report.TablesChecked)
			}
			if report.Summary.Errors > 0 {
				os.Exit(1)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&schemaFilter, "schema", "", "filter by schema name")
	// hidden deprecated alias retained for upstream parity
	cmd.Flags().StringVar(&schemaFilter, "schema-name", "", "deprecated alias for --schema")
	_ = cmd.Flags().MarkHidden("schema-name")
	cmd.Flags().BoolVar(&pretty, "pretty", false, "pretty-print JSON")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "output as JSON")
	return cmd
}

func driftCmd() *cobra.Command {
	var pretty, jsonOutput bool
	var against string

	cmd := &cobra.Command{
		Use:   "drift",
		Short: "Compare live database schema against saved snapshot",
		RunE: func(cmd *cobra.Command, args []string) error {
			// --against is the explicit snapshot path; wins over global --schema-file and auto-discovery.
			var saved *schema.SchemaSnapshot
			var err error
			if against != "" {
				saved, err = loadSchemaFile(against)
			} else {
				saved, err = loadSchemaForLint()
			}
			if err != nil {
				return fmt.Errorf("cannot load saved schema: %w", err)
			}

			ctx, conn, err := connectDBProd()
			if err != nil {
				return err
			}
			defer conn.Close()

			live, err := conn.Introspect(ctx)
			if err != nil {
				return err
			}

			report := diff.ClassifyDrift(saved, live)

			if jsonOutput {
				fmt.Println(string(marshalJSON(report, pretty)))
				return nil
			}

			if report.Direction == diff.DriftIdentical {
				fmt.Printf("No drift detected. Schema hash: %s\n", report.SavedHash)
				return nil
			}

			fmt.Printf("Drift: %s\n", report.Direction)
			fmt.Printf("  saved: %s\n", report.SavedHash)
			fmt.Printf("  live:  %s\n", report.LiveHash)
			fmt.Printf("  %d added, %d removed, %d modified\n\n",
				report.AddedCount, report.RemovedCount, report.ModifiedCount)

			for _, c := range report.Delta.Changes {
				fmt.Printf("  %s %s\n", diff.Marker(c), diff.Describe(c))
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&pretty, "pretty", false, "pretty-print JSON")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "output as JSON")
	cmd.Flags().StringVar(&against, "against", "", "explicit snapshot file path (wins over --schema-file)")
	return cmd
}

func snapshotCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "snapshot", Short: "Manage schema snapshots"}

	var historyDB string
	addHistFlag := func(c *cobra.Command) {
		c.Flags().StringVar(&historyDB, "history-db", "", "history database path")
	}

	var (
		pushAfter  bool
		pushRemote string
	)
	takeCmd := &cobra.Command{
		Use:   "take",
		Short: "Take a new snapshot (schema + planner + activity; primary only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, conn, err := connectDBProd()
			if err != nil {
				return err
			}
			defer conn.Close()

			cap, err := newPgxCapturer(cmd.Context(), conn.Pool())
			if err != nil {
				return err
			}
			defer cap.Close(cmd.Context())

			store, err := openHistoryStore(historyDB)
			if err != nil {
				return err
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

			snap, planner, activity, masked, err := runSnapshotTake(cmd.Context(), cap, store, key, policy)
			if err != nil {
				return err
			}
			fmt.Printf("Snapshot saved: %s\n", snap.ContentHash)
			fmt.Printf("  %d tables, %d views, %d functions\n", len(snap.Tables), len(snap.Views), len(snap.Functions))
			fmt.Printf("Planner stats saved: %s (%d tables, %d columns, %d indexes)\n",
				planner.ContentHash, len(planner.Tables), len(planner.Columns), len(planner.Indexes))
			if masked > 0 {
				fmt.Printf("  Masked: %d planner-stats columns\n", masked)
			}
			fmt.Printf("Activity stats saved: %s (label=primary, %d tables, %d indexes)\n",
				activity.ContentHash, len(activity.Tables), len(activity.Indexes))

			if pushAfter {
				dst, err := resolveSyncStore("", "", pushRemote)
				if err != nil {
					return err
				}
				return runSync(cmd.Context(), store, dst, false, fullScope(), os.Stdout)
			}
			return nil
		},
	}
	addHistFlag(takeCmd)
	takeCmd.Flags().StringVar(&flagMasksFile, "masks-file", "", "path to data-masking-policy.yml")
	takeCmd.Flags().StringSliceVar(&flagMaskPolicy, "mask-policy", nil, "masking policy name (repeatable, comma-separated)")
	takeCmd.Flags().BoolVar(&flagNoMasks, "no-masks", false, "disable planner-stats masking (raw stats land in history.db)")
	takeCmd.Flags().BoolVar(&pushAfter, "push", false, "push the snapshot to a remote after capture")
	takeCmd.Flags().StringVar(&pushRemote, "remote", "", "configured [[remote]] name (with --push)")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List saved snapshots",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			key := resolveSnapshotKey()
			kinds, err := store.ListKinds(cmd.Context(), key)
			if err != nil {
				return err
			}
			var summaries []history.SnapshotSummary
			for _, k := range kinds {
				list, err := store.List(cmd.Context(), key, k, history.TimeRange{})
				if err != nil {
					return err
				}
				for i := range list {
					list[i].Kind = k
				}
				summaries = append(summaries, list...)
			}
			if len(summaries) == 0 {
				fmt.Println("No snapshots found for this database.")
				return nil
			}
			sort.Slice(summaries, func(i, j int) bool {
				return summaries[i].Timestamp.After(summaries[j].Timestamp)
			})
			typeW := len("TYPE")
			for _, s := range summaries {
				if n := len(s.Kind.String()); n > typeW {
					typeW = n
				}
			}
			for _, s := range summaries {
				hash := s.ContentHash
				if len(hash) > 16 {
					hash = hash[:16]
				}
				fmt.Printf("%s  %-*s  %s  %s\n",
					s.Timestamp.Format("2006-01-02 15:04:05"),
					typeW, s.Kind.String(),
					hash, s.Database)
			}
			fmt.Printf("\n%d snapshot(s) total\n", len(summaries))
			return nil
		},
	}
	addHistFlag(listCmd)

	diffCmd := newDiffCmd(&historyDB)
	addHistFlag(diffCmd)

	deleteCmd := snapshotDeleteCmd(&historyDB)
	addHistFlag(deleteCmd)

	cmd.AddCommand(takeCmd, listCmd, diffCmd, deleteCmd, snapshotActivityCmd(),
		snapshotPushCmd(), snapshotPullCmd())
	return cmd
}

func profileCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "profile", Short: "Manage dryrun.toml profiles"}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List profiles",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfgPath, cfg, err := loadProjectConfig()
			if err != nil {
				return err
			}
			fmt.Printf("Config: %s\n", cfgPath)
			if cfg.Default != nil && cfg.Default.Profile != nil {
				fmt.Printf("Default profile: %s\n", *cfg.Default.Profile)
			}
			fmt.Println()
			if len(cfg.Profiles) == 0 {
				fmt.Println("No profiles defined.")
				return nil
			}
			for name, p := range cfg.Profiles {
				source := "empty"
				if p.DBURL != nil {
					source = "db_url"
				} else if p.SchemaFile != nil {
					source = "schema_file"
				}
				fmt.Printf("  %s (%s)\n", name, source)
			}
			return nil
		},
	}

	showCmd := &cobra.Command{
		Use:   "show [name]",
		Short: "Show profile details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, cfg, err := loadProjectConfig()
			if err != nil {
				return err
			}
			name := args[0]
			p, ok := cfg.Profiles[name]
			if !ok {
				return fmt.Errorf("profile '%s' not found", name)
			}
			fmt.Printf("Profile: %s\n", name)
			if p.DBURL != nil {
				fmt.Printf("  db_url: %s\n", *p.DBURL)
			}
			if p.SchemaFile != nil {
				fmt.Printf("  schema_file: %s\n", *p.SchemaFile)
			}
			return nil
		},
	}

	cmd.AddCommand(listCmd, showCmd)
	return cmd
}

func statsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "stats", Short: "Manage statistics injection"}

	applyCmd := &cobra.Command{
		Use:   "apply",
		Short: "Inject production planner stats into local database for realistic EXPLAIN plans",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, conn, err := connectDB()
			if err != nil {
				return err
			}
			defer conn.Close()

			probe, err := conn.Probe(ctx)
			if err != nil {
				return fmt.Errorf("probe: %w", err)
			}

			store, err := openHistoryStore("")
			if err != nil {
				return fmt.Errorf("open history store: %w", err)
			}
			defer store.Close()

			annotated, err := store.GetAnnotated(cmd.Context(), resolveSnapshotKey(), history.NewRefLatest())
			if err != nil {
				return fmt.Errorf("load annotated snapshot from history: %w", err)
			}

			if err := schema.CanInjectStats(annotated); err != nil {
				return err
			}

			result, err := schema.InjectStats(ctx, conn.Pool(), annotated, probe.Version.Major)
			if err != nil {
				return err
			}

			fmt.Fprintf(os.Stderr, "Stats applied (%s): %d tables, %d indexes, %d columns updated\n",
				result.Method, result.TablesUpdated, result.IndexesUpdated, result.ColumnsUpdated)
			for _, w := range result.Warnings {
				fmt.Fprintf(os.Stderr, "  warning: %s\n", w)
			}
			return nil
		},
	}

	cmd.AddCommand(applyCmd)
	return cmd
}

func requireDB() (string, error) {
	if flagDB != "" {
		return flagDB, nil
	}
	if url, ok := dbURLFromProfile(); ok {
		return url, nil
	}
	return "", fmt.Errorf("--db, DATABASE_URL, or a profile with db_url is required")
}

func dbURLFromProfile() (string, bool) {
	cwd, _ := os.Getwd()
	_, cfg, err := loadProjectConfig()
	if err != nil {
		return "", false
	}
	resolved, err := cfg.ResolveProfile(nil, nil, nilIfEmpty(flagProfile), cwd)
	if err != nil || resolved.DBURL == nil || *resolved.DBURL == "" {
		return "", false
	}
	return *resolved.DBURL, true
}

// connectDB calls requireDB then opens a schema connection.
func connectDB() (context.Context, *schema.DryRun, error) {
	return connectDBFor("")
}

// override wins; empty falls back to --db / profile / DATABASE_URL.
func connectDBFor(override string) (context.Context, *schema.DryRun, error) {
	dbURL := override
	if dbURL == "" {
		u, err := requireDB()
		if err != nil {
			return nil, nil, err
		}
		dbURL = u
	}
	guards := schema.DefaultSessionGuards()
	guards.StatementTimeout = flagStmtTimeout
	guards.LockTimeout = flagLockTimeout
	guards.IdleInTxTimeout = flagIdleTxTimeout
	ctx := context.Background()
	conn, err := schema.ConnectWithGuards(ctx, dbURL, guards)
	if err != nil {
		return nil, nil, err
	}
	return ctx, conn, nil
}

// prod-reading variant: refuses privileged roles unless --allow-privileged.
func connectDBProd() (context.Context, *schema.DryRun, error) {
	return connectDBProdFor("")
}

func connectDBProdFor(override string) (context.Context, *schema.DryRun, error) {
	ctx, conn, err := connectDBFor(override)
	if err != nil {
		return nil, nil, err
	}
	report, err := conn.RoleReport(ctx)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if err := rolePreflight(report, flagAllowPrivileged); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return ctx, conn, nil
}

// fail closed on privileged roles; --allow-privileged downgrades to a loud warning
func rolePreflight(report *schema.RoleReport, allowPrivileged bool) error {
	if !report.Privileged() {
		return nil
	}
	privs := strings.Join(report.Privileges(), ", ")
	if !allowPrivileged {
		return fmt.Errorf("role %q is privileged (%s); dryrun refuses to read production with it.\n"+
			"       Use a read-only role (see dryrun-readonly-role.sql) or pass --allow-privileged",
			report.Rolname, privs)
	}
	slog.Warn("connected with a privileged role", "role", report.Rolname, "privileges", privs)
	return nil
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

func marshalJSON(v any, pretty bool) []byte {
	if pretty {
		b, _ := json.MarshalIndent(v, "", "  ")
		return b
	}
	b, _ := json.Marshal(v)
	return b
}

func writeJSONFile(path string, v any, pretty bool) error {
	return os.WriteFile(path, marshalJSON(v, pretty), 0o644)
}

func loadProjectConfig() (string, *config.ProjectConfig, error) {
	if flagConfig != "" {
		cfg, err := config.Load(flagConfig)
		if err != nil {
			return "", nil, err
		}
		return flagConfig, cfg, nil
	}
	cwd, _ := os.Getwd()
	path, cfg, found := config.Discover(cwd)
	if !found {
		return "", nil, fmt.Errorf("no dryrun.toml found")
	}
	return path, cfg, nil
}

func loadLintConfig() lint.Config {
	_, cfg, err := loadProjectConfig()
	if err != nil {
		return lint.DefaultConfig()
	}
	return cfg.LintConfig()
}

func loadSchemaForLint() (*schema.SchemaSnapshot, error) {
	cwd, _ := os.Getwd()

	// try profile-based schema file
	if _, cfg, err := loadProjectConfig(); err == nil {
		resolved, err := cfg.ResolveProfile(nilIfEmpty(flagDB), nilIfEmpty(""), nilIfEmpty(flagProfile), cwd)
		if err == nil && resolved.SchemaFile != nil {
			return loadSchemaFile(*resolved.SchemaFile)
		}
	}

	if flagSchemaFile != "" {
		return loadSchemaFile(flagSchemaFile)
	}

	// try auto-discovered schema.json
	if dataDir, err := history.DefaultDataDir(); err == nil {
		candidate := dataDir + "/schema.json"
		if _, err := os.Stat(candidate); err == nil {
			return loadSchemaFile(candidate)
		}
	}

	// fall back to live DB
	if flagDB != "" {
		ctx := context.Background()
		conn, err := schema.Connect(ctx, flagDB)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		return conn.Introspect(ctx)
	}

	return nil, fmt.Errorf("no schema source found. Either:\n" +
		"1. Run 'dryrun --db <url> init' to create .dryrun/schema.json\n" +
		"2. Pass --db <url> for live database mode\n" +
		"3. Configure a profile in dryrun.toml")
}

func loadSchemaFile(path string) (*schema.SchemaSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var snap schema.SchemaSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func openHistoryStore(path string) (*history.Store, error) {
	var (
		s   *history.Store
		err error
	)
	if path != "" {
		s, err = history.Open(path)
	} else {
		s, err = history.OpenDefault()
	}
	if err == nil {
		warnIfIncompatible(s)
	}
	return s, err
}

// warn if history.db was written by a different dryrun
func warnIfIncompatible(s *history.Store) {
	switch s.Compat() {
	case history.CompatLegacy:
		fmt.Fprintln(os.Stderr, "warning: the history database was created by an older dryrun and cannot be read.")
		fmt.Fprintln(os.Stderr, "         Re-run 'dryrun init', or 'dryrun snapshot pull' to re-import its snapshots.")
	case history.CompatNewer:
		fmt.Fprintln(os.Stderr, "warning: the history database was written by a newer dryrun; some data may be unreadable. Please, upgrade dryrun.")
	}
}

func resolveSnapshotKey() history.SnapshotKey {
	cwd, _ := os.Getwd()
	if _, cfg, err := loadProjectConfig(); err == nil {
		// resolve the profile by name only: a --db override must not drop its database_id
		if resolved, rerr := cfg.ResolveProfile(nil, nil, nilIfEmpty(flagProfile), cwd); rerr == nil {
			return resolved.SnapshotKey()
		}
	}
	// no config — synthesize the same shape ResolvedProfile would have produced for a CLI override
	empty := &config.ProjectConfig{}
	pid := empty.ProjectID(cwd)
	return history.SnapshotKey{ProjectID: pid, DatabaseID: history.DatabaseId(pid)}
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func mcpServeCmd() *cobra.Command {
	var schemaFile, transport string
	var port int

	cmd := &cobra.Command{
		Use:   "mcp-serve",
		Short: "Start MCP server",
		RunE: func(cmd *cobra.Command, args []string) error {
			lintCfg := loadLintConfig()

			// Resolve schema source; --schema overrides global --schema-file
			effectiveSchemaFile := schemaFile
			if effectiveSchemaFile == "" && flagSchemaFile != "" {
				effectiveSchemaFile = flagSchemaFile
			}

			// reload_schema reuses this list later
			var candidates []string
			if effectiveSchemaFile != "" {
				candidates = append(candidates, effectiveSchemaFile)
			}
			cwd, _ := os.Getwd()
			if _, cfg, err := loadProjectConfig(); err == nil {
				if resolved, err := cfg.ResolveProfile(nilIfEmpty(flagDB), nil, nilIfEmpty(flagProfile), cwd); err == nil && resolved.SchemaFile != nil {
					candidates = append(candidates, *resolved.SchemaFile)
				}
			}
			if dataDir, err := history.DefaultDataDir(); err == nil {
				candidates = append(candidates, dataDir+"/schema.json")
			}

			if effectiveSchemaFile == "" {
				for _, c := range candidates {
					if _, err := os.Stat(c); err == nil {
						effectiveSchemaFile = c
						break
					}
				}
			}

			var pgMustardAPIKey string
			if _, cfg, err := loadProjectConfig(); err == nil && cfg.Services != nil && cfg.Services.PgMustardAPIKey != nil {
				pgMustardAPIKey = *cfg.Services.PgMustardAPIKey
			}

			var server *drmcp.Server
			switch {
			case effectiveSchemaFile != "":
				snap, err := loadSchemaFile(effectiveSchemaFile)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "dryrun: loaded schema from %s (%d tables, offline mode)\n",
					effectiveSchemaFile, len(snap.Tables))
				server = drmcp.NewOfflineServer(snap, lintCfg)
				server.SetSchemaCandidates(candidates)
				// history.db carries planner/activity stats; without it offline tools (vacuum_health, detect…) see nil sizing
				if h, err := history.OpenDefault(); err == nil {
					server.SetHistory(h)
					server.SetSnapshotKey(resolveSnapshotKey())
					if server.BootstrapFromHistory(context.Background()) {
						fmt.Fprintln(os.Stderr, "dryrun: attached planner/activity stats from history.db")
					}
				}
			case flagDB != "":
				ctx := context.Background()
				conn, err := schema.Connect(ctx, flagDB)
				if err != nil {
					return err
				}
				defer conn.Close()

				snap, err := conn.Introspect(ctx)
				if err != nil {
					return err
				}

				var hist *history.Store
				if h, err := history.OpenDefault(); err == nil {
					hist = h
				}

				server = drmcp.NewServer(conn.Pool(), flagDB, snap, hist, lintCfg, pgMustardAPIKey)
				server.SetSchemaCandidates(candidates)
				server.SetSnapshotKey(resolveSnapshotKey())
			default:
				fmt.Fprintln(os.Stderr, "dryrun: no schema found — starting in uninitialized mode")
				fmt.Fprintln(os.Stderr, "dryrun: use the reload_schema tool after running dump-schema")
				server = drmcp.NewOfflineServer(&schema.SchemaSnapshot{}, lintCfg)
				server.SetUninitialized(candidates)
			}

			mcpSrv := mcpserver.NewMCPServer("dryrun", buildinfo.Get(),
				mcpserver.WithInstructions(server.Instructions()),
				// a handler panic (malformed plan_json, bad SQL) returns a tool error instead of killing stdio
				mcpserver.WithRecovery(),
				// declared Enum/Required/type constraints reject bad args before handlers run
				mcpserver.WithInputSchemaValidation(),
				// a payload that drifts from its output schema fails here, not in the client
				mcpserver.WithOutputSchemaValidation(),
			)
			server.Register(mcpSrv)

			switch transport {
			case "stdio":
				fmt.Fprintln(os.Stderr, "dryrun: starting MCP server on stdio")
				return mcpserver.NewStdioServer(mcpSrv).Listen(context.Background(), os.Stdin, os.Stdout)
			default:
				return fmt.Errorf("unknown transport '%s' (expected: stdio)", transport)
			}
		},
	}

	cmd.Flags().StringVar(&schemaFile, "schema", os.Getenv("DRY_RUN_SCHEMA_FILE"), "path to schema JSON file")
	cmd.Flags().StringVar(&transport, "transport", "stdio", "transport (stdio)")
	cmd.Flags().IntVar(&port, "port", 3000, "port for HTTP transport")
	return cmd
}
