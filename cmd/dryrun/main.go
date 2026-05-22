package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/datamask"
	"github.com/boringsql/dryrun/internal/diff"
	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/lint"
	drmcp "github.com/boringsql/dryrun/internal/mcp"
	"github.com/boringsql/dryrun/internal/schema"
)

// version is set via ldflags: -X main.version=v0.1.0
var version string

func getVersion() string {
	if version != "" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		v := info.Main.Version
		if v != "" && v != "(devel)" && !strings.Contains(v, "0.0.0-") {
			return v
		}
	}
	return "dev"
}

var (
	flagDB         string
	flagProfile    string
	flagConfig     string
	flagSchemaFile string
	flagMasksFile  string
	flagMaskPolicy []string
	flagNoMasks    bool
)

func main() {
	root := &cobra.Command{
		Use:     "dryrun",
		Short:   "PostgreSQL schema intelligence",
		Version: getVersion(),
	}

	pf := root.PersistentFlags()
	pf.StringVar(&flagDB, "db", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL [env: DATABASE_URL]")
	pf.StringVar(&flagProfile, "profile", "", "config profile name")
	pf.StringVar(&flagConfig, "config", "", "path to dryrun.toml")
	pf.StringVar(&flagSchemaFile, "schema-file", os.Getenv("SCHEMA_FILE"), "path to schema JSON file")
	pf.StringVar(&flagMasksFile, "masks-file", "", "path to data-masking-policy.yml")
	pf.StringSliceVar(&flagMaskPolicy, "mask-policy", nil, "masking policy name (repeatable, comma-separated)")
	pf.BoolVar(&flagNoMasks, "no-masks", false, "disable planner-stats masking")

	root.AddCommand(
		probeCmd(), initCmd(), importCmd(), dumpSchemaCmd(),
		lintCmd(), driftCmd(), snapshotCmd(), profileCmd(),
		mcpServeCmd(), statsCmd(),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func probeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "probe",
		Short: "Check PostgreSQL connectivity and privileges",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, conn, err := connectDB()
			if err != nil {
				return err
			}
			defer conn.Close()

			result, err := conn.Probe(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("PostgreSQL %s\n", result.Version.String())
			fmt.Printf("  %s\n", result.VersionString)

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

			snap.ContentHash = schema.ComputeContentHash(snap)

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
			ctx, conn, err := connectDBFor(url)
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

			ctx, conn, err := connectDB()
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

			for _, c := range report.Changeset.Changes {
				name := c.Name
				if c.Schema != nil {
					name = *c.Schema + "." + name
				}
				fmt.Printf("  [%s] %s %s\n", c.Kind, c.ObjectType, name)
				for _, d := range c.Details {
					fmt.Printf("         %s\n", d)
				}
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

	takeCmd := &cobra.Command{
		Use:   "take",
		Short: "Take a new snapshot (schema + planner + activity; primary only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, conn, err := connectDB()
			if err != nil {
				return err
			}
			defer conn.Close()

			cap := pgxCapturer{pool: conn.Pool()}
			standby, err := cap.IsStandby(ctx)
			if err != nil {
				return fmt.Errorf("check standby status: %w", err)
			}
			if standby {
				return dryrun.NewError(dryrun.ErrReplicaCapture,
					"`dryrun snapshot take` must run against the primary; "+
						"use `dryrun snapshot activity --from <url> --label <name>` to capture activity from a replica")
			}

			store, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			snap, planner, activity, _, err := runPrimaryCapture(cmd.Context(), cap, store, resolveSnapshotKey(), "primary", nil)
			if err != nil {
				return err
			}
			fmt.Printf("Snapshot saved: %s\n", snap.ContentHash)
			fmt.Printf("  %d tables, %d views, %d functions\n", len(snap.Tables), len(snap.Views), len(snap.Functions))
			fmt.Printf("Planner stats saved: %s (%d tables, %d columns, %d indexes)\n",
				planner.ContentHash, len(planner.Tables), len(planner.Columns), len(planner.Indexes))
			fmt.Printf("Activity stats saved: %s (label=primary, %d tables, %d indexes)\n",
				activity.ContentHash, len(activity.Tables), len(activity.Indexes))
			return nil
		},
	}
	addHistFlag(takeCmd)

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

	var fromHash, toHash string
	var latest, prettyDiff bool

	diffCmd := &cobra.Command{
		Use:   "diff",
		Short: "Diff two snapshots",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openHistoryStore(historyDB)
			if err != nil {
				return err
			}
			defer store.Close()

			key := resolveSnapshotKey()
			loadByHash := func(h string) (*schema.SchemaSnapshot, error) {
				return store.GetSchema(cmd.Context(), key, history.NewRefHash(h))
			}

			var fromSnap *schema.SchemaSnapshot
			switch {
			case fromHash != "":
				fromSnap, err = loadByHash(fromHash)
			case latest:
				fromSnap, err = store.GetSchema(cmd.Context(), key, history.NewRefLatest())
			default:
				return fmt.Errorf("specify --from <hash> or --latest")
			}
			if err != nil {
				return err
			}

			var toSnap *schema.SchemaSnapshot
			if toHash != "" {
				toSnap, err = loadByHash(toHash)
			} else {
				ctx, conn, cerr := connectDB()
				if cerr != nil {
					return cerr
				}
				defer conn.Close()
				toSnap, err = conn.Introspect(ctx)
			}
			if err != nil {
				return err
			}

			changeset := diff.DiffSchemas(fromSnap, toSnap)
			fmt.Println(string(marshalJSON(changeset, prettyDiff)))
			return nil
		},
	}
	diffCmd.Flags().StringVar(&fromHash, "from", "", "source snapshot hash")
	diffCmd.Flags().StringVar(&toHash, "to", "", "target snapshot hash")
	diffCmd.Flags().BoolVar(&latest, "latest", false, "use latest saved snapshot as source")
	addHistFlag(diffCmd)
	diffCmd.Flags().BoolVar(&prettyDiff, "pretty", false, "pretty-print JSON")

	cmd.AddCommand(takeCmd, listCmd, diffCmd, snapshotExportCmd(), snapshotActivityCmd(),
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

			store, err := history.OpenDefault()
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
	ctx := context.Background()
	conn, err := schema.Connect(ctx, dbURL)
	if err != nil {
		return nil, nil, err
	}
	return ctx, conn, nil
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
	if path != "" {
		return history.Open(path)
	}
	return history.OpenDefault()
}

func resolveSnapshotKey() history.SnapshotKey {
	cwd, _ := os.Getwd()
	if _, cfg, err := loadProjectConfig(); err == nil {
		if resolved, rerr := cfg.ResolveProfile(nilIfEmpty(flagDB), nilIfEmpty(flagSchemaFile), nilIfEmpty(flagProfile), cwd); rerr == nil {
			return resolved.SnapshotKey()
		}
	}
	// no config — synthesize the same shape ResolvedProfile would have produced for a CLI override
	empty := &config.ProjectConfig{}
	pid := empty.ProjectID(cwd)
	return history.SnapshotKey{ProjectID: pid, DatabaseID: history.DatabaseId(pid)}
}

func resolveMaskPolicy() (*datamask.Policy, error) {
	return resolveMaskPolicyForKey(resolveSnapshotKey())
}

// precedence: --masks-file > profile masks_file > discovery; --no-masks opts out
func resolveMaskPolicyForKey(key history.SnapshotKey) (*datamask.Policy, error) {
	if flagNoMasks {
		return nil, nil
	}

	path := flagMasksFile
	policies := flagMaskPolicy
	if path == "" || len(policies) == 0 {
		cwd, _ := os.Getwd()
		if _, cfg, err := loadProjectConfig(); err == nil {
			if rp, rerr := cfg.ResolveProfile(nilIfEmpty(flagDB), nilIfEmpty(flagSchemaFile), nilIfEmpty(flagProfile), cwd); rerr == nil {
				if path == "" && rp.MasksFile != nil {
					path = *rp.MasksFile
				}
				if len(policies) == 0 {
					policies = rp.MaskPolicies
				}
			}
		}
	}

	if path == "" {
		cwd, _ := os.Getwd()
		if discovered, err := datamask.Discover(cwd); err == nil {
			path = discovered
		}
	}
	if path == "" {
		return nil, nil
	}
	return datamask.Load(path, key.DatabaseID, policies)
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

			mcpSrv := mcpserver.NewMCPServer("dryrun", getVersion(),
				mcpserver.WithInstructions(server.Instructions()),
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
