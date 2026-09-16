package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/query"
	"github.com/boringsql/dryrun/internal/schema"
)

// exitError carries the process exit code past cobra: 1 failed gate, 2 tool error.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return fmt.Sprintf("exit %d", e.code)
}

func (e *exitError) Unwrap() error { return e.err }

func checkCmd() *cobra.Command {
	var historyDB string

	cmd := &cobra.Command{
		Use:           "check",
		Short:         "Check migrations and queries against the snapshot",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	cmd.PersistentFlags().StringVar(&historyDB, "history-db", "", "history database path")
	cmd.AddCommand(checkMigrationCmd(&historyDB), checkQueryCmd(&historyDB))
	return cmd
}

func checkMigrationCmd(historyDB *string) *cobra.Command {
	var (
		direction          string
		jsonOutput, pretty bool
	)

	cmd := &cobra.Command{
		Use:           "migration <file>...",
		Short:         "Check migration files for lock safety",
		Args:          cobra.MinimumNArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			want := direction
			direction = strings.ToLower(strings.TrimSpace(direction))
			if direction != "up" && direction != "down" {
				return toolError(cmd, fmt.Errorf("--direction must be 'up' or 'down', got %q", want))
			}

			annotated, err := loadAnnotatedForCheck(cmd.Context(), *historyDB)
			if err != nil {
				return toolError(cmd, err)
			}

			reports := make([]query.MigrationReport, 0, len(args))
			worst := 0
			for _, path := range args {
				content, err := os.ReadFile(path)
				if err != nil {
					reportToolError(cmd, fmt.Errorf("%s: %w", path, err))
					worst = 2
					continue
				}
				report, err := query.CheckMigrationFile(string(content), direction, annotated)
				if err != nil {
					reportToolError(cmd, fmt.Errorf("%s: %w", path, err))
					worst = 2
					continue
				}
				report.File = path
				reports = append(reports, *report)
				if code := migrationExitCode(*report); code > worst {
					worst = code
				}
			}

			if jsonOutput {
				emitCheckJSON(cmd, reports, len(args) > 1, pretty)
			} else {
				for _, r := range reports {
					printMigrationReport(cmd.OutOrStdout(), r)
				}
			}
			if worst > 0 {
				return &exitError{code: worst}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&direction, "direction", "up", "migration direction to check: up or down")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "output as JSON")
	cmd.Flags().BoolVar(&pretty, "pretty", false, "pretty-print JSON")
	return cmd
}

// queryReport is one validate_query result plus the file it came from.
type queryReport struct {
	File                string                    `json:"file"`
	Valid               bool                      `json:"valid"`
	Errors              []string                  `json:"errors"`
	Warnings            []query.ValidationWarning `json:"warnings"`
	ReferencedObjects   []query.ReferencedTable   `json:"referenced_objects"`
	ResolvedStarColumns []query.ResolvedStar      `json:"resolved_star_columns"`
	CorrectedSQL        string                    `json:"corrected_sql,omitempty"`
	Fixes               []query.Fix               `json:"fixes,omitempty"`
}

func checkQueryCmd(historyDB *string) *cobra.Command {
	var jsonOutput, pretty bool

	cmd := &cobra.Command{
		Use:           "query <file>...",
		Short:         "Validate query files against the snapshot",
		Args:          cobra.MinimumNArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			annotated, err := loadAnnotatedForCheck(cmd.Context(), *historyDB)
			if err != nil {
				return toolError(cmd, err)
			}

			reports := make([]queryReport, 0, len(args))
			worst := 0
			for _, path := range args {
				content, err := os.ReadFile(path)
				if err != nil {
					reportToolError(cmd, fmt.Errorf("%s: %w", path, err))
					worst = 2
					continue
				}
				result, err := query.ValidateQuery(string(content), annotated.Schema)
				if err != nil {
					// a parse failure is a tool error, not a valid:false verdict
					reportToolError(cmd, fmt.Errorf("%s: %w", path, err))
					worst = 2
					continue
				}
				reports = append(reports, queryReport{
					File:                path,
					Valid:               result.Valid,
					Errors:              result.Errors,
					Warnings:            result.Warnings,
					ReferencedObjects:   result.ReferencedObjects,
					ResolvedStarColumns: result.ResolvedStarColumns,
					CorrectedSQL:        result.CorrectedSQL,
					Fixes:               result.Fixes,
				})
				if !result.Valid && worst < 1 {
					worst = 1
				}
			}

			if jsonOutput {
				emitCheckJSON(cmd, reports, len(args) > 1, pretty)
			} else {
				for _, r := range reports {
					printQueryReport(cmd.OutOrStdout(), r)
				}
			}
			if worst > 0 {
				return &exitError{code: worst}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "output as JSON")
	cmd.Flags().BoolVar(&pretty, "pretty", false, "pretty-print JSON")
	return cmd
}

// migrationExitCode: only dangerous fails the gate; caution is advice.
func migrationExitCode(r query.MigrationReport) int {
	for _, c := range r.Checks {
		if c.Safety == query.SafetyDangerous {
			return 1
		}
	}
	return 0
}

// loadAnnotatedForCheck reads the newest snapshot from history.db. Offline only:
// the CLI never introspects a live database.
func loadAnnotatedForCheck(ctx context.Context, historyDB string) (*schema.AnnotatedSchema, error) {
	var (
		store *history.Store
		err   error
	)
	if historyDB != "" {
		// history.Open creates a missing file; don't materialize an empty db next to a typo
		if _, serr := os.Stat(historyDB); serr != nil {
			return nil, fmt.Errorf("history database %s: %w", historyDB, serr)
		}
		store, err = history.Open(historyDB)
	} else {
		store, err = openExistingHistoryStore()
	}
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("no .dryrun/history.db for this project: run 'dryrun init --db <url>' or 'dryrun snapshot pull --from-path <dir>' first")
		}
		return nil, fmt.Errorf("open history database: %w", err)
	}
	defer store.Close()

	if store.Compat() == history.CompatNewer {
		return nil, errors.New("the history database was written by a newer dryrun; upgrade dryrun")
	}

	key := resolveSnapshotKey()
	annotated, err := store.GetAnnotated(ctx, key, history.NewRefLatest())
	if err != nil {
		if errors.Is(err, history.ErrSnapshotNotFound) {
			return nil, fmt.Errorf("no schema snapshot in %s for project=%s database=%s: run 'dryrun init --db <url>' or 'dryrun snapshot pull --from-path <dir>' first", historyPathLabel(historyDB), key.ProjectID, key.DatabaseID)
		}
		return nil, fmt.Errorf("read %s: %w", historyPathLabel(historyDB), err)
	}
	return annotated, nil
}

func historyPathLabel(historyDB string) string {
	if historyDB != "" {
		return historyDB
	}
	return ".dryrun/history.db"
}

// toolError prints the reason (cobra is silenced) and asks for exit 2.
func toolError(cmd *cobra.Command, err error) error {
	reportToolError(cmd, err)
	return &exitError{code: 2, err: err}
}

func reportToolError(cmd *cobra.Command, err error) {
	fmt.Fprintf(cmd.ErrOrStderr(), "Error: %s\n", err)
}

// emitCheckJSON: one file argument emits the object (MCP shape), several an
// array. multi comes from len(args), not len(reports): a failed file in a
// multi-file run must not flip the shape a script branches on.
func emitCheckJSON[T any](cmd *cobra.Command, reports []T, multi, pretty bool) {
	if len(reports) == 0 {
		return
	}
	var payload any = reports
	if !multi {
		payload = reports[0]
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(marshalJSON(payload, pretty)))
}

func printMigrationReport(w io.Writer, r query.MigrationReport) {
	fmt.Fprintf(w, "%s: %s, %s\n", r.File, r.Framework, r.Direction)
	if len(r.Checks) == 0 {
		fmt.Fprintln(w, "  no DDL operation identified")
	}
	for _, c := range r.Checks {
		fmt.Fprintf(w, "  [%s] %s", c.Safety, c.Operation)
		if c.Table != nil {
			fmt.Fprintf(w, " on %s", *c.Table)
		}
		fmt.Fprintln(w)
		var details []string
		if c.LockType != "" {
			details = append(details, "lock: "+c.LockType)
		}
		if c.LockDuration != "" {
			details = append(details, "duration: "+c.LockDuration)
		}
		if c.TableSize != nil {
			details = append(details, "size: "+*c.TableSize)
		}
		if c.RowEstimate != nil {
			details = append(details, fmt.Sprintf("rows: ~%d", int64(*c.RowEstimate)))
		}
		if len(details) > 0 {
			fmt.Fprintf(w, "      %s\n", strings.Join(details, ", "))
		}
		fmt.Fprintf(w, "      %s\n", c.Recommendation)
		for _, s := range c.SaferSQL {
			fmt.Fprintf(w, "      safer: %s\n", indentContinuation(s))
		}
		if c.RollbackDDL != nil {
			fmt.Fprintf(w, "      rollback: %s\n", indentContinuation(*c.RollbackDDL))
		}
	}
	if r.MigrationSQL != "" {
		fmt.Fprintln(w, "  migration_sql:")
		for _, line := range strings.Split(strings.TrimRight(r.MigrationSQL, "\n"), "\n") {
			fmt.Fprintf(w, "    %s\n", line)
		}
	}
	if r.Hint != "" {
		fmt.Fprintf(w, "  hint: %s\n", r.Hint)
	}
	fmt.Fprintln(w)
}

func printQueryReport(w io.Writer, r queryReport) {
	if r.Valid {
		fmt.Fprintf(w, "%s: valid\n", r.File)
	} else {
		fmt.Fprintf(w, "%s: invalid\n", r.File)
	}
	for _, e := range r.Errors {
		fmt.Fprintf(w, "  error: %s\n", e)
	}
	for _, warn := range r.Warnings {
		fmt.Fprintf(w, "  %s: %s\n", warn.Severity, warn.Message)
	}
	if r.CorrectedSQL != "" {
		fmt.Fprintln(w, "  corrected_sql:")
		for _, line := range strings.Split(strings.TrimRight(r.CorrectedSQL, "\n"), "\n") {
			fmt.Fprintf(w, "    %s\n", line)
		}
	}
	fmt.Fprintln(w)
}

func indentContinuation(s string) string {
	return strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n      ")
}
