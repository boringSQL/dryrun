package query

import (
	"fmt"
	"strings"

	"github.com/boringsql/dryrun/internal/schema"
)

type (
	// MigrationReport is one migration file's analysis, shared by the
	// check_migration MCP tool and `dryrun check migration`. File is CLI-only.
	MigrationReport struct {
		File         string           `json:"file,omitempty"`
		Framework    string           `json:"framework"`
		Direction    string           `json:"direction"`
		Checks       []MigrationCheck `json:"checks"`
		MigrationSQL string           `json:"migration_sql,omitempty"`
		Hint         string           `json:"hint,omitempty"`
	}
)

// CheckMigrationFile analyzes one direction of a migration file against the
// annotated snapshot.
func CheckMigrationFile(content, direction string, a *schema.AnnotatedSchema) (*MigrationReport, error) {
	direction = strings.ToLower(strings.TrimSpace(direction))
	if direction == "" {
		direction = "up"
	}
	env := ParseMigrationEnvelope(content)
	section, err := env.Section(direction)
	if err != nil {
		return nil, err
	}

	report := &MigrationReport{Framework: string(env.Framework), Direction: direction}

	checks, err := CheckMigration(section.DDL, a)
	if err != nil {
		return nil, err
	}
	MarkConcurrentInTransaction(env, section, checks)
	if len(checks) == 0 {
		report.Checks = []MigrationCheck{}
		report.Hint = "Could not identify a specific DDL operation to check."
		return report, nil
	}

	var unsafe, rewritten, multiStep int
	concurrentIndex := false
	for _, c := range checks {
		// a plain index can be safe; only CONCURRENTLY warrants the lecture
		if strings.Contains(c.Operation, "CONCURRENTLY") {
			concurrentIndex = true
		} else if c.Safety != SafetySafe {
			for _, s := range c.SaferSQL {
				if strings.Contains(s, "CONCURRENTLY") {
					concurrentIndex = true
					break
				}
			}
		}
		if c.Safety == SafetySafe {
			continue
		}
		unsafe++
		if len(c.SaferSQL) == 0 {
			continue
		}
		rewritten++
		if len(c.SaferSQL) > 1 {
			multiStep++
		}
	}

	composed := ComposeMigrationSQL(checks)
	// "" when no runnable file exists (tern + a CONCURRENTLY rewrite); the concurrency hint explains that case
	migrationSQL := FormatMigrationFile(env, direction, composed)

	hint := ""
	switch {
	case migrationSQL != "":
		hint = "safer_sql holds the rewrite; migration_sql is those statements, with the safe ones passed through unchanged, as one runnable file in order."
	case rewritten > 0 && rewritten == unsafe:
		hint = "safer_sql holds the rewrite: run those statements, in that order, instead of the input."
	case rewritten > 0:
		hint = "safer_sql holds the rewrite for the statements that have a mechanical one. The rest carry a recommendation only, because the safe form needs a decision this tool cannot make -- a batch size, a backfill window, a deploy order."
	case unsafe > 0:
		hint = "No mechanical rewrite for these. Read recommendation and rollback_ddl before applying anything."
	}
	// one wrapping transaction would hold the first statement's lock across the
	// scan in the second
	if multiStep > 0 && !section.NoTransaction {
		hint = joinHints(hint, "Run each statement in safer_sql in its own transaction. A migration runner that wraps the file in one holds the ACCESS EXCLUSIVE taken by the first statement across the scan in the second, which is worse than the input.")
	}
	if concurrentIndex {
		hint = joinHints(hint, migrationConcurrencyHint(env.Framework, direction, section.NoTransaction, migrationSQL != ""))
	}
	if direction == "up" && env.Down != nil {
		hint = joinHints(hint, "This file has a down section; call again with direction='down' to check the rollback too.")
	}

	report.MigrationSQL = migrationSQL
	report.Checks = checks
	report.Hint = hint
	return report, nil
}

func migrationConcurrencyHint(framework MigrationFramework, direction string, noTx, fileEmitted bool) string {
	if noTx {
		return "This migration is already configured to run without a surrounding transaction, which is what CREATE INDEX CONCURRENTLY needs."
	}
	switch framework {
	case FrameworkGoose:
		if fileEmitted {
			return "CREATE INDEX CONCURRENTLY cannot run inside a transaction, and goose wraps each migration in one by default -- migration_sql adds `-- +goose NO TRANSACTION` at the top, or the statement fails at runtime."
		}
		return "CREATE INDEX CONCURRENTLY cannot run inside a transaction, and goose wraps each migration in one by default -- the file needs `-- +goose NO TRANSACTION` at the top, or the statement fails at runtime."
	case FrameworkDbmate:
		marker := "-- migrate:up"
		if strings.EqualFold(direction, "down") {
			marker = "-- migrate:down"
		}
		if fileEmitted {
			return fmt.Sprintf("CREATE INDEX CONCURRENTLY cannot run inside a transaction, and dbmate wraps each migration in one by default -- migration_sql adds `transaction:false` to the `%s` marker, or the statement fails at runtime.", marker)
		}
		return fmt.Sprintf("CREATE INDEX CONCURRENTLY cannot run inside a transaction, and dbmate wraps each migration in one by default -- add `transaction:false` to the `%s` marker, or the statement fails at runtime.", marker)
	case FrameworkTern:
		return "CREATE INDEX CONCURRENTLY cannot run inside a transaction, and tern wraps every migration in one with no opt-out, so there is no migration_sql for it -- run this statement out-of-band."
	default:
		return "CREATE INDEX CONCURRENTLY cannot run inside a transaction at all, so that statement has to be outside whatever the runner wraps."
	}
}

func joinHints(parts ...string) string {
	var kept []string
	for _, p := range parts {
		if p != "" {
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, " ")
}
