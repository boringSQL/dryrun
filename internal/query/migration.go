package query

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/jit"
)

type (
	MigrationCheck struct {
		Operation       string       `json:"operation"`
		Table           *string      `json:"table,omitempty"`
		Safety          SafetyRating `json:"safety"`
		LockType        string       `json:"lock_type"`
		LockDuration    string       `json:"lock_duration"`
		TableSize       *string      `json:"table_size,omitempty"`
		RowEstimate     *float64     `json:"row_estimate,omitempty"`
		Recommendation  string       `json:"recommendation"`
		Rationale       *Rationale   `json:"rationale,omitempty"`
		VersionBehavior *string      `json:"version_behavior,omitempty"`
		RollbackDDL     *string      `json:"rollback_ddl,omitempty"`

		// SaferSQL is the mechanical rewrite as runnable statements, in order.
		SaferSQL []string `json:"safer_sql,omitempty"`

		// Statement is this check's own command deparsed standalone (ONLY/
		// IF EXISTS/quoting preserved) -- ComposeMigrationSQL's passthrough.
		// Off the wire: redundant with the input DDL or SaferSQL.
		Statement string `json:"-"`
	}

	// Rationale: Recommendation's reason as fields, so agents skip parsing prose.
	Rationale struct {
		Reason string `json:"reason"`
		Note   string `json:"note,omitempty"`
	}

	SafetyRating string
)

const (
	SafetySafe      SafetyRating = "safe"
	SafetyCaution   SafetyRating = "caution"
	SafetyDangerous SafetyRating = "dangerous"
)

// parses DDL and returns safety assessments per statement
func CheckMigration(ddl string, snap *schema.SchemaSnapshot) ([]MigrationCheck, error) {
	result, err := pg_query.Parse(ddl)
	if err != nil {
		return nil, fmt.Errorf("DDL parse error: %w", err)
	}

	var checks []MigrationCheck
	names := newNameAllocator(snap)

	for _, stmt := range result.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		switch n := stmt.Stmt.Node.(type) {
		case *pg_query.Node_AlterTableStmt:
			for _, cmdNode := range n.AlterTableStmt.Cmds {
				cmd, ok := cmdNode.Node.(*pg_query.Node_AlterTableCmd)
				if !ok {
					checks = append(checks, unmodeledCheck(topLevelStatement(stmt.Stmt)))
					continue
				}
				if check := analyzeAlterTableCmd(cmd.AlterTableCmd, n.AlterTableStmt, snap, names); check != nil {
					checks = append(checks, *check)
				} else {
					// this command's text alone -- siblings have their own checks
					checks = append(checks, unmodeledCheck(alterCmdStatement(n.AlterTableStmt, cmd.AlterTableCmd)))
				}
			}
		case *pg_query.Node_IndexStmt:
			checks = append(checks, analyzeCreateIndex(n.IndexStmt, snap, names))
		case *pg_query.Node_RenameStmt:
			checks = append(checks, analyzeRename(snap))
		case *pg_query.Node_DropStmt:
			switch n.DropStmt.RemoveType {
			case pg_query.ObjectType_OBJECT_TABLE:
				checks = append(checks, dropTableCheck())
			case pg_query.ObjectType_OBJECT_INDEX:
				checks = append(checks, dropIndexCheck(n.DropStmt, stmt.Stmt))
			default:
				checks = append(checks, unmodeledCheck(topLevelStatement(stmt.Stmt)))
			}
		case *pg_query.Node_TransactionStmt:
			checks = append(checks, transactionControlCheck())
		case *pg_query.Node_VariableSetStmt:
			checks = append(checks, passthroughCheck("SET", stmt.Stmt))
		default:
			// an unanalyzable statement must still become a check, or the
			// migration_sql gate would silently drop it from the file
			checks = append(checks, unmodeledCheck(topLevelStatement(stmt.Stmt)))
		}
	}

	return checks, nil
}

func analyzeAlterTableCmd(cmd *pg_query.AlterTableCmd, stmt *pg_query.AlterTableStmt, snap *schema.SchemaSnapshot, names *nameAllocator) *MigrationCheck {
	tableName := ""
	if stmt.Relation != nil {
		if stmt.Relation.Schemaname != "" {
			tableName = stmt.Relation.Schemaname + "." + stmt.Relation.Relname
		} else {
			tableName = stmt.Relation.Relname
		}
	}
	tableSize, rowEstimate := lookupTableStats(snap, tableName)
	statement := alterCmdStatement(stmt, cmd)

	subtype := pg_query.AlterTableType(cmd.Subtype)

	switch subtype {
	case pg_query.AlterTableType_AT_AddColumn:
		return analyzeAddColumn(cmd, tableName, tableSize, rowEstimate, statement)
	case pg_query.AlterTableType_AT_DropColumn:
		const rec = "Metadata-only operation. Column space reclaimed by VACUUM."
		return &MigrationCheck{
			Operation: "DROP COLUMN", Table: strp(tableName), Safety: SafetySafe,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
			TableSize: tableSize, RowEstimate: rowEstimate,
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			Statement:      statement,
		}
	case pg_query.AlterTableType_AT_DropConstraint:
		safety := SafetySafe
		rec := "Metadata-only operation. Dropping an index-backed constraint (primary key, unique) takes its index with it."
		if cmd.Behavior == pg_query.DropBehavior_DROP_CASCADE {
			safety = SafetyCaution
			rec = "Metadata-only, but CASCADE drops every object that depends on this constraint too -- including foreign keys in other tables. Confirm what references it first."
		}
		return &MigrationCheck{
			Operation: "DROP CONSTRAINT", Table: strp(tableName), Safety: safety,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
			TableSize: tableSize, RowEstimate: rowEstimate,
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			Statement:      statement,
		}
	case pg_query.AlterTableType_AT_SetNotNull:
		return analyzeSetNotNull(cmd.Name, tableName, tableSize, rowEstimate, snap, stmt, names, statement)
	case pg_query.AlterTableType_AT_AlterColumnType:
		colName := cmd.Name
		e := jit.AlterColumnType(tableName, colName, "<new_type>")
		return &MigrationCheck{
			Operation: "ALTER COLUMN TYPE", Table: strp(tableName), Safety: SafetyDangerous,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "proportional to table size (full rewrite)",
			TableSize: tableSize, RowEstimate: rowEstimate,
			Recommendation: e.String(),
			Rationale:      &Rationale{Reason: e.Reason, Note: e.Note},
			Statement:      statement,
		}
	case pg_query.AlterTableType_AT_AddConstraint:
		return analyzeAddConstraint(cmd, stmt, tableName, tableSize, rowEstimate, names, statement)
	case pg_query.AlterTableType_AT_ValidateConstraint:
		const rec = "Safe - validates existing rows with a weaker lock that allows concurrent reads and writes."
		return &MigrationCheck{
			Operation: "VALIDATE CONSTRAINT", Table: strp(tableName), Safety: SafetySafe,
			LockType:     "SHARE UPDATE EXCLUSIVE",
			LockDuration: "proportional to table size (but allows concurrent DML)",
			TableSize:    tableSize, RowEstimate: rowEstimate,
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			Statement:      statement,
		}
	}
	return nil
}

func analyzeAddColumn(cmd *pg_query.AlterTableCmd, tableName string, tableSize *string, rowEstimate *float64, statement string) *MigrationCheck {
	hasDefault := false
	colName := cmd.Name
	colType := "unknown"
	if cmd.Def != nil {
		if colDef, ok := cmd.Def.Node.(*pg_query.Node_ColumnDef); ok && colDef.ColumnDef != nil {
			if colDef.ColumnDef.Colname != "" {
				colName = colDef.ColumnDef.Colname
			}
			if colDef.ColumnDef.TypeName != nil {
				colType = deparse(colDef.ColumnDef.TypeName)
			}
			if colDef.ColumnDef.RawDefault != nil {
				hasDefault = true
			}
			for _, c := range colDef.ColumnDef.Constraints {
				if con, ok := c.Node.(*pg_query.Node_Constraint); ok && con.Constraint != nil {
					if pg_query.ConstrType(con.Constraint.Contype) == pg_query.ConstrType_CONSTR_DEFAULT {
						hasDefault = true
					}
				}
			}
		}
	}

	var safety SafetyRating
	var recommendation, lockDuration string
	var rationale *Rationale

	if !hasDefault {
		safety = SafetySafe
		recommendation = "Nullable column without DEFAULT - metadata-only change."
		rationale = &Rationale{Reason: recommendation}
		lockDuration = "brief (milliseconds)"
	} else {
		safety = SafetyCaution
		e := jit.AddColumnVolatileDefault(tableName, colName, colType, "<default>")
		const hedgedReason = "Column with DEFAULT is safe for immutable defaults (metadata-only). " +
			"Volatile defaults (now(), random()) still trigger a full table rewrite."
		recommendation = hedgedReason + "\n\n" + "If the default IS volatile:\n" + e.Fix
		// e.Reason asserts an unconditional rewrite; unprovable here, so it goes to Note
		rationale = &Rationale{Reason: hedgedReason, Note: joinNotes(e.Reason, e.Note)}
		lockDuration = "brief for immutable default, long for volatile"
	}

	var rollback *string
	if colName != "" {
		rollback = strp(fmt.Sprintf("ALTER TABLE ... DROP COLUMN %s;", colName))
	}

	return &MigrationCheck{
		Operation: "ADD COLUMN", Table: strp(tableName), Safety: safety,
		LockType: "ACCESS EXCLUSIVE", LockDuration: lockDuration,
		TableSize: tableSize, RowEstimate: rowEstimate,
		Recommendation: recommendation,
		Rationale:      rationale,
		RollbackDDL:    rollback,
		Statement:      statement,
	}
}

func deparse(typeName *pg_query.TypeName) string {
	if typeName == nil {
		return "unknown"
	}
	var parts []string
	for _, n := range typeName.Names {
		if s, ok := n.Node.(*pg_query.Node_String_); ok {
			if s.String_.Sval != "pg_catalog" {
				parts = append(parts, s.String_.Sval)
			}
		}
	}
	if len(parts) == 0 {
		return "unknown"
	}
	return strings.Join(parts, ".")
}

func analyzeSetNotNull(colName, tableName string, tableSize *string, rowEstimate *float64, snap *schema.SchemaSnapshot, stmt *pg_query.AlterTableStmt, names *nameAllocator, statement string) *MigrationCheck {
	displayCol := colName
	if displayCol == "" {
		displayCol = "<col>"
	}
	e := jit.SetNotNull(tableName, displayCol)

	safety := SafetyCaution

	safer := rewriteSetNotNull(stmt, colName, names)
	rec := e.String()
	if len(safer) > 0 {
		// two differently-named migrations in one response is worse than one
		rec = e.Warning()
	}

	// column NULL-fraction refinement migrated to AnnotatedSchema; CheckMigration doesn't carry one yet
	_ = snap

	return &MigrationCheck{
		Operation: "SET NOT NULL", Table: strp(tableName), Safety: safety,
		LockType:     "ACCESS EXCLUSIVE",
		LockDuration: "scan duration (skipped when a valid CHECK proves no NULLs)",
		TableSize:    tableSize, RowEstimate: rowEstimate,
		Recommendation:  rec,
		Rationale:       &Rationale{Reason: e.Reason, Note: e.Note},
		VersionBehavior: strp("Scan is skipped if a valid CHECK (col IS NOT NULL) exists."),
		RollbackDDL:     strp("ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL;"),
		SaferSQL:        safer,
		Statement:       statement,
	}
}

func analyzeAddConstraint(cmd *pg_query.AlterTableCmd, stmt *pg_query.AlterTableStmt, tableName string, tableSize *string, rowEstimate *float64, names *nameAllocator, statement string) *MigrationCheck {
	isNotValid := false
	operation := "ADD CONSTRAINT"

	if cmd.Def != nil {
		if con, ok := cmd.Def.Node.(*pg_query.Node_Constraint); ok && con.Constraint != nil {
			isNotValid = con.Constraint.SkipValidation
			conType := pg_query.ConstrType(con.Constraint.Contype)
			switch conType {
			case pg_query.ConstrType_CONSTR_FOREIGN:
				operation = "ADD FOREIGN KEY"
			case pg_query.ConstrType_CONSTR_CHECK:
				operation = "ADD CHECK CONSTRAINT"
			case pg_query.ConstrType_CONSTR_PRIMARY:
				operation = "ADD PRIMARY KEY"
			case pg_query.ConstrType_CONSTR_UNIQUE:
				operation = "ADD UNIQUE CONSTRAINT"
			case pg_query.ConstrType_CONSTR_EXCLUSION:
				operation = "ADD EXCLUSION CONSTRAINT"
			}
		}
	}

	var safety SafetyRating
	var recommendation, lockDuration, lockType string
	var rationale *Rationale
	if isNotValid {
		safety = SafetySafe
		recommendation = fmt.Sprintf("%s NOT VALID - metadata-only. Follow up with VALIDATE CONSTRAINT.", operation)
		rationale = &Rationale{Reason: recommendation}
		lockDuration = "brief (metadata-only)"
		lockType = "ACCESS EXCLUSIVE (brief)"
	}
	safer := rewriteAddConstraint(stmt, cmd, names)
	if !isNotValid {
		safety = SafetyDangerous
		con := constraintOf(cmd)
		var e jit.Entry
		switch operation {
		case "ADD FOREIGN KEY":
			e = jit.AddForeignKeyUnsafe(tableName, "<col>", "<ref_table>", "<ref_col>")
		case "ADD CHECK CONSTRAINT":
			e = jit.AddCheckConstraintUnsafe(tableName, "<expr>")
		default:
			// PRIMARY KEY, UNIQUE and EXCLUDE have no NOT VALID form at all
			e = jit.AddIndexBackedConstraint(tableName, indexBackedKind(operation),
				strings.Join(constraintColumns(con), ", "))
		}
		recommendation = e.String()
		rationale = &Rationale{Reason: e.Reason, Note: e.Note}
		if len(safer) > 0 {
			// two differently-named migrations in one response is worse than one
			recommendation = e.Warning()
		}
		lockDuration = "proportional to table size"
		lockType = "ACCESS EXCLUSIVE"
	}

	return &MigrationCheck{
		Operation: operation, Table: strp(tableName), Safety: safety,
		LockType: lockType, LockDuration: lockDuration,
		TableSize: tableSize, RowEstimate: rowEstimate,
		Recommendation: recommendation,
		Rationale:      rationale,
		RollbackDDL:    strp(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT <name>;", tableName)),
		SaferSQL:       safer,
		Statement:      statement,
	}
}

func indexBackedKind(operation string) string {
	switch operation {
	case "ADD PRIMARY KEY":
		return "PRIMARY KEY"
	case "ADD EXCLUSION CONSTRAINT":
		return "EXCLUDE"
	default:
		return "UNIQUE"
	}
}

func constraintColumns(con *pg_query.Constraint) []string {
	if con == nil {
		return []string{"<cols>"}
	}
	if cols := stringList(con.Keys); len(cols) > 0 {
		return cols
	}
	return []string{"<cols>"}
}

func analyzeCreateIndex(idx *pg_query.IndexStmt, snap *schema.SchemaSnapshot, names *nameAllocator) MigrationCheck {
	tableName := ""
	if idx.Relation != nil {
		if idx.Relation.Schemaname != "" {
			tableName = idx.Relation.Schemaname + "." + idx.Relation.Relname
		} else {
			tableName = idx.Relation.Relname
		}
	}
	tableSize, rowEstimate := lookupTableStats(snap, tableName)
	// index method and columns for jit
	idxMethod := "btree"
	if idx.AccessMethod != "" {
		idxMethod = idx.AccessMethod
	}
	var idxCols []string
	for _, param := range idx.IndexParams {
		if elem, ok := param.Node.(*pg_query.Node_IndexElem); ok && elem.IndexElem != nil {
			if elem.IndexElem.Name != "" {
				idxCols = append(idxCols, elem.IndexElem.Name)
			}
		}
	}
	colStr := strings.Join(idxCols, ", ")
	statement := indexStmtStatement(idx)

	var safety SafetyRating
	var recommendation, lockType string
	var rationale *Rationale
	if idx.Concurrent {
		safety = SafetySafe
		recommendation = "CREATE INDEX CONCURRENTLY - does not block reads or writes. Takes ~2-3x longer. " +
			"Cannot run inside a transaction. If it fails, drop the INVALID index."
		rationale = &Rationale{Reason: recommendation}
		lockType = "SHARE UPDATE EXCLUSIVE"
	} else {
		safety = SafetyDangerous
		lockType = "SHARE (blocks writes)"
	}

	safer, builtName := rewriteCreateIndex(idx, names)
	idxName := idx.Idxname
	if builtName != "" {
		idxName = builtName
	}
	if idxName == "" {
		idxName = "<auto>"
	}
	if !idx.Concurrent {
		e := jit.CreateIndexBlocking(tableName, idxName, idxMethod, colStr)
		recommendation = e.String()
		rationale = &Rationale{Reason: e.Reason, Note: e.Note}
		if len(safer) > 0 {
			recommendation = e.Warning()
		} else if t := lookupTable(snap, idx.GetRelation()); t != nil && t.PartitionInfo != nil {
			partitionNote := "CONCURRENTLY is rejected on a partitioned table. Build the index on each partition concurrently, then CREATE INDEX on the parent and ATTACH them."
			recommendation += "\nNOTE: " + partitionNote
			rationale.Note = joinNotes(rationale.Note, partitionNote)
		}
	}

	lockDuration := "proportional to table size (blocking)"
	if idx.Concurrent {
		lockDuration = "~2-3x normal build time (non-blocking)"
	}

	concStr := ""
	if idx.Concurrent {
		concStr = "CONCURRENTLY "
	}

	return MigrationCheck{
		Operation: fmt.Sprintf("CREATE %sINDEX", concStr),
		Table:     strp(tableName), Safety: safety,
		LockType: lockType, LockDuration: lockDuration,
		TableSize: tableSize, RowEstimate: rowEstimate,
		Recommendation: recommendation,
		Rationale:      rationale,
		RollbackDDL:    strp(fmt.Sprintf("DROP INDEX CONCURRENTLY %s;", idxName)),
		SaferSQL:       safer,
		Statement:      statement,
	}
}

// joinNotes joins notes with a newline, skipping the empty half.
func joinNotes(a, b string) string {
	if a == "" {
		return b
	}
	return a + "\n" + b
}

// ComposeMigrationSQL bundles checks into one runnable file, in order:
// unsafe checks replaced by their SaferSQL, safe ones passed through as
// Statement. Returns "" unless something is actually unsafe and every unsafe
// check has a rewrite -- a saveable-looking file that still holds an unsafe
// statement is worse than no file.
func ComposeMigrationSQL(checks []MigrationCheck) string {
	if len(checks) == 0 {
		return ""
	}
	unsafeCount := 0
	var stmts []string
	for _, c := range checks {
		if c.Safety == SafetySafe {
			if c.Statement == "" {
				return ""
			}
			stmts = append(stmts, c.Statement)
			continue
		}
		unsafeCount++
		if len(c.SaferSQL) == 0 {
			return ""
		}
		stmts = append(stmts, c.SaferSQL...)
	}
	if unsafeCount == 0 || len(stmts) == 0 {
		return ""
	}

	var header strings.Builder
	header.WriteString("-- Generated by check_migration: a safer rewrite of the input, not the input itself.\n")
	if len(stmts) > 1 {
		header.WriteString("-- Run each statement in its own transaction, in one session: one wrapping\n")
		header.WriteString("-- transaction holds the first statement's lock across the rest, while separate\n")
		header.WriteString("-- sessions would silently discard a leading SET (lock_timeout et al).\n")
	}
	for _, s := range stmts {
		if strings.Contains(s, "CONCURRENTLY") {
			header.WriteString("-- CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction at all.\n")
			break
		}
	}
	return header.String() + strings.Join(stmts, "\n")
}

func analyzeRename(snap *schema.SchemaSnapshot) MigrationCheck {
	e := jit.Rename("<old_name>", "<new_name>")
	return MigrationCheck{
		Operation: "RENAME", Safety: SafetyDangerous,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
		Recommendation: e.String(),
		Rationale:      &Rationale{Reason: e.Reason, Note: e.Note},
		RollbackDDL:    strp("ALTER TABLE/COLUMN ... RENAME TO <old_name>;"),
	}
}

func dropTableCheck() MigrationCheck {
	const rec = "Irreversible. Ensure no dependent objects or application code references this table."
	return MigrationCheck{
		Operation: "DROP TABLE", Safety: SafetyDangerous,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "brief",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
	}
}

// dropIndexCheck passes through into migration_sql: CONCURRENTLY is
// non-blocking, the plain form a brief metadata-only unlink. CASCADE is the
// exception -- same reach as DROP CONSTRAINT CASCADE.
func dropIndexCheck(drop *pg_query.DropStmt, stmtNode *pg_query.Node) MigrationCheck {
	statement := topLevelStatement(stmtNode)
	if drop.Behavior == pg_query.DropBehavior_DROP_CASCADE {
		const rec = "Metadata-only, but CASCADE drops every object that depends on this index too -- including the constraint it backs and foreign keys referencing it. Confirm what references it first."
		return MigrationCheck{
			Operation: "DROP INDEX", Safety: SafetyCaution,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			Statement:      statement,
		}
	}
	if drop.Concurrent {
		const rec = "DROP INDEX CONCURRENTLY - does not block reads or writes."
		return MigrationCheck{
			Operation: "DROP INDEX CONCURRENTLY", Safety: SafetySafe,
			LockType: "SHARE UPDATE EXCLUSIVE", LockDuration: "brief (non-blocking)",
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			Statement:      statement,
		}
	}
	const rec = "Brief ACCESS EXCLUSIVE lock while the index is unlinked -- no scan, no rewrite."
	return MigrationCheck{
		Operation: "DROP INDEX", Safety: SafetySafe,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      statement,
	}
}

// passthroughCheck is for statements with no lock or rewrite story of their
// own -- SET, common at the top of a migration file. Passes through into
// migration_sql unchanged and never blocks the gate.
func passthroughCheck(operation string, stmtNode *pg_query.Node) MigrationCheck {
	const rec = "Session-scoped statement -- no lock or rewrite of its own. It applies to what follows only if the file runs in one session; the composed file's header already calls for that."
	return MigrationCheck{
		Operation: operation, Safety: SafetySafe,
		LockType: "none", LockDuration: "none",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      topLevelStatement(stmtNode),
	}
}

// transactionControlCheck is BEGIN/COMMIT/ROLLBACK: not unsafe, but it
// cannot be composed around a rewrite (CONCURRENTLY rejects transactions;
// one wrapper holds the first lock across the rest). No Statement, so the
// safe-but-empty-Statement rule suppresses the whole file.
func transactionControlCheck() MigrationCheck {
	const rec = "Transaction control (BEGIN/COMMIT/ROLLBACK) is not included in migration_sql: bundling it around a rewrite could wrap CONCURRENTLY in a transaction it cannot run in, or hold one statement's lock across another's scan. Apply this input's own transaction wrapper separately from migration_sql, or drop it and let safer_sql's own per-statement transactions stand."
	return MigrationCheck{
		Operation: "TRANSACTION CONTROL", Safety: SafetySafe,
		LockType: "none", LockDuration: "none",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
	}
}

// unmodeledCheck stands in for a statement CheckMigration has no analysis
// for. No SaferSQL and no Statement, so the gate suppresses the whole file
// rather than silently omitting it. Caution, not asserted danger.
func unmodeledCheck(statement string) MigrationCheck {
	display := statement
	if display == "" {
		display = "this statement"
	}
	const rec = "check_migration does not analyze this statement. Review it manually before applying."
	return MigrationCheck{
		Operation: "UNRECOGNIZED", Safety: SafetyCaution,
		LockType: "unknown", LockDuration: "unknown",
		Recommendation: rec + "\n\n" + display,
		Rationale:      &Rationale{Reason: rec},
	}
}

func lookupTableStats(snap *schema.SchemaSnapshot, tableName string) (*string, *float64) {
	schemaPart, namePart := "public", tableName
	if i := strings.LastIndex(tableName, "."); i >= 0 {
		schemaPart = tableName[:i]
		namePart = tableName[i+1:]
	}

	// size/row hints come from AnnotatedSchema now; CheckMigration receives only DDL
	_ = namePart
	_ = schemaPart
	_ = snap
	return nil, nil
}

func stringList(nodes []*pg_query.Node) []string {
	var out []string
	for _, n := range nodes {
		if s, ok := n.GetNode().(*pg_query.Node_String_); ok {
			out = append(out, s.String_.Sval)
		}
	}
	return out
}
