package query

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/jit"
	"github.com/boringsql/dryrun/internal/schema"
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
		VersionBehavior *string      `json:"version_behavior,omitempty"`
		RollbackDDL     *string      `json:"rollback_ddl,omitempty"`
	}

	SafetyRating string
)

const (
	SafetySafe      SafetyRating = "safe"
	SafetyCaution   SafetyRating = "caution"
	SafetyDangerous SafetyRating = "dangerous"
)

// parses DDL and returns safety assessments per statement
func CheckMigration(ddl string, snap *schema.SchemaSnapshot, pgVersion *dryrun.PgVersion) ([]MigrationCheck, error) {
	result, err := pg_query.Parse(ddl)
	if err != nil {
		return nil, fmt.Errorf("DDL parse error: %w", err)
	}

	var checks []MigrationCheck

	for _, stmt := range result.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		switch n := stmt.Stmt.Node.(type) {
		case *pg_query.Node_AlterTableStmt:
			for _, cmdNode := range n.AlterTableStmt.Cmds {
				if cmd, ok := cmdNode.Node.(*pg_query.Node_AlterTableCmd); ok {
					if check := analyzeAlterTableCmd(cmd.AlterTableCmd, n.AlterTableStmt, snap, pgVersion); check != nil {
						checks = append(checks, *check)
					}
				}
			}
		case *pg_query.Node_IndexStmt:
			checks = append(checks, analyzeCreateIndex(n.IndexStmt, snap, pgVersion))
		case *pg_query.Node_RenameStmt:
			checks = append(checks, analyzeRename(snap))
		}
	}

	if len(checks) == 0 {
		if check := fallbackKeywordCheck(ddl); check != nil {
			checks = append(checks, *check)
		}
	}

	return checks, nil
}

func analyzeAlterTableCmd(cmd *pg_query.AlterTableCmd, stmt *pg_query.AlterTableStmt, snap *schema.SchemaSnapshot, pgVersion *dryrun.PgVersion) *MigrationCheck {
	tableName := ""
	if stmt.Relation != nil {
		if stmt.Relation.Schemaname != "" {
			tableName = stmt.Relation.Schemaname + "." + stmt.Relation.Relname
		} else {
			tableName = stmt.Relation.Relname
		}
	}
	tableSize, rowEstimate := lookupTableStats(snap, tableName)

	subtype := pg_query.AlterTableType(cmd.Subtype)

	switch subtype {
	case pg_query.AlterTableType_AT_AddColumn:
		return analyzeAddColumn(cmd, tableName, tableSize, rowEstimate, pgVersion)
	case pg_query.AlterTableType_AT_DropColumn:
		return &MigrationCheck{
			Operation: "DROP COLUMN", Table: strp(tableName), Safety: SafetySafe,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
			TableSize: tableSize, RowEstimate: rowEstimate,
			Recommendation: "Metadata-only operation. Column space reclaimed by VACUUM.",
		}
	case pg_query.AlterTableType_AT_SetNotNull:
		return analyzeSetNotNull(cmd.Name, tableName, tableSize, rowEstimate, pgVersion, snap)
	case pg_query.AlterTableType_AT_AlterColumnType:
		colName := cmd.Name
		e := jit.AlterColumnType(tableName, colName, "<new_type>")
		return &MigrationCheck{
			Operation: "ALTER COLUMN TYPE", Table: strp(tableName), Safety: SafetyDangerous,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "proportional to table size (full rewrite)",
			TableSize: tableSize, RowEstimate: rowEstimate,
			Recommendation: e.String(),
		}
	case pg_query.AlterTableType_AT_AddConstraint:
		return analyzeAddConstraint(cmd, tableName, tableSize, rowEstimate, pgVersion)
	case pg_query.AlterTableType_AT_ValidateConstraint:
		return &MigrationCheck{
			Operation: "VALIDATE CONSTRAINT", Table: strp(tableName), Safety: SafetySafe,
			LockType:     "SHARE UPDATE EXCLUSIVE",
			LockDuration: "proportional to table size (but allows concurrent DML)",
			TableSize:    tableSize, RowEstimate: rowEstimate,
			Recommendation: "Safe - validates existing rows with a weaker lock that allows concurrent reads and writes.",
		}
	}
	return nil
}

func analyzeAddColumn(cmd *pg_query.AlterTableCmd, tableName string, tableSize *string, rowEstimate *float64, pgVersion *dryrun.PgVersion) *MigrationCheck {
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

	if !hasDefault {
		safety = SafetySafe
		recommendation = "Nullable column without DEFAULT - metadata-only change."
		lockDuration = "brief (milliseconds)"
	} else if pgVersion != nil && pgVersion.Major >= 11 {
		safety = SafetyCaution
		e := jit.AddColumnVolatileDefault(tableName, colName, colType, "<default>")
		recommendation = "Column with DEFAULT on PG 11+ - safe for immutable defaults (metadata-only). " +
			"Volatile defaults (now(), random()) still trigger a full table rewrite.\n\n" +
			"If the default IS volatile:\n" + e.Fix
		lockDuration = "brief for immutable default, long for volatile"
	} else {
		e := jit.AddColumnPrePG11(tableName, colName, colType, "<default>")
		safety = SafetyDangerous
		recommendation = e.String()
		lockDuration = "proportional to table size"
	}

	var rollback *string
	if colName != "" {
		rollback = strp(fmt.Sprintf("ALTER TABLE ... DROP COLUMN %s;", colName))
	}

	return &MigrationCheck{
		Operation: "ADD COLUMN", Table: strp(tableName), Safety: safety,
		LockType: "ACCESS EXCLUSIVE", LockDuration: lockDuration,
		TableSize: tableSize, RowEstimate: rowEstimate,
		Recommendation:  recommendation,
		VersionBehavior: versionBehaviorAddColumn(pgVersion),
		RollbackDDL:     rollback,
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

func analyzeSetNotNull(colName, tableName string, tableSize *string, rowEstimate *float64, pgVersion *dryrun.PgVersion, snap *schema.SchemaSnapshot) *MigrationCheck {
	pgMajor := 0
	if pgVersion != nil {
		pgMajor = pgVersion.Major
	}

	displayCol := colName
	if displayCol == "" {
		displayCol = "<col>"
	}
	e := jit.SetNotNull(tableName, displayCol, pgMajor)

	safety := SafetyDangerous
	if pgMajor >= 12 {
		safety = SafetyCaution
	}

	rec := e.String()

	// column NULL-fraction refinement migrated to AnnotatedSchema; CheckMigration doesn't carry one yet
	_ = colName
	_ = snap

	return &MigrationCheck{
		Operation: "SET NOT NULL", Table: strp(tableName), Safety: safety,
		LockType:     "ACCESS EXCLUSIVE",
		LockDuration: "scan duration (unless CHECK exists on PG 12+)",
		TableSize:    tableSize, RowEstimate: rowEstimate,
		Recommendation:  rec,
		VersionBehavior: strp("PG 12+: skips scan if a valid CHECK (col IS NOT NULL) exists."),
		RollbackDDL:     strp("ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL;"),
	}
}

func findColumn(snap *schema.SchemaSnapshot, tableName, colName string) *schema.Column {
	schemaPart, namePart := "public", tableName
	if i := strings.LastIndex(tableName, "."); i >= 0 {
		schemaPart = tableName[:i]
		namePart = tableName[i+1:]
	}
	for i := range snap.Tables {
		t := &snap.Tables[i]
		if t.Name == namePart && t.Schema == schemaPart {
			for j := range t.Columns {
				if t.Columns[j].Name == colName {
					return &t.Columns[j]
				}
			}
		}
	}
	return nil
}

func analyzeAddConstraint(cmd *pg_query.AlterTableCmd, tableName string, tableSize *string, rowEstimate *float64, pgVersion *dryrun.PgVersion) *MigrationCheck {
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
			}
		}
	}

	var safety SafetyRating
	var recommendation, lockDuration, lockType string
	if isNotValid {
		safety = SafetySafe
		recommendation = fmt.Sprintf("%s NOT VALID - metadata-only. Follow up with VALIDATE CONSTRAINT.", operation)
		lockDuration = "brief (metadata-only)"
		lockType = "ACCESS EXCLUSIVE (brief)"
	} else {
		safety = SafetyDangerous
		var e jit.Entry
		switch operation {
		case "ADD FOREIGN KEY":
			e = jit.AddForeignKeyUnsafe(tableName, "<col>", "<ref_table>", "<ref_col>")
		case "ADD CHECK CONSTRAINT":
			e = jit.AddCheckConstraintUnsafe(tableName, "<expr>")
		default:
			e = jit.AddCheckConstraintUnsafe(tableName, "<expr>")
		}
		recommendation = e.String()
		lockDuration = "proportional to table size"
		lockType = "ACCESS EXCLUSIVE"
	}

	return &MigrationCheck{
		Operation: operation, Table: strp(tableName), Safety: safety,
		LockType: lockType, LockDuration: lockDuration,
		TableSize: tableSize, RowEstimate: rowEstimate,
		Recommendation: recommendation,
		RollbackDDL:    strp(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT <name>;", tableName)),
	}
}

func analyzeCreateIndex(idx *pg_query.IndexStmt, snap *schema.SchemaSnapshot, pgVersion *dryrun.PgVersion) MigrationCheck {
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

	var safety SafetyRating
	var recommendation, lockType string
	if idx.Concurrent {
		safety = SafetySafe
		recommendation = "CREATE INDEX CONCURRENTLY - does not block reads or writes. Takes ~2-3x longer. " +
			"Cannot run inside a transaction. If it fails, drop the INVALID index."
		lockType = "SHARE UPDATE EXCLUSIVE"
	} else {
		e := jit.CreateIndexBlocking(tableName, idx.Idxname, idxMethod, colStr)
		safety = SafetyDangerous
		recommendation = e.String()
		lockType = "SHARE (blocks writes)"
	}

	idxName := idx.Idxname
	if idxName == "" {
		idxName = "<auto>"
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
		RollbackDDL:    strp(fmt.Sprintf("DROP INDEX CONCURRENTLY %s;", idxName)),
	}
}

func analyzeRename(snap *schema.SchemaSnapshot) MigrationCheck {
	e := jit.Rename("<old_name>", "<new_name>")
	return MigrationCheck{
		Operation: "RENAME", Safety: SafetyDangerous,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
		Recommendation: e.String(),
		RollbackDDL:    strp("ALTER TABLE/COLUMN ... RENAME TO <old_name>;"),
	}
}

func fallbackKeywordCheck(ddl string) *MigrationCheck {
	upper := strings.ToUpper(ddl)
	if strings.Contains(upper, "DROP TABLE") {
		return &MigrationCheck{
			Operation: "DROP TABLE", Safety: SafetyDangerous,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "brief",
			Recommendation: "Irreversible. Ensure no dependent objects or application code references this table.",
		}
	}
	return nil
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

func formatBytes(bytes int64) string {
	switch {
	case bytes >= 1_073_741_824:
		return fmt.Sprintf("%.1f GB", float64(bytes)/1_073_741_824)
	case bytes >= 1_048_576:
		return fmt.Sprintf("%.1f MB", float64(bytes)/1_048_576)
	case bytes >= 1024:
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

func versionBehaviorAddColumn(pgVersion *dryrun.PgVersion) *string {
	if pgVersion == nil {
		return nil
	}
	if pgVersion.Major >= 11 {
		return strp("PG 11+: Immutable DEFAULT is metadata-only (no table rewrite).")
	}
	return strp("PG <11: Any DEFAULT triggers a full table rewrite.")
}
