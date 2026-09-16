package query

import (
	"fmt"
	"strings"
	"time"

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
func CheckMigration(ddl string, a *schema.AnnotatedSchema) ([]MigrationCheck, error) {
	result, err := pg_query.Parse(ddl)
	if err != nil {
		return nil, fmt.Errorf("DDL parse error: %w", err)
	}
	if a == nil {
		a = &schema.AnnotatedSchema{}
	}

	var checks []MigrationCheck
	names := newNameAllocator(a.Schema)
	cat := newFileCatalog()

	for _, stmt := range result.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		switch n := stmt.Stmt.Node.(type) {
		case *pg_query.Node_AlterTableStmt:
			// ATTACH PARTITION can bring rows into a table this file created empty
			if alterAttachesPartition(n.AlterTableStmt) {
				cat.invalidate(n.AlterTableStmt.GetRelation())
			}
			for _, cmdNode := range n.AlterTableStmt.Cmds {
				cmd, ok := cmdNode.Node.(*pg_query.Node_AlterTableCmd)
				if !ok {
					checks = append(checks, unmodeledCheck(topLevelStatement(stmt.Stmt)))
					continue
				}
				if check := analyzeAlterTableCmd(cmd.AlterTableCmd, n.AlterTableStmt, a, names, cat); check != nil {
					checks = append(checks, *check)
				} else {
					// this command's text alone -- siblings have their own checks
					checks = append(checks, unmodeledCheck(alterCmdStatement(n.AlterTableStmt, cmd.AlterTableCmd)))
				}
			}
		case *pg_query.Node_IndexStmt:
			checks = append(checks, analyzeCreateIndex(n.IndexStmt, a, names, cat))
		case *pg_query.Node_CreateStmt:
			checks = append(checks, analyzeCreateTable(n.CreateStmt, stmt.Stmt))
			rel := n.CreateStmt.GetRelation()
			if !n.CreateStmt.IfNotExists {
				cat.markCreated(rel, isPartitionedParent(n.CreateStmt), false)
			} else if !cat.hasCreated(rel) && a.Schema != nil && lookupTable(a.Schema, rel) == nil {
				cat.markCreated(rel, isPartitionedParent(n.CreateStmt), true)
			}
		case *pg_query.Node_CreateTableAsStmt:
			checks = append(checks, analyzeCreateTableAs(n.CreateTableAsStmt, stmt.Stmt))
			// CTAS is born populated: never tracked as empty
		case *pg_query.Node_InsertStmt:
			cat.invalidate(n.InsertStmt.GetRelation())
			checks = append(checks, analyzeDML("INSERT", n.InsertStmt.GetRelation(), true, a, cat, stmt.Stmt))
		case *pg_query.Node_UpdateStmt:
			checks = append(checks, analyzeDML("UPDATE", n.UpdateStmt.GetRelation(), n.UpdateStmt.WhereClause != nil, a, cat, stmt.Stmt))
		case *pg_query.Node_DeleteStmt:
			checks = append(checks, analyzeDML("DELETE", n.DeleteStmt.GetRelation(), n.DeleteStmt.WhereClause != nil, a, cat, stmt.Stmt))
		case *pg_query.Node_CommentStmt:
			checks = append(checks, analyzeComment(n.CommentStmt, a, stmt.Stmt))
		case *pg_query.Node_CopyStmt:
			if n.CopyStmt.IsFrom {
				cat.invalidate(n.CopyStmt.GetRelation())
			}
			checks = append(checks, unmodeledCheck(topLevelStatement(stmt.Stmt)))
		case *pg_query.Node_DoStmt:
			// procedural code can insert into any table we created
			cat.invalidateAll()
			checks = append(checks, doCheck())
		case *pg_query.Node_CallStmt:
			// a procedure can insert into any table we created
			cat.invalidateAll()
			checks = append(checks, unmodeledCheck(topLevelStatement(stmt.Stmt)))
		case *pg_query.Node_MergeStmt:
			// MERGE's WHEN NOT MATCHED branch inserts rows
			cat.invalidate(n.MergeStmt.GetRelation())
			checks = append(checks, unmodeledCheck(topLevelStatement(stmt.Stmt)))
		case *pg_query.Node_ReindexStmt:
			checks = append(checks, analyzeReindex(n.ReindexStmt, a, cat))
		case *pg_query.Node_RenameStmt:
			if n.RenameStmt.GetRenameType() == pg_query.ObjectType_OBJECT_TABLE {
				cat.rekey(n.RenameStmt.GetRelation(), n.RenameStmt.GetNewname())
			}
			checks = append(checks, analyzeRename(n.RenameStmt, stmt.Stmt))
		case *pg_query.Node_DropStmt:
			switch n.DropStmt.RemoveType {
			case pg_query.ObjectType_OBJECT_TABLE:
				for _, key := range dropTableKeys(n.DropStmt) {
					delete(cat.created, key)
					delete(cat.empty, key)
					delete(cat.partitioned, key)
					delete(cat.conditional, key)
				}
				checks = append(checks, dropTableCheck())
			case pg_query.ObjectType_OBJECT_INDEX:
				checks = append(checks, dropIndexCheck(n.DropStmt, stmt.Stmt))
			case pg_query.ObjectType_OBJECT_TRIGGER:
				checks = append(checks, dropTriggerCheck(n.DropStmt, a, stmt.Stmt))
			case pg_query.ObjectType_OBJECT_FUNCTION, pg_query.ObjectType_OBJECT_PROCEDURE, pg_query.ObjectType_OBJECT_ROUTINE:
				checks = append(checks, dropFunctionCheck(n.DropStmt, stmt.Stmt))
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

// fileCatalog tracks tables this file created, so later statements are not
// sized against a pre-existing table.
type (
	fileCatalog struct {
		created     map[string]bool // relKey of tables created in this file
		empty       map[string]bool // relKey of tables created empty in this file
		partitioned map[string]bool // relKey of partitioned parents created in this file
		conditional map[string]bool // relKey of entries created via IF NOT EXISTS
	}
)

func newFileCatalog() *fileCatalog {
	return &fileCatalog{
		created:     map[string]bool{},
		empty:       map[string]bool{},
		partitioned: map[string]bool{},
		conditional: map[string]bool{},
	}
}

func (c *fileCatalog) markCreated(rel *pg_query.RangeVar, partitioned, conditional bool) {
	if rel == nil {
		return
	}
	key := relKey(rel)
	c.created[key] = true
	c.empty[key] = true
	if partitioned {
		c.partitioned[key] = true
	}
	if conditional {
		c.conditional[key] = true
	}
}

func (c *fileCatalog) invalidate(rel *pg_query.RangeVar) {
	if rel == nil {
		return
	}
	delete(c.empty, relKey(rel))
	delete(c.conditional, relKey(rel))
}

func (c *fileCatalog) invalidateAll() {
	c.empty = map[string]bool{}
	c.conditional = map[string]bool{}
}

func (c *fileCatalog) isEmpty(rel *pg_query.RangeVar) bool {
	return c != nil && rel != nil && c.empty[relKey(rel)]
}

func (c *fileCatalog) isPartitioned(rel *pg_query.RangeVar) bool {
	return c != nil && rel != nil && c.partitioned[relKey(rel)]
}

func (c *fileCatalog) isConditional(rel *pg_query.RangeVar) bool {
	return c != nil && rel != nil && c.conditional[relKey(rel)]
}

func (c *fileCatalog) hasCreated(rel *pg_query.RangeVar) bool {
	return c != nil && rel != nil && c.created[relKey(rel)]
}

func (c *fileCatalog) rekey(oldRel *pg_query.RangeVar, newName string) {
	if oldRel == nil || newName == "" {
		return
	}
	schemaName := schemaOf(oldRel)
	oldKey := relKey(oldRel)
	newKey := schemaName + "." + newName
	if c.created[oldKey] {
		delete(c.created, oldKey)
		c.created[newKey] = true
	}
	if c.empty[oldKey] {
		delete(c.empty, oldKey)
		c.empty[newKey] = true
	}
	if c.partitioned[oldKey] {
		delete(c.partitioned, oldKey)
		c.partitioned[newKey] = true
	}
	if c.conditional[oldKey] {
		delete(c.conditional, oldKey)
		c.conditional[newKey] = true
	}
}

// A partition (Partbound set) is an ordinary empty table, not a parent.
func isPartitionedParent(c *pg_query.CreateStmt) bool {
	return c.GetPartspec() != nil && c.GetPartbound() == nil
}

func alterAttachesPartition(stmt *pg_query.AlterTableStmt) bool {
	for _, cmdNode := range stmt.GetCmds() {
		cmd, ok := cmdNode.Node.(*pg_query.Node_AlterTableCmd)
		if ok && pg_query.AlterTableType(cmd.AlterTableCmd.GetSubtype()) == pg_query.AlterTableType_AT_AttachPartition {
			return true
		}
	}
	return false
}

// DROP TABLE's objects are [schema, name] string lists.
func dropTableKeys(drop *pg_query.DropStmt) []string {
	var keys []string
	for _, obj := range drop.GetObjects() {
		list, ok := obj.GetNode().(*pg_query.Node_List)
		if !ok || list.List == nil {
			continue
		}
		var parts []string
		for _, item := range list.List.GetItems() {
			if s, ok := item.GetNode().(*pg_query.Node_String_); ok {
				parts = append(parts, s.String_.GetSval())
			}
		}
		switch len(parts) {
		case 1:
			keys = append(keys, "public."+parts[0])
		case 2:
			keys = append(keys, parts[0]+"."+parts[1])
		}
	}
	return keys
}

func analyzeAlterTableCmd(cmd *pg_query.AlterTableCmd, stmt *pg_query.AlterTableStmt, a *schema.AnnotatedSchema, names *nameAllocator, cat *fileCatalog) *MigrationCheck {
	tableName := ""
	if stmt.Relation != nil {
		if stmt.Relation.Schemaname != "" {
			tableName = stmt.Relation.Schemaname + "." + stmt.Relation.Relname
		} else {
			tableName = stmt.Relation.Relname
		}
	}
	qual := schema.QualifiedName{Schema: schemaOf(stmt.GetRelation()), Name: stmt.GetRelation().GetRelname()}
	tableSize, rowEstimate, small := lookupTableStats(a, qual)
	statement := alterCmdStatement(stmt, cmd)

	// empty table: the size-dependent verdicts below do not apply
	if cat.isEmpty(stmt.GetRelation()) {
		if check := analyzeEmptyTableAlterCmd(cmd, tableName, statement, cat.isConditional(stmt.GetRelation())); check != nil {
			return check
		}
	}

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
		return analyzeSetNotNull(cmd.Name, tableName, qual, tableSize, rowEstimate, a, stmt, names, statement)
	case pg_query.AlterTableType_AT_AlterColumnType:
		colName := cmd.Name
		e := jit.AlterColumnType(tableName, colName, "<new_type>")
		safety := SafetyDangerous
		recommendation := e.String()
		rationale := &Rationale{Reason: e.Reason, Note: e.Note}
		if small {
			safety = SafetyCaution
			note := smallTableNote(*rowEstimate, *tableSize)
			recommendation = note + "\n\n" + e.Caution().String()
			rationale.Note = joinNotes(rationale.Note, note)
		}
		return &MigrationCheck{
			Operation: "ALTER COLUMN TYPE", Table: strp(tableName), Safety: safety,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "proportional to table size (full rewrite)",
			TableSize: tableSize, RowEstimate: rowEstimate,
			Recommendation: recommendation,
			Rationale:      rationale,
			Statement:      statement,
		}
	case pg_query.AlterTableType_AT_AddConstraint:
		return analyzeAddConstraint(cmd, stmt, tableName, tableSize, rowEstimate, small, names, statement)
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
	case pg_query.AlterTableType_AT_ColumnDefault:
		return metadataAlterCmd("SET/DROP DEFAULT", "ACCESS EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: the default is stored in the catalog and applies to future inserts only. Existing rows are unchanged.")
	case pg_query.AlterTableType_AT_DropNotNull:
		return metadataAlterCmd("DROP NOT NULL", "ACCESS EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: the constraint is removed from the catalog, no scan of existing rows.")
	case pg_query.AlterTableType_AT_SetStatistics:
		return metadataAlterCmd("SET STATISTICS", "SHARE UPDATE EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: changes the target for the next ANALYZE, which is where the sampling cost lands. Takes only SHARE UPDATE EXCLUSIVE.")
	case pg_query.AlterTableType_AT_SetOptions, pg_query.AlterTableType_AT_ResetOptions:
		return metadataAlterCmd("SET/RESET COLUMN OPTIONS", "SHARE UPDATE EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: column-level planner options are catalog entries, and changing them takes only SHARE UPDATE EXCLUSIVE.")
	case pg_query.AlterTableType_AT_SetStorage:
		return metadataAlterCmd("SET STORAGE", "ACCESS EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: changes how future writes are stored; existing rows keep the old storage until rewritten.")
	case pg_query.AlterTableType_AT_SetCompression:
		return metadataAlterCmd("SET COMPRESSION", "ACCESS EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: applies to future writes; existing rows are not recompressed.")
	case pg_query.AlterTableType_AT_ChangeOwner:
		return metadataAlterCmd("OWNER TO", "ACCESS EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: ownership is a catalog field. The new owner needs the privileges the role is expected to have.")
	case pg_query.AlterTableType_AT_ClusterOn:
		return metadataAlterCmd("CLUSTER ON", "SHARE UPDATE EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: records the cluster index; the data is only reordered by a later CLUSTER. Takes only SHARE UPDATE EXCLUSIVE.")
	case pg_query.AlterTableType_AT_DropCluster:
		return metadataAlterCmd("SET WITHOUT CLUSTER", "SHARE UPDATE EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: clears the recorded cluster index; the current physical order is untouched. Takes only SHARE UPDATE EXCLUSIVE.")
	case pg_query.AlterTableType_AT_ReplicaIdentity:
		return metadataAlterCmd("REPLICA IDENTITY", "ACCESS EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only, but it changes what logical replication can see: with REPLICA IDENTITY FULL every column of every update lands in the WAL, and without a usable identity updates and deletes are rejected downstream.")
	case pg_query.AlterTableType_AT_SetRelOptions, pg_query.AlterTableType_AT_ResetRelOptions, pg_query.AlterTableType_AT_ReplaceRelOptions:
		return metadataAlterCmd("SET/RESET TABLE OPTIONS", "SHARE UPDATE EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Metadata-only: fillfactor, toast and autovacuum options take SHARE UPDATE EXCLUSIVE; planner options apply from the next table lock. Existing pages keep the old setting until rewritten.")
	case pg_query.AlterTableType_AT_DetachPartition, pg_query.AlterTableType_AT_DetachPartitionFinalize:
		if detachConcurrently(cmd) {
			return metadataAlterCmd("DETACH PARTITION CONCURRENTLY", "SHARE UPDATE EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
				"DETACH PARTITION CONCURRENTLY - SHARE UPDATE EXCLUSIVE on parent and partition, brief. Cannot run inside a transaction block.")
		}
		return metadataAlterCmd("DETACH PARTITION", "ACCESS EXCLUSIVE (partition) / SHARE UPDATE EXCLUSIVE (parent)", tableName, tableSize, rowEstimate, statement,
			"Unlinks the partition from the catalog: ACCESS EXCLUSIVE on the partition, SHARE UPDATE EXCLUSIVE on the parent. CONCURRENTLY is weaker but cannot run in a transaction.")
	case pg_query.AlterTableType_AT_DisableTrig, pg_query.AlterTableType_AT_DisableTrigAll, pg_query.AlterTableType_AT_DisableTrigUser:
		return behaviorCautionAlterCmd("DISABLE TRIGGER", "SHARE ROW EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Brief SHARE ROW EXCLUSIVE lock. Whatever the trigger enforced -- auditing, denormalization, FK checks -- stops being enforced from here on, and the change is easy to forget about.")
	case pg_query.AlterTableType_AT_EnableRowSecurity, pg_query.AlterTableType_AT_ForceRowSecurity:
		return behaviorCautionAlterCmd("ENABLE ROW LEVEL SECURITY", "ACCESS EXCLUSIVE", tableName, tableSize, rowEstimate, statement,
			"Brief lock. If no policies exist yet, row-level security is default-deny: every non-owner query on the table returns no rows. Add the policies in the same migration, before the application reads.")
	case pg_query.AlterTableType_AT_AttachPartition:
		return analyzeAttachPartition(tableName, tableSize, rowEstimate, statement)
	case pg_query.AlterTableType_AT_SetLogged, pg_query.AlterTableType_AT_SetUnLogged:
		return analyzeRewriteAlterCmd("SET LOGGED/UNLOGGED", tableName, tableSize, rowEstimate, small, statement,
			"Rewrites the whole table to change its persistence and WAL-logging: proportional to table size, and doubles the WAL during the rewrite.")
	case pg_query.AlterTableType_AT_SetTableSpace:
		return analyzeRewriteAlterCmd("SET TABLESPACE", tableName, tableSize, rowEstimate, small, statement,
			"Copies every data file to the new tablespace under ACCESS EXCLUSIVE: proportional to table size and needs free space in the destination.")
	case pg_query.AlterTableType_AT_SetAccessMethod:
		return analyzeRewriteAlterCmd("SET ACCESS METHOD", tableName, tableSize, rowEstimate, small, statement,
			"Rewrites the whole table into the new storage engine: proportional to table size.")
	}
	return nil
}

func metadataAlterCmd(operation, lockType, tableName string, tableSize *string, rowEstimate *float64, statement, reason string) *MigrationCheck {
	return &MigrationCheck{
		Operation: operation, Table: strp(tableName), Safety: SafetySafe,
		LockType: lockType, LockDuration: "brief (metadata-only)",
		TableSize: tableSize, RowEstimate: rowEstimate,
		Recommendation: reason,
		Rationale:      &Rationale{Reason: reason},
		Statement:      statement,
	}
}

// behaviorCautionAlterCmd: like metadataAlterCmd but the change silently
// alters behavior.
func behaviorCautionAlterCmd(operation, lockType, tableName string, tableSize *string, rowEstimate *float64, statement, reason string) *MigrationCheck {
	return &MigrationCheck{
		Operation: operation, Table: strp(tableName), Safety: SafetyCaution,
		LockType: lockType, LockDuration: "brief (metadata-only)",
		TableSize: tableSize, RowEstimate: rowEstimate,
		Recommendation: reason,
		Rationale:      &Rationale{Reason: reason},
		Statement:      statement,
	}
}

// DETACH PARTITION CONCURRENTLY is the PartitionCmd's concurrent flag.
func detachConcurrently(cmd *pg_query.AlterTableCmd) bool {
	pc, ok := cmd.GetDef().GetNode().(*pg_query.Node_PartitionCmd)
	return ok && pc.PartitionCmd.GetConcurrent()
}

// analyzeAttachPartition: unlike plain metadata the attached table is scanned
// to validate its bound unless a matching CHECK already proves it.
func analyzeAttachPartition(tableName string, tableSize *string, rowEstimate *float64, statement string) *MigrationCheck {
	const rec = "ATTACH PARTITION takes SHARE UPDATE EXCLUSIVE on the parent and ACCESS EXCLUSIVE on the table being attached, and scans that table to validate the partition bound unless a matching CHECK constraint lets it skip the scan. Validate the constraint first when the table is large."
	return &MigrationCheck{
		Operation: "ATTACH PARTITION", Table: strp(tableName), Safety: SafetyCaution,
		LockType: "SHARE UPDATE EXCLUSIVE (parent)", LockDuration: "proportional to the attached table (validation scan)",
		TableSize: tableSize, RowEstimate: rowEstimate,
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      statement,
	}
}

// analyzeRewriteAlterCmd: full table rewrite; dangerous at size, caution on a
// known-small table.
func analyzeRewriteAlterCmd(operation, tableName string, tableSize *string, rowEstimate *float64, small bool, statement, reason string) *MigrationCheck {
	safety := SafetyDangerous
	recommendation := reason
	var note string
	if small {
		safety = SafetyCaution
		note = smallTableNote(*rowEstimate, *tableSize)
		recommendation = note + "\n\n" + reason
	}
	return &MigrationCheck{
		Operation: operation, Table: strp(tableName), Safety: safety,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "proportional to table size (full rewrite)",
		TableSize: tableSize, RowEstimate: rowEstimate,
		Recommendation: recommendation,
		Rationale:      &Rationale{Reason: reason, Note: note},
		Statement:      statement,
	}
}

// IF NOT EXISTS was a no-op if the table already existed.
const conditionalEmptyNote = "Assumes the snapshot is current: IF NOT EXISTS is a no-op if the table already exists, and then this verdict does not hold."

func analyzeEmptyTableAlterCmd(cmd *pg_query.AlterTableCmd, tableName, statement string, conditional bool) *MigrationCheck {
	var operation string
	switch pg_query.AlterTableType(cmd.Subtype) {
	case pg_query.AlterTableType_AT_AddColumn:
		operation = "ADD COLUMN"
	case pg_query.AlterTableType_AT_SetNotNull:
		operation = "SET NOT NULL"
	case pg_query.AlterTableType_AT_AlterColumnType:
		operation = "ALTER COLUMN TYPE"
	case pg_query.AlterTableType_AT_AddConstraint:
		operation = addConstraintOperation(cmd)
	default:
		return nil
	}

	const rec = "Table is created empty earlier in this migration: the scan or rewrite touches 0 rows and is instant. The lock is still taken and can queue behind an in-flight transaction -- set lock_timeout and retry on timeout."
	note := ""
	if pg_query.AlterTableType(cmd.Subtype) == pg_query.AlterTableType_AT_AddConstraint {
		if con := constraintOf(cmd); con != nil && pg_query.ConstrType(con.Contype) == pg_query.ConstrType_CONSTR_FOREIGN {
			if con.GetPktable() != nil {
				note = fmt.Sprintf("A brief SHARE ROW EXCLUSIVE lock is taken on the referenced table %s to install the FK triggers.", relationName(con.GetPktable()))
			} else {
				note = "A brief SHARE ROW EXCLUSIVE lock is taken on the referenced table to install the FK triggers."
			}
		}
	}
	recommendation := rec
	if conditional {
		note = joinNotes(note, conditionalEmptyNote)
		recommendation += "\n\n" + conditionalEmptyNote
	}
	zero := 0.0
	return &MigrationCheck{
		Operation: operation, Table: strp(tableName), Safety: SafetySafe,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (0 rows)",
		TableSize: strp("0 bytes"), RowEstimate: &zero,
		Recommendation: recommendation,
		Rationale:      &Rationale{Reason: rec, Note: note},
		Statement:      statement,
	}
}

func analyzeCreateTable(stmt *pg_query.CreateStmt, stmtNode *pg_query.Node) MigrationCheck {
	tableName := relationName(stmt.GetRelation())
	const rec = "Creates an empty table -- metadata-only, no data movement. Inline PRIMARY KEY/UNIQUE build empty indexes immediately."
	var inlineFK []string
	inlineFK = referencedTables(stmt)
	note := ""
	if len(inlineFK) > 0 {
		note = fmt.Sprintf("Inline foreign key references %s: a brief SHARE ROW EXCLUSIVE lock is taken there to install the FK triggers.", strings.Join(inlineFK, ", "))
	}
	zero := 0.0
	return MigrationCheck{
		Operation: "CREATE TABLE", Table: strp(tableName), Safety: SafetySafe,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
		TableSize: strp("0 bytes"), RowEstimate: &zero,
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec, Note: note},
		Statement:      topLevelStatement(stmtNode),
	}
}

func analyzeCreateTableAs(stmt *pg_query.CreateTableAsStmt, stmtNode *pg_query.Node) MigrationCheck {
	tableName := ""
	if stmt.GetInto() != nil {
		tableName = relationName(stmt.GetInto().GetRel())
	}
	const rec = "Materializes the query result into a new table. Duration tracks the source scan and row count; carries no indexes, constraints or defaults from the source."
	return MigrationCheck{
		Operation: "CREATE TABLE AS", Table: strp(tableName), Safety: SafetyCaution,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "proportional to query result size",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      topLevelStatement(stmtNode),
	}
}

func referencedTables(stmt *pg_query.CreateStmt) []string {
	var out []string
	collect := func(c *pg_query.Constraint) {
		if c == nil || pg_query.ConstrType(c.Contype) != pg_query.ConstrType_CONSTR_FOREIGN {
			return
		}
		if c.GetPktable() != nil {
			out = append(out, relationName(c.GetPktable()))
		}
	}
	for _, elt := range stmt.GetTableElts() {
		switch e := elt.GetNode().(type) {
		case *pg_query.Node_Constraint:
			collect(e.Constraint)
		case *pg_query.Node_ColumnDef:
			for _, cc := range e.ColumnDef.GetConstraints() {
				if c, ok := cc.GetNode().(*pg_query.Node_Constraint); ok {
					collect(c.Constraint)
				}
			}
		}
	}
	for _, con := range stmt.GetConstraints() {
		if c, ok := con.GetNode().(*pg_query.Node_Constraint); ok {
			collect(c.Constraint)
		}
	}
	return out
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

func analyzeSetNotNull(colName, tableName string, qual schema.QualifiedName, tableSize *string, rowEstimate *float64, a *schema.AnnotatedSchema, stmt *pg_query.AlterTableStmt, names *nameAllocator, statement string) *MigrationCheck {
	displayCol := colName
	if displayCol == "" {
		displayCol = "<col>"
	}
	e := jit.SetNotNull(tableName, displayCol).Caution()

	safety := SafetyCaution

	safer := rewriteSetNotNull(stmt, colName, names)
	rec := e.String()
	if len(safer) > 0 {
		// two differently-named migrations in one response is worse than one
		rec = e.Warning()
	}

	if colName != "" {
		if col := a.ColumnStats(qual, colName); col != nil && col.NullFrac != nil {
			nf := *col.NullFrac
			if nf == 0 {
				rec += "\n\nDATA CHECK: Column currently has 0% NULLs. The scan will pass, but ACCESS EXCLUSIVE lock is still held."
			} else if rowEstimate != nil && *rowEstimate >= 0 {
				nullRows := int64(nf * *rowEstimate)
				rec += fmt.Sprintf("\n\nDATA CHECK: Column has ~%.0f%% NULLs (~%d rows) that must be backfilled before this constraint can be applied.", nf*100, nullRows)
			} else {
				rec += fmt.Sprintf("\n\nDATA CHECK: Column has ~%.0f%% NULLs that must be backfilled before this constraint can be applied.", nf*100)
			}
		}
	}

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

func addConstraintOperation(cmd *pg_query.AlterTableCmd) string {
	con := constraintOf(cmd)
	if con == nil {
		return "ADD CONSTRAINT"
	}
	switch pg_query.ConstrType(con.Contype) {
	case pg_query.ConstrType_CONSTR_FOREIGN:
		return "ADD FOREIGN KEY"
	case pg_query.ConstrType_CONSTR_CHECK:
		return "ADD CHECK CONSTRAINT"
	case pg_query.ConstrType_CONSTR_PRIMARY:
		return "ADD PRIMARY KEY"
	case pg_query.ConstrType_CONSTR_UNIQUE:
		return "ADD UNIQUE CONSTRAINT"
	case pg_query.ConstrType_CONSTR_EXCLUSION:
		return "ADD EXCLUSION CONSTRAINT"
	}
	return "ADD CONSTRAINT"
}

func analyzeAddConstraint(cmd *pg_query.AlterTableCmd, stmt *pg_query.AlterTableStmt, tableName string, tableSize *string, rowEstimate *float64, small bool, names *nameAllocator, statement string) *MigrationCheck {
	isNotValid := false
	operation := addConstraintOperation(cmd)
	if con := constraintOf(cmd); con != nil {
		isNotValid = con.SkipValidation
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
		lead := ""
		if small {
			safety = SafetyCaution
			lead = smallTableNote(*rowEstimate, *tableSize)
			e = e.Caution()
		}
		if len(safer) > 0 {
			// two differently-named migrations in one response is worse than one
			recommendation = e.Warning()
		} else {
			recommendation = e.String()
		}
		if lead != "" {
			recommendation = lead + "\n\n" + recommendation
		}
		rationale = &Rationale{Reason: e.Reason, Note: e.Note}
		if lead != "" {
			rationale.Note = joinNotes(rationale.Note, lead)
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

func analyzeCreateIndex(idx *pg_query.IndexStmt, a *schema.AnnotatedSchema, names *nameAllocator, cat *fileCatalog) MigrationCheck {
	tableName := ""
	if idx.Relation != nil {
		if idx.Relation.Schemaname != "" {
			tableName = idx.Relation.Schemaname + "." + idx.Relation.Relname
		} else {
			tableName = idx.Relation.Relname
		}
	}
	qual := schema.QualifiedName{Schema: schemaOf(idx.GetRelation()), Name: idx.GetRelation().GetRelname()}
	tableSize, rowEstimate, small := lookupTableStats(a, qual)
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

	// empty table: the blocking form is fine, no CONCURRENTLY rewrite needed
	if !idx.Concurrent && cat.isEmpty(idx.GetRelation()) {
		const rec = "Table is created empty earlier in this migration: the index build touches 0 rows and is instant. The lock is still taken and can queue behind an in-flight transaction -- set lock_timeout and retry on timeout."
		note := ""
		recommendation := rec
		if cat.isConditional(idx.GetRelation()) {
			note = conditionalEmptyNote
			recommendation += "\n\n" + conditionalEmptyNote
		}
		zero := 0.0
		return MigrationCheck{
			Operation: "CREATE INDEX", Table: strp(tableName), Safety: SafetySafe,
			LockType: "SHARE (brief, 0 rows)", LockDuration: "brief (0 rows)",
			TableSize: strp("0 bytes"), RowEstimate: &zero,
			Recommendation: recommendation,
			Rationale:      &Rationale{Reason: rec, Note: note},
			Statement:      statement,
		}
	}

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

	safer, builtName := rewriteCreateIndex(idx, names, cat)
	idxName := idx.Idxname
	if builtName != "" {
		idxName = builtName
	}
	if idxName == "" {
		idxName = "<auto>"
	}
	if !idx.Concurrent {
		e := jit.CreateIndexBlocking(tableName, idxName, idxMethod, colStr)
		lead := ""
		if small {
			safety = SafetyCaution
			lead = smallTableNote(*rowEstimate, *tableSize)
			e = e.Caution()
		}
		if len(safer) > 0 {
			recommendation = e.Warning()
		} else {
			recommendation = e.String()
			if t := lookupTable(a.Schema, idx.GetRelation()); t != nil && t.PartitionInfo != nil {
				partitionNote := "CONCURRENTLY is rejected on a partitioned table. Build the index on each partition concurrently, then CREATE INDEX on the parent and ATTACH them."
				recommendation += "\nNOTE: " + partitionNote
				e.Note = joinNotes(e.Note, partitionNote)
			}
		}
		if lead != "" {
			recommendation = lead + "\n\n" + recommendation
		}
		rationale = &Rationale{Reason: e.Reason, Note: e.Note}
		if lead != "" {
			rationale.Note = joinNotes(rationale.Note, lead)
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
	header.WriteString("-- Generated by check_migration: the runnable input, with blocking rewrites and transaction fixes applied.\n")
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

// RENAME is dangerous for what callers name (table, column, view), safe for an
// index, and caution for a constraint or sequence.
func analyzeRename(stmt *pg_query.RenameStmt, stmtNode *pg_query.Node) MigrationCheck {
	statement := topLevelStatement(stmtNode)
	switch stmt.GetRenameType() {
	case pg_query.ObjectType_OBJECT_INDEX:
		const rec = "Renaming an index is metadata-only (brief ACCESS EXCLUSIVE on the index): queries never name indexes, so no query breaks. Only planner hints (pg_hint_plan) and scripts or later migrations that reference the old name -- pg_indexes readers, DROP INDEX, REINDEX INDEX -- need updating."
		return MigrationCheck{
			Operation: "RENAME", Safety: SafetySafe,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			RollbackDDL:    strp("ALTER INDEX ... RENAME TO <old_name>;"),
			Statement:      indexRenameStatement(stmt, statement),
		}
	case pg_query.ObjectType_OBJECT_TABCONSTRAINT:
		const rec = "Renaming a constraint is metadata-only (brief ACCESS EXCLUSIVE); its backing index is renamed with it. Only callers that spell out the old name break: a later migration or Down file with DROP CONSTRAINT <old_name> or REINDEX INDEX <old_name>, ON CONFLICT ON CONSTRAINT <old_name>, code matching constraint names in error messages, and scripts reading pg_constraint. Grep for the old name before applying."
		return MigrationCheck{
			Operation: "RENAME", Safety: SafetyCaution,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			RollbackDDL:    strp("ALTER TABLE ... RENAME CONSTRAINT ... TO <old_name>;"),
			Statement:      statement,
		}
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		const rec = "Renaming a sequence is metadata-only (brief ACCESS EXCLUSIVE). DEFAULT expressions and OWNED BY reference it by OID and follow the rename, so only a literal nextval('<old_name>') in SQL or application code breaks -- plus any ORM that infers the sequence name from the table and column. Grep for the old name before applying."
		return MigrationCheck{
			Operation: "RENAME", Safety: SafetyCaution,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			RollbackDDL:    strp("ALTER SEQUENCE ... RENAME TO <old_name>;"),
			Statement:      statement,
		}
	case pg_query.ObjectType_OBJECT_VIEW, pg_query.ObjectType_OBJECT_MATVIEW:
		e := jit.Rename("<old_name>", "<new_name>")
		return MigrationCheck{
			Operation: "RENAME", Safety: SafetyDangerous,
			LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
			Recommendation: e.String(),
			Rationale:      &Rationale{Reason: e.Reason, Note: e.Note},
			RollbackDDL:    strp("ALTER VIEW/MATERIALIZED VIEW ... RENAME TO <old_name>;"),
			Statement:      statement,
		}
	}

	// table, column and any unmodeled kind: callers name it, worst case
	e := jit.Rename("<old_name>", "<new_name>")
	return MigrationCheck{
		Operation: "RENAME", Safety: SafetyDangerous,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
		Recommendation: e.String(),
		Rationale:      &Rationale{Reason: e.Reason, Note: e.Note},
		RollbackDDL:    strp("ALTER TABLE/COLUMN ... RENAME TO <old_name>;"),
		Statement:      statement,
	}
}

// A hand-built fallback: an empty Statement would make a safe index rename
// suppress migration_sql entirely.
func indexRenameStatement(stmt *pg_query.RenameStmt, deparsed string) string {
	if deparsed != "" {
		return deparsed
	}
	return fmt.Sprintf("ALTER INDEX %s RENAME TO %s;", relationName(stmt.GetRelation()), quoteIdent(stmt.GetNewname()))
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

// Fails at apply in every transaction mode: only a different statement helps.
func rejectedDropIndexConcurrently(drop *pg_query.DropStmt, statement string) MigrationCheck {
	multi := len(drop.GetObjects()) > 1
	cascade := drop.Behavior == pg_query.DropBehavior_DROP_CASCADE
	var reason string
	switch {
	case multi && cascade:
		reason = "PostgreSQL rejects this form: DROP INDEX CONCURRENTLY does not support CASCADE or dropping multiple objects. Split it into one plain DROP INDEX CONCURRENTLY per index."
	case multi:
		reason = "PostgreSQL rejects this form: DROP INDEX CONCURRENTLY does not support dropping multiple objects. Split it into one DROP INDEX CONCURRENTLY per index."
	default:
		reason = "PostgreSQL rejects this form: DROP INDEX CONCURRENTLY does not support CASCADE. Drop the objects that CASCADE would take first, then drop the index without CASCADE."
	}
	rec := reason + " If the index backs a constraint, drop the constraint instead -- plain DROP INDEX on it fails too."
	ifExists := ""
	if drop.GetMissingOk() {
		ifExists = "IF EXISTS "
	}
	safer := make([]string, 0, len(drop.GetObjects()))
	for _, name := range dropIndexNames(drop) {
		safer = append(safer, fmt.Sprintf("DROP INDEX CONCURRENTLY %s%s;", ifExists, name))
	}
	if len(safer) == 0 {
		safer = nil
	}
	return MigrationCheck{
		Operation: "DROP INDEX CONCURRENTLY", Safety: SafetyDangerous,
		LockType: "SHARE UPDATE EXCLUSIVE", LockDuration: "brief (non-blocking)",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: reason},
		Statement:      statement,
		SaferSQL:       safer,
	}
}

// DROP INDEX objects are qualified-name string lists.
func dropIndexNames(drop *pg_query.DropStmt) []string {
	var names []string
	for _, obj := range drop.GetObjects() {
		list, ok := obj.GetNode().(*pg_query.Node_List)
		if !ok || list.List == nil {
			continue
		}
		var parts []string
		for _, item := range list.List.GetItems() {
			if s, ok := item.GetNode().(*pg_query.Node_String_); ok {
				parts = append(parts, quoteIdent(s.String_.GetSval()))
			}
		}
		if len(parts) > 0 {
			names = append(names, strings.Join(parts, "."))
		}
	}
	return names
}

// dropIndexCheck passes through into migration_sql: CONCURRENTLY is
// non-blocking, plain CASCADE the exception (same reach as DROP CONSTRAINT
// CASCADE). CONCURRENTLY with CASCADE or several names is rejected outright.
func dropIndexCheck(drop *pg_query.DropStmt, stmtNode *pg_query.Node) MigrationCheck {
	statement := topLevelStatement(stmtNode)
	if drop.Concurrent && (drop.Behavior == pg_query.DropBehavior_DROP_CASCADE || len(drop.GetObjects()) > 1) {
		return rejectedDropIndexConcurrently(drop, statement)
	}
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

// DML: ROW EXCLUSIVE never blocks reads or unrelated writes, so only a
// no-WHERE write on a table that is not known small is a caution.
func analyzeDML(operation string, rel *pg_query.RangeVar, bounded bool, a *schema.AnnotatedSchema, cat *fileCatalog, stmtNode *pg_query.Node) MigrationCheck {
	tableName := relationName(rel)
	qual := schema.QualifiedName{Schema: schemaOf(rel), Name: rel.GetRelname()}
	tableSize, rowEstimate, small := lookupTableStats(a, qual)
	statement := topLevelStatement(stmtNode)

	base := MigrationCheck{
		Operation: operation, Table: strp(tableName), Safety: SafetySafe,
		LockType: "ROW EXCLUSIVE", LockDuration: "brief (row locks held to commit)",
		TableSize: tableSize, RowEstimate: rowEstimate,
		Statement: statement,
	}

	// bounded: an INSERT, or an UPDATE/DELETE whose WHERE limits the rows.
	if bounded {
		const rec = "Leaf-level DML: ROW EXCLUSIVE does not block reads or unrelated writes. Row locks are held until the transaction commits, so keep the batch bounded."
		base.Recommendation = rec
		base.Rationale = &Rationale{Reason: rec}
		return base
	}

	// full-table write: no WHERE
	if cat.isEmpty(rel) || (rowEstimate != nil && *rowEstimate == 0) {
		const rec = "Table is empty or created empty earlier in this migration: the statement touches 0 rows and is instant."
		zero := 0.0
		base.LockDuration = "brief (0 rows)"
		base.TableSize = strp("0 bytes")
		base.RowEstimate = &zero
		base.Recommendation = rec
		base.Rationale = &Rationale{Reason: rec}
		return base
	}

	if small {
		note := smallTableNote(*rowEstimate, *tableSize)
		const rec = "No WHERE clause: every row is written in one statement. The table is small, so it is brief, but the same statement on a grown table is not -- add a WHERE clause or batch it."
		base.Recommendation = note
		base.Rationale = &Rationale{Reason: rec, Note: note}
		return base
	}

	const rec = "No WHERE clause on a table that is not known small: the statement rewrites or removes every row, holding row locks and generating WAL for the whole table. Batch it (keyset pagination, ~10k rows per transaction) so locks, WAL and replication lag stay bounded, or add a WHERE clause."
	var note string
	if tableSize != nil && rowEstimate != nil {
		note = smallTableNote(*rowEstimate, *tableSize)
	}
	base.Safety = SafetyCaution
	base.LockDuration = "proportional to row count"
	base.Recommendation = rec
	base.Rationale = &Rationale{Reason: rec, Note: note}
	return base
}

func analyzeComment(stmt *pg_query.CommentStmt, a *schema.AnnotatedSchema, stmtNode *pg_query.Node) MigrationCheck {
	const rec = "COMMENT ON takes SHARE UPDATE EXCLUSIVE: it does not block reads or writes, and the comment text lives only in the catalog."
	c := MigrationCheck{
		Operation: "COMMENT",
		Safety:    SafetySafe,
		LockType:  "SHARE UPDATE EXCLUSIVE", LockDuration: "brief (metadata-only)",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      topLevelStatement(stmtNode),
	}
	if rel := commentRelation(stmt); rel != nil {
		c.Table = strp(relationName(rel))
		size, rows, _ := lookupTableStats(a, schema.QualifiedName{Schema: schemaOf(rel), Name: rel.GetRelname()})
		c.TableSize = size
		c.RowEstimate = rows
	}
	return c
}

// commentRelation: object path is [table] or [schema, table] for tables, and
// [table, col] or [schema, table, col] for columns.
func commentRelation(stmt *pg_query.CommentStmt) *pg_query.RangeVar {
	list, ok := stmt.GetObject().GetNode().(*pg_query.Node_List)
	if !ok || list.List == nil {
		return nil
	}
	var parts []string
	for _, item := range list.List.GetItems() {
		if s, ok := item.GetNode().(*pg_query.Node_String_); ok {
			parts = append(parts, s.String_.GetSval())
		}
	}
	switch stmt.GetObjtype() {
	case pg_query.ObjectType_OBJECT_TABLE:
		switch len(parts) {
		case 1:
			return &pg_query.RangeVar{Relname: parts[0]}
		case 2:
			return &pg_query.RangeVar{Schemaname: parts[0], Relname: parts[1]}
		}
	case pg_query.ObjectType_OBJECT_COLUMN:
		switch len(parts) {
		case 2:
			return &pg_query.RangeVar{Relname: parts[0]}
		case 3:
			return &pg_query.RangeVar{Schemaname: parts[0], Relname: parts[1]}
		}
	}
	return nil
}

// DROP TRIGGER takes ACCESS EXCLUSIVE on the table, though only briefly: it is
// a catalog unlink, and enforcement stops at once.
func dropTriggerCheck(drop *pg_query.DropStmt, a *schema.AnnotatedSchema, stmtNode *pg_query.Node) MigrationCheck {
	statement := topLevelStatement(stmtNode)
	safety := SafetySafe
	rec := "Brief ACCESS EXCLUSIVE lock to unlink the trigger from the catalog -- no scan, no rewrite. Whatever the trigger enforced (auditing, denormalization, sync) stops being enforced immediately."
	if drop.Behavior == pg_query.DropBehavior_DROP_CASCADE {
		safety = SafetyCaution
		rec = "Metadata-only, but CASCADE also drops everything that depends on the trigger. Confirm what references it first."
	}
	c := MigrationCheck{
		Operation: "DROP TRIGGER", Safety: safety,
		LockType: "ACCESS EXCLUSIVE", LockDuration: "brief (metadata-only)",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      statement,
	}
	if rel := dropTriggerRelation(drop); rel != nil {
		c.Table = strp(relationName(rel))
		size, rows, _ := lookupTableStats(a, schema.QualifiedName{Schema: schemaOf(rel), Name: rel.GetRelname()})
		c.TableSize = size
		c.RowEstimate = rows
	}
	return c
}

// DROP TRIGGER's objects are [table, trigger] or [schema, table, trigger].
func dropTriggerRelation(drop *pg_query.DropStmt) *pg_query.RangeVar {
	if len(drop.GetObjects()) == 0 {
		return nil
	}
	list, ok := drop.GetObjects()[0].GetNode().(*pg_query.Node_List)
	if !ok || list.List == nil {
		return nil
	}
	var parts []string
	for _, item := range list.List.GetItems() {
		if s, ok := item.GetNode().(*pg_query.Node_String_); ok {
			parts = append(parts, s.String_.GetSval())
		}
	}
	switch len(parts) {
	case 2:
		return &pg_query.RangeVar{Relname: parts[0]}
	case 3:
		return &pg_query.RangeVar{Schemaname: parts[0], Relname: parts[1]}
	}
	return nil
}

// DROP FUNCTION/PROCEDURE is catalog-only; CASCADE can take triggers and
// expression indexes that use it.
func dropFunctionCheck(drop *pg_query.DropStmt, stmtNode *pg_query.Node) MigrationCheck {
	statement := topLevelStatement(stmtNode)
	operation := "DROP FUNCTION"
	switch drop.RemoveType {
	case pg_query.ObjectType_OBJECT_PROCEDURE:
		operation = "DROP PROCEDURE"
	case pg_query.ObjectType_OBJECT_ROUTINE:
		operation = "DROP ROUTINE"
	}
	if drop.Behavior == pg_query.DropBehavior_DROP_CASCADE {
		const rec = "Metadata-only, but CASCADE drops everything that depends on it -- triggers, expression indexes, defaults and other functions. Confirm what references it first."
		return MigrationCheck{
			Operation: operation, Safety: SafetyCaution,
			LockType: "none (catalog only)", LockDuration: "brief",
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			Statement:      statement,
		}
	}
	const rec = "Catalog-only: no table is locked and nothing is rewritten. RESTRICT fails if anything still depends on the routine."
	return MigrationCheck{
		Operation: operation, Safety: SafetySafe,
		LockType: "none (catalog only)", LockDuration: "brief",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      statement,
	}
}

// analyzeReindex: the blocking form locks out writes and blocks reads through
// the index; CONCURRENTLY is non-blocking but cannot run in a transaction.
func analyzeReindex(stmt *pg_query.ReindexStmt, a *schema.AnnotatedSchema, cat *fileCatalog) MigrationCheck {
	concurrent := reindexConcurrent(stmt)
	statement := reindexStatement(stmt)

	kind := "INDEX"
	switch stmt.GetKind() {
	case pg_query.ReindexObjectType_REINDEX_OBJECT_TABLE:
		kind = "TABLE"
	case pg_query.ReindexObjectType_REINDEX_OBJECT_SCHEMA:
		kind = "SCHEMA"
	case pg_query.ReindexObjectType_REINDEX_OBJECT_DATABASE:
		kind = "DATABASE"
	case pg_query.ReindexObjectType_REINDEX_OBJECT_SYSTEM:
		kind = "SYSTEM"
	}

	if concurrent {
		const rec = "REINDEX CONCURRENTLY - does not block reads or writes. Takes significantly longer, needs two table scans and waits for in-flight transactions. Cannot run inside a transaction. On failure it leaves an INVALID _ccnew index behind; drop it and retry."
		return MigrationCheck{
			Operation: "REINDEX CONCURRENTLY", Safety: SafetySafe,
			LockType: "SHARE UPDATE EXCLUSIVE", LockDuration: "~2-3x normal rebuild time (non-blocking)",
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			Statement:      statement,
		}
	}

	// created empty earlier in this migration: instant, 0 rows
	if stmt.GetKind() == pg_query.ReindexObjectType_REINDEX_OBJECT_TABLE && cat.isEmpty(stmt.GetRelation()) {
		zero := 0.0
		const rec = "Table is created empty earlier in this migration: the reindex touches 0 rows and is instant."
		return MigrationCheck{
			Operation: "REINDEX TABLE", Table: strp(relationName(stmt.GetRelation())), Safety: SafetySafe,
			LockType: "SHARE (brief, 0 rows)", LockDuration: "brief (0 rows)",
			TableSize: strp("0 bytes"), RowEstimate: &zero,
			Recommendation: rec,
			Rationale:      &Rationale{Reason: rec},
			Statement:      statement,
		}
	}

	base := MigrationCheck{
		Operation: "REINDEX " + kind, Safety: SafetyDangerous,
		LockType:     "SHARE (blocks writes) + ACCESS EXCLUSIVE on the index",
		LockDuration: "proportional to table size (blocking)",
		Statement:    statement,
	}

	// only a single relation can be sized; SCHEMA/DATABASE/SYSTEM stay dangerous
	if stmt.GetKind() == pg_query.ReindexObjectType_REINDEX_OBJECT_INDEX ||
		stmt.GetKind() == pg_query.ReindexObjectType_REINDEX_OBJECT_TABLE {
		hasSchema := stmt.GetRelation().GetSchemaname() != ""
		qual := schema.QualifiedName{Schema: schemaOf(stmt.GetRelation()), Name: stmt.GetRelation().GetRelname()}
		if stmt.GetKind() == pg_query.ReindexObjectType_REINDEX_OBJECT_INDEX {
			qual = tableOfIndex(a, qual, hasSchema)
		} else {
			base.Table = strp(relationName(stmt.GetRelation()))
		}
		size, rows, small := lookupTableStats(a, qual)
		base.TableSize = size
		base.RowEstimate = rows
		if small {
			base.Safety = SafetyCaution
			base.Rationale = &Rationale{Reason: "REINDEX blocks writes while it rebuilds; on a small table that is brief, but the lock still queues behind any in-flight transaction.", Note: smallTableNote(*rows, *size)}
		} else {
			base.Rationale = &Rationale{Reason: "Blocks writes for the whole rebuild and takes ACCESS EXCLUSIVE on the index, blocking reads that would use it."}
		}
	} else {
		base.Rationale = &Rationale{Reason: "Rebuilds every index in scope, blocking writes throughout. Scope the reindex to a single index or table to make it measurable."}
	}

	base.Recommendation = base.Rationale.Reason
	if base.Safety == SafetyCaution {
		base.Recommendation = base.Rationale.Note + "\n\n" + base.Rationale.Reason
	}
	return base
}

// tableOfIndex resolves REINDEX INDEX's parent table so it can be sized. Unless
// the statement qualified the schema, match the index name across schemas.
func tableOfIndex(a *schema.AnnotatedSchema, idx schema.QualifiedName, hasSchema bool) schema.QualifiedName {
	if a == nil || a.Schema == nil {
		return idx
	}
	for _, t := range a.Schema.Tables {
		for _, i := range t.Indexes {
			if i.Name == idx.Name && (!hasSchema || idx.Schema == t.Schema) {
				return schema.QualifiedName{Schema: t.Schema, Name: t.Name}
			}
		}
	}
	return idx
}

func reindexConcurrent(stmt *pg_query.ReindexStmt) bool {
	for _, p := range stmt.GetParams() {
		def, ok := p.GetNode().(*pg_query.Node_DefElem)
		if !ok || def.DefElem.GetDefname() != "concurrently" {
			continue
		}
		// a bare CONCURRENTLY has no argument and means true
		if def.DefElem.GetArg() == nil {
			return true
		}
		return defElemBool(def.DefElem.GetArg())
	}
	return false
}

func defElemBool(node *pg_query.Node) bool {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_Integer:
		return n.Integer.GetIval() != 0
	case *pg_query.Node_String_:
		switch strings.ToLower(n.String_.GetSval()) {
		case "true", "on", "1":
			return true
		}
	case *pg_query.Node_Boolean:
		return n.Boolean.GetBoolval()
	}
	return false
}

func reindexStatement(stmt *pg_query.ReindexStmt) string {
	node := &pg_query.Node{Node: &pg_query.Node_ReindexStmt{ReindexStmt: stmt}}
	return topLevelStatement(node)
}

// doCheck: a DO body is invisible to the parser and can take any lock or change
// data, and no Statement means it suppresses migration_sql.
func doCheck() MigrationCheck {
	const rec = "DO runs an anonymous procedural block. Its body can take any lock, rewrite any table, or change data, so check_migration cannot analyze it. Review the body by hand."
	return MigrationCheck{
		Operation: "DO", Safety: SafetyCaution,
		LockType: "unknown", LockDuration: "unknown",
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
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

const (
	smallTableMaxRows = 100_000

	// Planner sizing older than this is not evidence: tables grow. Stale or undated
	// captures read as unknown size, which keeps the worst-case verdict.
	plannerStaleAfter = 7 * 24 * time.Hour
)

func lookupTableStats(a *schema.AnnotatedSchema, q schema.QualifiedName) (sizeText *string, rows *float64, small bool) {
	if a == nil || a.Planner == nil || a.Planner.Timestamp.IsZero() || time.Since(a.Planner.Timestamp) > plannerStaleAfter {
		return nil, nil, false
	}
	sz := a.SizingFor(q)
	if sz == nil {
		return nil, nil, false
	}
	size := formatBytes(sz.TableSize)
	r := sz.Reltuples
	return &size, &r, r >= 0 && r <= smallTableMaxRows
}

func smallTableNote(rows float64, sizeText string) string {
	return fmt.Sprintf("Table is small (~%d rows, %s): the operation is brief, but the lock still queues behind any in-flight transaction -- set lock_timeout and retry on timeout.", int64(rows), sizeText)
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

func stringList(nodes []*pg_query.Node) []string {
	var out []string
	for _, n := range nodes {
		if s, ok := n.GetNode().(*pg_query.Node_String_); ok {
			out = append(out, s.String_.Sval)
		}
	}
	return out
}
