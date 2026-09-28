package query

import (
	"fmt"
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/jit"
)

type (
	// constraintEntry is the snapshot row overlaid with this file's changes.
	constraintEntry struct {
		name        string
		kind        schema.ConstraintKind
		notValid    bool
		notEnforced bool
		inFile      bool
		gone        bool
		proves      []string
		fkTable     string
	}

	proofState int

	notNullProof struct {
		state proofState
		name  string
	}
)

const (
	proofNone proofState = iota
	proofValidated
	proofNotValid
)

func entryFromSnapshot(c schema.Constraint) *constraintEntry {
	e := &constraintEntry{
		name:        c.Name,
		kind:        c.Kind,
		notValid:    c.IsNotValid(),
		notEnforced: c.IsNotEnforced(),
	}
	if c.FKTable != nil {
		e.fkTable = *c.FKTable
	}
	if c.Kind == schema.ConstraintCheck && c.Definition != nil {
		e.proves = provedColumns(checkExprOf(*c.Definition))
	}
	return e
}

func entryFromStatement(con *pg_query.Constraint) *constraintEntry {
	if con == nil || con.GetConname() == "" {
		return nil
	}
	e := &constraintEntry{name: con.GetConname(), notValid: con.GetSkipValidation(), inFile: true}
	switch pg_query.ConstrType(con.Contype) {
	case pg_query.ConstrType_CONSTR_CHECK:
		e.kind = schema.ConstraintCheck
		e.proves = provedColumns(con.GetRawExpr())
	case pg_query.ConstrType_CONSTR_FOREIGN:
		e.kind = schema.ConstraintForeignKey
		e.fkTable = con.GetPktable().GetRelname()
	default:
		return nil
	}
	return e
}

// pg_get_constraintdef output is not a statement, so give it a host.
func checkExprOf(def string) *pg_query.Node {
	res, err := pg_query.Parse("ALTER TABLE t ADD " + def)
	if err != nil || len(res.Stmts) != 1 {
		return nil
	}
	alter := res.Stmts[0].GetStmt().GetAlterTableStmt()
	if alter == nil || len(alter.Cmds) != 1 {
		return nil
	}
	return constraintOf(alter.Cmds[0].GetAlterTableCmd()).GetRawExpr()
}

// provedColumns is conservative: a false proof would promise a skipped scan.
func provedColumns(n *pg_query.Node) []string {
	switch v := n.GetNode().(type) {
	case *pg_query.Node_NullTest:
		if v.NullTest.GetNulltesttype() != pg_query.NullTestType_IS_NOT_NULL {
			return nil
		}
		if col := bareColumn(v.NullTest.GetArg()); col != "" {
			return []string{col}
		}
	case *pg_query.Node_BoolExpr:
		if v.BoolExpr.GetBoolop() != pg_query.BoolExprType_AND_EXPR {
			return nil
		}
		var cols []string
		for _, arg := range v.BoolExpr.GetArgs() {
			cols = append(cols, provedColumns(arg)...)
		}
		return cols
	}
	return nil
}

func bareColumn(n *pg_query.Node) string {
	ref := n.GetColumnRef()
	if ref == nil || len(ref.GetFields()) != 1 {
		return ""
	}
	return ref.GetFields()[0].GetString_().GetSval()
}

func constraintsFor(snap *schema.SchemaSnapshot, cat *fileCatalog, rel *pg_query.RangeVar) map[string]*constraintEntry {
	out := map[string]*constraintEntry{}
	if rel == nil {
		return out
	}
	if t := lookupTable(snap, rel); t != nil {
		for _, c := range t.Constraints {
			out[c.Name] = entryFromSnapshot(c)
		}
	}
	if cat != nil {
		for name, e := range cat.cons[relKey(rel)] {
			if e.gone {
				delete(out, name)
			} else {
				out[name] = e
			}
		}
	}
	return out
}

func sortedEntries(m map[string]*constraintEntry) []*constraintEntry {
	out := make([]*constraintEntry, 0, len(m))
	for _, e := range m {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// A composite column never gets the skip: IS NOT NULL on a row value is a different test.
func columnIsComposite(snap *schema.SchemaSnapshot, rel *pg_query.RangeVar, col string) bool {
	t := lookupTable(snap, rel)
	if t == nil {
		return false
	}
	for _, c := range t.Columns {
		if c.Name != col {
			continue
		}
		typ := strings.Trim(c.TypeName[strings.LastIndex(c.TypeName, ".")+1:], `"`)
		for _, comp := range snap.Composites {
			if comp.Name == typ {
				return true
			}
		}
	}
	return false
}

func findNotNullProof(snap *schema.SchemaSnapshot, cat *fileCatalog, rel *pg_query.RangeVar, col string) notNullProof {
	best := notNullProof{}
	if col == "" || columnIsComposite(snap, rel, col) {
		return best
	}
	for _, e := range sortedEntries(constraintsFor(snap, cat, rel)) {
		if e.kind != schema.ConstraintCheck || e.notEnforced || !containsString(e.proves, col) {
			continue
		}
		if !e.notValid {
			return notNullProof{state: proofValidated, name: e.name}
		}
		if best.state == proofNone {
			best = notNullProof{state: proofNotValid, name: e.name}
		}
	}
	return best
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func (c *fileCatalog) entryFor(snap *schema.SchemaSnapshot, rel *pg_query.RangeVar, name string) *constraintEntry {
	if e, ok := c.cons[relKey(rel)][name]; ok {
		return e
	}
	if t := lookupTable(snap, rel); t != nil {
		for _, sc := range t.Constraints {
			if sc.Name == name {
				return entryFromSnapshot(sc)
			}
		}
	}
	return nil
}

// observeAlter records a command's constraint changes for later statements.
func (c *fileCatalog) observeAlter(snap *schema.SchemaSnapshot, stmt *pg_query.AlterTableStmt, cmd *pg_query.AlterTableCmd) {
	rel := stmt.GetRelation()
	if c == nil || rel == nil {
		return
	}
	key := relKey(rel)
	set := func(e *constraintEntry) {
		if c.cons[key] == nil {
			c.cons[key] = map[string]*constraintEntry{}
		}
		c.cons[key][e.name] = e
	}
	switch pg_query.AlterTableType(cmd.Subtype) {
	case pg_query.AlterTableType_AT_AddConstraint:
		if e := entryFromStatement(constraintOf(cmd)); e != nil {
			set(e)
		}
	case pg_query.AlterTableType_AT_ValidateConstraint:
		if e := c.entryFor(snap, rel, cmd.Name); e != nil {
			v := *e
			v.notValid = false
			set(&v)
		}
	case pg_query.AlterTableType_AT_DropConstraint:
		set(&constraintEntry{name: cmd.Name, gone: true})
	}
}

func (c *fileCatalog) renameConstraint(snap *schema.SchemaSnapshot, rel *pg_query.RangeVar, oldName, newName string) {
	if c == nil || rel == nil || oldName == "" || newName == "" {
		return
	}
	e := c.entryFor(snap, rel, oldName)
	if e == nil {
		return
	}
	key := relKey(rel)
	if c.cons[key] == nil {
		c.cons[key] = map[string]*constraintEntry{}
	}
	v := *e
	v.name = newName
	c.cons[key][oldName] = &constraintEntry{name: oldName, gone: true}
	c.cons[key][newName] = &v
}

// listed once per table per file, not per ALTER.
func (c *fileCatalog) unvalidatedNote(snap *schema.SchemaSnapshot, rel *pg_query.RangeVar, except string) []string {
	if c == nil || rel == nil || c.reported[relKey(rel)] {
		return nil
	}
	var names []string
	for _, e := range sortedEntries(constraintsFor(snap, c, rel)) {
		if e.notValid && !e.notEnforced && e.name != except {
			names = append(names, e.name)
		}
	}
	if len(names) > 0 {
		c.reported[relKey(rel)] = true
	}
	return names
}

func attachUnvalidated(check *MigrationCheck, snap *schema.SchemaSnapshot, cat *fileCatalog, stmt *pg_query.AlterTableStmt, cmd *pg_query.AlterTableCmd) {
	except := ""
	if pg_query.AlterTableType(cmd.Subtype) == pg_query.AlterTableType_AT_ValidateConstraint {
		except = cmd.Name
	}
	names := cat.unvalidatedNote(snap, stmt.GetRelation(), except)
	if len(names) == 0 {
		return
	}
	check.UnvalidatedConstraints = names
	note := fmt.Sprintf("Unvalidated on this table: %s. They are enforced for new and updated rows only, and the planner does not rely on them until VALIDATE CONSTRAINT.", strings.Join(names, ", "))
	check.Recommendation += "\n\n" + note
	if check.Rationale != nil {
		check.Rationale.Note = joinNotes(check.Rationale.Note, note)
	}
}

func analyzeValidateConstraint(cmd *pg_query.AlterTableCmd, stmt *pg_query.AlterTableStmt, tableName string, tableSize *string, rowEstimate *float64, snap *schema.SchemaSnapshot, cat *fileCatalog, statement string) *MigrationCheck {
	rel := stmt.GetRelation()
	e := cat.entryFor(snap, rel, cmd.Name)
	if e != nil && e.gone {
		e = nil
	}
	rec := "Safe - validates existing rows with a weaker lock that allows concurrent reads and writes."
	safety := SafetySafe
	switch {
	case e == nil:
		if lookupTable(snap, rel) != nil {
			rec += " The constraint is not in the snapshot, so its current state is unknown."
		}
	case e.notEnforced:
		safety = SafetyCaution
		rec = fmt.Sprintf("%s is NOT ENFORCED; Postgres will not validate it until it is enforced.", cmd.Name)
	case !e.notValid:
		when := "in the snapshot"
		if e.inFile {
			when = "earlier in this migration"
		} else if !snap.Timestamp.IsZero() {
			when = "in the snapshot captured " + snap.Timestamp.Format("2006-01-02")
		}
		rec = fmt.Sprintf("No-op: %s is already validated %s, so this returns without scanning.", cmd.Name, when)
	default:
		rec += " It holds SHARE UPDATE EXCLUSIVE for the whole scan, which also blocks autovacuum on the table."
		if e.kind == schema.ConstraintForeignKey {
			ref := "the referenced table"
			if e.fkTable != "" {
				ref = e.fkTable
			}
			rec += fmt.Sprintf(" As a foreign key it also takes ROW SHARE on %s, and the scan joins both tables, so the cost depends on both sizes.", ref)
		}
	}
	return &MigrationCheck{
		Operation: "VALIDATE CONSTRAINT", Table: strp(tableName), Safety: safety,
		LockType:     "SHARE UPDATE EXCLUSIVE",
		LockDuration: "proportional to table size (but allows concurrent DML)",
		TableSize:    tableSize, RowEstimate: rowEstimate,
		Recommendation: rec,
		Rationale:      &Rationale{Reason: rec},
		Statement:      statement,
	}
}

// validated proof skips the scan; NOT VALID means only the VALIDATE step is missing.
func setNotNullWithProof(proof notNullProof, colName, tableName string, tableSize *string, rowEstimate *float64, stmt *pg_query.AlterTableStmt, statement string) *MigrationCheck {
	rel := stmt.GetRelation()
	if proof.state == proofValidated {
		rec := fmt.Sprintf("Metadata-only in practice: the validated CHECK %s proves %s has no NULLs, so Postgres skips the table scan. The ACCESS EXCLUSIVE lock is brief. The CHECK is now redundant and can be dropped afterwards.", proof.name, colName)
		return &MigrationCheck{
			Operation: "SET NOT NULL", Table: strp(tableName), Safety: SafetySafe,
			LockType:     "ACCESS EXCLUSIVE",
			LockDuration: "brief (scan skipped by validated CHECK " + proof.name + ")",
			TableSize:    tableSize, RowEstimate: rowEstimate,
			Recommendation:  rec,
			Rationale:       &Rationale{Reason: rec},
			VersionBehavior: strp("Scan is skipped because a valid CHECK (col IS NOT NULL) exists."),
			RollbackDDL:     strp("ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL;"),
			Statement:       statement,
		}
	}
	e := jit.SetNotNull(tableName, colName).Caution()
	rec := fmt.Sprintf("The CHECK %s on %s is NOT VALID, so Postgres cannot use it to skip the scan: SET NOT NULL would scan the whole table under ACCESS EXCLUSIVE. Run VALIDATE CONSTRAINT %s first (SHARE UPDATE EXCLUSIVE, concurrent DML allowed); SET NOT NULL is then brief.", proof.name, colName, proof.name)
	var safer []string
	if rel != nil && rel.GetInh() && !stmt.GetMissingOk() {
		table := relationName(rel)
		safer = runnable([]string{
			fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", table, quoteIdent(proof.name)),
			fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", table, quoteIdent(colName)),
		})
	}
	return &MigrationCheck{
		Operation: "SET NOT NULL", Table: strp(tableName), Safety: SafetyCaution,
		LockType:     "ACCESS EXCLUSIVE",
		LockDuration: "scan duration (the existing CHECK is NOT VALID, so the scan is not skipped)",
		TableSize:    tableSize, RowEstimate: rowEstimate,
		Recommendation:  rec,
		Rationale:       &Rationale{Reason: rec, Note: e.Note},
		VersionBehavior: strp("Scan is skipped only once the CHECK (col IS NOT NULL) is validated."),
		RollbackDDL:     strp("ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL;"),
		SaferSQL:        safer,
		Statement:       statement,
	}
}
