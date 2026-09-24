package query

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/boringsql/dryrun/internal/dryrun"
	"github.com/boringsql/dryrun/internal/schema"
)

const (
	// generated names are clipped the way Postgres clips them, or CREATE and
	// VALIDATE disagree on the constraint name
	pgIdentMax = 63
)

type (
	// nameAllocator disambiguates duplicate generated names with a numeric
	// suffix, the way Postgres does.
	nameAllocator struct {
		snap  *schema.SchemaSnapshot
		taken map[string]map[string]bool
	}
)

func newNameAllocator(snap *schema.SchemaSnapshot) *nameAllocator {
	return &nameAllocator{snap: snap, taken: map[string]map[string]bool{}}
}

func (a *nameAllocator) claim(bucket, base string) string {
	held := a.taken[bucket]
	base = clipIdent(base, pgIdentMax)
	name := base
	for i := 1; held[name]; i++ {
		suffix := strconv.Itoa(i)
		name = clipIdent(base, pgIdentMax-len(suffix)) + suffix
	}
	held[name] = true
	return name
}

// Constraint names are unique per table; the bucket is seeded from the snapshot.
func (a *nameAllocator) constraintBucket(rel *pg_query.RangeVar) string {
	bucket := "constraint:" + relKey(rel)
	if _, ok := a.taken[bucket]; ok {
		return bucket
	}
	held := map[string]bool{}
	a.taken[bucket] = held
	if t := lookupTable(a.snap, rel); t != nil {
		for _, c := range t.Constraints {
			held[c.Name] = true
		}
	}
	return bucket
}

// Index names are unique per schema, not per table.
func (a *nameAllocator) indexBucket(rel *pg_query.RangeVar) string {
	sch := schemaOf(rel)
	bucket := "index:" + sch
	if _, ok := a.taken[bucket]; ok {
		return bucket
	}
	held := map[string]bool{}
	a.taken[bucket] = held
	if a.snap != nil {
		for _, t := range a.snap.Tables {
			if t.Schema != sch {
				continue
			}
			for _, idx := range t.Indexes {
				held[idx.Name] = true
			}
		}
	}
	return bucket
}

// runnable returns the steps only if they parse. This also confines NOT VALID
// to FOREIGN KEY and CHECK: Postgres rejects it on PRIMARY KEY, UNIQUE and
// EXCLUDE, so those fail here rather than needing a case of their own.
func runnable(steps []string) []string {
	if _, err := pg_query.Parse(strings.Join(steps, "\n")); err != nil {
		return nil
	}
	return steps
}

// fkNotValidRejected: partitioned tables reject NOT VALID FKs before PG18.
// Unknown version counts as older.
func fkNotValidRejected(snap *schema.SchemaSnapshot, cat *fileCatalog, rel *pg_query.RangeVar) bool {
	if rel == nil {
		return false
	}
	partitioned := cat.isPartitioned(rel)
	if t := lookupTable(snap, rel); t != nil && t.PartitionInfo != nil {
		partitioned = true
	}
	if !partitioned {
		return false
	}
	if snap == nil {
		return true
	}
	v, err := dryrun.ParsePgVersion(snap.PgVersion)
	return err != nil || v.Major < 18
}

// rewriteAddConstraint flips SkipValidation on the parsed constraint and lets
// pg_query render it back, so no DDL is rebuilt by hand and lost on the way.
func rewriteAddConstraint(stmt *pg_query.AlterTableStmt, cmd *pg_query.AlterTableCmd, names *nameAllocator, fkNoNotValid bool) []string {
	con := constraintOf(cmd)
	if con == nil || con.SkipValidation || stmt.GetRelation() == nil {
		return nil
	}
	if fkNoNotValid && pg_query.ConstrType(con.Contype) == pg_query.ConstrType_CONSTR_FOREIGN {
		return nil
	}
	one, oneCmd := singleCmdStmt(stmt, cmd)
	newCon := constraintOf(oneCmd)
	newCon.SkipValidation = true
	if newCon.Conname == "" {
		rel := stmt.GetRelation()
		newCon.Conname = names.claim(names.constraintBucket(rel), constraintName(rel, newCon))
	}
	add, err := deparseStmt(&pg_query.Node{Node: &pg_query.Node_AlterTableStmt{AlterTableStmt: one}})
	if err != nil {
		return nil
	}
	// deparsed so ONLY, IF EXISTS and quoting match the statement being validated
	validate, err := deparseStmt(&pg_query.Node{Node: &pg_query.Node_AlterTableStmt{AlterTableStmt: &pg_query.AlterTableStmt{
		Relation:  one.GetRelation(),
		Objtype:   one.GetObjtype(),
		MissingOk: one.GetMissingOk(),
		Cmds: []*pg_query.Node{{Node: &pg_query.Node_AlterTableCmd{AlterTableCmd: &pg_query.AlterTableCmd{
			Subtype: pg_query.AlterTableType_AT_ValidateConstraint,
			Name:    newCon.GetConname(),
		}}}},
	}}})
	if err != nil {
		return nil
	}
	return runnable([]string{add + ";", validate + ";"})
}

// rewriteInlineColumnConstraints strips an inline FOREIGN KEY, CHECK or UNIQUE
// off the column and re-adds it as its own statement, so it can use
// NOT VALID/VALIDATE or a concurrent index. PRIMARY KEY and NOT NULL cannot.
func rewriteInlineColumnConstraints(stmt *pg_query.AlterTableStmt, cmd *pg_query.AlterTableCmd, colName string, cons inlineColConstraints, names *nameAllocator, fkNoNotValid bool) []string {
	rel := stmt.GetRelation()
	if rel == nil || (cons.fk == nil && cons.check == nil && cons.unique == nil) {
		return nil
	}
	// IF NOT EXISTS skips the constraint with the column; a split would not
	if cmd.GetMissingOk() {
		return nil
	}
	// no partial rewrite: the FK would have to stay inline, still blocking
	if cons.fk != nil && fkNoNotValid {
		return nil
	}
	// CONCURRENTLY is rejected on a partitioned parent (no partial rewrite)
	if cons.unique != nil {
		if t := lookupTable(names.snap, rel); t != nil && t.PartitionInfo != nil {
			return nil
		}
		// USING INDEX cannot be deferrable; the generated index is plain
		if cons.unique.Deferrable || cons.unique.Initdeferred || cons.unique.NullsNotDistinct {
			return nil
		}
	}
	one, oneCmd := singleCmdStmt(stmt, cmd)
	cd := columnDefOf(oneCmd)
	if cd == nil {
		return nil
	}
	var kept []*pg_query.Node
	stripAttrs := false
	for _, n := range cd.Constraints {
		if con, ok := n.GetNode().(*pg_query.Node_Constraint); ok && con.Constraint != nil {
			switch pg_query.ConstrType(con.Constraint.Contype) {
			case pg_query.ConstrType_CONSTR_FOREIGN, pg_query.ConstrType_CONSTR_CHECK, pg_query.ConstrType_CONSTR_UNIQUE:
				stripAttrs = true
				continue
			case pg_query.ConstrType_CONSTR_ATTR_DEFERRABLE, pg_query.ConstrType_CONSTR_ATTR_NOT_DEFERRABLE,
				pg_query.ConstrType_CONSTR_ATTR_DEFERRED, pg_query.ConstrType_CONSTR_ATTR_IMMEDIATE:
				// attributes follow their constraint; keep them only if it stays
				if stripAttrs {
					continue
				}
			default:
				stripAttrs = false
			}
		}
		kept = append(kept, n)
	}
	cd.Constraints = kept

	add, err := deparseStmt(&pg_query.Node{Node: &pg_query.Node_AlterTableStmt{AlterTableStmt: one}})
	if err != nil {
		return nil
	}
	steps := []string{add + ";"}

	for _, src := range []*pg_query.Constraint{cons.fk, cons.check} {
		if src == nil {
			continue
		}
		c := proto.Clone(src).(*pg_query.Constraint)
		// an inline FK carries no explicit attrs; the column is the key
		if pg_query.ConstrType(c.Contype) == pg_query.ConstrType_CONSTR_FOREIGN && len(c.FkAttrs) == 0 && colName != "" {
			c.FkAttrs = []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: colName}}}}
		}
		if c.Conname == "" {
			c.Conname = names.claim(names.constraintBucket(rel), constraintName(rel, c))
		}
		c.SkipValidation = true
		addSQL, err := deparseAlterCmd(one, &pg_query.AlterTableCmd{
			Subtype: pg_query.AlterTableType_AT_AddConstraint,
			Def:     &pg_query.Node{Node: &pg_query.Node_Constraint{Constraint: c}},
		})
		if err != nil {
			return nil
		}
		validateSQL, err := deparseAlterCmd(one, &pg_query.AlterTableCmd{
			Subtype: pg_query.AlterTableType_AT_ValidateConstraint,
			Name:    c.Conname,
		})
		if err != nil {
			return nil
		}
		steps = append(steps, addSQL+";", validateSQL+";")
	}

	if cons.unique != nil {
		// a named constraint keeps its name; one base for both, as Postgres does
		conBase := cons.unique.Conname
		idxBase := cons.unique.Conname
		if conBase == "" {
			conBase = rel.GetRelname() + "_" + colName + "_key"
			idxBase = rel.GetRelname() + "_" + colName + "_idx"
		}
		idxName := names.claim(names.indexBucket(rel), idxBase)
		conName := names.claim(names.constraintBucket(rel), conBase)
		idx := &pg_query.IndexStmt{
			Idxname:      idxName,
			Relation:     rel,
			AccessMethod: "btree",
			Unique:       true,
			Concurrent:   true,
			IndexParams: []*pg_query.Node{{Node: &pg_query.Node_IndexElem{
				IndexElem: &pg_query.IndexElem{Name: colName},
			}}},
		}
		idxSQL, err := deparseStmt(&pg_query.Node{Node: &pg_query.Node_IndexStmt{IndexStmt: idx}})
		if err != nil {
			return nil
		}
		conSQL, err := deparseAlterCmd(one, &pg_query.AlterTableCmd{
			Subtype: pg_query.AlterTableType_AT_AddConstraint,
			Def: &pg_query.Node{Node: &pg_query.Node_Constraint{Constraint: &pg_query.Constraint{
				Contype:   pg_query.ConstrType_CONSTR_UNIQUE,
				Conname:   conName,
				Indexname: idxName,
			}}},
		})
		if err != nil {
			return nil
		}
		steps = append(steps, idxSQL+";", conSQL+";")
	}

	return runnable(steps)
}

// deparseAlterCmd reuses one's Relation/Objtype/MissingOk (ONLY, IF EXISTS).
func deparseAlterCmd(one *pg_query.AlterTableStmt, cmd *pg_query.AlterTableCmd) (string, error) {
	s := proto.Clone(one).(*pg_query.AlterTableStmt)
	s.Cmds = []*pg_query.Node{{Node: &pg_query.Node_AlterTableCmd{AlterTableCmd: cmd}}}
	return deparseStmt(&pg_query.Node{Node: &pg_query.Node_AlterTableStmt{AlterTableStmt: s}})
}

func rewriteCreateIndex(idx *pg_query.IndexStmt, names *nameAllocator, cat *fileCatalog) (steps []string, name string) {
	if idx == nil || idx.Concurrent || idx.GetRelation() == nil {
		return nil, ""
	}
	// CONCURRENTLY is rejected on a partitioned parent; the file catalog
	// catches one created earlier in this same migration
	if t := lookupTable(names.snap, idx.GetRelation()); t != nil && t.PartitionInfo != nil {
		return nil, ""
	}
	if cat != nil && cat.isPartitioned(idx.GetRelation()) {
		return nil, ""
	}
	c := proto.Clone(idx).(*pg_query.IndexStmt)
	c.Concurrent = true
	// a failed concurrent build leaves an INVALID index behind, so name it
	if c.Idxname == "" {
		c.Idxname = names.claim(names.indexBucket(idx.GetRelation()), defaultIndexName(idx))
	}
	s, err := deparseStmt(&pg_query.Node{Node: &pg_query.Node_IndexStmt{IndexStmt: c}})
	if err != nil {
		return nil, ""
	}
	return runnable([]string{s + ";"}), c.Idxname
}

// A validated CHECK proves no NULLs, which lets SET NOT NULL skip the scan.
func rewriteSetNotNull(stmt *pg_query.AlterTableStmt, col string, names *nameAllocator) []string {
	rel := stmt.GetRelation()
	if rel == nil || col == "" {
		return nil
	}
	// ONLY and IF EXISTS change what the rewrite can promise; both are
	// decisions, not rewrites.
	if !rel.GetInh() || stmt.GetMissingOk() {
		return nil
	}
	table := relationName(rel)
	column := quoteIdent(col)
	name := quoteIdent(names.claim(names.constraintBucket(rel), rel.GetRelname()+"_"+col+"_not_null"))
	return runnable([]string{
		fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s IS NOT NULL) NOT VALID;", table, name, column),
		fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s;", table, name),
		fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", table, column),
		fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", table, name),
	})
}

// alterCmdStatement deparses this one command as its own ALTER TABLE (via
// singleCmdStmt, so ONLY/IF EXISTS/quoting survive) -- the passthrough half
// of ComposeMigrationSQL.
func alterCmdStatement(stmt *pg_query.AlterTableStmt, cmd *pg_query.AlterTableCmd) string {
	one, _ := singleCmdStmt(stmt, cmd)
	s, err := deparseStmt(&pg_query.Node{Node: &pg_query.Node_AlterTableStmt{AlterTableStmt: one}})
	if err != nil {
		return ""
	}
	return s + ";"
}

// indexStmtStatement deparses a CREATE INDEX as-parsed, same passthrough
// purpose as alterCmdStatement.
func indexStmtStatement(idx *pg_query.IndexStmt) string {
	s, err := deparseStmt(&pg_query.Node{Node: &pg_query.Node_IndexStmt{IndexStmt: idx}})
	if err != nil {
		return ""
	}
	return s + ";"
}

// topLevelStatement deparses a whole top-level statement as-parsed -- DROP
// INDEX, SET, or a statement CheckMigration cannot analyze at all.
func topLevelStatement(node *pg_query.Node) string {
	s, err := deparseStmt(node)
	if err != nil {
		return ""
	}
	return s + ";"
}

// singleCmdStmt isolates the one command being reported from the rest of the
// ALTER TABLE.
func singleCmdStmt(stmt *pg_query.AlterTableStmt, cmd *pg_query.AlterTableCmd) (*pg_query.AlterTableStmt, *pg_query.AlterTableCmd) {
	s := proto.Clone(stmt).(*pg_query.AlterTableStmt)
	c := proto.Clone(cmd).(*pg_query.AlterTableCmd)
	s.Cmds = []*pg_query.Node{{Node: &pg_query.Node_AlterTableCmd{AlterTableCmd: c}}}
	return s, c
}

func constraintOf(cmd *pg_query.AlterTableCmd) *pg_query.Constraint {
	if cmd == nil || cmd.Def == nil {
		return nil
	}
	if con, ok := cmd.Def.Node.(*pg_query.Node_Constraint); ok {
		return con.Constraint
	}
	return nil
}

func deparseStmt(node *pg_query.Node) (string, error) {
	return pg_query.Deparse(&pg_query.ParseResult{Stmts: []*pg_query.RawStmt{{Stmt: node}}})
}

// constraintName picks a Postgres-style name for an unnamed constraint, which
// VALIDATE CONSTRAINT needs. Close to Postgres' shape, not an attempt at parity.
func constraintName(rel *pg_query.RangeVar, con *pg_query.Constraint) string {
	parts := []string{rel.GetRelname()}
	suffix := "check"
	if pg_query.ConstrType(con.Contype) == pg_query.ConstrType_CONSTR_FOREIGN {
		suffix = "fkey"
		parts = append(parts, stringList(con.FkAttrs)...)
	} else {
		parts = append(parts, columnsIn(con.RawExpr)...)
	}
	return strings.Join(append(parts, suffix), "_")
}

// Postgres names an unnamed index <table>_<cols>_idx.
func defaultIndexName(idx *pg_query.IndexStmt) string {
	parts := []string{idx.GetRelation().GetRelname()}
	for _, p := range idx.GetIndexParams() {
		if e, ok := p.Node.(*pg_query.Node_IndexElem); ok && e.IndexElem.GetName() != "" {
			parts = append(parts, e.IndexElem.GetName())
		}
	}
	return strings.Join(append(parts, "idx"), "_")
}

func columnsIn(node *pg_query.Node) []string {
	var out []string
	seen := map[string]bool{}
	walkNode(node, func(n *pg_query.Node) {
		cr, ok := n.GetNode().(*pg_query.Node_ColumnRef)
		if !ok {
			return
		}
		fc := extractFilterColumn(cr.ColumnRef)
		if fc == nil || seen[fc.Column] {
			return
		}
		seen[fc.Column] = true
		out = append(out, fc.Column)
	})
	return out
}

func relationName(rel *pg_query.RangeVar) string {
	if rel.GetSchemaname() == "" {
		return quoteIdent(rel.GetRelname())
	}
	return quoteIdent(rel.GetSchemaname()) + "." + quoteIdent(rel.GetRelname())
}

func schemaOf(rel *pg_query.RangeVar) string {
	if s := rel.GetSchemaname(); s != "" {
		return s
	}
	return "public"
}

func relKey(rel *pg_query.RangeVar) string {
	return schemaOf(rel) + "." + rel.GetRelname()
}

func lookupTable(snap *schema.SchemaSnapshot, rel *pg_query.RangeVar) *schema.Table {
	if snap == nil {
		return nil
	}
	key := relKey(rel)
	for i := range snap.Tables {
		if snap.Tables[i].Schema+"."+snap.Tables[i].Name == key {
			return &snap.Tables[i]
		}
	}
	return nil
}

// Postgres clips to whole characters; cutting mid-rune would break the name.
func clipIdent(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
