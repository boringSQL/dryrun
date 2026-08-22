// Package ddl renders a captured table back into the SQL that would create it.
package ddl

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

const (
	// the grammar bundled with pg_query_go v6. A snapshot from a newer server
	// can legitimately carry syntax this cannot read.
	parserMajor = 17
)

type (
	Result struct {
		// the same DDL twice: joined for pasting, split for diffing statement
		// by statement
		SQL        string   `json:"sql"`
		Statements []string `json:"statements,omitempty"`
		// what the snapshot cannot reproduce, so the caller knows what running
		// this would still be missing
		Omitted []string `json:"omitted,omitempty"`
		// false when the snapshot is from a server newer than the bundled
		// grammar and the syntax check could not run
		ParseChecked bool   `json:"parse_checked"`
		ParseError   string `json:"parse_error,omitempty"`
	}
)

// Never captured, and no way to infer them from what is.
var alwaysOmitted = []string{
	"GRANT and ownership",
	"tablespaces, UNLOGGED, per-column STORAGE and COMPRESSION, and non-default collations",
}

// Renders the table as CREATE TABLE plus the statements that finish it. Returns
// an error rather than partial SQL: DDL that looks pasteable and is not is worse
// than none.
func RenderTable(snap *snapshot.SchemaSnapshot, t *snapshot.Table) (*Result, error) {
	if t == nil {
		return nil, fmt.Errorf("no table")
	}
	stmts, err := statements(snap, t, false)
	if err != nil {
		return nil, err
	}
	res := &Result{SQL: strings.Join(stmts, "\n"), Statements: stmts, Omitted: omitted(snap, t)}

	// the bundled grammar is PG17; VIRTUAL is PG18 syntax, so the check runs
	// against a STORED variant of the same shape
	check := stmts
	if hasVirtual(t) {
		if check, err = statements(snap, t, true); err != nil {
			return nil, err
		}
	}
	if _, err := pg_query.Parse(strings.Join(check, "\n")); err != nil {
		// these strings are mostly Postgres' own deparse output, so on a newer
		// server a parse failure means the grammar is old, not the SQL wrong
		if major(snap) > parserMajor {
			res.ParseError = err.Error()
			return res, nil
		}
		return nil, fmt.Errorf("rendered ddl does not parse: %w", err)
	}
	res.ParseChecked = true
	return res, nil
}

func statements(snap *snapshot.SchemaSnapshot, t *snapshot.Table, storedForVirtual bool) ([]string, error) {
	// a partition inherits its parent's defaults, constraints and indexes, and
	// the snapshot does not record which of its copies are local. Re-owning the
	// parent's sequence to a partition is the one that bites: dropping the
	// partition would take the sequence with it.
	part := isPartition(snap, t)

	var out []string
	if !part {
		out = append(out, sequences(t)...)
	}
	create, err := createTable(snap, t, storedForVirtual)
	if err != nil {
		return nil, err
	}
	out = append(out, create)
	if part {
		return append(out, comments(t)...), nil
	}
	out = append(out, ownedSequences(t)...)

	// after the table exists, and after the referenced tables do
	for _, c := range t.Constraints {
		if c.Kind == snapshot.ConstraintForeignKey && c.Definition != nil {
			out = append(out, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s;",
				qual(t.Schema, t.Name), quoteIdent(c.Name), *c.Definition))
		}
	}
	backing := backingIndexes(t)
	for _, idx := range t.Indexes {
		// the constraint already creates its index; emitting it again fails on
		// the name. backs_constraint is absent from older captures, so the
		// shared name is what this actually goes on
		if idx.Definition == "" || idx.BacksConstraint || idx.IsPrimary || backing[idx.Name] {
			continue
		}
		out = append(out, attachable(idx.Definition)+";")
	}
	for _, c := range t.Columns {
		if c.StatisticsTarget != nil {
			out = append(out, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET STATISTICS %d;",
				qual(t.Schema, t.Name), quoteIdent(c.Name), *c.StatisticsTarget))
		}
	}
	if t.RLSEnabled {
		out = append(out, fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY;", qual(t.Schema, t.Name)))
	}
	for _, p := range t.Policies {
		out = append(out, policy(t, p))
	}
	for _, tr := range t.Triggers {
		if tr.Definition != "" {
			out = append(out, tr.Definition+";")
		}
	}
	return append(out, comments(t)...), nil
}

// pg_get_indexdef says ON ONLY for an index on a partitioned table, which
// leaves it detached and unused until every partition attaches one.
func attachable(def string) string {
	return strings.Replace(def, " ON ONLY ", " ON ", 1)
}

func createTable(snap *snapshot.SchemaSnapshot, t *snapshot.Table, storedForVirtual bool) (string, error) {
	name := qual(t.Schema, t.Name)

	// a partition inherits its shape; emitting the columns again would create a
	// standalone table with the same name
	if parent, bound, ok := partitionOf(snap, t); ok {
		if bound == "" {
			return "", fmt.Errorf("%s is a partition of %s but the snapshot has no bound for it", name, parent)
		}
		out := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s %s", name, parent, bound)
		// a partition can itself be partitioned
		if t.PartitionInfo != nil {
			out += " PARTITION BY " + t.PartitionInfo.Key
		}
		return out + reloptions(t) + ";", nil
	}

	var body []string
	for _, c := range t.Columns {
		line, err := column(c, storedForVirtual)
		if err != nil {
			return "", err
		}
		body = append(body, "    "+line)
	}
	if len(body) == 0 {
		return "", fmt.Errorf("%s has no columns in the snapshot", name)
	}
	for _, c := range t.Constraints {
		if c.Kind == snapshot.ConstraintForeignKey || c.Definition == nil {
			continue
		}
		body = append(body, fmt.Sprintf("    CONSTRAINT %s %s", quoteIdent(c.Name), *c.Definition))
	}

	out := fmt.Sprintf("CREATE TABLE %s (\n%s\n)", name, strings.Join(body, ",\n"))
	if t.PartitionInfo != nil {
		out += " PARTITION BY " + t.PartitionInfo.Key
	}
	return out + reloptions(t) + ";", nil
}

func reloptions(t *snapshot.Table) string {
	if len(t.Reloptions) == 0 {
		return ""
	}
	return " WITH (" + strings.Join(t.Reloptions, ", ") + ")"
}

func column(c snapshot.Column, storedForVirtual bool) (string, error) {
	if c.TypeName == "" {
		return "", fmt.Errorf("column %s has no type in the snapshot", c.Name)
	}
	out := quoteIdent(c.Name) + " " + c.TypeName

	switch {
	case c.Generated != nil && c.GenerationExpr != nil:
		kind := strings.ToUpper(*c.Generated)
		if storedForVirtual {
			kind = "STORED"
		}
		out += fmt.Sprintf(" GENERATED ALWAYS AS (%s) %s", *c.GenerationExpr, kind)
	case c.Identity != nil:
		if *c.Identity == "always" {
			out += " GENERATED ALWAYS AS IDENTITY"
		} else {
			out += " GENERATED BY DEFAULT AS IDENTITY"
		}
	case c.Default != nil:
		out += " DEFAULT " + *c.Default
	}
	if !c.Nullable {
		out += " NOT NULL"
	}
	return out, nil
}

// The snapshot records no sequences, so a nextval default would reference
// nothing. The name is in the default itself; its start and increment are not.
func sequences(t *snapshot.Table) []string {
	var out []string
	for _, c := range t.Columns {
		if name := nextvalSequence(c); name != "" {
			out = append(out, fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s;", name))
		}
	}
	return out
}

func ownedSequences(t *snapshot.Table) []string {
	var out []string
	for _, c := range t.Columns {
		if name := nextvalSequence(c); name != "" {
			out = append(out, fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s.%s;",
				name, qual(t.Schema, t.Name), quoteIdent(c.Name)))
		}
	}
	return out
}

// nextval('public.t_id_seq'::regclass) -> public.t_id_seq
func nextvalSequence(c snapshot.Column) string {
	if c.Default == nil {
		return ""
	}
	i := strings.Index(*c.Default, "nextval('")
	if i < 0 {
		return ""
	}
	rest := (*c.Default)[i+len("nextval('"):]
	j := strings.Index(rest, "'")
	if j <= 0 {
		return ""
	}
	return rest[:j]
}

func policy(t *snapshot.Table, p snapshot.RlsPolicy) string {
	out := fmt.Sprintf("CREATE POLICY %s ON %s", quoteIdent(p.Name), qual(t.Schema, t.Name))
	if !p.Permissive {
		out += " AS RESTRICTIVE"
	}
	if p.Command != "" && p.Command != "ALL" {
		out += " FOR " + p.Command
	}
	if len(p.Roles) > 0 {
		roles := make([]string, len(p.Roles))
		for i, r := range p.Roles {
			roles[i] = quoteIdent(r)
		}
		out += " TO " + strings.Join(roles, ", ")
	}
	if p.UsingExpr != nil {
		out += fmt.Sprintf(" USING (%s)", *p.UsingExpr)
	}
	if p.WithCheckExpr != nil {
		out += fmt.Sprintf(" WITH CHECK (%s)", *p.WithCheckExpr)
	}
	return out + ";"
}

func comments(t *snapshot.Table) []string {
	name := qual(t.Schema, t.Name)
	var out []string
	if t.Comment != nil {
		out = append(out, fmt.Sprintf("COMMENT ON TABLE %s IS %s;", name, quoteLiteral(*t.Comment)))
	}
	for _, c := range t.Columns {
		if c.Comment != nil {
			out = append(out, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s;",
				name, quoteIdent(c.Name), quoteLiteral(*c.Comment)))
		}
	}
	for _, c := range t.Constraints {
		if c.Comment != nil {
			out = append(out, fmt.Sprintf("COMMENT ON CONSTRAINT %s ON %s IS %s;",
				quoteIdent(c.Name), name, quoteLiteral(*c.Comment)))
		}
	}
	return out
}

// The parent is recorded on the parent, not the child, so it is found by
// looking for a table that claims this one.
func partitionOf(snap *snapshot.SchemaSnapshot, t *snapshot.Table) (parent, bound string, ok bool) {
	if snap == nil {
		return "", "", false
	}
	for i := range snap.Tables {
		p := &snap.Tables[i]
		if p.PartitionInfo == nil {
			continue
		}
		for _, c := range p.PartitionInfo.Children {
			if c.Schema == t.Schema && c.Name == t.Name {
				return qual(p.Schema, p.Name), c.Bound, true
			}
		}
	}
	return "", "", false
}

func isPartition(snap *snapshot.SchemaSnapshot, t *snapshot.Table) bool {
	_, _, ok := partitionOf(snap, t)
	return ok
}

// Postgres names a constraint's index after the constraint, and only these
// kinds have one.
func backingIndexes(t *snapshot.Table) map[string]bool {
	out := map[string]bool{}
	for _, c := range t.Constraints {
		if c.BackingIndex != nil {
			out[*c.BackingIndex] = true
		}
		switch c.Kind {
		case snapshot.ConstraintPrimaryKey, snapshot.ConstraintUnique, snapshot.ConstraintExclusion:
			out[c.Name] = true
		}
	}
	return out
}

func hasVirtual(t *snapshot.Table) bool {
	for _, c := range t.Columns {
		if c.Generated != nil && !strings.EqualFold(*c.Generated, "stored") {
			return true
		}
	}
	return false
}

// What would stop this running, plus the two things never captured. Anything
// referenced by name that the snapshot does not carry belongs here; a list of
// everything Postgres can express would be noise.
func omitted(snap *snapshot.SchemaSnapshot, t *snapshot.Table) []string {
	out := append([]string{}, alwaysOmitted...)
	add := func(s string) { out = append(out, s) }

	if isPartition(snap, t) {
		add("the parent table, and any constraint or index this partition adds of its own")
	} else if t.PartitionInfo != nil && len(t.PartitionInfo.Children) > 0 {
		add(fmt.Sprintf("%d partitions of this table, each its own CREATE TABLE", len(t.PartitionInfo.Children)))
	}
	if refs := referencedTables(t); len(refs) > 0 {
		add("the tables these foreign keys point at: " + strings.Join(refs, ", "))
	}
	if types := unknownTypes(snap, t); len(types) > 0 {
		add("CREATE TYPE for " + strings.Join(types, ", "))
	}
	for _, c := range t.Columns {
		if nextvalSequence(c) != "" {
			add("the start and increment of the sequences created above")
			break
		}
	}
	if len(t.Triggers) > 0 {
		add("the functions these triggers call")
	}
	if hasExpressions(t) {
		add("functions and operator classes used by defaults, generated columns, indexes and policies")
	}
	if snap != nil && len(snap.Extensions) > 0 {
		add("CREATE EXTENSION for anything these types or operators come from")
	}
	return out
}

func referencedTables(t *snapshot.Table) []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range t.Constraints {
		if c.Kind != snapshot.ConstraintForeignKey || c.FKTable == nil || seen[*c.FKTable] {
			continue
		}
		seen[*c.FKTable] = true
		out = append(out, *c.FKTable)
	}
	sort.Strings(out)
	return out
}

// A column typed as something the snapshot names but does not define needs the
// type created first.
func unknownTypes(snap *snapshot.SchemaSnapshot, t *snapshot.Table) []string {
	if snap == nil {
		return nil
	}
	known := map[string]bool{}
	for _, e := range snap.Enums {
		known[e.Schema+"."+e.Name] = true
	}
	for _, d := range snap.Domains {
		known[d.Schema+"."+d.Name] = true
	}
	for _, c := range snap.Composites {
		known[c.Schema+"."+c.Name] = true
	}

	seen := map[string]bool{}
	var out []string
	for _, c := range t.Columns {
		base := strings.TrimSuffix(c.TypeName, "[]")
		qualified := base
		if !strings.Contains(base, ".") {
			qualified = t.Schema + "." + base
		}
		if known[qualified] && !seen[base] {
			seen[base] = true
			out = append(out, base)
		}
	}
	sort.Strings(out)
	return out
}

func hasExpressions(t *snapshot.Table) bool {
	for _, c := range t.Columns {
		if c.GenerationExpr != nil {
			return true
		}
		if c.Default != nil && strings.Contains(*c.Default, "(") && nextvalSequence(c) == "" {
			return true
		}
	}
	for _, idx := range t.Indexes {
		if idx.HasExpressions || idx.Predicate != nil {
			return true
		}
	}
	return len(t.Policies) > 0
}

func major(snap *snapshot.SchemaSnapshot) int {
	if snap == nil {
		return 0
	}
	// "18.3.0" and "PostgreSQL 17.2 on x86_64" are both shapes the capture stores
	v := snap.PgVersion
	start := strings.IndexFunc(v, func(r rune) bool { return r >= '0' && r <= '9' })
	if start < 0 {
		return 0
	}
	end := start
	for end < len(v) && v[end] >= '0' && v[end] <= '9' {
		end++
	}
	n, err := strconv.Atoi(v[start:end])
	if err != nil {
		return 0
	}
	return n
}

func qual(schemaName, name string) string {
	return quoteIdent(schemaName) + "." + quoteIdent(name)
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func quoteIdent(s string) string {
	if !needsQuote(s) {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

var keywordCache sync.Map

func needsQuote(s string) bool {
	if s == "" {
		return true
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c == '_':
		case (c >= '0' && c <= '9') || c == '$':
			if i == 0 {
				return true
			}
		default:
			return true
		}
	}
	return isKeyword(s)
}

// Asks the scanner rather than carrying a keyword list: a column named "order"
// is legal in Postgres and unparseable unquoted. Unreserved keywords are fine
// as names, which is the distinction pg_dump's fmtId draws too.
func isKeyword(s string) bool {
	if v, ok := keywordCache.Load(s); ok {
		return v.(bool)
	}
	kw := false
	if r, err := pg_query.Scan(s); err == nil {
		for _, tok := range r.GetTokens() {
			switch tok.GetKeywordKind() {
			case pg_query.KeywordKind_RESERVED_KEYWORD,
				pg_query.KeywordKind_COL_NAME_KEYWORD,
				pg_query.KeywordKind_TYPE_FUNC_NAME_KEYWORD:
				kw = true
			}
		}
	}
	keywordCache.Store(s, kw)
	return kw
}
