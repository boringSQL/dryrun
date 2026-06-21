package audit

import (
	"fmt"
	"strings"

	"github.com/boringsql/dryrun/pkg/bloat"
	"github.com/boringsql/dryrun/pkg/lint"
	"github.com/boringsql/dryrun/pkg/snapshot"
	"github.com/boringsql/dryrun/pkg/vacuum"
)

func runAllRules(a *snapshot.AnnotatedSchema, config *Config) []lint.Finding {
	snap := a.Schema
	var findings []lint.Finding
	disabled := make(map[string]bool)
	for _, r := range config.DisabledRules {
		disabled[r] = true
	}

	type rule struct {
		id string
		fn func(*snapshot.SchemaSnapshot, *Config) []lint.Finding
	}
	rules := []rule{
		{"indexes/duplicate", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkDuplicateIndexes(s) }},
		{"indexes/redundant", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkRedundantIndexes(s) }},
		{"indexes/too_many", func(s *snapshot.SchemaSnapshot, c *Config) []lint.Finding { return checkTooManyIndexes(s, c) }},
		{"indexes/wide_columns", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkWideColumnIndexes(s) }},
		{"indexes/bloated", func(_ *snapshot.SchemaSnapshot, c *Config) []lint.Finding { return checkBloatedIndexes(a, c) }},
		{"tables/bloated", func(_ *snapshot.SchemaSnapshot, c *Config) []lint.Finding { return checkBloatedTables(a, c) }},
		{"fk/type_mismatch", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkFKTypeMismatch(s) }},
		{"fk/circular", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkCircularFKs(s) }},
		{"fk/orphan", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkOrphanTables(s) }},
		{"pk/non_sequential", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkPKNonSequential(s) }},
		{"naming/bool_prefix", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkBoolPrefix(s) }},
		{"naming/reserved", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkReservedWords(s) }},
		{"naming/id_mismatch", func(s *snapshot.SchemaSnapshot, _ *Config) []lint.Finding { return checkIDMismatch(s) }},
		{"docs/no_comment", func(s *snapshot.SchemaSnapshot, c *Config) []lint.Finding { return checkNoComment(s, c) }},
	}

	for _, r := range rules {
		if !disabled[r.id] {
			findings = append(findings, r.fn(snap, config)...)
		}
	}

	// vacuum spans several vacuum/* ids; gate each
	for _, f := range vacuum.Findings(a) {
		if !disabled[f.Rule] {
			findings = append(findings, f)
		}
	}
	return findings
}

var wideTypes = []string{"text", "varchar", "bytea", "jsonb", "json", "xml"}

func checkDuplicateIndexes(snap *snapshot.SchemaSnapshot) []lint.Finding {
	var findings []lint.Finding
	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		var nonPrimary []snapshot.Index
		for _, idx := range t.Indexes {
			if !idx.IsPrimary {
				nonPrimary = append(nonPrimary, idx)
			}
		}
		for i, a := range nonPrimary {
			for _, b := range nonPrimary[i+1:] {
				// invalid/not-ready (failed/in-flight CIC) isn't safe to drop
				if !a.IsValid || !b.IsValid || !a.IsReady || !b.IsReady {
					continue
				}
				if !sliceEqual(a.Columns, b.Columns) || a.IndexType != b.IndexType {
					continue
				}

				// both back constraints; can't just drop one (UNIQUE/PK on one side,
				// FK depending on other), so emit warning without DDL fix
				if a.BacksConstraint && b.BacksConstraint {
					findings = append(findings, lint.Finding{
						Rule: "indexes/duplicate", Severity: lint.SeverityWarning,
						Tables: []string{qualified},
						Message: fmt.Sprintf("Indexes '%s' and '%s' have identical columns [%s] but both back constraints",
							a.Name, b.Name, strings.Join(a.Columns, ", ")),
						Recommendation: "One index is redundant but a FK depends on it — drop the FK first, then the extra index, then re-create the FK so PG picks the remaining index",
					})
					continue
				}

				// keep the constraint-backing one
				toDrop, toKeep := b, a
				if a.BacksConstraint && !b.BacksConstraint {
					toDrop, toKeep = b, a
				} else if !a.BacksConstraint && b.BacksConstraint {
					toDrop, toKeep = a, b
				}
				suffix := " is sufficient"
				if toKeep.BacksConstraint {
					suffix = " backs a constraint"
				}
				findings = append(findings, lint.Finding{
					Rule: "indexes/duplicate", Severity: lint.SeverityError,
					Tables: []string{qualified},
					Message: fmt.Sprintf("Indexes '%s' and '%s' have identical columns: [%s]",
						toDrop.Name, toKeep.Name, strings.Join(a.Columns, ", ")),
					Recommendation: fmt.Sprintf("Drop '%s' — '%s'%s", toDrop.Name, toKeep.Name, suffix),
					DDLFix:         new(fmt.Sprintf("DROP INDEX %s;", toDrop.Name)),
				})
			}
		}
	}
	return findings
}

func checkRedundantIndexes(snap *snapshot.SchemaSnapshot) []lint.Finding {
	var findings []lint.Finding
	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		var btree []snapshot.Index
		for _, idx := range t.Indexes {
			if !idx.IsPrimary && idx.IndexType == "btree" && idx.Predicate == nil {
				btree = append(btree, idx)
			}
		}
		for _, a := range btree {
			// unique indexes carry constraint purpose beyond queries, skip
			if a.IsUnique {
				continue
			}
			for _, b := range btree {
				if a.Name == b.Name {
					continue
				}
				// non-unique redundant with unique covering same prefix
				if !a.IsUnique && b.IsUnique && sliceEqual(a.Columns, b.Columns) {
					findings = append(findings, lint.Finding{
						Rule: "indexes/redundant", Severity: lint.SeverityWarning,
						Tables:         []string{qualified},
						Message:        fmt.Sprintf("Non-unique '%s' is redundant with unique '%s' on same columns [%s]", a.Name, b.Name, strings.Join(a.Columns, ", ")),
						Recommendation: fmt.Sprintf("Drop non-unique index '%s' - unique index '%s' serves the same queries", a.Name, b.Name),
						DDLFix:         new(fmt.Sprintf("DROP INDEX %s;", a.Name)),
					})
					continue
				}
				if len(a.Columns) < len(b.Columns) && isPrefix(a.Columns, b.Columns) {
					findings = append(findings, lint.Finding{
						Rule: "indexes/redundant", Severity: lint.SeverityWarning,
						Tables: []string{qualified},
						Message: fmt.Sprintf("'%s' [%s] is a prefix of '%s' [%s]",
							a.Name, strings.Join(a.Columns, ", "), b.Name, strings.Join(b.Columns, ", ")),
						Recommendation: fmt.Sprintf("Index '%s' is redundant - the wider index '%s' covers same queries", a.Name, b.Name),
						DDLFix:         new(fmt.Sprintf("DROP INDEX %s;", a.Name)),
					})
				}
			}
		}
	}
	return findings
}

func checkTooManyIndexes(snap *snapshot.SchemaSnapshot, config *Config) []lint.Finding {
	var findings []lint.Finding
	for _, t := range snap.Tables {
		if len(t.Indexes) > config.MaxIndexesPerTable {
			findings = append(findings, lint.Finding{
				Rule: "indexes/too_many", Severity: lint.SeverityInfo,
				Tables:         []string{t.Schema + "." + t.Name},
				Message:        fmt.Sprintf("Table has %d indexes (threshold: %d) - write amplification risk", len(t.Indexes), config.MaxIndexesPerTable),
				Recommendation: "Review indexes for unused or redundant ones",
			})
		}
	}
	return findings
}

func checkWideColumnIndexes(snap *snapshot.SchemaSnapshot) []lint.Finding {
	var findings []lint.Finding
	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		colTypes := make(map[string]string)
		for _, c := range t.Columns {
			colTypes[c.Name] = c.TypeName
		}
		for _, idx := range t.Indexes {
			var wideCols []string
			for _, col := range idx.Columns {
				if ct, ok := colTypes[col]; ok {
					for _, w := range wideTypes {
						if strings.HasPrefix(ct, w) {
							wideCols = append(wideCols, col)
							break
						}
					}
				}
			}
			if len(wideCols) > 0 {
				findings = append(findings, lint.Finding{
					Rule: "indexes/wide_columns", Severity: lint.SeverityWarning,
					Tables:         []string{qualified},
					Message:        fmt.Sprintf("Index '%s' includes wide column(s): [%s] - bloated index pages", idx.Name, strings.Join(wideCols, ", ")),
					Recommendation: "Consider expression index, prefix index, or hash index instead",
				})
			}
		}
	}
	return findings
}

// stats-dependent: no findings without planner sizing (DDL-only snapshots)
func checkBloatedIndexes(a *snapshot.AnnotatedSchema, config *Config) []lint.Finding {
	if a == nil || a.Schema == nil {
		return nil
	}
	var findings []lint.Finding
	for i := range a.Schema.Tables {
		t := &a.Schema.Tables[i]
		qualified := t.Schema + "." + t.Name
		for _, idx := range t.Indexes {
			sz := a.IndexSizingFor(t.Qual(), idx.Name)
			if sz == nil {
				continue
			}
			est, ok := bloat.EstimateIndexBloat(*sz, idx.Columns, idx.IncludeColumns, *t, idx.IndexType)
			if !ok || est.BloatRatio <= config.BloatThreshold {
				continue
			}
			approx := ""
			if idx.Predicate != nil || idx.HasExpressions {
				approx = " (approximate for expression/partial index)"
			}
			findings = append(findings, lint.Finding{
				Rule: "indexes/bloated", Severity: lint.SeverityWarning,
				Tables: []string{qualified},
				Message: fmt.Sprintf("Index '%s' is ~%.1fx larger than expected (%d vs %d pages)%s",
					idx.Name, est.BloatRatio, est.ActualPages, est.ExpectedPages, approx),
				Recommendation: fmt.Sprintf("REINDEX INDEX CONCURRENTLY %s to reclaim bloat", idx.Name),
			})
		}
	}
	return findings
}

// reads the bloat estimate Annotate computed (it gathers measured column widths)
func checkBloatedTables(a *snapshot.AnnotatedSchema, config *Config) []lint.Finding {
	if a == nil || a.Schema == nil {
		return nil
	}
	var findings []lint.Finding
	for i := range a.Schema.Tables {
		t := &a.Schema.Tables[i]
		est := a.TableBloatFor(t.Qual())
		if est == nil || est.BloatRatio <= config.BloatThreshold {
			continue
		}
		findings = append(findings, lint.Finding{
			Rule: "tables/bloated", Severity: lint.SeverityWarning,
			Tables: []string{t.Schema + "." + t.Name},
			Message: fmt.Sprintf("Table is ~%.1fx larger than expected (%d vs %d pages)",
				est.BloatRatio, est.ActualPages, est.ExpectedPages),
			Recommendation: "VACUUM FULL or pg_repack to reclaim space; check for long-running transactions holding back vacuum",
		})
	}
	return findings
}

func checkFKTypeMismatch(snap *snapshot.SchemaSnapshot) []lint.Finding {
	var findings []lint.Finding
	tableMap := make(map[string]*snapshot.Table)
	for i := range snap.Tables {
		tableMap[snap.Tables[i].Schema+"."+snap.Tables[i].Name] = &snap.Tables[i]
	}

	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		colTypes := make(map[string]string)
		for _, c := range t.Columns {
			colTypes[c.Name] = c.TypeName
		}
		for _, con := range t.Constraints {
			if con.Kind != snapshot.ConstraintForeignKey || con.FKTable == nil {
				continue
			}
			refTable, ok := tableMap[*con.FKTable]
			if !ok {
				continue
			}
			refColTypes := make(map[string]string)
			for _, c := range refTable.Columns {
				refColTypes[c.Name] = c.TypeName
			}
			for i, fkCol := range con.Columns {
				if i >= len(con.FKColumns) {
					break
				}
				refCol := con.FKColumns[i]
				fkType := colTypes[fkCol]
				refType := refColTypes[refCol]
				if fkType != "" && refType != "" && normalizeType(fkType) != normalizeType(refType) {
					findings = append(findings, lint.Finding{
						Rule: "fk/type_mismatch", Severity: lint.SeverityError,
						Tables:         []string{qualified, *con.FKTable},
						Message:        fmt.Sprintf("FK column %s.%s (%s) references %s.%s (%s) - type mismatch", t.Name, fkCol, fkType, refTable.Name, refCol, refType),
						Recommendation: fmt.Sprintf("Alter %s.%s to match type '%s'", t.Name, fkCol, refType),
						DDLFix:         new(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;", qualified, fkCol, refType)),
					})
				}
			}
		}
	}
	return findings
}

func checkCircularFKs(snap *snapshot.SchemaSnapshot) []lint.Finding {
	edges := make(map[string][]string)
	nodes := make(map[string]bool)
	for _, t := range snap.Tables {
		q := t.Schema + "." + t.Name
		nodes[q] = true
		for _, con := range t.Constraints {
			if con.Kind == snapshot.ConstraintForeignKey && con.FKTable != nil {
				edges[q] = append(edges[q], *con.FKTable)
				nodes[*con.FKTable] = true
			}
		}
	}

	var findings []lint.Finding
	color := make(map[string]int) // 0=white, 1=gray, 2=black
	var path []string
	var cycles [][]string

	var dfs func(string)
	dfs = func(node string) {
		color[node] = 1
		path = append(path, node)
		for _, neighbor := range edges[node] {
			switch color[neighbor] {
			case 0:
				dfs(neighbor)
			case 1:
				for i, n := range path {
					if n == neighbor {
						cycle := make([]string, len(path[i:]))
						copy(cycle, path[i:])
						cycle = append(cycle, neighbor)
						cycles = append(cycles, cycle)
						break
					}
				}
			}
		}
		path = path[:len(path)-1]
		color[node] = 2
	}

	for node := range nodes {
		if color[node] == 0 {
			dfs(node)
		}
	}

	for _, cycle := range cycles {
		findings = append(findings, lint.Finding{
			Rule: "fk/circular", Severity: lint.SeverityWarning,
			Tables:         cycle,
			Message:        fmt.Sprintf("Circular FK dependency: %s", strings.Join(cycle, " → ")),
			Recommendation: "Circular FKs complicate migrations and cascade deletes - consider breaking the cycle",
		})
	}
	return findings
}

func checkOrphanTables(snap *snapshot.SchemaSnapshot) []lint.Finding {
	inDeg := make(map[string]int)
	outDeg := make(map[string]int)
	for _, t := range snap.Tables {
		q := t.Schema + "." + t.Name
		for _, con := range t.Constraints {
			if con.Kind == snapshot.ConstraintForeignKey && con.FKTable != nil {
				outDeg[q]++
				inDeg[*con.FKTable]++
			}
		}
	}

	var findings []lint.Finding
	for _, t := range snap.Tables {
		q := t.Schema + "." + t.Name
		if inDeg[q] == 0 && outDeg[q] == 0 {
			findings = append(findings, lint.Finding{
				Rule: "fk/orphan", Severity: lint.SeverityInfo,
				Tables:         []string{q},
				Message:        "Table has no FK relationships (no incoming, no outgoing) - data island",
				Recommendation: "Verify this table is intentionally standalone or add FK relationships",
			})
		}
	}
	return findings
}

func checkPKNonSequential(snap *snapshot.SchemaSnapshot) []lint.Finding {
	var findings []lint.Finding
	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		for _, con := range t.Constraints {
			if con.Kind != snapshot.ConstraintPrimaryKey {
				continue
			}
			for _, pkCol := range con.Columns {
				for _, col := range t.Columns {
					if col.Name == pkCol && strings.Contains(strings.ToLower(col.TypeName), "uuid") {
						findings = append(findings, lint.Finding{
							Rule: "pk/non_sequential", Severity: lint.SeverityInfo,
							Tables:         []string{qualified},
							Message:        fmt.Sprintf("PK column '%s' uses UUID type - causes btree page splits", pkCol),
							Recommendation: "Consider UUIDv7 (time-ordered) or bigint IDENTITY for better insert performance",
						})
					}
				}
			}
		}
	}
	return findings
}

var boolPrefixes = []string{"is_", "has_", "can_", "should_", "was_", "will_"}

func checkBoolPrefix(snap *snapshot.SchemaSnapshot) []lint.Finding {
	var findings []lint.Finding
	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		for _, col := range t.Columns {
			norm := strings.ToLower(col.TypeName)
			if norm != "boolean" && norm != "bool" {
				continue
			}
			hasPrefix := false
			for _, p := range boolPrefixes {
				if strings.HasPrefix(col.Name, p) {
					hasPrefix = true
					break
				}
			}
			if !hasPrefix {
				findings = append(findings, lint.Finding{
					Rule: "naming/bool_prefix", Severity: lint.SeverityInfo,
					Tables:         []string{qualified},
					Message:        fmt.Sprintf("Boolean column '%s' missing prefix (is_, has_, can_, ...)", col.Name),
					Recommendation: fmt.Sprintf("Rename to 'is_%s' or similar for clarity", col.Name),
					DDLFix:         new(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO is_%s;", qualified, col.Name, col.Name)),
				})
			}
		}
	}
	return findings
}

var reservedWords = map[string]bool{
	"all": true, "alter": true, "and": true, "any": true, "as": true, "between": true,
	"by": true, "case": true, "check": true, "column": true, "constraint": true,
	"create": true, "cross": true, "default": true, "delete": true, "distinct": true,
	"drop": true, "exists": true, "false": true, "for": true, "foreign": true,
	"from": true, "full": true, "group": true, "having": true, "in": true, "index": true,
	"inner": true, "insert": true, "into": true, "is": true, "join": true, "key": true,
	"left": true, "like": true, "limit": true, "not": true, "null": true, "offset": true,
	"on": true, "or": true, "order": true, "outer": true, "primary": true, "references": true,
	"right": true, "select": true, "set": true, "table": true, "then": true, "to": true,
	"true": true, "union": true, "unique": true, "update": true, "user": true,
	"using": true, "values": true, "when": true, "where": true, "with": true,
}

func checkReservedWords(snap *snapshot.SchemaSnapshot) []lint.Finding {
	var findings []lint.Finding
	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		if reservedWords[strings.ToLower(t.Name)] {
			findings = append(findings, lint.Finding{
				Rule: "naming/reserved", Severity: lint.SeverityError,
				Tables:         []string{qualified},
				Message:        fmt.Sprintf("Table name '%s' is a SQL reserved word - requires quoting everywhere", t.Name),
				Recommendation: fmt.Sprintf("Rename table '%s' to avoid quoting issues", t.Name),
			})
		}
		for _, col := range t.Columns {
			if reservedWords[strings.ToLower(col.Name)] {
				findings = append(findings, lint.Finding{
					Rule: "naming/reserved", Severity: lint.SeverityError,
					Tables:         []string{qualified},
					Message:        fmt.Sprintf("Column '%s' in table '%s' is a SQL reserved word", col.Name, t.Name),
					Recommendation: fmt.Sprintf("Rename column '%s' to avoid quoting hell", col.Name),
				})
			}
		}
	}
	return findings
}

func checkIDMismatch(snap *snapshot.SchemaSnapshot) []lint.Finding {
	type ref struct {
		colName string
		source  string
	}
	refNames := make(map[string][]ref)

	for _, t := range snap.Tables {
		qualified := t.Schema + "." + t.Name
		for _, con := range t.Constraints {
			if con.Kind != snapshot.ConstraintForeignKey || con.FKTable == nil || len(con.Columns) != 1 {
				continue
			}
			refNames[*con.FKTable] = append(refNames[*con.FKTable], ref{con.Columns[0], qualified})
		}
	}

	var findings []lint.Finding
	for target, refs := range refNames {
		names := make(map[string]bool)
		for _, r := range refs {
			names[r.colName] = true
		}
		if len(names) > 1 {
			var tables []string
			seen := make(map[string]bool)
			for _, r := range refs {
				if !seen[r.source] {
					seen[r.source] = true
					tables = append(tables, r.source)
				}
			}
			var nameList []string
			for n := range names {
				nameList = append(nameList, "'"+n+"'")
			}
			findings = append(findings, lint.Finding{
				Rule: "naming/id_mismatch", Severity: lint.SeverityWarning,
				Tables:         tables,
				Message:        fmt.Sprintf("Table '%s' referenced inconsistently: %s used as FK column names", target, strings.Join(nameList, ", ")),
				Recommendation: "Standardize FK column naming for consistency",
			})
		}
	}
	return findings
}

func checkNoComment(snap *snapshot.SchemaSnapshot, config *Config) []lint.Finding {
	var findings []lint.Finding
	for _, t := range snap.Tables {
		if len(t.Columns) < config.NoCommentMinColumns {
			continue
		}
		qualified := t.Schema + "." + t.Name
		if t.Comment == nil {
			findings = append(findings, lint.Finding{
				Rule: "docs/no_comment", Severity: lint.SeverityInfo,
				Tables:         []string{qualified},
				Message:        fmt.Sprintf("Table '%s' has %d columns but no table comment", t.Name, len(t.Columns)),
				Recommendation: fmt.Sprintf("Add comment: COMMENT ON TABLE %s IS '...';", qualified),
			})
		}
	}
	return findings
}

func normalizeType(t string) string {
	switch t {
	case "int4", "integer", "int":
		return "integer"
	case "int8", "bigint":
		return "bigint"
	case "int2", "smallint":
		return "smallint"
	case "float4", "real":
		return "real"
	case "float8", "double precision":
		return "double precision"
	case "bool", "boolean":
		return "boolean"
	case "timestamptz", "timestamp with time zone":
		return "timestamptz"
	case "timestamp", "timestamp without time zone":
		return "timestamp"
	default:
		return t
	}
}

func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func isPrefix(prefix, full []string) bool {
	if len(prefix) > len(full) {
		return false
	}
	for i := range prefix {
		if prefix[i] != full[i] {
			return false
		}
	}
	return true
}
