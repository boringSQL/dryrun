package query

import (
	"fmt"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/boringsql/dryrun/internal/schema"
)

var comparisonOps = []string{"=", "<>", "!=", "<", ">", "<=", ">="}

// No implicit int/float -> text cast: "text_col = 1" fails; a quoted literal is unknown-typed.
func validateComparisonTypes(sql string, parsed *ParsedQuery, snap *schema.SchemaSnapshot, errors *[]string) {
	tree, err := pg_query.Parse(rewriteNamedParams(sql))
	if err != nil {
		return
	}
	for _, stmt := range tree.Stmts {
		walkNode(stmt.Stmt, func(n *pg_query.Node) {
			e := n.GetAExpr()
			if e == nil || e.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP || len(e.GetName()) != 1 {
				return
			}
			op := e.GetName()[0].GetString_().GetSval()
			if !slices.Contains(comparisonOps, op) {
				return
			}
			for _, pair := range [][2]*pg_query.Node{{e.GetLexpr(), e.GetRexpr()}, {e.GetRexpr(), e.GetLexpr()}} {
				col := pair[0].GetColumnRef()
				if col == nil || !isNumberLiteral(pair[1]) {
					continue
				}
				if name, typ, ok := resolveColumnType(col, parsed, snap); ok && isTextFamily(typ) {
					*errors = append(*errors, fmt.Sprintf(
						"operator does not exist: column '%s' is %s but is compared with the number %s; quote the literal or cast the column",
						name, typ, deparseExpr(pair[1])))
				}
			}
		})
	}
}

func isNumberLiteral(n *pg_query.Node) bool {
	c := n.GetAConst()
	return c != nil && !c.GetIsnull() && (c.GetIval() != nil || c.GetFval() != nil)
}

func isTextFamily(typ string) bool {
	t := strings.ToLower(typ)
	return t == "text" || strings.HasPrefix(t, "character") || strings.HasPrefix(t, "varchar")
}

// Resolves only unambiguous refs: qualified via alias, unqualified when one table has the column.
func resolveColumnType(col *pg_query.ColumnRef, parsed *ParsedQuery, snap *schema.SchemaSnapshot) (name, typ string, ok bool) {
	var parts []string
	for _, f := range col.GetFields() {
		s := f.GetString_()
		if s == nil {
			return "", "", false
		}
		parts = append(parts, s.GetSval())
	}
	if len(parts) == 0 || len(parts) > 2 {
		return "", "", false
	}
	name = parts[len(parts)-1]

	var found []string
	for _, ref := range parsed.Info.Tables {
		if ref.Schema == nil && slices.Contains(parsed.Info.cteNames, ref.Name) {
			continue
		}
		if len(parts) == 2 && !((ref.Alias != nil && *ref.Alias == parts[0]) || ref.Name == parts[0]) {
			continue
		}
		schemaName := "public"
		if ref.Schema != nil {
			schemaName = *ref.Schema
		}
		for _, t := range snap.Tables {
			if t.Name != ref.Name || t.Schema != schemaName {
				continue
			}
			for _, c := range t.Columns {
				if c.Name == name {
					found = append(found, c.TypeName)
				}
			}
		}
	}
	if len(found) != 1 {
		return "", "", false
	}
	return name, found[0], true
}
