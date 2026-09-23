package query

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type (
	ParsedQuery struct {
		SQL  string    `json:"sql"`
		Info QueryInfo `json:"info"`
	}

	QueryInfo struct {
		Tables             []ReferencedTable   `json:"tables"`
		FilterColumns      []FilterColumn      `json:"filter_columns"`
		ReferencedColumns  []FilterColumn      `json:"referenced_columns"`
		FuncWrappedColumns []FuncWrappedColumn `json:"func_wrapped_columns,omitempty"`
		UpdateTargets      []string            `json:"update_targets,omitempty"`
		ProceduralBodies   []ProceduralBody    `json:"procedural_bodies,omitempty"`
		HasSelectStar      bool                `json:"has_select_star"`
		HasLimit           bool                `json:"has_limit"`
		HasWhere           bool                `json:"has_where"`
		HasJoin            bool                `json:"has_join"`
		StatementType      string              `json:"statement_type"`

		// a CTE shadows any real table of the same name, so it must not be
		// reported missing -- or worse, "corrected" to one
		cteNames []string

		// per top-level SELECT: its outermost FROM identifiers (alias or
		// relation name) and the column pairs linking them; used to tell a
		// real Cartesian product (disconnected FROM) from an implicit join
		cartesianScopes []cartesianScope
	}

	cartesianScope struct {
		tables []string
		pairs  []colPair
	}

	colPair struct {
		a, b string
	}

	// accumulates predicate columns and the column pairs linking tables
	predicateCollector struct {
		columns []FilterColumn
		pairs   []colPair
	}

	// body content is opaque to pg_query, so it escapes static validation.
	ProceduralBody struct {
		Kind     string `json:"kind"`     // "DO", "CREATE FUNCTION", "CREATE PROCEDURE"
		Language string `json:"language"` // e.g. "plpgsql"
	}

	ReferencedTable struct {
		Schema  *string `json:"schema,omitempty"`
		Name    string  `json:"name"`
		Alias   *string `json:"alias,omitempty"`
		Context string  `json:"context"`

		// byte offsets of every occurrence, for rewriting the name in place
		locs []int32
	}

	FilterColumn struct {
		Table  *string `json:"table,omitempty"`
		Column string  `json:"column"`

		loc int32
	}

	FuncWrappedColumn struct {
		Table    *string `json:"table,omitempty"`
		Column   string  `json:"column"`
		FuncName string  `json:"func_name"`
	}
)

func ParseSQL(sql string) (*ParsedQuery, error) {
	result, err := pg_query.Parse(rewriteNamedParams(sql))
	if err != nil {
		return nil, fmt.Errorf("SQL parse error: %w", err)
	}

	var (
		tables             []ReferencedTable
		filterColumns      []FilterColumn
		referencedColumns  []FilterColumn
		funcWrappedColumns []FuncWrappedColumn
		proceduralBodies   []ProceduralBody
		updateTargets      []string
		cteNames           []string
		cartesianScopes    []cartesianScope
		hasSelectStar      bool
		hasJoin            bool
		hasWhere           bool
		hasLimit           bool
		stmtType           string
	)

	seenTables := make(map[string]int)
	existsSelects := make(map[*pg_query.SelectStmt]bool)
	sublinkSelects := make(map[*pg_query.SelectStmt]bool)

	for _, stmt := range result.Stmts {
		node := stmt.Stmt
		if node == nil {
			continue
		}

		switch n := node.Node.(type) {
		case *pg_query.Node_SelectStmt:
			if stmtType == "" {
				stmtType = "SELECT"
			}
			if n.SelectStmt.WhereClause != nil {
				hasWhere = true
			}
		case *pg_query.Node_InsertStmt:
			if stmtType == "" {
				stmtType = "INSERT"
			}
		case *pg_query.Node_UpdateStmt:
			if stmtType == "" {
				stmtType = "UPDATE"
			}
			if n.UpdateStmt.WhereClause != nil {
				hasWhere = true
			}
			for _, tl := range n.UpdateStmt.TargetList {
				if rt, ok := tl.Node.(*pg_query.Node_ResTarget); ok && rt.ResTarget != nil {
					if rt.ResTarget.Name != "" {
						updateTargets = append(updateTargets, rt.ResTarget.Name)
					}
				}
			}
		case *pg_query.Node_DeleteStmt:
			if stmtType == "" {
				stmtType = "DELETE"
			}
			if n.DeleteStmt.WhereClause != nil {
				hasWhere = true
			}
		case *pg_query.Node_MergeStmt:
			if stmtType == "" {
				stmtType = "MERGE"
			}
			if n.MergeStmt.JoinCondition != nil {
				hasWhere = true
			}
		case *pg_query.Node_DoStmt:
			if stmtType == "" {
				stmtType = "DO"
			}
			proceduralBodies = append(proceduralBodies, ProceduralBody{
				Kind:     "DO",
				Language: doStmtLanguage(n.DoStmt),
			})
		case *pg_query.Node_CreateFunctionStmt:
			kind := "CREATE FUNCTION"
			if n.CreateFunctionStmt.IsProcedure {
				kind = "CREATE PROCEDURE"
			}
			if stmtType == "" {
				stmtType = kind
			}
			proceduralBodies = append(proceduralBodies, ProceduralBody{
				Kind:     kind,
				Language: createFunctionLanguage(n.CreateFunctionStmt),
			})
		}

		// WHERE for func-wrapped columns (date_trunc(col), col::date, ...)
		var whereClause *pg_query.Node
		switch n := node.Node.(type) {
		case *pg_query.Node_SelectStmt:
			whereClause = n.SelectStmt.WhereClause
		case *pg_query.Node_UpdateStmt:
			whereClause = n.UpdateStmt.WhereClause
		case *pg_query.Node_DeleteStmt:
			whereClause = n.DeleteStmt.WhereClause
		}
		if whereClause != nil {
			collectFuncWrappedColumns(whereClause, &funcWrappedColumns)
		}
		collectCteFuncWrapped(node, &funcWrappedColumns)

		// filter columns come from predicates only (WHERE/JOIN/ON CONFLICT);
		// SELECT list, GROUP BY, HAVING and ORDER BY are not filters
		pred := &predicateCollector{}
		pred.collect(node)
		filterColumns = append(filterColumns, pred.columns...)
		if sel, ok := node.Node.(*pg_query.Node_SelectStmt); ok {
			cartesianScopes = append(cartesianScopes, cartesianScope{
				tables: topLevelFromTables(sel.SelectStmt),
				pairs:  pred.pairs,
			})
		}

		// DML only: walking CREATE TABLE would report its own relation as a read
		if !isDMLRoot(node) {
			continue
		}
		walkNode(node, func(child *pg_query.Node) {
			if child == nil {
				return
			}
			for _, cte := range withClauseOf(child).GetCtes() {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok && c.CommonTableExpr != nil {
					cteNames = append(cteNames, c.CommonTableExpr.Ctename)
				}
			}
			switch cn := child.Node.(type) {
			case *pg_query.Node_SubLink:
				if cn.SubLink == nil {
					return
				}
				if cn.SubLink.SubLinkType == pg_query.SubLinkType_EXISTS_SUBLINK {
					markExistsSelect(cn.SubLink.Subselect, existsSelects)
				}
				walkNode(cn.SubLink.Subselect, func(n *pg_query.Node) {
					if sel, ok := n.GetNode().(*pg_query.Node_SelectStmt); ok && sel.SelectStmt != nil {
						sublinkSelects[sel.SelectStmt] = true
					}
				})
			case *pg_query.Node_SelectStmt:
				// EXISTS ignores its target list, so SELECT * there costs nothing
				star := &hasSelectStar
				if existsSelects[cn.SelectStmt] {
					star = new(bool)
				}
				// a LIMIT inside IN/EXISTS/scalar subqueries does not bound the rows returned
				limit := &hasLimit
				if sublinkSelects[cn.SelectStmt] {
					limit = new(bool)
				}
				noteSelectFacts(cn.SelectStmt, limit, star)
			case *pg_query.Node_RangeVar:
				rv := cn.RangeVar
				if rv == nil {
					return
				}
				ctx := "select"
				if stmtType == "INSERT" || stmtType == "UPDATE" || stmtType == "DELETE" || stmtType == "MERGE" {
					ctx = "dml"
				}
				// schema is part of the key: a.orders and b.orders are two tables
				key := rv.Schemaname + "." + rv.Relname + ":" + ctx
				if i, ok := seenTables[key]; ok {
					tables[i].locs = append(tables[i].locs, rv.Location)
					return
				}
				seenTables[key] = len(tables)
				t := ReferencedTable{
					Name:    rv.Relname,
					Context: ctx,
					locs:    []int32{rv.Location},
				}
				if rv.Schemaname != "" {
					t.Schema = strp(rv.Schemaname)
				}
				if rv.Alias != nil && rv.Alias.Aliasname != "" {
					t.Alias = strp(rv.Alias.Aliasname)
				}
				tables = append(tables, t)
			case *pg_query.Node_JoinExpr:
				_ = cn
				hasJoin = true
			case *pg_query.Node_ColumnRef:
				// every column reference, for validation
				cr := cn.ColumnRef
				if cr == nil || len(cr.Fields) == 0 {
					return
				}
				fc := extractFilterColumn(cr)
				if fc != nil {
					fc.loc = cr.Location
					referencedColumns = append(referencedColumns, *fc)
				}
			}
		})
	}

	return &ParsedQuery{
		SQL: sql,
		Info: QueryInfo{
			Tables:             tables,
			FilterColumns:      filterColumns,
			ReferencedColumns:  referencedColumns,
			HasSelectStar:      hasSelectStar,
			HasLimit:           hasLimit,
			HasWhere:           hasWhere,
			HasJoin:            hasJoin,
			FuncWrappedColumns: funcWrappedColumns,
			UpdateTargets:      updateTargets,
			cteNames:           cteNames,
			cartesianScopes:    cartesianScopes,
			ProceduralBodies:   proceduralBodies,
			StatementType:      stmtType,
		},
	}, nil
}

func isDMLRoot(node *pg_query.Node) bool {
	switch node.GetNode().(type) {
	case *pg_query.Node_SelectStmt, *pg_query.Node_InsertStmt, *pg_query.Node_UpdateStmt,
		*pg_query.Node_DeleteStmt, *pg_query.Node_MergeStmt:
		return true
	}
	return false
}

func withClauseOf(node *pg_query.Node) *pg_query.WithClause {
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		return n.SelectStmt.GetWithClause()
	case *pg_query.Node_InsertStmt:
		return n.InsertStmt.GetWithClause()
	case *pg_query.Node_UpdateStmt:
		return n.UpdateStmt.GetWithClause()
	case *pg_query.Node_DeleteStmt:
		return n.DeleteStmt.GetWithClause()
	case *pg_query.Node_MergeStmt:
		return n.MergeStmt.GetWithClause()
	}
	return nil
}

// the walk visits a SubLink before its subselect, so marks land in time
func markExistsSelect(node *pg_query.Node, marks map[*pg_query.SelectStmt]bool) {
	sel, ok := node.GetNode().(*pg_query.Node_SelectStmt)
	if !ok || sel.SelectStmt == nil {
		return
	}
	var mark func(*pg_query.SelectStmt)
	mark = func(s *pg_query.SelectStmt) {
		if s == nil {
			return
		}
		marks[s] = true
		mark(s.Larg)
		mark(s.Rarg)
	}
	mark(sel.SelectStmt)
}

// runs per SelectStmt so FROM subqueries/CTEs/set-ops count too; count(*)
// is a FuncCall, not a ResTarget column, so it stays excluded
func noteSelectFacts(s *pg_query.SelectStmt, hasLimit, hasSelectStar *bool) {
	if s == nil {
		return
	}
	if s.LimitCount != nil || s.LimitOffset != nil {
		*hasLimit = true
	}
	for _, target := range s.TargetList {
		rt, ok := target.Node.(*pg_query.Node_ResTarget)
		if !ok || rt.ResTarget == nil || rt.ResTarget.Val == nil {
			continue
		}
		cr, ok := rt.ResTarget.Val.Node.(*pg_query.Node_ColumnRef)
		if !ok || cr.ColumnRef == nil {
			continue
		}
		for _, f := range cr.ColumnRef.Fields {
			if _, ok := f.Node.(*pg_query.Node_AStar); ok {
				*hasSelectStar = true
			}
		}
	}
}

func extractFilterColumn(cr *pg_query.ColumnRef) *FilterColumn {
	fields := cr.Fields
	switch len(fields) {
	case 1:
		if s, ok := fields[0].Node.(*pg_query.Node_String_); ok {
			return &FilterColumn{Column: s.String_.Sval}
		}
	case 2:
		var table, col string
		if s, ok := fields[0].Node.(*pg_query.Node_String_); ok {
			table = s.String_.Sval
		}
		if s, ok := fields[1].Node.(*pg_query.Node_String_); ok {
			col = s.String_.Sval
		}
		if col != "" {
			fc := FilterColumn{Column: col}
			if table != "" {
				fc.Table = strp(table)
			}
			return &fc
		}
	}
	return nil
}

// Predicate-scoped column collection: descends into WHERE/JOIN predicates and
// nested predicates, never into SELECT lists or HAVING (post-aggregation, so an
// index on a HAVING column cannot serve it). Also records equality pairs between
// qualified columns, for Cartesian-join connectivity.
func (p *predicateCollector) collect(node *pg_query.Node) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		s := n.SelectStmt
		if s == nil {
			return
		}
		p.collectCtes(node)
		for _, f := range s.FromClause {
			p.collect(f)
		}
		p.collect(s.WhereClause)
		if s.Larg != nil {
			p.collect(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s.Larg}})
		}
		if s.Rarg != nil {
			p.collect(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s.Rarg}})
		}
	case *pg_query.Node_InsertStmt:
		if n.InsertStmt == nil {
			return
		}
		p.collectCtes(node)
		p.collect(n.InsertStmt.SelectStmt)
		if n.InsertStmt.OnConflictClause != nil {
			p.collect(&pg_query.Node{Node: &pg_query.Node_OnConflictClause{OnConflictClause: n.InsertStmt.OnConflictClause}})
		}
	case *pg_query.Node_OnConflictClause:
		oc := n.OnConflictClause
		if oc == nil {
			return
		}
		if oc.Infer != nil {
			p.collect(oc.Infer.WhereClause)
		}
		p.collect(oc.WhereClause)
	case *pg_query.Node_MergeStmt:
		s := n.MergeStmt
		if s == nil {
			return
		}
		p.collectCtes(node)
		p.collect(s.SourceRelation)
		p.collect(s.JoinCondition)
		for _, w := range s.MergeWhenClauses {
			p.collect(w)
		}
	case *pg_query.Node_MergeWhenClause:
		if n.MergeWhenClause != nil {
			p.collect(n.MergeWhenClause.Condition)
		}
	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt == nil {
			return
		}
		p.collectCtes(node)
		for _, f := range n.UpdateStmt.FromClause {
			p.collect(f)
		}
		p.collect(n.UpdateStmt.WhereClause)
	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt == nil {
			return
		}
		p.collectCtes(node)
		for _, u := range n.DeleteStmt.UsingClause {
			p.collect(u)
		}
		p.collect(n.DeleteStmt.WhereClause)
	case *pg_query.Node_JoinExpr:
		j := n.JoinExpr
		if j == nil {
			return
		}
		p.collect(j.Quals)
		p.collect(j.Larg)
		p.collect(j.Rarg)
		// USING/NATURAL carry no quals but still connect the two sides
		if j.IsNatural || len(j.UsingClause) > 0 {
			var left, right []string
			appendFromTable(j.Larg, &left)
			appendFromTable(j.Rarg, &right)
			if len(left) > 0 && len(right) > 0 {
				p.pairs = append(p.pairs, colPair{left[0], right[0]})
			}
		}
	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			p.collect(n.RangeSubselect.Subquery)
		}
	case *pg_query.Node_AExpr:
		e := n.AExpr
		if e == nil {
			return
		}
		p.pairs = append(p.pairs, operandPairs(e)...)
		p.collect(e.Lexpr)
		p.collect(e.Rexpr)
	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, a := range n.BoolExpr.Args {
				p.collect(a)
			}
		}
	case *pg_query.Node_FuncCall:
		if n.FuncCall != nil {
			for _, a := range n.FuncCall.Args {
				p.collect(a)
			}
		}
	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			p.collect(n.TypeCast.Arg)
		}
	case *pg_query.Node_SubLink:
		if n.SubLink == nil {
			return
		}
		p.collect(n.SubLink.Testexpr)
		p.collect(n.SubLink.Subselect)
	case *pg_query.Node_List:
		if n.List != nil {
			for _, i := range n.List.Items {
				p.collect(i)
			}
		}
	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			p.collect(n.CaseExpr.Arg)
			for _, a := range n.CaseExpr.Args {
				p.collect(a)
			}
			p.collect(n.CaseExpr.Defresult)
		}
	case *pg_query.Node_CaseWhen:
		if n.CaseWhen != nil {
			p.collect(n.CaseWhen.Expr)
			p.collect(n.CaseWhen.Result)
		}
	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			for _, a := range n.CoalesceExpr.Args {
				p.collect(a)
			}
		}
	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			p.collect(n.NullTest.Arg)
		}
	case *pg_query.Node_BooleanTest:
		if n.BooleanTest != nil {
			p.collect(n.BooleanTest.Arg)
		}
	case *pg_query.Node_RowExpr:
		if n.RowExpr != nil {
			for _, a := range n.RowExpr.Args {
				p.collect(a)
			}
		}
	case *pg_query.Node_MinMaxExpr:
		if n.MinMaxExpr != nil {
			for _, a := range n.MinMaxExpr.Args {
				p.collect(a)
			}
		}
	case *pg_query.Node_CollateClause:
		if n.CollateClause != nil {
			p.collect(n.CollateClause.Arg)
		}
	case *pg_query.Node_AIndirection:
		if n.AIndirection != nil {
			p.collect(n.AIndirection.Arg)
		}
	case *pg_query.Node_AArrayExpr:
		if n.AArrayExpr != nil {
			for _, e := range n.AArrayExpr.Elements {
				p.collect(e)
			}
		}
	case *pg_query.Node_ColumnRef:
		cr := n.ColumnRef
		if cr == nil || len(cr.Fields) == 0 {
			return
		}
		if fc := extractFilterColumn(cr); fc != nil {
			fc.loc = cr.Location
			p.columns = append(p.columns, *fc)
		}
	}
}

// CTE-body predicates are index candidates too
func (p *predicateCollector) collectCtes(node *pg_query.Node) {
	for _, cte := range withClauseOf(node).GetCtes() {
		if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok && c.CommonTableExpr != nil {
			p.collect(c.CommonTableExpr.Ctequery)
		}
	}
}

// operandPairs links every table qualifier on the left of a comparison to every
// one on the right. Any A_Expr kind qualifies (=, <, BETWEEN, = ANY, IN, LIKE,
// IS [NOT] DISTINCT FROM): each ranges over both tables. Casts, function calls
// and arithmetic around the columns still link.
func operandPairs(e *pg_query.A_Expr) []colPair {
	var lhs, rhs []string
	qualifiedTables(e.Lexpr, &lhs)
	qualifiedTables(e.Rexpr, &rhs)
	var out []colPair
	for _, a := range lhs {
		for _, b := range rhs {
			out = append(out, colPair{a, b})
		}
	}
	return out
}

// unqualified columns are skipped (cannot attribute to a relation)
func qualifiedTables(node *pg_query.Node, out *[]string) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_ColumnRef:
		cr := n.ColumnRef
		if cr == nil || len(cr.Fields) != 2 {
			return
		}
		if t := columnTable(cr); t != "" {
			*out = append(*out, t)
		}
	case *pg_query.Node_TypeCast:
		qualifiedTables(n.TypeCast.GetArg(), out)
	case *pg_query.Node_CollateClause:
		qualifiedTables(n.CollateClause.GetArg(), out)
	case *pg_query.Node_AIndirection:
		qualifiedTables(n.AIndirection.GetArg(), out)
	case *pg_query.Node_FuncCall:
		for _, a := range n.FuncCall.GetArgs() {
			qualifiedTables(a, out)
		}
	case *pg_query.Node_AExpr:
		qualifiedTables(n.AExpr.GetLexpr(), out)
		qualifiedTables(n.AExpr.GetRexpr(), out)
	case *pg_query.Node_List:
		// BETWEEN bounds and IN lists
		for _, i := range n.List.GetItems() {
			qualifiedTables(i, out)
		}
	case *pg_query.Node_CoalesceExpr:
		for _, a := range n.CoalesceExpr.GetArgs() {
			qualifiedTables(a, out)
		}
	}
}

func columnTable(cr *pg_query.ColumnRef) string {
	fc := extractFilterColumn(cr)
	if fc == nil || fc.Table == nil {
		return ""
	}
	return *fc.Table
}

// Identifiers (alias if present, else relation name) in the outermost FROM,
// stopping at subqueries and set-op branches.
func topLevelFromTables(s *pg_query.SelectStmt) []string {
	if s == nil {
		return nil
	}
	var out []string
	for _, item := range s.FromClause {
		appendFromTable(item, &out)
	}
	return out
}

func appendFromTable(node *pg_query.Node, out *[]string) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		rv := n.RangeVar
		if rv == nil {
			return
		}
		name := rv.Relname
		if rv.Alias != nil && rv.Alias.Aliasname != "" {
			name = rv.Alias.Aliasname
		}
		*out = append(*out, name)
	case *pg_query.Node_RangeSubselect:
		rs := n.RangeSubselect
		// a LATERAL subquery is correlated with the outer FROM by construction;
		// its link sits in its own WHERE, out of this scope's reach
		if rs != nil && !rs.Lateral && rs.Alias != nil && rs.Alias.Aliasname != "" {
			*out = append(*out, rs.Alias.Aliasname)
		}
	case *pg_query.Node_JoinExpr:
		if n.JoinExpr == nil {
			return
		}
		appendFromTable(n.JoinExpr.Larg, out)
		appendFromTable(n.JoinExpr.Rarg, out)
	}
}

func collectFuncWrappedColumns(node *pg_query.Node, out *[]FuncWrappedColumn) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_FuncCall:
		fc := n.FuncCall
		if fc == nil {
			return
		}
		funcName := extractFuncName(fc.Funcname)
		for _, arg := range fc.Args {
			if cr := asColumnRef(arg); cr != nil {
				if fwc := buildFuncWrapped(cr, funcName); fwc != nil {
					*out = append(*out, *fwc)
				}
			} else {
				collectFuncWrappedColumns(arg, out)
			}
		}
	case *pg_query.Node_TypeCast:
		tc := n.TypeCast
		if tc == nil {
			return
		}
		if cr := asColumnRef(tc.Arg); cr != nil {
			typeName := "::" + extractTypeName(tc.TypeName)
			if fwc := buildFuncWrapped(cr, typeName); fwc != nil {
				*out = append(*out, *fwc)
			}
		} else {
			collectFuncWrappedColumns(tc.Arg, out)
		}
	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, a := range n.BoolExpr.Args {
				collectFuncWrappedColumns(a, out)
			}
		}
	case *pg_query.Node_AExpr:
		if n.AExpr != nil {
			collectFuncWrappedColumns(n.AExpr.Lexpr, out)
			collectFuncWrappedColumns(n.AExpr.Rexpr, out)
		}
	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			collectFuncWrappedColumns(n.SubLink.Testexpr, out)
		}
	}
}

// date_trunc(col) inside a CTE body counts like one in the outer WHERE
func collectCteFuncWrapped(node *pg_query.Node, out *[]FuncWrappedColumn) {
	for _, cte := range withClauseOf(node).GetCtes() {
		c, ok := cte.Node.(*pg_query.Node_CommonTableExpr)
		if !ok || c.CommonTableExpr == nil || c.CommonTableExpr.Ctequery == nil {
			continue
		}
		body := c.CommonTableExpr.Ctequery
		var where *pg_query.Node
		switch n := body.Node.(type) {
		case *pg_query.Node_SelectStmt:
			if n.SelectStmt != nil {
				where = n.SelectStmt.WhereClause
			}
		case *pg_query.Node_UpdateStmt:
			if n.UpdateStmt != nil {
				where = n.UpdateStmt.WhereClause
			}
		case *pg_query.Node_DeleteStmt:
			if n.DeleteStmt != nil {
				where = n.DeleteStmt.WhereClause
			}
		}
		if where != nil {
			collectFuncWrappedColumns(where, out)
		}
		collectCteFuncWrapped(body, out)
	}
}

func asColumnRef(node *pg_query.Node) *pg_query.ColumnRef {
	if node == nil {
		return nil
	}
	if cr, ok := node.Node.(*pg_query.Node_ColumnRef); ok && cr.ColumnRef != nil {
		return cr.ColumnRef
	}
	return nil
}

func buildFuncWrapped(cr *pg_query.ColumnRef, funcName string) *FuncWrappedColumn {
	fc := extractFilterColumn(cr)
	if fc == nil {
		return nil
	}
	return &FuncWrappedColumn{
		Table:    fc.Table,
		Column:   fc.Column,
		FuncName: funcName,
	}
}

func extractFuncName(funcname []*pg_query.Node) string {
	if len(funcname) == 0 {
		return ""
	}
	last := funcname[len(funcname)-1]
	if s, ok := last.Node.(*pg_query.Node_String_); ok {
		return strings.ToLower(s.String_.Sval)
	}
	return ""
}

func extractTypeName(tn *pg_query.TypeName) string {
	if tn == nil || len(tn.Names) == 0 {
		return ""
	}
	last := tn.Names[len(tn.Names)-1]
	if s, ok := last.Node.(*pg_query.Node_String_); ok {
		return s.String_.Sval
	}
	return ""
}

// DO defaults to plpgsql when no LANGUAGE is given.
func doStmtLanguage(s *pg_query.DoStmt) string {
	if s == nil {
		return "plpgsql"
	}
	if lang := defElemLanguage(s.Args); lang != "" {
		return lang
	}
	return "plpgsql"
}

func createFunctionLanguage(s *pg_query.CreateFunctionStmt) string {
	if s == nil {
		return ""
	}
	return defElemLanguage(s.Options)
}

func defElemLanguage(opts []*pg_query.Node) string {
	for _, opt := range opts {
		de, ok := opt.Node.(*pg_query.Node_DefElem)
		if !ok || de.DefElem == nil || de.DefElem.Defname != "language" {
			continue
		}
		if s, ok := de.DefElem.Arg.Node.(*pg_query.Node_String_); ok {
			return strings.ToLower(s.String_.Sval)
		}
	}
	return ""
}

func strp(s string) *string { return &s }
