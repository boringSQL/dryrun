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
			walkSelect(n.SelectStmt, &hasWhere, &hasLimit, &hasSelectStar)
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

		// walk tree for tables, joins, referenced columns
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

func walkSelect(s *pg_query.SelectStmt, hasWhere, hasLimit, hasSelectStar *bool) {
	if s == nil {
		return
	}
	if s.WhereClause != nil {
		*hasWhere = true
	}
	if s.LimitCount != nil || s.LimitOffset != nil {
		*hasLimit = true
	}
	for _, target := range s.TargetList {
		if rt, ok := target.Node.(*pg_query.Node_ResTarget); ok {
			if rt.ResTarget != nil && rt.ResTarget.Val != nil {
				if cr, ok := rt.ResTarget.Val.Node.(*pg_query.Node_ColumnRef); ok {
					for _, f := range cr.ColumnRef.Fields {
						if _, ok := f.Node.(*pg_query.Node_AStar); ok {
							*hasSelectStar = true
						}
					}
				}
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

// recursive walk over pg_query nodes; protobuf reflection would be heavier so we handle the cases we need
func walkNode(node *pg_query.Node, fn func(*pg_query.Node)) {
	if node == nil {
		return
	}
	fn(node)
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		s := n.SelectStmt
		if s == nil {
			return
		}
		for _, t := range s.TargetList {
			walkNode(t, fn)
		}
		for _, f := range s.FromClause {
			walkNode(f, fn)
		}
		walkNode(s.WhereClause, fn)
		for _, g := range s.GroupClause {
			walkNode(g, fn)
		}
		walkNode(s.HavingClause, fn)
		walkNode(s.LimitCount, fn)
		walkNode(s.LimitOffset, fn)
		for _, s := range s.SortClause {
			walkNode(s, fn)
		}
		walkNode(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s.Larg}}, fn)
		walkNode(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s.Rarg}}, fn)
	case *pg_query.Node_InsertStmt:
		s := n.InsertStmt
		if s == nil {
			return
		}
		if s.Relation != nil {
			walkNode(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: s.Relation}}, fn)
		}
		if s.SelectStmt != nil {
			walkNode(s.SelectStmt, fn)
		}
		if s.OnConflictClause != nil {
			walkNode(&pg_query.Node{Node: &pg_query.Node_OnConflictClause{OnConflictClause: s.OnConflictClause}}, fn)
		}
	case *pg_query.Node_UpdateStmt:
		s := n.UpdateStmt
		if s == nil {
			return
		}
		if s.Relation != nil {
			walkNode(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: s.Relation}}, fn)
		}
		for _, f := range s.FromClause {
			walkNode(f, fn)
		}
		walkNode(s.WhereClause, fn)
	case *pg_query.Node_DeleteStmt:
		s := n.DeleteStmt
		if s == nil {
			return
		}
		if s.Relation != nil {
			walkNode(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: s.Relation}}, fn)
		}
		walkNode(s.WhereClause, fn)
	case *pg_query.Node_JoinExpr:
		j := n.JoinExpr
		if j == nil {
			return
		}
		walkNode(j.Larg, fn)
		walkNode(j.Rarg, fn)
		walkNode(j.Quals, fn)
	case *pg_query.Node_RangeVar:
		// leaf node
	case *pg_query.Node_BoolExpr:
		b := n.BoolExpr
		if b == nil {
			return
		}
		for _, a := range b.Args {
			walkNode(a, fn)
		}
	case *pg_query.Node_AExpr:
		e := n.AExpr
		if e == nil {
			return
		}
		walkNode(e.Lexpr, fn)
		walkNode(e.Rexpr, fn)
	case *pg_query.Node_ResTarget:
		rt := n.ResTarget
		if rt == nil {
			return
		}
		walkNode(rt.Val, fn)
	case *pg_query.Node_ColumnRef:
		// leaf
	case *pg_query.Node_SubLink:
		sl := n.SubLink
		if sl == nil {
			return
		}
		walkNode(sl.Subselect, fn)
		walkNode(sl.Testexpr, fn)
	case *pg_query.Node_FuncCall:
		fc := n.FuncCall
		if fc == nil {
			return
		}
		for _, a := range fc.Args {
			walkNode(a, fn)
		}
	case *pg_query.Node_TypeCast:
		tc := n.TypeCast
		if tc == nil {
			return
		}
		walkNode(tc.Arg, fn)
	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			walkNode(n.RangeSubselect.Subquery, fn)
		}
	case *pg_query.Node_OnConflictClause:
		oc := n.OnConflictClause
		if oc == nil {
			return
		}
		if oc.Infer != nil {
			for _, ie := range oc.Infer.IndexElems {
				walkNode(ie, fn)
			}
			walkNode(oc.Infer.WhereClause, fn)
		}
		for _, tl := range oc.TargetList {
			walkNode(tl, fn)
		}
		walkNode(oc.WhereClause, fn)
	case *pg_query.Node_MergeStmt:
		s := n.MergeStmt
		if s == nil {
			return
		}
		if s.Relation != nil {
			walkNode(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: s.Relation}}, fn)
		}
		walkNode(s.SourceRelation, fn)
		walkNode(s.JoinCondition, fn)
		for _, w := range s.MergeWhenClauses {
			walkNode(w, fn)
		}
	case *pg_query.Node_MergeWhenClause:
		m := n.MergeWhenClause
		if m == nil {
			return
		}
		walkNode(m.Condition, fn)
		for _, tl := range m.TargetList {
			walkNode(tl, fn)
		}
		for _, v := range m.Values {
			walkNode(v, fn)
		}
	case *pg_query.Node_CaseExpr:
		e := n.CaseExpr
		if e == nil {
			return
		}
		walkNode(e.Arg, fn)
		for _, a := range e.Args {
			walkNode(a, fn)
		}
		walkNode(e.Defresult, fn)
	case *pg_query.Node_CaseWhen:
		w := n.CaseWhen
		if w == nil {
			return
		}
		walkNode(w.Expr, fn)
		walkNode(w.Result, fn)
	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			for _, a := range n.CoalesceExpr.Args {
				walkNode(a, fn)
			}
		}
	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			walkNode(n.NullTest.Arg, fn)
		}
	case *pg_query.Node_BooleanTest:
		if n.BooleanTest != nil {
			walkNode(n.BooleanTest.Arg, fn)
		}
	case *pg_query.Node_List:
		if n.List != nil {
			for _, i := range n.List.Items {
				walkNode(i, fn)
			}
		}
	case *pg_query.Node_RowExpr:
		if n.RowExpr != nil {
			for _, a := range n.RowExpr.Args {
				walkNode(a, fn)
			}
		}
	case *pg_query.Node_MinMaxExpr:
		if n.MinMaxExpr != nil {
			for _, a := range n.MinMaxExpr.Args {
				walkNode(a, fn)
			}
		}
	case *pg_query.Node_CollateClause:
		if n.CollateClause != nil {
			walkNode(n.CollateClause.Arg, fn)
		}
	case *pg_query.Node_AIndirection:
		if n.AIndirection != nil {
			walkNode(n.AIndirection.Arg, fn)
			for _, i := range n.AIndirection.Indirection {
				walkNode(i, fn)
			}
		}
	case *pg_query.Node_AArrayExpr:
		if n.AArrayExpr != nil {
			for _, e := range n.AArrayExpr.Elements {
				walkNode(e, fn)
			}
		}
	}
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
		for _, f := range n.UpdateStmt.FromClause {
			p.collect(f)
		}
		p.collect(n.UpdateStmt.WhereClause)
	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt == nil {
			return
		}
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
		if a, b, ok := equalityPair(e); ok {
			p.pairs = append(p.pairs, colPair{a, b})
		}
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

// equalityPair returns the two table qualifiers of a qualified column-to-column
// comparison (a.x = b.y, a.x > b.y, ...), the shape that links two relations.
// Function-wrapped or cast columns still link: the comparison ranges over both
// tables either way.
func equalityPair(e *pg_query.A_Expr) (string, string, bool) {
	if e.Kind != pg_query.A_Expr_Kind_AEXPR_OP || len(e.Name) == 0 {
		return "", "", false
	}
	lhs, rhs := firstQualifiedColumn(e.Lexpr), firstQualifiedColumn(e.Rexpr)
	if lhs == nil || rhs == nil {
		return "", "", false
	}
	a, b := columnTable(lhs), columnTable(rhs)
	if a == "" || b == "" {
		return "", "", false
	}
	return a, b, true
}

// firstQualifiedColumn digs through casts and function calls for a table-qualified
// column; unqualified columns are skipped (cannot attribute to a relation).
func firstQualifiedColumn(node *pg_query.Node) *pg_query.ColumnRef {
	if node == nil {
		return nil
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_ColumnRef:
		cr := n.ColumnRef
		if cr != nil && len(cr.Fields) == 2 {
			return cr
		}
	case *pg_query.Node_TypeCast:
		return firstQualifiedColumn(n.TypeCast.GetArg())
	case *pg_query.Node_CollateClause:
		return firstQualifiedColumn(n.CollateClause.GetArg())
	case *pg_query.Node_AIndirection:
		return firstQualifiedColumn(n.AIndirection.GetArg())
	case *pg_query.Node_FuncCall:
		for _, a := range n.FuncCall.GetArgs() {
			if cr := firstQualifiedColumn(a); cr != nil {
				return cr
			}
		}
	}
	return nil
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
		if rs != nil && rs.Alias != nil && rs.Alias.Aliasname != "" {
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
