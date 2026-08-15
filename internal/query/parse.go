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
		FuncWrappedColumns []FuncWrappedColumn `json:"func_wrapped_columns,omitempty"`
		UpdateTargets      []string            `json:"update_targets,omitempty"`
		ProceduralBodies   []ProceduralBody    `json:"procedural_bodies,omitempty"`
		HasSelectStar      bool                `json:"has_select_star"`
		HasLimit           bool                `json:"has_limit"`
		HasWhere           bool                `json:"has_where"`
		HasJoin            bool                `json:"has_join"`
		StatementType      string              `json:"statement_type"`
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
	}

	FilterColumn struct {
		Table  *string `json:"table,omitempty"`
		Column string  `json:"column"`
	}

	FuncWrappedColumn struct {
		Table    *string `json:"table,omitempty"`
		Column   string  `json:"column"`
		FuncName string  `json:"func_name"`
	}
)

func ParseSQL(sql string) (*ParsedQuery, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse error: %w", err)
	}

	var (
		tables             []ReferencedTable
		filterColumns      []FilterColumn
		funcWrappedColumns []FuncWrappedColumn
		proceduralBodies   []ProceduralBody
		updateTargets      []string
		hasSelectStar      bool
		hasJoin            bool
		hasWhere           bool
		hasLimit           bool
		stmtType           string
	)

	seenTables := make(map[string]bool)

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

		// walk tree for tables, joins, filter columns
		walkNode(node, func(child *pg_query.Node) {
			if child == nil {
				return
			}
			switch cn := child.Node.(type) {
			case *pg_query.Node_RangeVar:
				rv := cn.RangeVar
				if rv == nil {
					return
				}
				ctx := "select"
				if stmtType == "INSERT" || stmtType == "UPDATE" || stmtType == "DELETE" {
					ctx = "dml"
				}
				key := rv.Relname + ":" + ctx
				if !seenTables[key] {
					seenTables[key] = true
					t := ReferencedTable{
						Name:    rv.Relname,
						Context: ctx,
					}
					if rv.Schemaname != "" {
						t.Schema = strp(rv.Schemaname)
					}
					if rv.Alias != nil && rv.Alias.Aliasname != "" {
						t.Alias = strp(rv.Alias.Aliasname)
					}
					tables = append(tables, t)
				}
			case *pg_query.Node_JoinExpr:
				_ = cn
				hasJoin = true
			case *pg_query.Node_ColumnRef:
				// filter columns from WHERE
				cr := cn.ColumnRef
				if cr == nil || len(cr.Fields) == 0 {
					return
				}
				fc := extractFilterColumn(cr)
				if fc != nil {
					filterColumns = append(filterColumns, *fc)
				}
			}
		})
	}

	return &ParsedQuery{
		SQL: sql,
		Info: QueryInfo{
			Tables:             tables,
			FilterColumns:      filterColumns,
			HasSelectStar:      hasSelectStar,
			HasLimit:           hasLimit,
			HasWhere:           hasWhere,
			HasJoin:            hasJoin,
			FuncWrappedColumns: funcWrappedColumns,
			UpdateTargets:      updateTargets,
			ProceduralBodies:   proceduralBodies,
			StatementType:      stmtType,
		},
	}, nil
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
