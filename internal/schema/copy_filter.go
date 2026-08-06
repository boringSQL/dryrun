package schema

import (
	"strings"
	"unicode"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/boringsql/qshape"
)

// dropUnsafeCopy drops COPY statements that could leak a literal: pg_stat_statements
// stores utility statements verbatim. Filters in place.
func dropUnsafeCopy(queries []qshape.Query) []qshape.Query {
	out := queries[:0]
	for _, q := range queries {
		if !isCopy(q.Raw) || copyIsLiteralFree(q.Raw) {
			out = append(out, q)
		}
	}
	return out
}

// isCopy avoids parsing non-COPY statements. It must match the capture whitelist,
// so it trims Postgres' `\s` and skips leading `-- comment` lines the same way.
func isCopy(sql string) bool {
	for {
		sql = strings.TrimLeftFunc(sql, unicode.IsSpace)
		if !strings.HasPrefix(sql, "--") {
			break
		}
		nl := strings.IndexByte(sql, '\n')
		if nl < 0 {
			return false // all comment, no statement to match
		}
		sql = sql[nl+1:]
	}
	return strings.HasPrefix(strings.ToLower(sql), "copy")
}

// copyIsLiteralFree keeps only STDIN/STDOUT COPY: anything else (filename, PROGRAM,
// inner query, WHERE) can embed a user literal in the stored text.
func copyIsLiteralFree(sql string) bool {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return false
	}
	if len(tree.Stmts) == 0 {
		return false
	}
	for _, raw := range tree.Stmts {
		c := raw.GetStmt().GetCopyStmt()
		switch {
		case c == nil: // not a COPY after all, or a multi-statement text: undecidable
			return false
		case c.GetFilename() != "": // a path, or with IsProgram a command line
			return false
		case c.GetQuery() != nil: // COPY (SELECT …) TO: the inner query carries constants
			return false
		case c.GetWhereClause() != nil: // COPY … FROM STDIN WHERE …: arbitrary expression
			return false
		}
	}
	return true
}
