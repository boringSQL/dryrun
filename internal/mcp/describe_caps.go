package mcp

import (
	"fmt"
	"strings"

	"github.com/boringsql/dryrun/internal/schema"
)

type sectionCap struct {
	key      string
	label    string
	omitted  int
	total    int
	retained bool
}

// selectColumns narrows columns and their profiles to the named ones,
// reporting unknown names rather than silently dropping them.
func selectColumns(result map[string]any, t *schema.Table, want []string) error {
	known := map[string]bool{}
	for _, c := range t.Columns {
		known[c.Name] = true
	}
	var unknown []string
	keep := map[string]bool{}
	for _, w := range want {
		if !known[w] {
			unknown = append(unknown, w)
			continue
		}
		keep[w] = true
	}
	if len(unknown) > 0 {
		return fmt.Errorf("no column named %s on %s.%s", strings.Join(unknown, ", "), t.Schema, t.Name)
	}
	// absent is not empty: the declared schema types these as arrays, so
	// creating the key with a nil value fails output validation
	narrow(result, "columns", "name", keep)
	narrow(result, "column_profiles", "column", keep)
	return nil
}

func narrow(result map[string]any, key, field string, keep map[string]bool) {
	if _, ok := result[key]; !ok {
		return
	}
	result[key] = filterByName(result[key], field, keep)
}

// capTableSections caps sections that scale with table width or partition
// count; structural columns survive the cap. selected columns bypass it.
func capTableSections(result map[string]any, t *schema.Table, max int, selected bool) []sectionCap {
	if max <= 0 {
		return nil
	}
	var caps []sectionCap
	add := func(key, label string, omitted, total int, retained bool) {
		if omitted > 0 {
			result[key+"_omitted"] = omitted
			caps = append(caps, sectionCap{key, label, omitted, total, retained})
		}
	}

	if rows, ok := toRows(result["columns"]); ok && !selected {
		kept, omitted, retained, dropped := capRetaining(rows, "name", structuralColumns(t), max)
		result["columns"] = kept
		if len(dropped) > 0 {
			result["columns_omitted_names"] = dropped
		}
		add("columns", "columns", omitted, len(rows), retained > 0)
	}
	// keep profiles aligned with the surviving columns
	if cols, ok := toRows(result["columns"]); ok {
		shown := map[string]bool{}
		for _, c := range cols {
			if name, ok := fieldString(c, "name"); ok {
				shown[name] = true
			}
		}
		narrow(result, "column_profiles", "column", shown)
	}

	for _, s := range []struct{ key, label string }{
		{"indexes", "indexes"},
		{"constraints", "constraints"},
		{"partition_child_sizing", "partition sizing rows"},
	} {
		rows, ok := toRows(result[s.key])
		if !ok {
			continue
		}
		kept, omitted, _, _ := capRetaining(rows, "", nil, max)
		result[s.key] = kept
		add(s.key, s.label, omitted, len(rows), false)
	}

	// index children scale with partition count, so cap them too
	if rows, ok := toRows(result["indexes"]); ok {
		omitted, total := 0, 0
		for _, r := range rows {
			idx, ok := r.(map[string]any)
			if !ok {
				continue
			}
			kids, ok := toRows(idx["children"])
			if !ok {
				continue
			}
			total += len(kids)
			if len(kids) > max {
				idx["children"] = kids[:max]
				idx["children_omitted"] = len(kids) - max
				omitted += len(kids) - max
			}
		}
		add("index_children", "index children", omitted, total, false)
	}

	// detail=full marshals the raw table, so its children never met the cap
	if pi, ok := result["partition_info"].(map[string]any); ok {
		if children, ok := toRows(pi["children"]); ok && len(children) > max {
			pi["children_total"] = len(children)
			pi["children"] = children[:max]
			add("partition_children", "partition children", len(children)-max, len(children), false)
		}
	}
	return caps
}

// Columns a reader needs to write correct SQL against the table.
func structuralColumns(t *schema.Table) map[string]bool {
	keep := map[string]bool{}
	for _, c := range t.Constraints {
		switch c.Kind {
		case schema.ConstraintPrimaryKey, schema.ConstraintForeignKey, schema.ConstraintUnique:
			for _, col := range c.Columns {
				keep[col] = true
			}
		}
	}
	for _, idx := range t.Indexes {
		// expression indexes carry deparsed text, not column names, and never match
		for _, col := range idx.Columns {
			keep[col] = true
		}
	}
	return keep
}

// capRetaining keeps the first max rows plus every retained row beyond them,
// in order, and names what it dropped. field == "" retains nothing.
func capRetaining(rows []any, field string, retain map[string]bool, max int) (kept []any, omitted, retained int, dropped []string) {
	if len(rows) <= max {
		return rows, 0, 0, nil
	}
	kept = append(kept, rows[:max]...)
	for _, r := range rows[max:] {
		name, named := fieldString(r, field)
		if field != "" && named && retain[name] {
			kept = append(kept, r)
			retained++
			continue
		}
		omitted++
		if named {
			dropped = append(dropped, name)
		}
	}
	return kept, omitted, retained, dropped
}

// capRetainingBy is capRetaining for a typed slice of struct rows.
func capRetainingBy[T any](rows []T, retain func(T) bool, max int) (kept []T, omitted, retained int) {
	if max <= 0 || len(rows) <= max {
		return rows, 0, 0
	}
	kept = append(kept, rows[:max]...)
	for _, r := range rows[max:] {
		if retain(r) {
			kept = append(kept, r)
			retained++
			continue
		}
		omitted++
	}
	return kept, omitted, retained
}

func filterByName(v any, field string, keep map[string]bool) any {
	rows, ok := toRows(v)
	if !ok {
		return v
	}
	out := make([]any, 0, len(rows))
	for _, r := range rows {
		if name, ok := fieldString(r, field); ok && keep[name] {
			out = append(out, r)
		}
	}
	return out
}

// Sections arrive either marshalled as []any or assigned as []map[string]any.
func toRows(v any) ([]any, bool) {
	switch rows := v.(type) {
	case []any:
		return rows, true
	case []map[string]any:
		out := make([]any, len(rows))
		for i, r := range rows {
			out[i] = r
		}
		return out, true
	}
	return nil, false
}

func fieldString(row any, field string) (string, bool) {
	m, ok := row.(map[string]any)
	if !ok {
		return "", false
	}
	s, ok := m[field].(string)
	return s, ok
}

func capHint(caps []sectionCap) string {
	if len(caps) == 0 {
		return ""
	}
	var parts []string
	var columnsCapped, retained bool
	for _, c := range caps {
		parts = append(parts, fmt.Sprintf("%d of %d %s", c.omitted, c.total, c.label))
		if c.key == "columns" {
			columnsCapped = true
			retained = c.retained
		}
	}
	hint := "Omitted " + strings.Join(parts, ", ") + ". Re-run with limit=0 for everything"
	if columnsCapped {
		hint += ", or name the ones you need with columns= (the omitted names are in columns_omitted_names)"
	}
	hint += "."
	if retained {
		hint += " Key, unique and indexed columns are kept past the cap."
	}
	return hint
}
