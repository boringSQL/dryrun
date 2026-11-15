package ddl

import (
	"fmt"
	"sort"
	"strings"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// Classes the DDL must create by hand.
var neededClasses = map[string]bool{
	"pg_proc":      true,
	"pg_operator":  true,
	"pg_opclass":   true,
	"pg_opfamily":  true,
	"pg_collation": true,
}

type objKey struct{ class, identity string }

// dependentOrigins maps identities tied to this table to why they are needed.
func dependentOrigins(t *snapshot.Table) map[objKey]string {
	q := qual(t.Schema, t.Name)
	out := map[objKey]string{{"pg_class", q}: "used by the table"}
	for _, i := range t.Indexes {
		out[objKey{"pg_class", qual(t.Schema, i.Name)}] = "used by index " + quoteIdent(i.Name)
	}
	for _, c := range t.Constraints {
		out[objKey{"pg_constraint", quoteIdent(c.Name) + " on " + q}] = "used by constraint " + quoteIdent(c.Name)
	}
	for _, tr := range t.Triggers {
		out[objKey{"pg_trigger", quoteIdent(tr.Name) + " on " + q}] = "called by trigger " + quoteIdent(tr.Name)
	}
	for _, c := range t.Columns {
		out[objKey{"pg_class", q + "." + quoteIdent(c.Name)}] = "used by column " + quoteIdent(c.Name)
	}
	return out
}

// DependentVia is an immediate parent of a transitive dependent.
type DependentVia struct {
	Schema  string
	Name    string
	Columns []string
}

// DependentView is a captured view that reads a table, directly or through
// another view. Columns holds table columns at depth 1; deeper views carry
// Via, the parent views and the columns read from each.
type DependentView struct {
	Schema       string
	Name         string
	Materialized bool
	Depth        int
	Columns      []string
	Via          []DependentVia
}

// ViewDependents lists captured views and materialized views reading t.
func ViewDependents(snap *snapshot.SchemaSnapshot, t *snapshot.Table) []DependentView {
	if snap == nil || t == nil || snap.DependenciesHash == "" {
		return nil
	}
	root := qual(t.Schema, t.Name)
	views := make(map[string]DependentView, len(snap.Views))
	nodes := map[string]bool{root: true}
	for _, v := range snap.Views {
		id := qual(v.Schema, v.Name)
		views[id] = DependentView{Schema: v.Schema, Name: v.Name, Materialized: v.IsMaterialized}
		nodes[id] = true
	}

	type edge struct{ src, column string }
	incoming := make(map[string][]edge)
	outgoing := make(map[string][]string)
	for _, d := range snap.Dependencies {
		if d.DependentClass != "pg_class" || d.ReferencedClass != "pg_class" {
			continue
		}
		child := d.DependentIdentity
		if _, ok := views[child]; !ok {
			continue
		}
		src, col, ok := identitySource(d.ReferencedIdentity, nodes)
		if !ok {
			continue
		}
		outgoing[src] = append(outgoing[src], child)
		incoming[child] = append(incoming[child], edge{src: src, column: col})
	}

	depth := map[string]int{root: 0}
	queue := []string{root}
	for len(queue) > 0 {
		src := queue[0]
		queue = queue[1:]
		for _, child := range outgoing[src] {
			if _, seen := depth[child]; seen {
				continue
			}
			depth[child] = depth[src] + 1
			queue = append(queue, child)
		}
	}

	out := make([]DependentView, 0, len(depth))
	for id, at := range depth {
		if at == 0 {
			continue
		}
		v := views[id]
		v.Depth = at
		hops := make(map[string]map[string]struct{})
		for _, e := range incoming[id] {
			if depth[e.src] != at-1 {
				continue
			}
			if hops[e.src] == nil {
				hops[e.src] = make(map[string]struct{})
			}
			if e.column != "" {
				hops[e.src][e.column] = struct{}{}
			}
		}
		if at == 1 {
			v.Columns = sortedSet(hops[root])
		} else {
			for src, cols := range hops {
				p := views[src]
				v.Via = append(v.Via, DependentVia{Schema: p.Schema, Name: p.Name, Columns: sortedSet(cols)})
			}
			sort.Slice(v.Via, func(i, j int) bool {
				if v.Via[i].Schema != v.Via[j].Schema {
					return v.Via[i].Schema < v.Via[j].Schema
				}
				return v.Via[i].Name < v.Via[j].Name
			})
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Depth != out[j].Depth {
			return out[i].Depth < out[j].Depth
		}
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// identitySource resolves a referenced identity to the table or view it
// belongs to; a column reference returns the column name too.
func identitySource(identity string, nodes map[string]bool) (string, string, bool) {
	if nodes[identity] {
		return identity, "", true
	}
	parent, col, ok := cutLastIdent(identity)
	if !ok || !nodes[parent] {
		return "", "", false
	}
	return parent, unquoteIdent(col), true
}

func unquoteIdent(s string) string {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return s
	}
	return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
}

func cutLastIdent(identity string) (string, string, bool) {
	last, quoted := -1, false
	for i := 0; i < len(identity); i++ {
		switch identity[i] {
		case '"':
			if quoted && i+1 < len(identity) && identity[i+1] == '"' {
				i++
				continue
			}
			quoted = !quoted
		case '.':
			if !quoted {
				last = i
			}
		}
	}
	if last < 0 {
		return "", "", false
	}
	return identity[:last], identity[last+1:], true
}

func sortedSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for item := range set {
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

// requiredObjects resolves what this DDL names but does not create.
func requiredObjects(snap *snapshot.SchemaSnapshot, t *snapshot.Table) []string {
	if snap == nil || t == nil || snap.DependenciesHash == "" {
		return nil
	}
	type need struct{ kind, from string }
	origins := dependentOrigins(t)
	owners := map[objKey]string{}
	needs := map[objKey]need{}
	for _, d := range snap.Dependencies {
		if d.Deptype == "e" && d.ReferencedClass == "pg_extension" {
			owners[objKey{d.DependentClass, d.DependentIdentity}] = d.ReferencedIdentity
		}
		from, ok := origins[objKey{d.DependentClass, d.DependentIdentity}]
		if !ok || !neededClasses[d.ReferencedClass] {
			continue
		}
		ref := objKey{d.ReferencedClass, d.ReferencedIdentity}
		if prev, dup := needs[ref]; !dup || from < prev.from {
			needs[ref] = need{d.ReferencedType, from}
		}
	}

	var out []string
	for obj, n := range needs {
		if ext, owned := owners[obj]; owned {
			out = append(out, fmt.Sprintf("CREATE EXTENSION %s (provides %s, %s)", ext, obj.identity, n.from))
		} else {
			out = append(out, fmt.Sprintf("%s %s (%s)", n.kind, obj.identity, n.from))
		}
	}
	sort.Strings(out)
	return out
}
