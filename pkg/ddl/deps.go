package ddl

import (
	"fmt"
	"sort"

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

// DependentView is a captured view that reads a table.
type DependentView struct {
	Schema       string
	Name         string
	Materialized bool
	Columns      []string
}

// ViewDependents lists captured views and materialized views reading t.
func ViewDependents(snap *snapshot.SchemaSnapshot, t *snapshot.Table) []DependentView {
	if snap == nil || t == nil || snap.DependenciesHash == "" {
		return nil
	}
	q := qual(t.Schema, t.Name)
	targets := map[objKey]string{{"pg_class", q}: ""}
	for _, c := range t.Columns {
		targets[objKey{"pg_class", q + "." + quoteIdent(c.Name)}] = c.Name
	}
	views := make(map[objKey]DependentView, len(snap.Views))
	for _, v := range snap.Views {
		views[objKey{"pg_class", qual(v.Schema, v.Name)}] = DependentView{
			Schema: v.Schema, Name: v.Name, Materialized: v.IsMaterialized,
		}
	}

	columns := make(map[objKey]map[string]struct{})
	for _, d := range snap.Dependencies {
		if d.DependentClass != "pg_class" {
			continue
		}
		col, ok := targets[objKey{"pg_class", d.ReferencedIdentity}]
		if !ok {
			continue
		}
		key := objKey{"pg_class", d.DependentIdentity}
		if _, ok := views[key]; !ok {
			continue
		}
		if columns[key] == nil {
			columns[key] = make(map[string]struct{})
		}
		if col != "" {
			columns[key][col] = struct{}{}
		}
	}

	out := make([]DependentView, 0, len(columns))
	for key := range columns {
		v := views[key]
		v.Columns = sortedSet(columns[key])
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Name < out[j].Name
	})
	return out
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
