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
