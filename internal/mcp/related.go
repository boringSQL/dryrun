package mcp

import (
	"fmt"
	"sort"
	"strings"

	"github.com/boringsql/dryrun/internal/query"
	"github.com/boringsql/dryrun/internal/schema"
)

const (
	maxHintNames = 5 // per referencing action, so a hub table stays readable

	actionUnknown = "UNKNOWN"
)

var (
	// no action is a prefix of another, so a prefix match tells them apart
	fkActions = []string{"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}
)

type (
	// edgeTarget is the unquoted other end of a foreign key, for follow-up resolution.
	edgeTarget struct {
		schema, name string
	}
)

func (t edgeTarget) qualified() string { return qualify(t.schema, t.name) }

// fkAction reads ON DELETE / ON UPDATE out of pg_get_constraintdef. An absent
// clause means the default, NO ACTION; a nil definition means UNKNOWN.
func fkAction(def *string, clause string) string {
	if def == nil {
		return actionUnknown
	}
	i := strings.Index(*def, clause+" ")
	if i < 0 {
		return "NO ACTION"
	}
	rest := (*def)[i+len(clause)+1:]
	for _, a := range fkActions {
		if strings.HasPrefix(rest, a) {
			return a
		}
	}
	return actionUnknown
}

func hasClause(def *string, clause string) bool {
	return def != nil && strings.Contains(*def, clause)
}

// joinClause builds a paste-ready, fully qualified JOIN. A self-join gets the
// alias Postgres requires.
func joinClause(other, local, selfAlias string, otherCols, localCols []string) string {
	if len(otherCols) == 0 || len(otherCols) != len(localCols) {
		return ""
	}
	lhs := other
	if other == local {
		lhs = selfAlias
		other += " AS " + selfAlias
	}
	preds := make([]string, len(otherCols))
	for i := range preds {
		preds[i] = fmt.Sprintf("%s.%s = %s.%s",
			lhs, query.QuoteIdent(otherCols[i]), local, query.QuoteIdent(localCols[i]))
	}
	return "JOIN " + other + " ON " + strings.Join(preds, " AND ")
}

func qualify(schemaName, name string) string {
	return query.QuoteIdent(schemaName) + "." + query.QuoteIdent(name)
}

// partitionRoots maps each partition to its root ancestor; partitioning nests,
// so the walk goes past the immediate parent.
func partitionRoots(snap *schema.SchemaSnapshot) map[string]string {
	parent := map[string]string{}
	for _, t := range snap.Tables {
		if t.PartitionInfo == nil {
			continue
		}
		p := t.Schema + "." + t.Name
		for _, c := range t.PartitionInfo.Children {
			parent[c.Schema+"."+c.Name] = p
		}
	}
	roots := map[string]string{}
	for child := range parent {
		seen := map[string]bool{child: true}
		root := parent[child]
		for {
			next, ok := parent[root]
			if !ok || seen[root] {
				break
			}
			seen[root] = true
			root = next
		}
		roots[child] = root
	}
	return roots
}

func edgeFrom(c schema.Constraint, other edgeTarget, cols, refCols []string, local, selfAlias string) relatedEdge {
	return relatedEdge{
		Table:       other.qualified(),
		TableSchema: other.schema,
		TableName:   other.name,
		Constraint:  c.Name,
		Columns:     nonNil(cols),
		RefColumns:  nonNil(refCols),
		OnDelete:    fkAction(c.Definition, "ON DELETE"),
		OnUpdate:    fkAction(c.Definition, "ON UPDATE"),
		NotValid:    hasClause(c.Definition, "NOT VALID"),
		Join:        joinClause(other.qualified(), local, selfAlias, refCols, cols),
		target:      other,
		deferred:    hasClause(c.Definition, "DEFERRABLE") && hasClause(c.Definition, "INITIALLY DEFERRED"),
	}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func outgoingEdges(t *schema.Table, local string) []relatedEdge {
	var out []relatedEdge
	for _, c := range t.Constraints {
		if c.Kind != schema.ConstraintForeignKey || c.FKTable == nil {
			continue
		}
		out = append(out, edgeFrom(c, splitRef(*c.FKTable), c.Columns, c.FKColumns, local, "parent"))
	}
	return out
}

func incomingEdges(snap *schema.SchemaSnapshot, qualified, local string) []relatedEdge {
	roots := partitionRoots(snap)

	type key struct{ table, cols, refCols, onDelete, onUpdate string }
	byKey := map[key]int{}
	var out []relatedEdge

	add := func(t edgeTarget, c schema.Constraint, folded bool) {
		k := key{t.qualified(), strings.Join(c.Columns, ","), strings.Join(c.FKColumns, ","),
			fkAction(c.Definition, "ON DELETE"), fkAction(c.Definition, "ON UPDATE")}
		if i, ok := byKey[k]; ok {
			if folded {
				out[i].PartitionsFolded++
			}
			return
		}
		byKey[k] = len(out)
		out = append(out, edgeFrom(c, t, c.FKColumns, c.Columns, local, "child"))
	}

	// roots first, so a partition's copy folds into the root's edge
	for pass := 0; pass < 2; pass++ {
		for _, o := range snap.Tables {
			ref := o.Schema + "." + o.Name
			// a self-reference is already reported as an outgoing key
			if ref == qualified {
				continue
			}
			root, isChild := roots[ref]
			if isChild == (pass == 0) {
				continue
			}
			for _, c := range o.Constraints {
				if c.Kind != schema.ConstraintForeignKey || c.FKTable == nil || *c.FKTable != qualified {
					continue
				}
				if isChild {
					add(splitRef(root), c, true)
					continue
				}
				add(edgeTarget{o.Schema, o.Name}, c, false)
			}
		}
	}
	return out
}

// splitRef splits fk_table (nspname.relname); a bare name means a legacy
// snapshot, where public is the best guess.
func splitRef(ref string) edgeTarget {
	if i := strings.Index(ref, "."); i >= 0 {
		return edgeTarget{ref[:i], ref[i+1:]}
	}
	return edgeTarget{"public", ref}
}

// destructive reports whether the edge's ON DELETE acts on the referencing
// rows (or is unrecorded): these must survive the cap.
func destructive(e relatedEdge) bool {
	switch e.OnDelete {
	case "CASCADE", "SET NULL", "SET DEFAULT", actionUnknown:
		return true
	}
	return false
}

func namesOf(edges []relatedEdge) []string {
	out := make([]string, len(edges))
	for i, e := range edges {
		out[i] = e.Table
	}
	return out
}

func listNames(names []string) string {
	if len(names) <= maxHintNames {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:maxHintNames], ", "), len(names)-maxHintNames)
}

// deleteHint summarizes what a DELETE here does to referencing rows, and
// returns the cascade targets worth a follow-up call.
func deleteHint(incoming []relatedEdge) (string, []edgeTarget) {
	byAction := map[string][]relatedEdge{}
	for _, e := range incoming {
		byAction[e.OnDelete] = append(byAction[e.OnDelete], e)
	}
	// sorted, so the hint and the follow-up call lead with the same name
	group := func(actions ...string) []relatedEdge {
		var out []relatedEdge
		for _, a := range actions {
			out = append(out, byAction[a]...)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
		return out
	}

	var parts []string
	cascades := group("CASCADE")
	if len(cascades) > 0 {
		parts = append(parts, "deletes matching rows in "+listNames(namesOf(cascades)))
	}
	if cleared := group("SET NULL", "SET DEFAULT"); len(cleared) > 0 {
		parts = append(parts, "clears the referencing column in "+listNames(namesOf(cleared)))
	}
	if blocked := group("RESTRICT", "NO ACTION"); len(blocked) > 0 {
		clause := "fails while rows exist in " + listNames(namesOf(blocked))
		// a deferred NO ACTION raises at COMMIT instead
		for _, e := range blocked {
			if e.OnDelete == "NO ACTION" && e.deferred {
				clause += " (at COMMIT, where the constraint is deferred)"
				break
			}
		}
		parts = append(parts, clause)
	}
	if unknown := group(actionUnknown); len(unknown) > 0 {
		parts = append(parts, "does something unrecorded in this snapshot to "+listNames(namesOf(unknown)))
	}
	if len(parts) == 0 {
		return "", nil
	}

	targets := make([]edgeTarget, len(cascades))
	for i, e := range cascades {
		targets[i] = e.target
	}
	return "Deleting a row here " + strings.Join(parts, "; ") + ".", targets
}
