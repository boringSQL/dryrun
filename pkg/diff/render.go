package diff

import (
	"fmt"
	"io"
)

func Marker(c Change) string {
	switch c.Type.Category() {
	case "added":
		return "+"
	case "removed":
		return "-"
	default:
		return "~"
	}
}

func (r ObjectRef) Qualified() string {
	if r.Schema != nil && *r.Schema != "" {
		return *r.Schema + "." + r.Name
	}
	return r.Name
}

// Human +/~/- tree for schema, sizing/stat movers for planner, counter movers for activity.
func RenderConsole(w io.Writer, env *SnapshotDiff) {
	if env.Planner != nil {
		RenderPlannerConsole(w, env, DefaultMinPct)
		return
	}
	if env.Activity != nil {
		RenderActivityConsole(w, env, DefaultMinPct)
		return
	}
	if env.Schema == nil {
		fmt.Fprintf(w, "%s diff %s → %s: no renderer yet\n", env.Kind, short(env.FromHash), short(env.ToHash))
		return
	}
	fmt.Fprintf(w, "schema diff  %s → %s\n", short(env.FromHash), short(env.ToHash))
	if env.Schema.IsEmpty() {
		fmt.Fprintln(w, "  no changes")
		return
	}

	var added, removed, modified int
	for _, c := range env.Schema.Changes {
		switch c.Type.Category() {
		case "added":
			added++
		case "removed":
			removed++
		default:
			modified++
		}
	}
	fmt.Fprintf(w, "  %s: %d added, %d modified, %d removed\n\n",
		plural(len(env.Schema.Changes), "change", "changes"), added, modified, removed)

	for _, g := range groupByObject(env.Schema.Changes) {
		if len(g) == 1 && isObjectLevel(g[0]) {
			c := g[0]
			line := fmt.Sprintf("%s %s %s", Marker(c), c.Object.Kind, c.Object.Qualified())
			if c.Note != "" {
				line += " (" + c.Note + ")"
			}
			fmt.Fprintln(w, line)
			continue
		}
		fmt.Fprintf(w, "~ %s %s\n", g[0].Object.Kind, g[0].Object.Qualified())
		for _, c := range g {
			fmt.Fprintf(w, "    %s %s\n", Marker(c), Describe(c))
		}
	}
}

// whole-object changes render as one line, no per-attribute children.
func isObjectLevel(c Change) bool {
	switch c.Type {
	case TableAdded, TableDropped, TableRenamed,
		ViewAdded, ViewDropped, ViewDefinitionChanged,
		FunctionAdded, FunctionDropped,
		ObjectAdded, ObjectDropped, ObjectModified:
		return true
	}
	return false
}

func groupByObject(changes []Change) [][]Change {
	var groups [][]Change
	type key struct{ kind, schema, name string }
	idx := make(map[key]int)
	for _, c := range changes {
		k := key{c.Object.Kind, ptrStr(c.Object.Schema), c.Object.Name}
		if i, ok := idx[k]; ok {
			groups[i] = append(groups[i], c)
			continue
		}
		idx[k] = len(groups)
		groups = append(groups, []Change{c})
	}
	return groups
}

// Human one-liner for a single change (child line / drift output).
func Describe(c Change) string {
	switch c.Type {
	case TableAdded:
		return fmt.Sprintf("table %s added", c.Object.Qualified())
	case TableDropped:
		return fmt.Sprintf("table %s dropped", c.Object.Qualified())
	case TableRenamed:
		if c.Rename != nil {
			return fmt.Sprintf("table renamed %s → %s", c.Rename.FromName, c.Rename.ToName)
		}
		return "table renamed"
	case TableCommentChanged:
		return "comment changed"
	case ColumnAdded:
		null := "NOT NULL"
		if c.Column.Nullable != nil && *c.Column.Nullable {
			null = "nullable"
		}
		s := fmt.Sprintf("column %s added (%s", c.Column.Name, null)
		if c.Column.DefaultKind != "" && c.Column.DefaultKind != DefaultNone {
			s += ", " + string(c.Column.DefaultKind) + " default"
		}
		return s + ")"
	case ColumnDropped:
		return fmt.Sprintf("column %s dropped", c.Column.Name)
	case ColumnTypeChanged:
		kind := "rewrite"
		if c.Column.Widening != nil && *c.Column.Widening {
			kind = "widening"
		}
		return fmt.Sprintf("column %s: %s → %s [%s]", c.Column.Name, c.Column.FromType, c.Column.ToType, kind)
	case ColumnSetNotNull:
		return fmt.Sprintf("column %s: SET NOT NULL", c.Column.Name)
	case ColumnDropNotNull:
		return fmt.Sprintf("column %s: DROP NOT NULL", c.Column.Name)
	case ColumnDefaultChanged:
		return fmt.Sprintf("column %s: default %s → %s", c.Column.Name, dflt(c.Column.FromDefault), dflt(c.Column.ToDefault))
	case ColumnCommentChanged:
		return fmt.Sprintf("column %s: comment changed", c.Column.Name)
	case IndexAdded:
		return "index " + c.Index.Name + " added"
	case IndexDropped:
		s := "index " + c.Index.Name + " dropped"
		if c.Index.BacksConstraint {
			s += " (backs constraint)"
		}
		return s
	case ConstraintAdded:
		return fmt.Sprintf("constraint %s added (%s)", c.Constraint.Name, c.Constraint.Kind)
	case ConstraintDropped:
		return fmt.Sprintf("constraint %s dropped (%s)", c.Constraint.Name, c.Constraint.Kind)
	case RLSToggled:
		if c.RLS != nil && c.RLS.Enabled {
			return "RLS enabled"
		}
		return "RLS disabled"
	case ViewAdded:
		return fmt.Sprintf("view %s added", c.Object.Qualified())
	case ViewDropped:
		return fmt.Sprintf("view %s dropped", c.Object.Qualified())
	case ViewDefinitionChanged:
		return "definition changed"
	case FunctionAdded:
		return fmt.Sprintf("function %s added", c.Object.Qualified())
	case FunctionDropped:
		return fmt.Sprintf("function %s dropped", c.Object.Qualified())
	case FuncSecurityDefiner:
		if c.Function != nil && c.Function.SecurityDefiner != nil && *c.Function.SecurityDefiner {
			return "SECURITY DEFINER added"
		}
		return "SECURITY DEFINER removed"
	case FuncVolatilityChanged:
		return fmt.Sprintf("volatility %s → %s", c.Function.FromVolatility, c.Function.ToVolatility)
	case FuncReturnTypeChanged:
		return fmt.Sprintf("return type %s → %s", c.Function.FromReturnType, c.Function.ToReturnType)
	case ObjectAdded:
		return fmt.Sprintf("%s %s added", c.Object.Kind, c.Object.Name)
	case ObjectDropped:
		return fmt.Sprintf("%s %s dropped", c.Object.Kind, c.Object.Name)
	case ObjectModified:
		return fmt.Sprintf("%s %s changed", c.Object.Kind, c.Object.Name)
	}
	return string(c.Type)
}

func dflt(p *string) string {
	if p == nil {
		return "∅"
	}
	return *p
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}
