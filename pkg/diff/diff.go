// Package diff is the raw snapshot-delta contract: stdlib + pkg/snapshot only so
// the predict engine can import it. No judgment (severity/risk/forecast) here.
package diff

import (
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

// Envelope carrying exactly one typed delta, whichever kind was diffed.
type (
	SnapshotDiff struct {
		Kind        string         `json:"kind"` // schema | planner | activity
		FromHash    string         `json:"from_hash"`
		ToHash      string         `json:"to_hash"`
		FromTakenAt time.Time      `json:"from_taken_at"`
		ToTakenAt   time.Time      `json:"to_taken_at"`
		Schema      *SchemaDelta   `json:"schema,omitempty"`
		Planner     *PlannerDelta  `json:"planner,omitempty"`  // D3
		Activity    *ActivityDelta `json:"activity,omitempty"` // D4
	}

	PlannerDelta  struct{} // D3
	ActivityDelta struct{} // D4

	SchemaDelta struct {
		FromHash string   `json:"from_hash"`
		ToHash   string   `json:"to_hash"`
		Changes  []Change `json:"changes"`
	}
)

func (d *SchemaDelta) IsEmpty() bool { return d == nil || len(d.Changes) == 0 }

// Tagged union: Type selects which detail pointer is populated, so the cloud
// matches on Type + typed detail, never on prose.
type (
	Change struct {
		Type   ChangeType `json:"type"`
		Object ObjectRef  `json:"object"`

		Column     *ColumnChange     `json:"column,omitempty"`
		Index      *IndexChange      `json:"index,omitempty"`
		Constraint *ConstraintChange `json:"constraint,omitempty"`
		Function   *FunctionChange   `json:"function,omitempty"`
		RLS        *RLSChange        `json:"rls,omitempty"`
		Rename     *RenameChange     `json:"rename,omitempty"`

		Note string `json:"note,omitempty"` // human hint, not load-bearing
	}

	ChangeType string

	// OID-first identity; survives rename. 0 when the source carries none
	ObjectRef struct {
		Kind   string  `json:"kind"`
		Schema *string `json:"schema,omitempty"`
		Name   string  `json:"name"`
		OID    uint32  `json:"oid,omitempty"`
	}

	ColumnChange struct {
		Name        string      `json:"name"`
		Nullable    *bool       `json:"nullable,omitempty"`
		DefaultKind DefaultKind `json:"default_kind,omitempty"`
		FromType    string      `json:"from_type,omitempty"`
		ToType      string      `json:"to_type,omitempty"`
		Widening    *bool       `json:"widening,omitempty"`
		FromDefault *string     `json:"from_default,omitempty"`
		ToDefault   *string     `json:"to_default,omitempty"`
	}

	IndexChange struct {
		Name            string `json:"name"`
		Unique          bool   `json:"unique,omitempty"`
		BacksConstraint bool   `json:"backs_constraint,omitempty"`
	}

	ConstraintChange struct {
		Name string                  `json:"name"`
		Kind snapshot.ConstraintKind `json:"kind"`
	}

	FunctionChange struct {
		SecurityDefiner *bool  `json:"security_definer,omitempty"` // true=added
		FromVolatility  string `json:"from_volatility,omitempty"`
		ToVolatility    string `json:"to_volatility,omitempty"`
		FromReturnType  string `json:"from_return_type,omitempty"`
		ToReturnType    string `json:"to_return_type,omitempty"`
	}

	RLSChange struct {
		Enabled bool `json:"enabled"`
	}

	RenameChange struct {
		FromName string `json:"from_name"`
		ToName   string `json:"to_name"`
	}

	DefaultKind string
)

const (
	TableAdded          ChangeType = "table_added"
	TableDropped        ChangeType = "table_dropped"
	TableRenamed        ChangeType = "table_renamed"
	TableCommentChanged ChangeType = "table_comment_changed"

	ColumnAdded          ChangeType = "column_added"
	ColumnDropped        ChangeType = "column_dropped"
	ColumnTypeChanged    ChangeType = "column_type_changed"
	ColumnSetNotNull     ChangeType = "column_set_not_null"
	ColumnDropNotNull    ChangeType = "column_drop_not_null"
	ColumnDefaultChanged ChangeType = "column_default_changed"
	ColumnCommentChanged ChangeType = "column_comment_changed"

	IndexAdded   ChangeType = "index_added"
	IndexDropped ChangeType = "index_dropped"

	ConstraintAdded   ChangeType = "constraint_added"
	ConstraintDropped ChangeType = "constraint_dropped"

	RLSToggled ChangeType = "rls_toggled"

	ViewAdded             ChangeType = "view_added"
	ViewDropped           ChangeType = "view_dropped"
	ViewDefinitionChanged ChangeType = "view_definition_changed"

	FunctionAdded         ChangeType = "function_added"
	FunctionDropped       ChangeType = "function_dropped"
	FuncSecurityDefiner   ChangeType = "func_security_definer"
	FuncVolatilityChanged ChangeType = "func_volatility_changed"
	FuncReturnTypeChanged ChangeType = "func_return_type_changed"

	// enum/domain/composite/extension: no risk-relevant detail
	ObjectAdded    ChangeType = "object_added"
	ObjectDropped  ChangeType = "object_dropped"
	ObjectModified ChangeType = "object_modified"
)

const (
	DefaultNone     DefaultKind = "none"
	DefaultConstant DefaultKind = "constant"
	DefaultVolatile DefaultKind = "volatile"
)

// Folds the typed vocabulary back to added/removed/modified.
func (t ChangeType) Category() string {
	switch t {
	case TableAdded, ColumnAdded, IndexAdded, ConstraintAdded, ViewAdded, FunctionAdded, ObjectAdded:
		return "added"
	case TableDropped, ColumnDropped, IndexDropped, ConstraintDropped, ViewDropped, FunctionDropped, ObjectDropped:
		return "removed"
	default:
		return "modified"
	}
}

// error is for symmetry with DiffPlanner/DiffActivity (D3/D4); schema diffing never fails.
func DiffSchema(from, to *snapshot.SchemaSnapshot) (*SchemaDelta, error) {
	var changes []Change

	diffTables(from.Tables, to.Tables, &changes)
	diffViews(from.Views, to.Views, &changes)
	diffFunctions(from.Functions, to.Functions, &changes)
	diffGeneric("enum", from.Enums, to.Enums, func(e snapshot.EnumType) string { return e.Schema + "." + e.Name }, &changes)
	diffGeneric("domain", from.Domains, to.Domains, func(d snapshot.DomainType) string { return d.Schema + "." + d.Name }, &changes)
	diffGeneric("composite_type", from.Composites, to.Composites, func(c snapshot.CompositeType) string { return c.Schema + "." + c.Name }, &changes)
	diffGeneric("extension", from.Extensions, to.Extensions, func(e snapshot.Extension) string { return e.Name }, &changes)

	sortChanges(changes)
	return &SchemaDelta{FromHash: from.ContentHash, ToHash: to.ContentHash, Changes: changes}, nil
}

func tableRef(t *snapshot.Table) ObjectRef {
	s := t.Schema
	return ObjectRef{Kind: "table", Schema: &s, Name: t.Name, OID: t.OID}
}

func diffTables(from, to []snapshot.Table, changes *[]Change) {
	type key struct{ schema, name string }
	fromMap := make(map[key]*snapshot.Table, len(from))
	for i := range from {
		fromMap[key{from[i].Schema, from[i].Name}] = &from[i]
	}
	toMap := make(map[key]*snapshot.Table, len(to))
	for i := range to {
		toMap[key{to[i].Schema, to[i].Name}] = &to[i]
	}

	paired := make(map[*snapshot.Table]*snapshot.Table, len(from))
	pairedTo := make(map[*snapshot.Table]bool, len(to))
	for k, old := range fromMap {
		if nw, ok := toMap[k]; ok {
			paired[old] = nw
			pairedTo[nw] = true
		}
	}
	toByOID := make(map[uint32]*snapshot.Table, len(to))
	for i := range to {
		if to[i].OID != 0 {
			toByOID[to[i].OID] = &to[i]
		}
	}
	for i := range from {
		old := &from[i]
		if _, done := paired[old]; done || old.OID == 0 {
			continue
		}
		nw, ok := toByOID[old.OID]
		if !ok || pairedTo[nw] {
			continue // OID gone, or its to-table already matched by name
		}
		paired[old] = nw
		pairedTo[nw] = true
		*changes = append(*changes, Change{Type: TableRenamed, Object: tableRef(nw), Rename: &RenameChange{
			FromName: old.Qual().String(), ToName: nw.Qual().String(),
		}})
	}

	for i := range to {
		if nw := &to[i]; !pairedTo[nw] {
			*changes = append(*changes, Change{Type: TableAdded, Object: tableRef(nw), Note: plural(len(nw.Columns), "column", "columns")})
		}
	}
	for i := range from {
		old := &from[i]
		if nw, ok := paired[old]; ok {
			diffTableBody(old, nw, changes)
		} else {
			*changes = append(*changes, Change{Type: TableDropped, Object: tableRef(old)})
		}
	}
}

func diffTableBody(old, nw *snapshot.Table, changes *[]Change) {
	ref := tableRef(nw) // nw so a renamed table's body changes attach under the new name

	oldCols := indexBy(old.Columns, func(c snapshot.Column) string { return c.Name })
	newCols := indexBy(nw.Columns, func(c snapshot.Column) string { return c.Name })

	for name, col := range newCols {
		if _, ok := oldCols[name]; !ok {
			nullable := col.Nullable
			*changes = append(*changes, Change{Type: ColumnAdded, Object: ref, Column: &ColumnChange{
				Name: name, Nullable: &nullable, DefaultKind: classifyDefault(col.Default),
			}})
		}
	}
	for name := range oldCols {
		if _, ok := newCols[name]; !ok {
			*changes = append(*changes, Change{Type: ColumnDropped, Object: ref, Column: &ColumnChange{Name: name}})
		}
	}
	for name, oc := range oldCols {
		nc, ok := newCols[name]
		if !ok {
			continue
		}
		if oc.TypeName != nc.TypeName {
			w := typeWidens(oc.TypeName, nc.TypeName)
			*changes = append(*changes, Change{Type: ColumnTypeChanged, Object: ref, Column: &ColumnChange{
				Name: name, FromType: oc.TypeName, ToType: nc.TypeName, Widening: &w,
			}})
		}
		if oc.Nullable != nc.Nullable {
			t := ColumnSetNotNull // nullable → NOT NULL
			if nc.Nullable {
				t = ColumnDropNotNull
			}
			*changes = append(*changes, Change{Type: t, Object: ref, Column: &ColumnChange{Name: name}})
		}
		if ptrStr(oc.Default) != ptrStr(nc.Default) {
			*changes = append(*changes, Change{Type: ColumnDefaultChanged, Object: ref, Column: &ColumnChange{
				Name: name, FromDefault: oc.Default, ToDefault: nc.Default,
			}})
		}
		if ptrStr(oc.Comment) != ptrStr(nc.Comment) {
			*changes = append(*changes, Change{Type: ColumnCommentChanged, Object: ref, Column: &ColumnChange{Name: name}})
		}
	}

	diffIndexes(old.Indexes, nw.Indexes, ref, changes)
	diffConstraints(old.Constraints, nw.Constraints, ref, changes)

	if ptrStr(old.Comment) != ptrStr(nw.Comment) {
		*changes = append(*changes, Change{Type: TableCommentChanged, Object: ref})
	}
	if old.RLSEnabled != nw.RLSEnabled {
		*changes = append(*changes, Change{Type: RLSToggled, Object: ref, RLS: &RLSChange{Enabled: nw.RLSEnabled}})
	}
}

func diffIndexes(old, nw []snapshot.Index, ref ObjectRef, changes *[]Change) {
	oldMap := indexBy(old, func(i snapshot.Index) string { return i.Name })
	newMap := indexBy(nw, func(i snapshot.Index) string { return i.Name })
	for name, idx := range newMap {
		if _, ok := oldMap[name]; !ok {
			*changes = append(*changes, Change{Type: IndexAdded, Object: ref, Index: &IndexChange{
				Name: name, Unique: idx.IsUnique, BacksConstraint: idx.BacksConstraint,
			}})
		}
	}
	for name, idx := range oldMap {
		if _, ok := newMap[name]; !ok {
			*changes = append(*changes, Change{Type: IndexDropped, Object: ref, Index: &IndexChange{
				Name: name, Unique: idx.IsUnique, BacksConstraint: idx.BacksConstraint,
			}})
		}
	}
}

func diffConstraints(old, nw []snapshot.Constraint, ref ObjectRef, changes *[]Change) {
	oldMap := indexBy(old, func(c snapshot.Constraint) string { return c.Name })
	newMap := indexBy(nw, func(c snapshot.Constraint) string { return c.Name })
	for name, c := range newMap {
		if _, ok := oldMap[name]; !ok {
			*changes = append(*changes, Change{Type: ConstraintAdded, Object: ref, Constraint: &ConstraintChange{Name: name, Kind: c.Kind}})
		}
	}
	for name, c := range oldMap {
		if _, ok := newMap[name]; !ok {
			*changes = append(*changes, Change{Type: ConstraintDropped, Object: ref, Constraint: &ConstraintChange{Name: name, Kind: c.Kind}})
		}
	}
}

func diffViews(from, to []snapshot.View, changes *[]Change) {
	type key struct{ schema, name string }
	fromMap := make(map[key]*snapshot.View, len(from))
	for i := range from {
		fromMap[key{from[i].Schema, from[i].Name}] = &from[i]
	}
	toMap := make(map[key]*snapshot.View, len(to))
	for i := range to {
		toMap[key{to[i].Schema, to[i].Name}] = &to[i]
	}
	viewRef := func(v *snapshot.View) ObjectRef {
		s := v.Schema
		return ObjectRef{Kind: "view", Schema: &s, Name: v.Name}
	}
	for k, v := range toMap {
		if _, ok := fromMap[k]; !ok {
			*changes = append(*changes, Change{Type: ViewAdded, Object: viewRef(v)})
		}
	}
	for k, v := range fromMap {
		if _, ok := toMap[k]; !ok {
			*changes = append(*changes, Change{Type: ViewDropped, Object: viewRef(v)})
		}
	}
	for k, old := range fromMap {
		if nw, ok := toMap[k]; ok && old.Definition != nw.Definition {
			*changes = append(*changes, Change{Type: ViewDefinitionChanged, Object: viewRef(old)})
		}
	}
}

func diffFunctions(from, to []snapshot.Function, changes *[]Change) {
	type key struct{ schema, name, args string }
	fromMap := make(map[key]*snapshot.Function, len(from))
	for i := range from {
		fromMap[key{from[i].Schema, from[i].Name, from[i].IdentityArgs}] = &from[i]
	}
	toMap := make(map[key]*snapshot.Function, len(to))
	for i := range to {
		toMap[key{to[i].Schema, to[i].Name, to[i].IdentityArgs}] = &to[i]
	}
	fnRef := func(f *snapshot.Function) ObjectRef {
		s := f.Schema
		return ObjectRef{Kind: "function", Schema: &s, Name: f.Name + "(" + f.IdentityArgs + ")"}
	}
	for k, f := range toMap {
		if _, ok := fromMap[k]; !ok {
			*changes = append(*changes, Change{Type: FunctionAdded, Object: fnRef(f)})
		}
	}
	for k, f := range fromMap {
		if _, ok := toMap[k]; !ok {
			*changes = append(*changes, Change{Type: FunctionDropped, Object: fnRef(f)})
		}
	}
	for k, old := range fromMap {
		nw, ok := toMap[k]
		if !ok {
			continue
		}
		ref := fnRef(old)
		if old.SecurityDefiner != nw.SecurityDefiner {
			added := nw.SecurityDefiner
			*changes = append(*changes, Change{Type: FuncSecurityDefiner, Object: ref, Function: &FunctionChange{SecurityDefiner: &added}})
		}
		if old.Volatility != nw.Volatility {
			*changes = append(*changes, Change{Type: FuncVolatilityChanged, Object: ref, Function: &FunctionChange{
				FromVolatility: string(old.Volatility), ToVolatility: string(nw.Volatility),
			}})
		}
		if old.ReturnType != nw.ReturnType {
			*changes = append(*changes, Change{Type: FuncReturnTypeChanged, Object: ref, Function: &FunctionChange{
				FromReturnType: old.ReturnType, ToReturnType: nw.ReturnType,
			}})
		}
	}
}

// modified = JSON inequality (definition changed).
func diffGeneric[T any](kind string, from, to []T, keyFn func(T) string, changes *[]Change) {
	fromMap := jsonIndex(from, keyFn)
	toMap := jsonIndex(to, keyFn)
	for k := range toMap {
		if _, ok := fromMap[k]; !ok {
			*changes = append(*changes, Change{Type: ObjectAdded, Object: ObjectRef{Kind: kind, Name: k}})
		}
	}
	for k := range fromMap {
		if _, ok := toMap[k]; !ok {
			*changes = append(*changes, Change{Type: ObjectDropped, Object: ObjectRef{Kind: kind, Name: k}})
		}
	}
	for k, oldJSON := range fromMap {
		if newJSON, ok := toMap[k]; ok && oldJSON != newJSON {
			*changes = append(*changes, Change{Type: ObjectModified, Object: ObjectRef{Kind: kind, Name: k}})
		}
	}
}
