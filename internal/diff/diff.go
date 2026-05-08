package diff

import (
	"encoding/json"
	"fmt"

	"github.com/boringsql/dryrun/internal/schema"
)

type (
	SchemaChangeset struct {
		FromHash      string   `json:"from_hash"`
		ToHash        string   `json:"to_hash"`
		FromTimestamp string   `json:"from_timestamp"`
		ToTimestamp   string   `json:"to_timestamp"`
		Changes       []Change `json:"changes"`
	}

	Change struct {
		Kind       ChangeKind `json:"kind"`
		ObjectType string     `json:"object_type"`
		Schema     *string    `json:"schema,omitempty"`
		Name       string     `json:"name"`
		Details    []string   `json:"details"`
	}

	ChangeKind string
)

const (
	Added    ChangeKind = "added"
	Removed  ChangeKind = "removed"
	Modified ChangeKind = "modified"
)

func (cs *SchemaChangeset) IsEmpty() bool {
	return len(cs.Changes) == 0
}

func DiffSchemas(from, to *schema.SchemaSnapshot) *SchemaChangeset {
	var changes []Change

	diffTables(from.Tables, to.Tables, &changes)
	diffViews(from.Views, to.Views, &changes)
	diffFunctions(from.Functions, to.Functions, &changes)
	diffNamed("enum", from.Enums, to.Enums, &changes, func(e schema.EnumType) string {
		return e.Schema + "." + e.Name
	})
	diffNamed("domain", from.Domains, to.Domains, &changes, func(d schema.DomainType) string {
		return d.Schema + "." + d.Name
	})
	diffNamed("composite_type", from.Composites, to.Composites, &changes, func(c schema.CompositeType) string {
		return c.Schema + "." + c.Name
	})
	diffNamed("extension", from.Extensions, to.Extensions, &changes, func(e schema.Extension) string {
		return e.Name
	})

	return &SchemaChangeset{
		FromHash:      from.ContentHash,
		ToHash:        to.ContentHash,
		FromTimestamp: from.Timestamp.Format("2006-01-02T15:04:05Z07:00"),
		ToTimestamp:   to.Timestamp.Format("2006-01-02T15:04:05Z07:00"),
		Changes:       changes,
	}
}

func strPtr(s string) *string { return &s }

func diffTables(from, to []schema.Table, changes *[]Change) {
	type tableKey struct{ schema, name string }

	fromMap := make(map[tableKey]*schema.Table, len(from))
	for i := range from {
		fromMap[tableKey{from[i].Schema, from[i].Name}] = &from[i]
	}
	toMap := make(map[tableKey]*schema.Table, len(to))
	for i := range to {
		toMap[tableKey{to[i].Schema, to[i].Name}] = &to[i]
	}

	for k, t := range toMap {
		if _, ok := fromMap[k]; !ok {
			*changes = append(*changes, Change{
				Kind:       Added,
				ObjectType: "table",
				Schema:     strPtr(t.Schema),
				Name:       t.Name,
				Details:    []string{fmt.Sprintf("%d columns", len(t.Columns))},
			})
		}
	}
	for k, t := range fromMap {
		if _, ok := toMap[k]; !ok {
			*changes = append(*changes, Change{
				Kind:       Removed,
				ObjectType: "table",
				Schema:     strPtr(t.Schema),
				Name:       t.Name,
			})
		}
	}
	for k, old := range fromMap {
		if new, ok := toMap[k]; ok {
			details := diffTableDetails(old, new)
			if len(details) > 0 {
				*changes = append(*changes, Change{
					Kind:       Modified,
					ObjectType: "table",
					Schema:     strPtr(old.Schema),
					Name:       old.Name,
					Details:    details,
				})
			}
		}
	}
}

func diffTableDetails(old, new *schema.Table) []string {
	var details []string

	oldCols := make(map[string]*schema.Column, len(old.Columns))
	for i := range old.Columns {
		oldCols[old.Columns[i].Name] = &old.Columns[i]
	}
	newCols := make(map[string]*schema.Column, len(new.Columns))
	for i := range new.Columns {
		newCols[new.Columns[i].Name] = &new.Columns[i]
	}

	for name, col := range newCols {
		if _, ok := oldCols[name]; !ok {
			details = append(details, fmt.Sprintf("column added: %s (%s)", name, col.TypeName))
		}
	}
	for name := range oldCols {
		if _, ok := newCols[name]; !ok {
			details = append(details, fmt.Sprintf("column removed: %s", name))
		}
	}
	for name, oldCol := range oldCols {
		newCol, ok := newCols[name]
		if !ok {
			continue
		}
		if oldCol.TypeName != newCol.TypeName {
			details = append(details, fmt.Sprintf("column %s: type changed %s -> %s", name, oldCol.TypeName, newCol.TypeName))
		}
		if oldCol.Nullable != newCol.Nullable {
			change := "NOT NULL added"
			if newCol.Nullable {
				change = "NOT NULL removed"
			}
			details = append(details, fmt.Sprintf("column %s: %s", name, change))
		}
		if ptrStr(oldCol.Default) != ptrStr(newCol.Default) {
			details = append(details, fmt.Sprintf("column %s: default changed %v -> %v", name, oldCol.Default, newCol.Default))
		}
		if ptrStr(oldCol.Comment) != ptrStr(newCol.Comment) {
			details = append(details, fmt.Sprintf("column %s: comment changed %v -> %v", name, oldCol.Comment, newCol.Comment))
		}
	}

	diffNamedItems("constraint", old.Constraints, new.Constraints, &details, func(c schema.Constraint) string { return c.Name })
	diffNamedItems("index", old.Indexes, new.Indexes, &details, func(i schema.Index) string { return i.Name })

	if ptrStr(old.Comment) != ptrStr(new.Comment) {
		details = append(details, fmt.Sprintf("comment changed: %v -> %v", old.Comment, new.Comment))
	}
	if old.RLSEnabled != new.RLSEnabled {
		state := "enabled"
		if !new.RLSEnabled {
			state = "disabled"
		}
		details = append(details, fmt.Sprintf("RLS %s", state))
	}

	return details
}

func diffNamedItems[T any](label string, old, new []T, details *[]string, nameFn func(T) string) {
	oldNames := make(map[string]bool, len(old))
	for _, item := range old {
		oldNames[nameFn(item)] = true
	}
	newNames := make(map[string]bool, len(new))
	for _, item := range new {
		newNames[nameFn(item)] = true
	}

	for name := range newNames {
		if !oldNames[name] {
			*details = append(*details, fmt.Sprintf("%s added: %s", label, name))
		}
	}
	for name := range oldNames {
		if !newNames[name] {
			*details = append(*details, fmt.Sprintf("%s removed: %s", label, name))
		}
	}
}

func diffViews(from, to []schema.View, changes *[]Change) {
	type vKey struct{ schema, name string }

	fromMap := make(map[vKey]*schema.View, len(from))
	for i := range from {
		fromMap[vKey{from[i].Schema, from[i].Name}] = &from[i]
	}
	toMap := make(map[vKey]*schema.View, len(to))
	for i := range to {
		toMap[vKey{to[i].Schema, to[i].Name}] = &to[i]
	}

	for k, v := range toMap {
		if _, ok := fromMap[k]; !ok {
			*changes = append(*changes, Change{
				Kind:       Added,
				ObjectType: "view",
				Schema:     strPtr(v.Schema),
				Name:       v.Name,
			})
		}
	}
	for k, v := range fromMap {
		if _, ok := toMap[k]; !ok {
			*changes = append(*changes, Change{
				Kind:       Removed,
				ObjectType: "view",
				Schema:     strPtr(v.Schema),
				Name:       v.Name,
			})
		}
	}
	for k, old := range fromMap {
		if new, ok := toMap[k]; ok {
			if old.Definition != new.Definition {
				*changes = append(*changes, Change{
					Kind:       Modified,
					ObjectType: "view",
					Schema:     strPtr(old.Schema),
					Name:       old.Name,
					Details:    []string{"definition changed"},
				})
			}
		}
	}
}

func diffFunctions(from, to []schema.Function, changes *[]Change) {
	type fKey struct{ schema, name, args string }

	fromMap := make(map[fKey]*schema.Function, len(from))
	for i := range from {
		fromMap[fKey{from[i].Schema, from[i].Name, from[i].IdentityArgs}] = &from[i]
	}
	toMap := make(map[fKey]*schema.Function, len(to))
	for i := range to {
		toMap[fKey{to[i].Schema, to[i].Name, to[i].IdentityArgs}] = &to[i]
	}

	for k, f := range toMap {
		if _, ok := fromMap[k]; !ok {
			*changes = append(*changes, Change{
				Kind:       Added,
				ObjectType: "function",
				Schema:     strPtr(f.Schema),
				Name:       fmt.Sprintf("%s(%s)", f.Name, f.IdentityArgs),
			})
		}
	}
	for k, f := range fromMap {
		if _, ok := toMap[k]; !ok {
			*changes = append(*changes, Change{
				Kind:       Removed,
				ObjectType: "function",
				Schema:     strPtr(f.Schema),
				Name:       fmt.Sprintf("%s(%s)", f.Name, f.IdentityArgs),
			})
		}
	}
	for k, old := range fromMap {
		new, ok := toMap[k]
		if !ok {
			continue
		}
		var details []string
		if old.ReturnType != new.ReturnType {
			details = append(details, fmt.Sprintf("return type: %s -> %s", old.ReturnType, new.ReturnType))
		}
		if old.Volatility != new.Volatility {
			details = append(details, fmt.Sprintf("volatility: %s -> %s", old.Volatility, new.Volatility))
		}
		if old.SecurityDefiner != new.SecurityDefiner {
			state := "SECURITY DEFINER added"
			if !new.SecurityDefiner {
				state = "SECURITY DEFINER removed"
			}
			details = append(details, state)
		}
		if len(details) > 0 {
			*changes = append(*changes, Change{
				Kind:       Modified,
				ObjectType: "function",
				Schema:     strPtr(old.Schema),
				Name:       fmt.Sprintf("%s(%s)", old.Name, old.IdentityArgs),
				Details:    details,
			})
		}
	}
}

func diffNamed[T any](objectType string, from, to []T, changes *[]Change, keyFn func(T) string) {
	fromMap := make(map[string]T, len(from))
	for _, x := range from {
		fromMap[keyFn(x)] = x
	}
	toMap := make(map[string]T, len(to))
	for _, x := range to {
		toMap[keyFn(x)] = x
	}

	for key := range toMap {
		if _, ok := fromMap[key]; !ok {
			*changes = append(*changes, Change{
				Kind:       Added,
				ObjectType: objectType,
				Name:       key,
			})
		}
	}
	for key := range fromMap {
		if _, ok := toMap[key]; !ok {
			*changes = append(*changes, Change{
				Kind:       Removed,
				ObjectType: objectType,
				Name:       key,
			})
		}
	}
	for key, old := range fromMap {
		new, ok := toMap[key]
		if !ok {
			continue
		}
		oldJSON, _ := json.Marshal(old)
		newJSON, _ := json.Marshal(new)
		if string(oldJSON) != string(newJSON) {
			*changes = append(*changes, Change{
				Kind:       Modified,
				ObjectType: objectType,
				Name:       key,
				Details:    []string{"definition changed"},
			})
		}
	}
}

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
