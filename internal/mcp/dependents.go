package mcp

import (
	"fmt"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/ddl"
)

type (
	dependentView struct {
		Schema  string   `json:"schema"`
		Name    string   `json:"name"`
		Kind    string   `json:"kind"`
		Columns []string `json:"columns,omitempty"`
	}

	tableDependents struct {
		Views     []dependentView `json:"views"`
		Count     int             `json:"count"`
		Omitted   int             `json:"omitted,omitempty"`
		Available bool            `json:"available"`
	}
)

// buildDependents is describe_table's dependents section: views reading this table.
func buildDependents(snap *schema.SchemaSnapshot, t *schema.Table, max int) (tableDependents, string) {
	if snap.DependenciesHash == "" {
		return tableDependents{Views: []dependentView{}, Available: false},
			"This snapshot predates dependency capture, so views reading this table are unknown."
	}
	views := ddl.ViewDependents(snap, t)
	kept, omitted := capItems(views, max)
	out := tableDependents{
		Views:     make([]dependentView, len(kept)),
		Count:     len(views),
		Omitted:   omitted,
		Available: true,
	}
	for i, v := range kept {
		kind := "view"
		if v.Materialized {
			kind = "materialized_view"
		}
		out.Views[i] = dependentView{Schema: v.Schema, Name: v.Name, Kind: kind, Columns: v.Columns}
	}
	switch {
	case omitted > 0:
		return out, fmt.Sprintf("Omitted %d of %d dependents. Re-run with limit=0 for all.", omitted, len(views))
	case len(views) == 0:
		return out, "No captured views read this table."
	}
	return out, ""
}
