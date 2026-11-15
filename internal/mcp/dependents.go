package mcp

import (
	"fmt"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/ddl"
)

type (
	dependentVia struct {
		Schema  string   `json:"schema"`
		Name    string   `json:"name"`
		Columns []string `json:"columns,omitempty"`
	}

	dependentView struct {
		Schema  string         `json:"schema"`
		Name    string         `json:"name"`
		Kind    string         `json:"kind"`
		Depth   int            `json:"depth"`
		Columns []string       `json:"columns,omitempty"`
		Via     []dependentVia `json:"via,omitempty"`
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
		via := make([]dependentVia, len(v.Via))
		for j, p := range v.Via {
			via[j] = dependentVia{Schema: p.Schema, Name: p.Name, Columns: p.Columns}
		}
		out.Views[i] = dependentView{Schema: v.Schema, Name: v.Name, Kind: kind, Depth: v.Depth, Columns: v.Columns, Via: via}
	}
	switch {
	case omitted > 0:
		return out, fmt.Sprintf("Omitted %d of %d dependents. Re-run with limit=0 for all.", omitted, len(views))
	case len(views) == 0:
		return out, "No captured views read this table."
	}
	return out, ""
}
