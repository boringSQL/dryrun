package query

import (
	"fmt"

	"github.com/boringsql/dryrun/internal/schema"
)

type (
	ValidationResult struct {
		Valid               bool                `json:"valid"`
		Errors              []string            `json:"errors"`
		Warnings            []ValidationWarning `json:"warnings"`
		ReferencedObjects   []ReferencedTable   `json:"referenced_objects"`
		ResolvedStarColumns []ResolvedStar      `json:"resolved_star_columns"`
	}

	ValidationWarning struct {
		Severity WarningSeverity `json:"severity"`
		Message  string          `json:"message"`
	}

	WarningSeverity string

	ResolvedStar struct {
		Table   string   `json:"table"`
		Columns []string `json:"columns"`
	}
)

const (
	SeverityInfo    WarningSeverity = "info"
	SeverityWarning WarningSeverity = "warning"
	SeverityError   WarningSeverity = "error"
)

// parses SQL and validates references against the schema
func ValidateQuery(sql string, snap *schema.SchemaSnapshot) (*ValidationResult, error) {
	parsed, err := ParseSQL(sql)
	if err != nil {
		return nil, err
	}

	var (
		errors       []string
		warnings     []ValidationWarning
		resolvedStar []ResolvedStar
	)

	// check each referenced table exists
	for _, ref := range parsed.Info.Tables {
		tableName := ref.Name
		schemaName := "public"
		if ref.Schema != nil {
			schemaName = *ref.Schema
		}

		found := false
		for _, t := range snap.Tables {
			if t.Name == tableName && t.Schema == schemaName {
				found = true
				break
			}
		}
		if !found {
			isView := false
			for _, v := range snap.Views {
				if v.Name == tableName && v.Schema == schemaName {
					isView = true
					break
				}
			}
			if !isView {
				errors = append(errors, fmt.Sprintf(
					"table or view '%s.%s' does not exist", schemaName, tableName))
			}
		}
	}

	validateFilterColumns(parsed, snap, &errors)

	// resolve SELECT *
	if parsed.Info.HasSelectStar {
		for _, ref := range parsed.Info.Tables {
			schemaName := "public"
			if ref.Schema != nil {
				schemaName = *ref.Schema
			}
			for _, t := range snap.Tables {
				if t.Name == ref.Name && t.Schema == schemaName {
					cols := make([]string, len(t.Columns))
					for i, c := range t.Columns {
						cols[i] = c.Name
					}
					resolvedStar = append(resolvedStar, ResolvedStar{
						Table:   t.Schema + "." + t.Name,
						Columns: cols,
					})
					break
				}
			}
		}
	}

	detectAntipatterns(parsed, snap, &warnings)

	return &ValidationResult{
		Valid:               len(errors) == 0,
		Errors:              errors,
		Warnings:            warnings,
		ReferencedObjects:   parsed.Info.Tables,
		ResolvedStarColumns: resolvedStar,
	}, nil
}

func validateFilterColumns(parsed *ParsedQuery, snap *schema.SchemaSnapshot, errors *[]string) {
	for _, fc := range parsed.Info.FilterColumns {
		if fc.Table == nil {
			continue
		}
		alias := *fc.Table

		var ref *ReferencedTable
		for i := range parsed.Info.Tables {
			t := &parsed.Info.Tables[i]
			if (t.Alias != nil && *t.Alias == alias) || t.Name == alias {
				ref = t
				break
			}
		}
		if ref == nil {
			continue
		}

		schemaName := "public"
		if ref.Schema != nil {
			schemaName = *ref.Schema
		}
		for _, t := range snap.Tables {
			if t.Name == ref.Name && t.Schema == schemaName {
				found := false
				for _, c := range t.Columns {
					if c.Name == fc.Column {
						found = true
						break
					}
				}
				if !found {
					*errors = append(*errors, fmt.Sprintf(
						"column '%s' does not exist on table '%s.%s'",
						fc.Column, t.Schema, t.Name))
				}
				break
			}
		}
	}
}
