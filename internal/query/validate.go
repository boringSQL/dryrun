package query

import (
	"fmt"
	"slices"

	"github.com/boringsql/dryrun/internal/schema"
)

type (
	ValidationResult struct {
		Valid               bool                `json:"valid"`
		Errors              []string            `json:"errors"`
		Warnings            []ValidationWarning `json:"warnings"`
		ReferencedObjects   []ReferencedTable   `json:"referenced_objects"`
		ResolvedStarColumns []ResolvedStar      `json:"resolved_star_columns"`

		// present only when every error was a name with one obvious candidate;
		// the query below has the fixes applied and validates clean
		CorrectedSQL string `json:"corrected_sql,omitempty"`
		Fixes        []Fix  `json:"fixes,omitempty"`
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

	maxCorrectionRounds = 3
)

// parses SQL and validates references against the schema
func ValidateQuery(sql string, snap *schema.SchemaSnapshot) (*ValidationResult, error) {
	result, plan, err := validate(sql, snap)
	if err != nil || result.Valid {
		return result, err
	}

	// A wrong table name hides the column errors under it, so correcting is a
	// loop: fix what resolves, re-validate, look again. Only a pass that ends
	// with the query clean is ever handed back.
	cur, curPlan := sql, plan
	var applied []Fix

	for range maxCorrectionRounds {
		cands, fixes := correctOnce(cur, curPlan)
		if len(cands) == 0 {
			break
		}
		advanced := false
		for _, c := range cands {
			res, p, err := validate(c, snap)
			if err != nil {
				continue
			}
			cur, curPlan = c, p
			applied = append(applied, fixes...)
			advanced = true
			if res.Valid {
				result.CorrectedSQL, result.Fixes = cur, applied
			}
			break
		}
		if !advanced || result.CorrectedSQL != "" {
			break
		}
	}
	return result, nil
}

// correct() re-validates its own rewrite, so the correction pass must not
// recurse back into it.
func validate(sql string, snap *schema.SchemaSnapshot) (*ValidationResult, fixPlan, error) {
	var plan fixPlan

	parsed, err := ParseSQL(sql)
	if err != nil {
		return nil, plan, err
	}

	var (
		errors       []string
		warnings     []ValidationWarning
		resolvedStar []ResolvedStar
	)

	// check each referenced table exists
	for _, ref := range parsed.Info.Tables {
		if ref.Schema == nil && slices.Contains(parsed.Info.cteNames, ref.Name) {
			continue
		}
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
				plan.add(tableFix(snap, schemaName, ref))
			}
		}
	}

	validateFilterColumns(parsed, snap, &errors, &plan)

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
	}, plan, nil
}

func validateFilterColumns(parsed *ParsedQuery, snap *schema.SchemaSnapshot, errors *[]string, plan *fixPlan) {
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
					plan.add(columnFix(&t, alias, fc))
				}
				break
			}
		}
	}
}

// An unqualified name that exists in exactly one other schema is a missing
// qualifier, not a typo; that outranks any distance guess.
func tableFix(snap *schema.SchemaSnapshot, schemaName string, ref ReferencedTable) *pendingFix {
	written := ref.Name
	if ref.Schema != nil {
		written = *ref.Schema + "." + ref.Name
	}
	expect := make([]string, len(ref.locs))
	for i := range expect {
		expect[i] = written
	}
	mk := func(to string) *pendingFix {
		return &pendingFix{kind: "table", from: written, to: to, locs: ref.locs, expect: expect}
	}

	if ref.Schema == nil {
		var elsewhere []string
		for _, r := range relations(snap) {
			if r.name == ref.Name && r.schema != schemaName {
				elsewhere = append(elsewhere, r.schema)
			}
		}
		if len(elsewhere) == 1 {
			return mk(elsewhere[0] + "." + ref.Name)
		}
		if len(elsewhere) > 1 {
			return nil
		}
	}

	var pool []string
	for _, r := range relations(snap) {
		if r.schema == schemaName {
			pool = append(pool, r.name)
		}
	}
	cand, ok := bestCandidate(ref.Name, pool)
	if !ok {
		return nil
	}
	if ref.Schema != nil {
		return mk(*ref.Schema + "." + cand)
	}
	return mk(cand)
}

func columnFix(t *schema.Table, alias string, fc FilterColumn) *pendingFix {
	pool := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		pool[i] = c.Name
	}
	cand, ok := bestCandidate(fc.Column, pool)
	if !ok {
		return nil
	}
	return &pendingFix{
		kind:     "column",
		from:     fc.Column,
		to:       cand,
		locs:     []int32{fc.loc},
		lastOnly: true,
		expect:   []string{alias + "." + fc.Column},
	}
}

type relation struct{ schema, name string }

func relations(snap *schema.SchemaSnapshot) []relation {
	out := make([]relation, 0, len(snap.Tables)+len(snap.Views))
	for _, t := range snap.Tables {
		out = append(out, relation{t.Schema, t.Name})
	}
	for _, v := range snap.Views {
		out = append(out, relation{v.Schema, v.Name})
	}
	return out
}
