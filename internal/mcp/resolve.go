package mcp

import (
	"fmt"
	"sort"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
)

type (
	// A resolved table plus the schema and name it actually resolved to, which
	// is not always what the caller passed.
	tableRef struct {
		Table  *schema.Table
		Schema string
		Name   string
		Note   string
	}
)

const maxNearMatches = 5

// Order matters. A table name may legally contain a dot ("foo.bar"), so the
// literal argument is tried before any split; an explicit schema is never
// overridden by one; and a bare name that lives in several schemas is reported
// rather than guessed.
func resolveTable(snap *schema.SchemaSnapshot, req mcp.CallToolRequest) (tableRef, *mcp.CallToolResult) {
	raw := getArg(req, "table")
	if raw == "" {
		return tableRef{}, errResult("table argument is required")
	}
	explicit := getArg(req, "schema")
	lookIn := explicit
	if lookIn == "" {
		lookIn = "public"
	}

	if t := findTable(snap, lookIn, raw); t != nil {
		return tableRef{Table: t, Schema: lookIn, Name: raw}, nil
	}

	// split at the first dot only: a trailing dotted name still resolves
	// (app."foo.bar"), and the schema half is the part that cannot contain one
	if i := strings.Index(raw, "."); i > 0 {
		schemaPart, namePart := raw[:i], raw[i+1:]
		if t := findTable(snap, schemaPart, namePart); t != nil {
			// the literal name was tried first and does not exist, so a
			// qualified argument is the more specific of the two and wins
			note := ""
			if explicit != "" && schemaPart != explicit {
				note = fmt.Sprintf("resolved to %s.%s; the qualified table argument outranks schema=%s.",
					schemaPart, namePart, explicit)
			}
			return tableRef{t, schemaPart, namePart, note}, nil
		}
	}

	if explicit == "" {
		var hits []*schema.Table
		for i := range snap.Tables {
			if snap.Tables[i].Name == raw {
				hits = append(hits, &snap.Tables[i])
			}
		}
		if len(hits) == 1 {
			return tableRef{Table: hits[0], Schema: hits[0].Schema, Name: hits[0].Name}, nil
		}
		if len(hits) > 1 {
			return tableRef{}, errResult(fmt.Sprintf(
				"table '%s' exists in %d schemas: %s. pass schema= to choose one.",
				raw, len(hits), strings.Join(qualifiedNames(hits), ", ")))
		}
	}

	return tableRef{}, errResult(notFoundMessage(snap, lookIn, raw))
}

// Match is exact: relname is stored as PostgreSQL folded it at creation, so
// folding again here would resolve "Orders" onto orders, which can coexist.
func findTable(snap *schema.SchemaSnapshot, schemaName, name string) *schema.Table {
	for i := range snap.Tables {
		if snap.Tables[i].Name == name && snap.Tables[i].Schema == schemaName {
			return &snap.Tables[i]
		}
	}
	return nil
}

func qualifiedNames(tables []*schema.Table) []string {
	out := make([]string, len(tables))
	for i, t := range tables {
		out[i] = t.Schema + "." + t.Name
	}
	sort.Strings(out)
	return out
}

// A miss ends the agent's line of enquiry unless it carries somewhere to go.
func notFoundMessage(snap *schema.SchemaSnapshot, schemaName, raw string) string {
	msg := fmt.Sprintf("table '%s.%s' not found", schemaName, raw)
	if v := findView(snap, raw); v != "" {
		return msg + fmt.Sprintf("; %s is a view, and describe_table covers tables only", v)
	}
	if near := nearMatches(snap, raw); len(near) > 0 {
		msg += "; did you mean " + strings.Join(near, ", ")
	}
	return msg + fmt.Sprintf(`; find_objects {"query":"%s"} searches every schema`, searchTerm(raw))
}

// A view read out of find_objects is the likeliest remaining miss.
func findView(snap *schema.SchemaSnapshot, raw string) string {
	name := searchTerm(raw)
	for _, v := range snap.Views {
		if v.Name == name {
			return v.Schema + "." + v.Name
		}
	}
	return ""
}

// The searchable part of a name the caller may have qualified already.
func searchTerm(raw string) string {
	if i := strings.LastIndex(raw, "."); i >= 0 && i < len(raw)-1 {
		return raw[i+1:]
	}
	return raw
}

func nearMatches(snap *schema.SchemaSnapshot, raw string) []string {
	term := strings.ToLower(searchTerm(raw))
	if term == "" {
		return nil
	}
	type nearMatch struct {
		qualified string
		distance  int
	}
	var hits []nearMatch
	for i := range snap.Tables {
		name := strings.ToLower(snap.Tables[i].Name)
		if !strings.Contains(name, term) && !strings.Contains(term, name) {
			continue
		}
		// a short table name is a substring of half the schema, so rank by how
		// much of the name the term accounts for before capping
		d := len(name) - len(term)
		if d < 0 {
			d = -d
		}
		hits = append(hits, nearMatch{snap.Tables[i].Schema + "." + snap.Tables[i].Name, d})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].distance != hits[j].distance {
			return hits[i].distance < hits[j].distance
		}
		return hits[i].qualified < hits[j].qualified
	})
	if len(hits) > maxNearMatches {
		hits = hits[:maxNearMatches]
	}
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.qualified
	}
	return out
}

// Resolves a filter without erroring: a filter may match nothing. The note
// explains why; callers emit it only for an empty result.
func resolveTableFilter(snap *schema.SchemaSnapshot, schemaF, tableF string) (string, string, string) {
	if tableF == "" || tableExists(snap, schemaF, tableF) {
		return schemaF, tableF, ""
	}
	if i := strings.Index(tableF, "."); i > 0 {
		schemaPart, namePart := tableF[:i], tableF[i+1:]
		if findTable(snap, schemaPart, namePart) != nil {
			if schemaF == "" || schemaF == schemaPart {
				return schemaPart, namePart, ""
			}
			// don't widen the scope silently; match nothing and say why
			return schemaF, tableF, fmt.Sprintf(
				"schema=%s and table=%s name different schemas, so this filter matched nothing.",
				schemaF, tableF)
		}
	}
	return schemaF, tableF, missNote(snap, schemaF, tableF)
}

func missNote(snap *schema.SchemaSnapshot, schemaF, tableF string) string {
	qualified := tableF
	if schemaF != "" {
		qualified = schemaF + "." + tableF
	}
	note := fmt.Sprintf("no table named %s is in the snapshot, so this filter matched nothing", qualified)
	if near := nearMatches(snap, tableF); len(near) > 0 {
		note += "; did you mean " + strings.Join(near, ", ")
	}
	return note + fmt.Sprintf(`; find_objects {"query":"%s"} searches every schema.`, searchTerm(tableF))
}

func tableExists(snap *schema.SchemaSnapshot, schemaF, name string) bool {
	for i := range snap.Tables {
		if snap.Tables[i].Name == name && (schemaF == "" || snap.Tables[i].Schema == schemaF) {
			return true
		}
	}
	return false
}
