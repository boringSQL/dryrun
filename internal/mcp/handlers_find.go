package mcp

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
)

const (
	// maxMatchDetail caps every free-text field on the wire.
	maxMatchDetail = 160

	matchedName       = "name"
	matchedComment    = "comment"
	matchedDefinition = "definition"
	matchedLabels     = "labels"
)

type (
	matchField struct {
		on   string
		text string
	}

	candidate struct {
		match     objectMatch
		rank      int
		kindRank  int
		sortName  string
		rows      float64
		size      int64
		hasSizing bool
	}
)

// Match tiers: exact name > prefix > substring > prose hit.
const (
	rankExact = iota
	rankPrefix
	rankSubstring
	rankMetadata
)

var findObjectKinds = map[string]int{
	"table": 0, "column": 1, "index": 2,
	"view": 3, "materialized_view": 4, "function": 5, "enum": 6,
}

func (s *Server) handleFindObjects(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	query := getArg(req, "query")
	kind := getArg(req, "kind")
	if kind != "" {
		if _, ok := findObjectKinds[kind]; !ok {
			return errResult(fmt.Sprintf("unknown kind %q; one of: %s", kind, strings.Join(sortedKinds(), ", "))), nil
		}
	}
	// A bare call is a table inventory, not a dump of every column.
	if kind == "" && query == "" {
		kind = "table"
	}
	schemaFilter := getArg(req, "schema")
	sortBy := getArg(req, "sort")
	switch sortBy {
	case "", "name", "rows", "size":
	default:
		return errResult(fmt.Sprintf("unknown sort %q; one of: name, rows, size", sortBy)), nil
	}

	cands, foldedParts := collect(a, strings.ToLower(query), kind, schemaFilter)
	orderCandidates(cands, sortBy, query)

	total := len(cands)
	offset := int(getFloatArg(req, "offset", 0))
	limit := limitArgOr(req, defaultMaxItems)

	res := findObjectsResult{Query: query, Kind: kind, Objects: []objectMatch{}, Count: total, Offset: offset}
	if total == 0 {
		res.Meta = s.newMeta(joinHints(noMatchHint(query, kind, schemaFilter), foldNote(foldedParts)), nil)
		return structuredTextResult(res, s.wrapText(emptyBody(query, kind), "")), nil
	}
	if offset >= total {
		msg := fmt.Sprintf("%d %s. Offset %d is beyond the end.", total, noun(query, kind), offset)
		res.Meta = s.newMeta(joinHints(msg, foldNote(foldedParts)), nil)
		return structuredTextResult(res, s.wrapText(msg, "")), nil
	}

	end := pageEnd(offset, limit, total)
	page := cands[offset:end]
	res.Objects = make([]objectMatch, len(page))
	lines := make([]string, len(page))
	for i, c := range page {
		res.Objects[i] = c.match
		lines[i] = renderMatch(c.match)
	}

	hint, next := s.findFollowups(page, query, kind, schemaFilter, sortBy, offset, total, end)
	hint = joinHints(hint, foldNote(foldedParts))
	res.Meta = s.newMeta(hint, next)

	var header string
	if offset == 0 && end == total {
		header = fmt.Sprintf("%d %s", total, noun(query, kind))
	} else {
		header = fmt.Sprintf("Showing %d-%d of %d %s", offset+1, end, total, noun(query, kind))
	}
	if query != "" {
		header += fmt.Sprintf(" for '%s'", query)
	}
	return structuredTextResult(res, s.wrapText(header+":\n"+strings.Join(lines, "\n"), hint)), nil
}

// collect walks the snapshot once per object family. An empty query matches
// everything of the requested kind.
func collect(a *schema.AnnotatedSchema, q, kind, schemaFilter string) ([]candidate, int) {
	snap := a.Schema
	var out []candidate
	want := func(k string) bool { return kind == "" || kind == k }

	// Inventories fold partition children into their parent; a name search still finds them.
	children := map[schema.QualifiedName]schema.QualifiedName{}
	folded := 0
	tabular := want("table") || want("column") || want("index")
	if q == "" && tabular {
		children = partitionChildren(snap)
	}

	for i := 0; tabular && i < len(snap.Tables); i++ {
		t := &snap.Tables[i]
		if schemaFilter != "" && t.Schema != schemaFilter {
			continue
		}
		// pg_partman children live in their own schema, so fold only when the parent passes the filter.
		if parent, ok := children[t.Qual()]; ok && (schemaFilter == "" || parent.Schema == schemaFilter) {
			folded++
			continue
		}
		qualified := t.Schema + "." + t.Name

		if want("table") {
			if r, on, ok := matchNameOr(t.Name, q, comments(t.Comment)); ok {
				m := objectMatch{
					Kind: "table", Schema: t.Schema, Name: t.Name, Object: qualified,
					Comment: shortComment(t.Comment), MatchedOn: on,
				}
				c := candidate{match: m, rank: r, kindRank: findObjectKinds["table"], sortName: qualified}
				applySizing(a, t, &c)
				out = append(out, c)
			}
		}

		if want("column") {
			for _, col := range t.Columns {
				r, on, ok := matchNameOr(col.Name, q, comments(col.Comment))
				if !ok {
					continue
				}
				c := candidate{
					match: objectMatch{
						Kind: "column", Schema: t.Schema, Name: col.Name,
						Object: qualified + "." + col.Name, Table: t.Name,
						Detail: col.TypeName, Comment: shortComment(col.Comment), MatchedOn: on,
					},
					rank: r, kindRank: findObjectKinds["column"], sortName: qualified + "." + col.Name,
				}
				applySizing(a, t, &c)
				out = append(out, c)
			}
		}

		if want("index") {
			for _, idx := range t.Indexes {
				flatDef := collapse(idx.Definition)
				r, on, ok := matchNameOr(idx.Name, q, definition(flatDef))
				if !ok {
					continue
				}
				c := candidate{
					match: objectMatch{
						Kind: "index", Schema: t.Schema, Name: idx.Name,
						Object: qualified + "." + idx.Name, Table: t.Name,
						Detail: excerpt(flatDef, q, on == matchedDefinition), MatchedOn: on,
					},
					rank: r, kindRank: findObjectKinds["index"], sortName: qualified + "." + idx.Name,
				}
				applySizing(a, t, &c)
				out = append(out, c)
			}
		}
	}

	for _, v := range snap.Views {
		k := "view"
		if v.IsMaterialized {
			k = "materialized_view"
		}
		if !want(k) || (schemaFilter != "" && v.Schema != schemaFilter) {
			continue
		}
		flatDef := collapse(v.Definition)
		r, on, ok := matchNameOr(v.Name, q, comments(v.Comment), definition(flatDef))
		if !ok {
			continue
		}
		detail := ""
		if on == matchedDefinition {
			detail = excerpt(flatDef, q, true)
		}
		out = append(out, candidate{
			match: objectMatch{
				Kind: k, Schema: v.Schema, Name: v.Name, Object: v.Schema + "." + v.Name,
				Detail: detail, Comment: shortComment(v.Comment), MatchedOn: on,
			},
			rank: r, kindRank: findObjectKinds[k], sortName: v.Schema + "." + v.Name,
		})
	}

	if want("function") {
		for _, f := range snap.Functions {
			if schemaFilter != "" && f.Schema != schemaFilter {
				continue
			}
			r, on, ok := matchNameOr(f.Name, q, comments(f.Comment))
			if !ok {
				continue
			}
			// Overloads share schema.name, so the identity carries the args.
			identity := fmt.Sprintf("%s.%s(%s)", f.Schema, f.Name, f.IdentityArgs)
			out = append(out, candidate{
				match: objectMatch{
					Kind: "function", Schema: f.Schema, Name: f.Name,
					Object:  identity,
					Detail:  "returns " + f.ReturnType,
					Comment: shortComment(f.Comment), MatchedOn: on,
				},
				rank: r, kindRank: findObjectKinds["function"], sortName: identity,
			})
		}
	}

	if want("enum") {
		for _, e := range snap.Enums {
			if schemaFilter != "" && e.Schema != schemaFilter {
				continue
			}
			r, on, ok := matchNameOr(e.Name, q, enumFields(e.Labels)...)
			if !ok {
				continue
			}
			out = append(out, candidate{
				match: objectMatch{
					Kind: "enum", Schema: e.Schema, Name: e.Name, Object: e.Schema + "." + e.Name,
					Detail: excerpt(strings.Join(e.Labels, ", "), q, on == matchedLabels), MatchedOn: on,
				},
				rank: r, kindRank: findObjectKinds["enum"], sortName: e.Schema + "." + e.Name,
			})
		}
	}
	return out, folded
}

// partitionChildren maps every partition child to its parent.
func partitionChildren(snap *schema.SchemaSnapshot) map[schema.QualifiedName]schema.QualifiedName {
	out := map[schema.QualifiedName]schema.QualifiedName{}
	for i := range snap.Tables {
		p := snap.Tables[i].PartitionInfo
		if p == nil {
			continue
		}
		for _, c := range p.Children {
			out[schema.QualifiedName{Schema: c.Schema, Name: c.Name}] = snap.Tables[i].Qual()
		}
	}
	return out
}

// matchNameOr ranks a name hit above a hit in any extra field.
func matchNameOr(name, q string, extras ...matchField) (rank int, on string, ok bool) {
	if q == "" {
		return rankSubstring, "", true
	}
	lower := strings.ToLower(name)
	switch {
	case lower == q:
		return rankExact, matchedName, true
	case strings.HasPrefix(lower, q):
		return rankPrefix, matchedName, true
	case strings.Contains(lower, q):
		return rankSubstring, matchedName, true
	}
	for _, e := range extras {
		if e.text != "" && strings.Contains(strings.ToLower(e.text), q) {
			return rankMetadata, e.on, true
		}
	}
	return 0, "", false
}

func enumFields(labels []string) []matchField {
	out := make([]matchField, len(labels))
	for i, l := range labels {
		out[i] = matchField{on: matchedLabels, text: l}
	}
	return out
}

func comments(c *string) matchField {
	return matchField{on: matchedComment, text: commentOf(c)}
}

func definition(text string) matchField {
	return matchField{on: matchedDefinition, text: text}
}

func applySizing(a *schema.AnnotatedSchema, t *schema.Table, c *candidate) {
	sizing := a.SizingFor(t.Qual())
	if sizing == nil {
		return
	}
	c.rows, c.size, c.hasSizing = sizing.Reltuples, sizing.TableSize, true
	// Only the table row reports sizing as its own.
	if c.match.Kind != "table" {
		return
	}
	rows := int64(sizing.Reltuples)
	c.match.RowsEstimate = &rows
	c.match.SizeBytes = &sizing.TableSize
	if t.PartitionInfo != nil {
		c.match.Partitioned = &tablePartitionSummary{
			Strategy: string(t.PartitionInfo.Strategy),
			Key:      t.PartitionInfo.Key,
			Children: len(t.PartitionInfo.Children),
		}
	}
}

// Explicit sort wins; searches rank by match quality, inventories by name.
func orderCandidates(cands []candidate, sortBy, query string) {
	byName := func(i, j int) bool {
		if cands[i].kindRank != cands[j].kindRank {
			return cands[i].kindRank < cands[j].kindRank
		}
		return cands[i].sortName < cands[j].sortName
	}

	switch sortBy {
	case "rows", "size":
		sort.SliceStable(cands, func(i, j int) bool {
			// Unsized objects (views, functions, enums) sort last.
			if cands[i].hasSizing != cands[j].hasSizing {
				return cands[i].hasSizing
			}
			a, b := cands[i].rows, cands[j].rows
			if sortBy == "size" {
				a, b = float64(cands[i].size), float64(cands[j].size)
			}
			if a != b {
				return a > b
			}
			return byName(i, j)
		})
	case "name":
		sort.SliceStable(cands, byName)
	default:
		if query == "" {
			sort.SliceStable(cands, byName)
			return
		}
		sort.SliceStable(cands, func(i, j int) bool {
			if cands[i].rank != cands[j].rank {
				return cands[i].rank < cands[j].rank
			}
			return byName(i, j)
		})
	}
}

func (s *Server) findFollowups(page []candidate, query, kind, schemaFilter, sortBy string, offset, total, end int) (string, []NextCall) {
	var hint string
	var next []NextCall

	// The top match of a search's first page gets a describe_table follow-up.
	if query != "" && offset == 0 {
		for _, c := range page {
			if c.match.Kind != "table" && c.match.Kind != "column" && c.match.Kind != "index" {
				continue
			}
			name := c.match.Name
			if c.match.Table != "" {
				name = c.match.Table
			}
			next = append(next, NextCall{Tool: "describe_table", Args: map[string]any{
				"table": name, "schema": c.match.Schema,
			}})
			break
		}
	}

	if end < total {
		args := map[string]any{"limit": 0}
		for k, v := range map[string]string{"query": query, "kind": kind, "schema": schemaFilter, "sort": sortBy} {
			if v != "" {
				args[k] = v
			}
		}
		hint = fmt.Sprintf("Showing %d of %d; re-run with limit=0 for all of them.", end-offset, total)
		next = append(next, NextCall{Tool: "find_objects", Args: args})
	}
	return hint, next
}

// foldNote makes silent folding visible in the result's hint.
func foldNote(folded int) string {
	if folded == 0 {
		return ""
	}
	return fmt.Sprintf("%d partition children are folded into their parents, with their columns and indexes; search a child by name to reach it.", folded)
}

func noMatchHint(query, kind, schemaFilter string) string {
	if query == "" {
		return ""
	}
	var narrowed []string
	if kind != "" {
		narrowed = append(narrowed, "kind="+kind)
	}
	if schemaFilter != "" {
		narrowed = append(narrowed, "schema="+schemaFilter)
	}
	if len(narrowed) > 0 {
		return "Searched with " + strings.Join(narrowed, " and ") + "; drop the filter to search every kind and schema."
	}
	return "Names, comments, index and view definitions, and enum labels were all searched. Function bodies are not in the snapshot, so a name that only appears inside a function cannot be found here."
}

func emptyBody(query, kind string) string {
	what := "objects"
	if kind != "" {
		what = plural(kind)
	}
	if query == "" {
		return fmt.Sprintf("No %s in the snapshot.", what)
	}
	return fmt.Sprintf("No %s matching '%s'.", what, query)
}

func noun(query, kind string) string {
	if query != "" {
		return "match(es)"
	}
	if kind != "" {
		return plural(kind)
	}
	return "object(s)"
}

func plural(kind string) string {
	if kind == "index" {
		return "indexes"
	}
	return kind + "s"
}

func renderMatch(m objectMatch) string {
	line := strings.ToUpper(strings.ReplaceAll(m.Kind, "_", " ")) + " " + m.Object
	if m.RowsEstimate != nil {
		line += fmt.Sprintf(" (~%d rows)", *m.RowsEstimate)
	}
	if m.Partitioned != nil {
		line += fmt.Sprintf(" [partitioned: %s(%s), %d parts]",
			m.Partitioned.Strategy, m.Partitioned.Key, m.Partitioned.Children)
	}
	if m.Detail != "" {
		line += " — " + m.Detail
	}
	if m.Comment != nil && *m.Comment != "" {
		line += " -- " + *m.Comment
	}
	if m.MatchedOn != "" && m.MatchedOn != matchedName {
		line += " [matched " + m.MatchedOn + "]"
	}
	return line
}

// shortComment keeps unbounded COMMENT ON text from becoming the response.
func shortComment(c *string) *string {
	if c == nil {
		return nil
	}
	short := truncate(collapse(*c))
	return &short
}

// excerpt windows the text around the match so the hit stays visible.
func excerpt(text, q string, atMatch bool) string {
	flat := collapse(text)
	r := []rune(flat)
	if len(r) <= maxMatchDetail {
		return flat
	}
	hit := -1
	if atMatch {
		hit = matchRuneIndex(flat, q)
	}
	if hit < 0 {
		return truncate(flat)
	}

	window := maxMatchDetail - 2 // the ellipses come out of the budget
	start := max(hit-window/3, 0)
	end := min(start+window, len(r))
	start = max(end-window, 0)

	out := string(r[start:end])
	if start > 0 {
		out = "…" + out
	}
	if end < len(r) {
		out += "…"
	}
	return out
}

// matchRuneIndex returns where q starts in text, in runes; strings.ToLower can
// change byte length, so a byte offset into the lowered copy is invalid.
func matchRuneIndex(text, q string) int {
	if q == "" {
		return -1
	}
	lower := strings.ToLower(text)
	byteIdx := strings.Index(lower, q)
	if byteIdx < 0 {
		return -1
	}
	return utf8.RuneCountInString(lower[:byteIdx])
}

func commentOf(c *string) string {
	if c == nil {
		return ""
	}
	return *c
}

func collapse(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func truncate(s string) string {
	r := []rune(s)
	if len(r) <= maxMatchDetail {
		return s
	}
	return string(r[:maxMatchDetail-1]) + "…"
}

func sortedKinds() []string {
	out := make([]string, 0, len(findObjectKinds))
	for k := range findObjectKinds {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return findObjectKinds[out[i]] < findObjectKinds[out[j]] })
	return out
}
