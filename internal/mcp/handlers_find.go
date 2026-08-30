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
	// caps every free-text field on the wire: comments, view and index
	// definitions, enum label lists. The row says why it matched, not what it says.
	maxMatchDetail = 160

	matchedName       = "name"
	matchedComment    = "comment"
	matchedDefinition = "definition"
	matchedLabels     = "labels"
)

type (
	// One searchable field of an object, carrying the matched_on it reports.
	matchField struct {
		on   string
		text string
	}

	// One candidate object plus what it took to match it. rank/kindRank/sortName
	// order the page; rows/size sort it when the caller asks for size order.
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

// match tiers: an exact name beats a prefix beats a substring beats a hit in
// prose. Searching "users" must not bury the users table under user_id columns.
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
	// with nothing to search for this is an inventory, and an inventory of
	// every column of every table is not orientation
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
// everything of the requested kind, which is what makes this an inventory tool
// as well as a search one.
func collect(a *schema.AnnotatedSchema, q, kind, schemaFilter string) ([]candidate, int) {
	snap := a.Schema
	var out []candidate
	want := func(k string) bool { return kind == "" || kind == k }

	// An inventory of a daily-partitioned table is 365 rows of noise, and the
	// parent already carries their rolled-up size. A search still finds them.
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
		// folding a child whose parent the filter excluded would drop it from
		// every answer: pg_partman puts children in a schema of their own
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
				// the definition is the useful payload even on a name match
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
		// a definition hit is the only reason to ship SQL text back
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
			// the snapshot carries signatures, never bodies
			r, on, ok := matchNameOr(f.Name, q, comments(f.Comment))
			if !ok {
				continue
			}
			// overloads share schema.name, so the identity has to carry args
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

// Every table that is some other table's partition. Sub-partitioned children
// are parents too, and are folded at whatever level they appear.
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

// matchNameOr ranks a name hit above a hit in any other field, and reports it
// once: an object whose name and comment both match is a name match. Each
// extra field names itself, so matched_on never has to be inferred from
// argument order.
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

// each label separately: joining them would match a query straddling two
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
	// only the table row claims them as its own; a column's page position may
	// depend on its table's size, but the column does not have a size
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

// Explicit sort wins; otherwise a query orders by match quality and an
// inventory orders by name. Every comparison falls through to the qualified
// name, so the order is total and paging is stable.
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
			// unsized objects (views, functions, enums) sort last rather than as zero
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

	// only the first page of a search: an inventory has no top match, and page
	// three's first row is not one either
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

// Folding is silent otherwise, and a count that excludes objects without
// saying so is the kind of thing an agent cannot detect.
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
	// the boundaries that explain a miss, in the order they cause one
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
	// sql comment syntax, so detail and comment stay tellable apart in text
	if m.Comment != nil && *m.Comment != "" {
		line += " -- " + *m.Comment
	}
	// why this row is here, when it is not obvious from the name
	if m.MatchedOn != "" && m.MatchedOn != matchedName {
		line += " [matched " + m.MatchedOn + "]"
	}
	return line
}

// A comment is unbounded text and rides every row of every page; a design doc
// pasted into COMMENT ON TABLE must not become the response.
func shortComment(c *string) *string {
	if c == nil {
		return nil
	}
	short := truncate(collapse(*c))
	return &short
}

// excerpt windows the text around the match. A hit at offset 3000 is invisible
// in the first 160 runes, and a detail that does not show the match proves
// nothing the matched_on field has not already said.
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

	// the ellipses come out of the budget, not on top of it
	window := maxMatchDetail - 2
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

// matchRuneIndex is where q starts in text, counted in text's own runes.
// strings.ToLower maps rune for rune but not byte for byte (Ⱥ is two bytes and
// lowers to three), so the byte offset of a hit in the lowered copy is not an
// offset into the original -- it can point past its end. Rune indices do line
// up, so count those.
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
