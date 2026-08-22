package query

import (
	"sort"
	"strings"
	"unicode/utf8"
)

type (
	// One name substitution, in the terms the caller wrote it.
	Fix struct {
		Kind string `json:"kind"` // "table" | "column"
		From string `json:"from"`
		To   string `json:"to"`
	}

	pendingFix struct {
		kind     string
		from     string // as written, for the fix list
		to       string // catalog name, unquoted
		locs     []int32
		lastOnly bool // column refs replace only the trailing field
		// what the parser read at each location, to check we are editing the
		// token it pointed at and not whatever else sits at that offset
		expect []string
	}

	// unfixable short-circuits the rewrite: with an error nobody has a
	// candidate for, the result could never validate anyway.
	fixPlan struct {
		fixes     []pendingFix
		unfixable bool
	}

	identPart struct {
		start, end int
		value      string
	}
)

func (p *fixPlan) add(f *pendingFix) {
	if f == nil {
		p.unfixable = true
		return
	}
	for i := range p.fixes {
		e := &p.fixes[i]
		if e.kind == f.kind && e.from == f.from && e.to == f.to && e.lastOnly == f.lastOnly {
			e.locs = append(e.locs, f.locs...)
			e.expect = append(e.expect, f.expect...)
			return
		}
	}
	p.fixes = append(p.fixes, *f)
}

// Produces the rewrite candidates for one pass. The unquoted form reads better
// in a patch, but a candidate that is a reserved word or mixed case only works
// quoted, so both are offered and the caller keeps whichever validates.
func correctOnce(sql string, plan fixPlan) ([]string, []Fix) {
	if plan.unfixable || len(plan.fixes) == 0 {
		return nil, nil
	}
	var out []string
	for _, quoteAll := range []bool{false, true} {
		s, ok := applyFixes(sql, plan.fixes, quoteAll)
		if !ok {
			return nil, nil
		}
		if s != sql && (len(out) == 0 || s != out[0]) {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	fixes := make([]Fix, 0, len(plan.fixes))
	for _, f := range plan.fixes {
		fixes = append(fixes, Fix{Kind: f.kind, From: f.from, To: f.to})
	}
	return out, fixes
}

func applyFixes(sql string, fixes []pendingFix, quoteAll bool) (string, bool) {
	type edit struct {
		start, end int
		text       string
	}
	var edits []edit

	for _, f := range fixes {
		for i, loc := range f.locs {
			if loc < 0 || int(loc) >= len(sql) {
				return "", false
			}
			parts, ok := scanQualifiedIdent(sql, int(loc))
			if !ok || strings.Join(values(parts), ".") != f.expect[i] {
				return "", false
			}
			target := parts
			if f.lastOnly {
				target = parts[len(parts)-1:]
			}
			edits = append(edits, edit{target[0].start, target[len(target)-1].end, renderName(f.to, quoteAll)})
		}
	}

	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	out := sql
	prev := len(sql) + 1
	for _, e := range edits {
		if e.end > prev {
			return "", false // overlapping edits; not something to guess at
		}
		prev = e.start
		out = out[:e.start] + e.text + out[e.end:]
	}
	return out, true
}

// A catalog name may be a keyword, mixed case, or hold punctuation; quoting is
// always correct, so only skip it when the plain form is unambiguous.
func renderName(name string, quoteAll bool) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		if quoteAll {
			parts[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
			continue
		}
		parts[i] = quoteIdent(p)
	}
	return strings.Join(parts, ".")
}

func quoteIdent(s string) string {
	if !needsQuote(s) {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func needsQuote(s string) bool {
	if s == "" {
		return true
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c == '_':
		case (c >= '0' && c <= '9') || c == '$':
			if i == 0 {
				return true
			}
		default:
			return true
		}
	}
	return false
}

func values(parts []identPart) []string {
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = p.value
	}
	return out
}

// Reads the `ident[.ident]*` run a RangeVar or ColumnRef location points at.
// Whitespace around the dot is legal but vanishingly rare: bailing costs a
// correction, guessing costs a wrong rewrite.
func scanQualifiedIdent(sql string, off int) ([]identPart, bool) {
	var parts []identPart
	for i := off; ; {
		p, ok := scanIdent(sql, i)
		if !ok {
			return nil, false
		}
		parts = append(parts, p)
		i = p.end
		if i < len(sql) && sql[i] == '.' {
			i++
			continue
		}
		return parts, true
	}
}

func scanIdent(sql string, i int) (identPart, bool) {
	if i >= len(sql) {
		return identPart{}, false
	}
	start := i
	if sql[i] == '"' {
		var b strings.Builder
		for i++; i < len(sql); i++ {
			if sql[i] != '"' {
				b.WriteByte(sql[i])
				continue
			}
			if i+1 < len(sql) && sql[i+1] == '"' {
				b.WriteByte('"')
				i++
				continue
			}
			return identPart{start, i + 1, b.String()}, true
		}
		return identPart{}, false
	}
	for i < len(sql) && isIdentByte(sql[i], i == start) {
		i++
	}
	if i == start {
		return identPart{}, false
	}
	// unquoted identifiers fold down, which is what the parse tree holds
	return identPart{start, i, strings.ToLower(sql[start:i])}, true
}

func isIdentByte(c byte, first bool) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_', c >= 0x80:
		return true
	case (c >= '0' && c <= '9') || c == '$':
		return !first
	}
	return false
}

// Picks the one candidate a name is obviously a typo of. Anything with a tie,
// or nothing close enough, is not mechanical and gets no fix.
func bestCandidate(bad string, pool []string) (string, bool) {
	lower := strings.ToLower(bad)

	var ci []string
	for _, c := range pool {
		if strings.ToLower(c) == lower {
			ci = append(ci, c)
		}
	}
	if len(ci) == 1 {
		return ci[0], true
	}
	if len(ci) > 1 {
		return "", false
	}

	max := maxDistance(lower)
	if max == 0 {
		return "", false
	}
	best, bestD, ties := "", max+1, 0
	for _, c := range pool {
		d := osaDistance(lower, strings.ToLower(c))
		switch {
		case d < bestD:
			best, bestD, ties = c, d, 1
		case d == bestD:
			ties++
		}
	}
	if bestD > max || ties != 1 {
		return "", false
	}
	return best, true
}

// One edit, and only for a name long enough that one edit is still a typo
// rather than a different word: two edits reaches created_by from created_at.
func maxDistance(name string) int {
	if utf8.RuneCountInString(name) < 4 {
		return 0
	}
	return 1
}

// Optimal string alignment: like Levenshtein but counts a transposition as one
// edit, which is what most typos are.
func osaDistance(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prev2 := make([]int, lb+1)
	prev := make([]int, lb+1)
	cur := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		cur[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(cur[j-1]+1, prev[j]+1, prev[j-1]+cost)
			if i > 1 && j > 1 && a[i-1] == b[j-2] && a[i-2] == b[j-1] {
				cur[j] = min(cur[j], prev2[j-2]+1)
			}
		}
		prev2, prev, cur = prev, cur, prev2
	}
	return prev[lb]
}
