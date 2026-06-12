package diff

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

func indexBy[T any](items []T, keyFn func(T) string) map[string]*T {
	m := make(map[string]*T, len(items))
	for i := range items {
		m[keyFn(items[i])] = &items[i]
	}
	return m
}

// jsonIndex keys items by keyFn, mapping to their canonical JSON for equality.
func jsonIndex[T any](items []T, keyFn func(T) string) map[string]string {
	m := make(map[string]string, len(items))
	for i := range items {
		b, _ := json.Marshal(items[i])
		m[keyFn(items[i])] = string(b)
	}
	return m
}

// Heuristic raw signal: a volatile default rewrites the table on column add.
func classifyDefault(def *string) DefaultKind {
	if def == nil {
		return DefaultNone
	}
	d := strings.ToLower(*def)
	volatile := []string{
		"nextval(", "now(", "current_timestamp", "current_date", "current_time",
		"localtimestamp", "localtime", "clock_timestamp(", "statement_timestamp(",
		"transaction_timestamp(", "timeofday(", "random(", "gen_random_uuid(",
		"uuid_generate", "nextval'",
	}
	for _, v := range volatile {
		if strings.Contains(d, v) {
			return DefaultVolatile
		}
	}
	return DefaultConstant
}

// Unknown pairs read as non-widening, so the cloud treats them as the riskier case.
func typeWidens(from, to string) bool {
	f, t := baseType(from), baseType(to)
	if f == t {
		return true
	}
	intRank := map[string]int{"int2": 1, "smallint": 1, "int4": 2, "integer": 2, "int": 2, "int8": 3, "bigint": 3}
	floatRank := map[string]int{"float4": 1, "real": 1, "float8": 2, "double precision": 2}
	if rf, okf := intRank[f]; okf {
		if rt, okt := intRank[t]; okt {
			return rt >= rf
		}
	}
	if rf, okf := floatRank[f]; okf {
		if rt, okt := floatRank[t]; okt {
			return rt >= rf
		}
	}
	// varchar(n) → text / unbounded varchar / wider varchar is widening
	charFamily := map[string]bool{"varchar": true, "character varying": true, "bpchar": true, "character": true}
	textTargets := map[string]bool{"text": true, "varchar": true, "character varying": true}
	return charFamily[f] && textTargets[t]
}

// baseType strips the length/precision modifier: "varchar(255)" → "varchar".
func baseType(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if i := strings.IndexByte(s, '('); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(strings.TrimSuffix(s, "[]"))
}

// Deterministic order is load-bearing: the cloud dedups on the delta.
func sortChanges(changes []Change) {
	sort.SliceStable(changes, func(i, j int) bool {
		a, b := changes[i], changes[j]
		if ka, kb := a.Object.Kind, b.Object.Kind; ka != kb {
			return ka < kb
		}
		if sa, sb := ptrStr(a.Object.Schema), ptrStr(b.Object.Schema); sa != sb {
			return sa < sb
		}
		if a.Object.Name != b.Object.Name {
			return a.Object.Name < b.Object.Name
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		return childName(a) < childName(b)
	})
}

func childName(c Change) string {
	switch {
	case c.Column != nil:
		return c.Column.Name
	case c.Index != nil:
		return c.Index.Name
	case c.Constraint != nil:
		return c.Constraint.Name
	}
	return ""
}
