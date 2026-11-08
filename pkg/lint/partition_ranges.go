package lint

import (
	"cmp"
	"math/big"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

const (
	boundMin boundKind = iota
	boundValue
	boundMax
)

type (
	boundKind int

	rangeBound struct {
		kind   boundKind
		raw    string
		num    *big.Rat
		at     time.Time
		isTime bool
	}

	rangeSpan struct {
		lower, upper rangeBound
	}
)

var (
	rangeBoundRe = regexp.MustCompile(`^FOR VALUES FROM \((.*?)\) TO \((.*)\)$`)

	// exponent capped so big.Rat can't be asked for a huge allocation
	plainNumberRe = regexp.MustCompile(`^-?\d+(\.\d+)?([eE][+-]?\d{1,3})?$`)

	partKeyRe = regexp.MustCompile(`^(?i:RANGE)\s*\((.*)\)$`)

	// format_type vocabulary: no int2/int4/int8, decimal or float
	numericKeyPrefixes = []string{
		"smallint", "integer", "bigint", "numeric", "real", "double precision",
	}

	// pg_get_expr prints date/timestamp[tz] bounds as quoted ISO literals; under
	// a non-ISO DateStyle they fail to parse and the table is skipped
	boundTimeLayouts = []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999Z07",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02",
	}
)

// Only a single plain key column of a numeric or date/time type has an order we
// can reproduce; expression and multi-column keys, and text, are left alone.
func rangeKeyType(t *snapshot.Table) (isTime, ok bool) {
	key := strings.TrimSpace(t.PartitionInfo.Key)
	if m := partKeyRe.FindStringSubmatch(key); m != nil {
		key = strings.TrimSpace(m[1])
	}
	name := strings.ReplaceAll(strings.Trim(key, `"`), `""`, `"`)
	for _, c := range t.Columns {
		if c.Name != name {
			continue
		}
		typeName := strings.ToLower(c.TypeName)
		if strings.HasPrefix(typeName, "date") || strings.HasPrefix(typeName, "timestamp") {
			return true, true
		}
		ok := slices.ContainsFunc(numericKeyPrefixes, func(p string) bool {
			return strings.HasPrefix(typeName, p)
		})
		return false, ok
	}
	return false, false
}

// parseRangeSpans reads every child's bound. ok=false means the table can't be
// judged (multi-column or unreadable bounds): one skipped child would look like
// a hole, so the whole table is skipped rather than guessed at.
func parseRangeSpans(children []snapshot.PartitionChild, isTime bool) ([]rangeSpan, bool) {
	var spans []rangeSpan
	for _, child := range children {
		bound := strings.TrimSpace(child.Bound)
		if bound == "DEFAULT" {
			continue
		}
		m := rangeBoundRe.FindStringSubmatch(bound)
		if m == nil {
			return nil, false
		}
		lo, up := strings.Split(m[1], ","), strings.Split(m[2], ",")
		if len(lo) != 1 || len(up) != 1 {
			return nil, false
		}
		lower, okLo := parseBoundValue(strings.TrimSpace(lo[0]), isTime)
		upper, okUp := parseBoundValue(strings.TrimSpace(up[0]), isTime)
		if !okLo || !okUp {
			return nil, false
		}
		spans = append(spans, rangeSpan{lower, upper})
	}
	return spans, true
}

// Numbers compare exactly (big.Rat) so int8 bounds near 2^63 don't collapse.
func parseBoundValue(s string, isTime bool) (rangeBound, bool) {
	switch s {
	case "MINVALUE":
		return rangeBound{kind: boundMin, raw: s}, true
	case "MAXVALUE":
		return rangeBound{kind: boundMax, raw: s}, true
	}
	val := s
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		val = strings.ReplaceAll(s[1:len(s)-1], "''", "'")
	}
	if !isTime {
		if !plainNumberRe.MatchString(val) {
			return rangeBound{}, false
		}
		n, ok := new(big.Rat).SetString(val)
		return rangeBound{kind: boundValue, raw: val, num: n}, ok
	}
	for _, layout := range boundTimeLayouts {
		if at, err := time.Parse(layout, val); err == nil {
			return rangeBound{kind: boundValue, raw: val, at: at, isTime: true}, true
		}
	}
	return rangeBound{}, false
}

func cmpBound(a, b rangeBound) int {
	if a.kind != boundValue || b.kind != boundValue {
		return cmp.Compare(a.kind, b.kind)
	}
	if a.isTime {
		return a.at.Compare(b.at)
	}
	return a.num.Cmp(b.num)
}
