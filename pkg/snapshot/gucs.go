package snapshot

import (
	"strconv"
)

// re-attaches the pg_settings unit, multiplying out block units like "8kB"
func (g GucSetting) CanonicalValue() string {
	if g.Unit == nil || *g.Unit == "" {
		return g.Setting
	}
	n, err := strconv.ParseInt(g.Setting, 10, 64)
	if err != nil || n < 0 {
		// negative = sentinel, valid without unit; non-integer falls through
		// bare too, so a float-with-unit GUC (vacuum_cost_delay) would lose
		// its unit — none is replayed today
		return g.Setting
	}
	unit := *g.Unit
	i := 0
	for i < len(unit) && unit[i] >= '0' && unit[i] <= '9' {
		i++
	}
	if i > 0 {
		mult, err := strconv.ParseInt(unit[:i], 10, 64)
		if err != nil {
			return g.Setting
		}
		return strconv.FormatInt(n*mult, 10) + unit[i:]
	}
	return g.Setting + unit
}
