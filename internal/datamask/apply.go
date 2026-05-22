package datamask

import (
	"github.com/boringsql/dryrun/internal/schema"
)

// returns matched count; caller must recompute snap.ContentHash since payload changed
func ApplyPlanner(p *Policy, snap *schema.PlannerStatsSnapshot) (masked int) {
	if p == nil || snap == nil {
		return 0
	}
	for i := range snap.Columns {
		c := &snap.Columns[i]
		if !p.IsSensitive(c.Table.Schema, c.Table.Name, c.Column) {
			continue
		}
		c.Stats.MostCommonVals = nil
		c.Stats.MostCommonFreqs = nil
		c.Stats.HistogramBounds = nil
		masked++
	}
	return masked
}
