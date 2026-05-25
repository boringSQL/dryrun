package datamask

import (
	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/schema"
)

// nil policy or nil snap are no-ops; rehash on any mutation
func MaskPlanner(p *masking.Policy, snap *schema.PlannerStatsSnapshot) int {
	if p == nil || snap == nil {
		return 0
	}
	n := 0
	for i := range snap.Columns {
		c := &snap.Columns[i]
		if !p.IsSensitive(c.Table.Schema, c.Table.Name, c.Column) {
			continue
		}
		c.Stats.MostCommonVals = nil
		c.Stats.MostCommonFreqs = nil
		c.Stats.HistogramBounds = nil
		n++
	}
	if n > 0 {
		snap.ContentHash = schema.ComputePlannerContentHash(snap)
	}
	return n
}
