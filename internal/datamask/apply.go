package datamask

import (
	"github.com/boringsql/fixturize/masking"

	"github.com/boringsql/dryrun/internal/schema"
)

// nil snap is a no-op; sets Masking so both capture paths record the same counts.
func MaskPlanner(p *masking.Policy, snap *schema.PlannerStatsSnapshot) {
	if snap == nil {
		return
	}
	snap.Masking = &schema.MaskingInfo{Applied: p != nil, JSONBStripped: true}
	if p == nil {
		return
	}

	for i := range snap.Columns {
		c := &snap.Columns[i]
		if !p.IsSensitive(c.Table.Schema, c.Table.Name, c.Column) {
			continue
		}
		c.Stats.MostCommonVals = nil
		c.Stats.MostCommonFreqs = nil
		c.Stats.HistogramBounds = nil
		snap.Masking.ColumnsMasked++
	}

	for i := range snap.ExtendedStats {
		e := &snap.ExtendedStats[i]
		if !extendedStatsSensitive(p, e) {
			continue
		}
		e.MCVValues = nil
		e.MCVFreqs = nil
		e.MCVBaseFreqs = nil
		snap.Masking.ExtendedStatsMasked++
	}

	if snap.Masking.ColumnsMasked > 0 || snap.Masking.ExtendedStatsMasked > 0 {
		snap.ContentHash = schema.ComputePlannerContentHash(snap)
	}
}

// An MCV tuple leaks every column it covers, so one sensitive column drops the
// whole list. Empty Columns can't be proven safe; mask it too.
func extendedStatsSensitive(p *masking.Policy, e *schema.ExtendedStatsEntry) bool {
	if len(e.Columns) == 0 {
		return true
	}
	for _, c := range e.Columns {
		if p.IsSensitive(e.Table.Schema, e.Table.Name, c) {
			return true
		}
	}
	return false
}
