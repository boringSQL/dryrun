package datamask

import "github.com/boringsql/dryrun/internal/schema"

// Implementations must keep snap.ContentHash in sync with any mutation.
type Masker interface {
	MaskPlanner(snap *schema.PlannerStatsSnapshot) int
}

type NullMasker struct{}

func (NullMasker) MaskPlanner(*schema.PlannerStatsSnapshot) int { return 0 }
