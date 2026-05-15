package diff

import "github.com/boringsql/dryrun/internal/schema"

type DriftDirection string

const (
	DriftIdentical DriftDirection = "identical"
	DriftAhead     DriftDirection = "ahead"
	DriftBehind    DriftDirection = "behind"
	DriftDiverged  DriftDirection = "diverged"
)

type DriftReport struct {
	Direction     DriftDirection   `json:"direction"`
	SavedHash     string           `json:"saved_hash"`
	LiveHash      string           `json:"live_hash"`
	Changeset     *SchemaChangeset `json:"changeset,omitempty"`
	AddedCount    int              `json:"added_count"`
	RemovedCount  int              `json:"removed_count"`
	ModifiedCount int              `json:"modified_count"`
}

func ClassifyDrift(saved, live *schema.SchemaSnapshot) *DriftReport {
	if saved.ContentHash == live.ContentHash {
		return &DriftReport{
			Direction: DriftIdentical,
			SavedHash: saved.ContentHash,
			LiveHash:  live.ContentHash,
		}
	}

	changeset := DiffSchemas(saved, live)

	var added, removed, modified int
	for _, c := range changeset.Changes {
		switch c.Kind {
		case Added:
			added++
		case Removed:
			removed++
		case Modified:
			modified++
		}
	}

	direction := DriftDiverged
	if added > 0 && removed == 0 {
		direction = DriftAhead
	} else if removed > 0 && added == 0 {
		direction = DriftBehind
	}

	return &DriftReport{
		Direction:     direction,
		SavedHash:     saved.ContentHash,
		LiveHash:      live.ContentHash,
		Changeset:     changeset,
		AddedCount:    added,
		RemovedCount:  removed,
		ModifiedCount: modified,
	}
}
