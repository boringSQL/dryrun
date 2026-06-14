package diff

import "github.com/boringsql/dryrun/pkg/snapshot"

type DriftDirection string

const (
	DriftIdentical DriftDirection = "identical"
	DriftAhead     DriftDirection = "ahead"
	DriftBehind    DriftDirection = "behind"
	DriftDiverged  DriftDirection = "diverged"
)

type DriftReport struct {
	Direction     DriftDirection `json:"direction"`
	SavedHash     string         `json:"saved_hash"`
	LiveHash      string         `json:"live_hash"`
	Delta         *SchemaDelta   `json:"delta,omitempty"`
	AddedCount    int            `json:"added_count"`
	RemovedCount  int            `json:"removed_count"`
	ModifiedCount int            `json:"modified_count"`
}

// Direction is raw: added-only=ahead, removed-only=behind. No risk judgment.
func ClassifyDrift(saved, live *snapshot.SchemaSnapshot) *DriftReport {
	if saved.ContentHash == live.ContentHash {
		return &DriftReport{Direction: DriftIdentical, SavedHash: saved.ContentHash, LiveHash: live.ContentHash}
	}

	delta, _ := DiffSchema(saved, live)

	var added, removed, modified int
	for _, c := range delta.Changes {
		switch c.Type.Category() {
		case "added":
			added++
		case "removed":
			removed++
		default:
			modified++
		}
	}

	direction := DriftDiverged
	switch {
	case added > 0 && removed == 0:
		direction = DriftAhead
	case removed > 0 && added == 0:
		direction = DriftBehind
	}

	return &DriftReport{
		Direction:     direction,
		SavedHash:     saved.ContentHash,
		LiveHash:      live.ContentHash,
		Delta:         delta,
		AddedCount:    added,
		RemovedCount:  removed,
		ModifiedCount: modified,
	}
}
