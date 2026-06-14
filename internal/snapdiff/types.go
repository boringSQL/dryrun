// Package snapdiff correlates the schema, planner, and activity streams for a
// before/after pair into one object-keyed, ranked report. The streams are
// captured separately with their own timestamps, so it joins by capture time
// within a window.
package snapdiff

import (
	"time"

	"github.com/boringsql/dryrun/pkg/diff"
)

const (
	// max gap from the anchor for a capture to count as the same moment
	DefaultWindow = 30 * time.Minute
	// movers below this |%| stay out of the object view
	minMoverPct = diff.DefaultMinPct
)

type Options struct {
	From   string        // ref token; default "latest~1"
	To     string        // ref token; default "latest"
	Kind   string        // schema|planner|activity; the timeline refs name. default schema
	Node   string        // activity node label, when several exist
	Window time.Duration // correlation window; default DefaultWindow
}

type (
	Result struct {
		PrimaryKind   string  `json:"primary_kind"`
		FromHash      string  `json:"from_hash"`
		ToHash        string  `json:"to_hash"`
		WindowMinutes float64 `json:"window_minutes"`

		Summary Summary        `json:"summary"`
		Objects []ObjectChange `json:"objects"`
		// set only in summary view when Objects was truncated
		OmittedObjects int `json:"omitted_objects,omitempty"`

		Correlation Correlation `json:"correlation"`

		// raw mechanical deltas; kept for the full view, dropped for summary
		SchemaDelta   *diff.SchemaDelta   `json:"schema_delta,omitempty"`
		PlannerDelta  *diff.PlannerDelta  `json:"planner_delta,omitempty"`
		ActivityDelta []NodeActivityDelta `json:"activity_delta,omitempty"`
	}

	// co-locates everything that moved for one object across the three streams
	ObjectChange struct {
		Kind       string   `json:"kind"`
		Schema     string   `json:"schema,omitempty"`
		Name       string   `json:"name"`
		Structural []string `json:"structural,omitempty"`
		Sizing     []string `json:"sizing,omitempty"`
		Activity   []string `json:"activity,omitempty"`
		score      float64
	}

	Summary struct {
		Headline       string         `json:"headline"`
		Schema         CategoryCounts `json:"schema"`
		PlannerMovers  int            `json:"planner_movers"`
		ActivityMovers int            `json:"activity_movers"`
		ObjectsChanged int            `json:"objects_changed"`
		TopObjects     []string       `json:"top_objects,omitempty"`
	}

	CategoryCounts struct {
		Added    int `json:"added"`
		Removed  int `json:"removed"`
		Modified int `json:"modified"`
	}

	NodeActivityDelta struct {
		Node  string              `json:"node"`
		Delta *diff.ActivityDelta `json:"delta"`
	}

	// which planner/activity captures matched each anchor, and the time skew
	Correlation struct {
		WindowMinutes float64  `json:"window_minutes"`
		From          SideCorr `json:"from"`
		To            SideCorr `json:"to"`
		Notes         []string `json:"notes,omitempty"`
	}

	SideCorr struct {
		Anchor   string      `json:"anchor"` // primary kind for this side
		Hash     string      `json:"hash"`
		TakenAt  time.Time   `json:"taken_at"`
		Schema   *MatchInfo  `json:"schema,omitempty"`
		Planner  *MatchInfo  `json:"planner,omitempty"`
		Activity []MatchInfo `json:"activity,omitempty"`
	}

	MatchInfo struct {
		Node        string    `json:"node,omitempty"`
		Hash        string    `json:"hash"`
		TakenAt     time.Time `json:"taken_at"`
		SkewSeconds float64   `json:"skew_seconds"`
		Source      string    `json:"source"` // anchor | schema_ref | window
	}
)

const summaryObjectCap = 20

func (r *Result) IsEmpty() bool {
	return len(r.Objects) == 0 &&
		r.SchemaDelta.IsEmpty() &&
		r.PlannerDelta.IsEmpty() &&
		!hasActivity(r.ActivityDelta)
}

func hasActivity(nds []NodeActivityDelta) bool {
	for _, nd := range nds {
		if !nd.Delta.IsEmpty() {
			return true
		}
	}
	return false
}

// full keeps the raw deltas; summary drops them and caps the object list
func (r *Result) ForView(view string) *Result {
	if view == "full" {
		return r
	}
	cp := *r
	cp.SchemaDelta = nil
	cp.PlannerDelta = nil
	cp.ActivityDelta = nil
	if len(cp.Objects) > summaryObjectCap {
		cp.OmittedObjects = len(cp.Objects) - summaryObjectCap
		cp.Objects = cp.Objects[:summaryObjectCap]
	}
	return &cp
}
