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
	Schema string        // narrow to one schema
	Table  string        // narrow to one table
}

type (
	Result struct {
		PrimaryKind   string  `json:"primary_kind"`
		FromHash      string  `json:"from_hash"`
		ToHash        string  `json:"to_hash"`
		WindowMinutes float64 `json:"window_minutes"`

		Summary Summary        `json:"summary"`
		Objects []ObjectChange `json:"objects"`

		Truncated      bool `json:"truncated,omitempty"`
		OmittedObjects int  `json:"omitted_objects,omitempty"`
		OmittedRows    int  `json:"omitted_rows,omitempty"` // raw delta rows dropped in full view

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

// full keeps the raw deltas (capped at limit); summary drops them. limit<=0 = all.
func (r *Result) ForView(view string, limit int) *Result {
	cp := *r
	cp.Objects, cp.OmittedObjects = capSlice(cp.Objects, limit)
	if view == "full" {
		var po, ao int
		cp.SchemaDelta, cp.OmittedRows = capSchemaDelta(cp.SchemaDelta, limit)
		cp.PlannerDelta, po = capPlannerDelta(cp.PlannerDelta, limit)
		cp.ActivityDelta, ao = capActivityDeltas(cp.ActivityDelta, limit)
		cp.OmittedRows += po + ao
	} else {
		cp.SchemaDelta = nil
		cp.PlannerDelta = nil
		cp.ActivityDelta = nil
	}
	cp.Truncated = cp.OmittedObjects > 0 || cp.OmittedRows > 0
	return &cp
}

func capSlice[T any](s []T, limit int) ([]T, int) {
	if limit <= 0 || len(s) <= limit {
		return s, 0
	}
	return s[:limit], len(s) - limit
}

func capSchemaDelta(d *diff.SchemaDelta, limit int) (*diff.SchemaDelta, int) {
	if d == nil {
		return nil, 0
	}
	ch, om := capSlice(d.Changes, limit)
	cp := *d
	cp.Changes = ch
	return &cp, om
}

func capPlannerDelta(d *diff.PlannerDelta, limit int) (*diff.PlannerDelta, int) {
	if d == nil {
		return nil, 0
	}
	sz, o1 := capSlice(d.Sizing, limit)
	st, o2 := capSlice(d.Stats, limit)
	cp := *d
	cp.Sizing, cp.Stats = sz, st
	return &cp, o1 + o2
}

func capActivityDeltas(nds []NodeActivityDelta, limit int) ([]NodeActivityDelta, int) {
	if nds == nil {
		return nil, 0
	}
	out := make([]NodeActivityDelta, len(nds))
	total := 0
	for i, nd := range nds {
		out[i] = nd
		if nd.Delta != nil {
			c, om := capSlice(nd.Delta.Counters, limit)
			cpd := *nd.Delta
			cpd.Counters = c
			out[i].Delta = &cpd
			total += om
		}
	}
	return out, total
}
