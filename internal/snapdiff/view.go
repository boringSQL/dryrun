package snapdiff

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/diff"
)

type objKey struct{ kind, schema, name string }

// folds the three deltas into one ranked, object-keyed list
func buildObjects(sd *diff.SchemaDelta, pd *diff.PlannerDelta, ad []NodeActivityDelta) []ObjectChange {
	idx := map[objKey]int{}
	var objs []ObjectChange
	at := func(k objKey) *ObjectChange {
		if i, ok := idx[k]; ok {
			return &objs[i]
		}
		idx[k] = len(objs)
		objs = append(objs, ObjectChange{Kind: k.kind, Schema: k.schema, Name: k.name})
		return &objs[len(objs)-1]
	}

	if sd != nil {
		for _, c := range sd.Changes {
			k := objKey{c.Object.Kind, ptrStr(c.Object.Schema), c.Object.Name}
			o := at(k)
			o.Structural = append(o.Structural, strings.TrimSpace(diff.Marker(c)+" "+diff.Describe(c)))
			o.score += schemaScore(c)
		}
	}

	if pd != nil {
		for _, r := range pd.Sizing {
			if !sizingMover(r) {
				continue
			}
			o := at(objKey{r.Identity.Kind, ptrStr(r.Identity.Schema), r.Identity.Name})
			o.Sizing = append(o.Sizing, diff.DescribeSizing(r))
			o.score += moverScore(r.Pct)
		}
		for _, r := range pd.Stats { // column stats: fold under the owning table
			if !sizingMover(r) {
				continue
			}
			table, col := splitColumn(r.Identity.Name)
			o := at(objKey{"table", ptrStr(r.Identity.Schema), table})
			o.Sizing = append(o.Sizing, col+" "+diff.DescribeSizing(r))
			o.score += moverScore(r.Pct)
		}
	}

	multiNode := len(ad) > 1
	for _, nd := range ad {
		if nd.Delta == nil {
			continue
		}
		for _, r := range nd.Delta.Counters {
			if !counterMover(r) {
				continue
			}
			o := at(objKey{r.Identity.Kind, ptrStr(r.Identity.Schema), r.Identity.Name})
			line := diff.DescribeCounter(r)
			if multiNode {
				line += " [" + nd.Node + "]"
			}
			o.Activity = append(o.Activity, line)
			o.score += counterScore(r)
		}
	}

	sort.SliceStable(objs, func(i, j int) bool {
		if objs[i].score != objs[j].score {
			return objs[i].score > objs[j].score
		}
		if objs[i].Kind != objs[j].Kind {
			return objs[i].Kind < objs[j].Kind
		}
		if objs[i].Schema != objs[j].Schema {
			return objs[i].Schema < objs[j].Schema
		}
		return objs[i].Name < objs[j].Name
	})
	return objs
}

func buildSummary(r *Result) Summary {
	var s Summary
	if r.SchemaDelta != nil {
		for _, c := range r.SchemaDelta.Changes {
			switch c.Type.Category() {
			case "added":
				s.Schema.Added++
			case "removed":
				s.Schema.Removed++
			default:
				s.Schema.Modified++
			}
		}
	}
	for _, o := range r.Objects {
		s.PlannerMovers += len(o.Sizing)
		s.ActivityMovers += len(o.Activity)
	}
	for _, nd := range r.QueryDelta {
		if nd.Delta.Incomparable != "" {
			s.QueryRefused++
			continue
		}
		for _, e := range nd.Delta.Entries {
			switch e.Status {
			case diff.QueryGrew, diff.QueryShrank, diff.QueryNew:
				s.QueryMovers++
			case diff.QueryReset, diff.QueryTruncated:
				// no subtractable baseline: growth is unknowable, so this is
				// not a mover
				s.QueryUnknown++
			}
		}
	}
	s.ObjectsChanged = len(r.Objects)
	for i, o := range r.Objects {
		if i >= 5 {
			break
		}
		s.TopObjects = append(s.TopObjects, qualifiedName(o))
	}
	s.Headline = headline(r, s)
	return s
}

func headline(r *Result, s Summary) string {
	if r.IsEmpty() {
		return fmt.Sprintf("no changes between %s and %s", short(r.FromHash), short(r.ToHash))
	}
	var parts []string
	if n := s.Schema.Added + s.Schema.Removed + s.Schema.Modified; n > 0 {
		parts = append(parts, fmt.Sprintf("%s (%d added, %d modified, %d removed)",
			plural(n, "schema change", "schema changes"), s.Schema.Added, s.Schema.Modified, s.Schema.Removed))
	}
	if s.PlannerMovers > 0 {
		parts = append(parts, fmt.Sprintf("%s of sizing/stats drift", plural(s.PlannerMovers, "mover", "movers")))
	}
	if s.ActivityMovers > 0 {
		parts = append(parts, fmt.Sprintf("%s of activity drift", plural(s.ActivityMovers, "mover", "movers")))
	}
	if s.QueryMovers > 0 {
		parts = append(parts, fmt.Sprintf("%s of query drift", plural(s.QueryMovers, "shape", "shapes")))
	}
	if s.QueryUnknown > 0 {
		parts = append(parts, fmt.Sprintf("%s with no comparable baseline", plural(s.QueryUnknown, "shape", "shapes")))
	}
	if s.QueryRefused > 0 {
		parts = append(parts, fmt.Sprintf("query stats not comparable on %s", plural(s.QueryRefused, "node", "nodes")))
	}
	if len(parts) == 0 {
		return fmt.Sprintf("no reportable changes between %s and %s", short(r.FromHash), short(r.ToHash))
	}
	// query shapes are not schema objects, so a query-only diff has none to
	// count and "across 0 objects" would read as a contradiction
	if s.ObjectsChanged > 0 {
		return fmt.Sprintf("%s across %s, %s → %s",
			strings.Join(parts, "; "), plural(s.ObjectsChanged, "object", "objects"), short(r.FromHash), short(r.ToHash))
	}
	return fmt.Sprintf("%s, %s → %s", strings.Join(parts, "; "), short(r.FromHash), short(r.ToHash))
}

func buildCorrelation(window time.Duration, fromKind, toKind history.SnapshotKind, from, to *moment) Correlation {
	c := Correlation{WindowMinutes: window.Minutes()}
	c.From = sideCorr(fromKind, from)
	c.To = sideCorr(toKind, to)
	c.Notes = correlationNotes(window, from, to)
	return c
}

func sideCorr(kind history.SnapshotKind, m *moment) SideCorr {
	return SideCorr{
		Anchor:   kind.String(),
		Hash:     m.hash,
		TakenAt:  m.takenAt,
		Schema:   m.schemaMatch,
		Planner:  m.plannerMatch,
		Activity: m.activityMatch,
	}
}

func correlationNotes(window time.Duration, from, to *moment) []string {
	var notes []string
	mins := int(window.Minutes())
	for _, side := range []struct {
		name string
		m    *moment
	}{{"from", from}, {"to", to}} {
		if side.m.planner == nil {
			notes = append(notes, fmt.Sprintf("%s: no planner capture within %dm of the anchor", side.name, mins))
		}
		if len(side.m.activity) == 0 {
			notes = append(notes, fmt.Sprintf("%s: no activity capture within %dm of the anchor", side.name, mins))
		}
	}
	return notes
}

// ranking weights, not risk judgment
func schemaScore(c diff.Change) float64 {
	switch c.Type {
	case diff.TableDropped, diff.ColumnDropped, diff.IndexDropped, diff.ConstraintDropped,
		diff.ViewDropped, diff.FunctionDropped, diff.ObjectDropped:
		return 100
	case diff.ColumnTypeChanged, diff.ColumnSetNotNull, diff.FuncSecurityDefiner, diff.RLSToggled:
		return 70
	}
	switch c.Type.Category() {
	case "added":
		return 30
	default:
		return 45
	}
}

func moverScore(pct *float64) float64 {
	if pct == nil { // from/to zero: newly present or vanished
		return 50
	}
	return math.Min(math.Abs(*pct), 1000) / 10
}

func counterScore(r diff.CounterDelta) float64 {
	if r.ResetBetween {
		return 40
	}
	return moverScore(r.Pct)
}

func sizingMover(r diff.SizingDelta) bool {
	if r.Delta == 0 {
		return false
	}
	return r.Pct == nil || math.Abs(*r.Pct) >= minMoverPct
}

func counterMover(r diff.CounterDelta) bool {
	if r.ResetBetween {
		return true
	}
	if r.Delta == nil || *r.Delta == 0 {
		return false
	}
	return r.Pct == nil || math.Abs(*r.Pct) >= minMoverPct
}

// keepRef matches a ref against the schema/table filter, resolving the row to its
// owning table so an index's sizing/scan drift surfaces under its table.
func keepRef(o diff.ObjectRef, schemaF, tableF string) bool {
	if schemaF != "" && ptrStr(o.Schema) != schemaF {
		return false
	}
	if tableF == "" {
		return true
	}
	return owningTable(o) == tableF
}

func owningTable(o diff.ObjectRef) string {
	if o.Table != nil && *o.Table != "" { // index rows carry their table
		return *o.Table
	}
	if o.Kind == "column" { // column rows are table.col
		t, _ := splitColumn(o.Name)
		return t
	}
	return o.Name // the table (or view/function) itself
}

func filterSchemaDelta(d *diff.SchemaDelta, sf, tf string) *diff.SchemaDelta {
	if d == nil {
		return nil
	}
	var ch []diff.Change
	for _, c := range d.Changes {
		if keepRef(c.Object, sf, tf) {
			ch = append(ch, c)
		}
	}
	cp := *d
	cp.Changes = ch
	return &cp
}

func filterPlannerDelta(d *diff.PlannerDelta, sf, tf string) *diff.PlannerDelta {
	if d == nil {
		return nil
	}
	cp := *d
	cp.Sizing = filterSizing(d.Sizing, sf, tf)
	cp.Stats = filterSizing(d.Stats, sf, tf)
	return &cp
}

func filterSizing(rows []diff.SizingDelta, sf, tf string) []diff.SizingDelta {
	var out []diff.SizingDelta
	for _, r := range rows {
		if keepRef(r.Identity, sf, tf) {
			out = append(out, r)
		}
	}
	return out
}

func filterActivityDeltas(nds []NodeActivityDelta, sf, tf string) []NodeActivityDelta {
	var out []NodeActivityDelta
	for _, nd := range nds {
		if nd.Delta == nil {
			continue
		}
		var cs []diff.CounterDelta
		for _, r := range nd.Delta.Counters {
			if keepRef(r.Identity, sf, tf) {
				cs = append(cs, r)
			}
		}
		if len(cs) == 0 {
			continue
		}
		cpd := *nd.Delta
		cpd.Counters = cs
		out = append(out, NodeActivityDelta{Node: nd.Node, Delta: &cpd})
	}
	return out
}

func qualifiedName(o ObjectChange) string {
	if o.Schema != "" {
		return o.Kind + " " + o.Schema + "." + o.Name
	}
	return o.Kind + " " + o.Name
}

func splitColumn(name string) (table, col string) {
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name[:i], name[i+1:]
	}
	return name, name
}

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}
