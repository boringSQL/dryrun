package diff

import (
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

type (
	PlannerDelta struct {
		FromHash string        `json:"from_hash"`
		ToHash   string        `json:"to_hash"`
		Sizing   []SizingDelta `json:"sizing"`
	}

	SizingDelta struct {
		Identity ObjectRef `json:"identity"`
		Metric   string    `json:"metric"`
		ValueA   float64   `json:"value_a"`
		ValueB   float64   `json:"value_b"`
		Delta    float64   `json:"delta"`
		Pct      *float64  `json:"pct,omitempty"`
	}
)

// planner payloads carry no OID, so identity is qualified-name keyed.
const (
	MetricReltuples    = "reltuples"
	MetricRelpages     = "relpages"
	MetricTableBytes   = "table_bytes"
	MetricTotalBytes   = "total_bytes"
	MetricIndexesBytes = "indexes_bytes"
	MetricToastBytes   = "toast_bytes"
	MetricIndexBytes   = "index_bytes"
)

func (d *PlannerDelta) IsEmpty() bool { return d == nil || len(d.Sizing) == 0 }

// error is for symmetry with the other Diff* funcs; sizing diffing never fails.
func DiffPlanner(from, to *snapshot.PlannerStatsSnapshot) (*PlannerDelta, error) {
	var rows []SizingDelta

	fromT := indexBy(from.Tables, func(e snapshot.TableSizingEntry) string { return e.Table.String() })
	toT := indexBy(to.Tables, func(e snapshot.TableSizingEntry) string { return e.Table.String() })
	for _, k := range unionKeys(fromT, toT) {
		ref, a, b := tableSizing(fromT[k], toT[k])
		rows = append(rows,
			sizingRow(ref, MetricReltuples, a.Reltuples, b.Reltuples),
			sizingRow(ref, MetricRelpages, float64(a.Relpages), float64(b.Relpages)),
			sizingRow(ref, MetricTableBytes, float64(a.TableSize), float64(b.TableSize)),
			sizingRow(ref, MetricTotalBytes, float64(a.TotalRelationSize), float64(b.TotalRelationSize)),
			sizingRow(ref, MetricIndexesBytes, float64(a.IndexesSize), float64(b.IndexesSize)),
		)
		if a.ToastSize != 0 || b.ToastSize != 0 {
			rows = append(rows, sizingRow(ref, MetricToastBytes, float64(a.ToastSize), float64(b.ToastSize)))
		}
	}

	fromI := indexBy(from.Indexes, func(e snapshot.IndexSizingEntry) string { return e.Table.String() + "\x00" + e.Index })
	toI := indexBy(to.Indexes, func(e snapshot.IndexSizingEntry) string { return e.Table.String() + "\x00" + e.Index })
	for _, k := range unionKeys(fromI, toI) {
		ref, a, b := indexSizing(fromI[k], toI[k])
		rows = append(rows,
			sizingRow(ref, MetricReltuples, a.Reltuples, b.Reltuples),
			sizingRow(ref, MetricRelpages, float64(a.Relpages), float64(b.Relpages)),
			sizingRow(ref, MetricIndexBytes, float64(a.Size), float64(b.Size)),
		)
	}

	sortSizing(rows)
	return &PlannerDelta{FromHash: from.ContentHash, ToHash: to.ContentHash, Sizing: rows}, nil
}

func sizingRow(ref ObjectRef, metric string, a, b float64) SizingDelta {
	d := SizingDelta{Identity: ref, Metric: metric, ValueA: a, ValueB: b, Delta: b - a}
	if a != 0 {
		p := (b - a) / a * 100
		d.Pct = &p
	}
	return d
}

func tableSizing(a, b *snapshot.TableSizingEntry) (ObjectRef, snapshot.TableSizing, snapshot.TableSizing) {
	e := a
	if e == nil {
		e = b
	}
	s := e.Table.Schema
	ref := ObjectRef{Kind: "table", Schema: &s, Name: e.Table.Name}
	var as, bs snapshot.TableSizing
	if a != nil {
		as = a.Sizing
	}
	if b != nil {
		bs = b.Sizing
	}
	return ref, as, bs
}

func indexSizing(a, b *snapshot.IndexSizingEntry) (ObjectRef, snapshot.IndexSizing, snapshot.IndexSizing) {
	e := a
	if e == nil {
		e = b
	}
	s := e.Table.Schema
	ref := ObjectRef{Kind: "index", Schema: &s, Name: e.Index}
	var as, bs snapshot.IndexSizing
	if a != nil {
		as = a.Sizing
	}
	if b != nil {
		bs = b.Sizing
	}
	return ref, as, bs
}

func unionKeys[T any](a, b map[string]*T) []string {
	seen := make(map[string]bool, len(a)+len(b))
	keys := make([]string, 0, len(a)+len(b))
	for _, m := range []map[string]*T{a, b} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys
}

var sizingMetricOrder = map[string]int{
	MetricReltuples: 0, MetricRelpages: 1, MetricTableBytes: 2,
	MetricTotalBytes: 3, MetricIndexesBytes: 4, MetricToastBytes: 5, MetricIndexBytes: 6,
}

// Deterministic order is load-bearing: the cloud dedups on the delta.
func sortSizing(rows []SizingDelta) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.Identity.Kind != b.Identity.Kind {
			return a.Identity.Kind < b.Identity.Kind
		}
		if sa, sb := ptrStr(a.Identity.Schema), ptrStr(b.Identity.Schema); sa != sb {
			return sa < sb
		}
		if a.Identity.Name != b.Identity.Name {
			return a.Identity.Name < b.Identity.Name
		}
		return sizingMetricOrder[a.Metric] < sizingMetricOrder[b.Metric]
	})
}

// console hides |Δ| below this; JSON keeps every row.
const DefaultMinPct = 1.0

func RenderPlannerConsole(w io.Writer, env *SnapshotDiff, minPct float64) {
	fmt.Fprintf(w, "planner diff  %s → %s\n", short(env.FromHash), short(env.ToHash))
	if env.Planner.IsEmpty() {
		fmt.Fprintln(w, "  no changes")
		return
	}
	groups := plannerMovers(env.Planner.Sizing, minPct)
	if len(groups) == 0 {
		fmt.Fprintf(w, "  no movers ≥ %g%%\n", minPct)
		return
	}
	fmt.Fprintf(w, "  %s changed, top movers:\n\n", plural(len(groups), "object", "objects"))
	for _, g := range groups {
		fmt.Fprintf(w, "~ %s %s\n", g.ref.Kind, g.ref.Qualified())
		for _, r := range g.rows {
			fmt.Fprintf(w, "    %s\n", describeSizing(r))
		}
	}
}

type sizingGroup struct {
	ref  ObjectRef
	rows []SizingDelta
	peak float64 // sort key; +Inf for newly-present objects
}

func plannerMovers(rows []SizingDelta, minPct float64) []sizingGroup {
	idx := make(map[string]int)
	var groups []sizingGroup
	for _, r := range rows {
		if r.Delta == 0 || (r.Pct != nil && math.Abs(*r.Pct) < minPct) {
			continue
		}
		mag := math.Inf(1)
		if r.Pct != nil {
			mag = math.Abs(*r.Pct)
		}
		k := r.Identity.Kind + "\x00" + ptrStr(r.Identity.Schema) + "\x00" + r.Identity.Name
		i, ok := idx[k]
		if !ok {
			i = len(groups)
			idx[k] = i
			groups = append(groups, sizingGroup{ref: r.Identity})
		}
		groups[i].rows = append(groups[i].rows, r)
		if mag > groups[i].peak {
			groups[i].peak = mag
		}
	}
	sort.SliceStable(groups, func(i, j int) bool { return groups[i].peak > groups[j].peak })
	return groups
}

func describeSizing(r SizingDelta) string {
	h := humanizeCount
	if isByteMetric(r.Metric) {
		h = humanizeBytes
	}
	if r.Pct == nil {
		return fmt.Sprintf("%s  %s → %s  (%s)", r.Metric, h(r.ValueA), h(r.ValueB), signed(r.Delta, h))
	}
	return fmt.Sprintf("%s  %s → %s  (%s, %+.1f%%)", r.Metric, h(r.ValueA), h(r.ValueB), signed(r.Delta, h), *r.Pct)
}

func isByteMetric(m string) bool {
	switch m {
	case MetricTableBytes, MetricTotalBytes, MetricIndexesBytes, MetricToastBytes, MetricIndexBytes:
		return true
	}
	return false
}

func signed(v float64, h func(float64) string) string {
	if v < 0 {
		return "-" + h(-v)
	}
	return "+" + h(v)
}

func humanizeCount(v float64) string {
	a := math.Abs(v)
	switch {
	case a >= 1e12:
		return fmt.Sprintf("%.1fT", v/1e12)
	case a >= 1e9:
		return fmt.Sprintf("%.1fB", v/1e9)
	case a >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case a >= 1e3:
		return fmt.Sprintf("%.1fK", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

func humanizeBytes(v float64) string {
	a := math.Abs(v)
	switch {
	case a >= 1<<40:
		return fmt.Sprintf("%.1f TB", v/(1<<40))
	case a >= 1<<30:
		return fmt.Sprintf("%.1f GB", v/(1<<30))
	case a >= 1<<20:
		return fmt.Sprintf("%.1f MB", v/(1<<20))
	case a >= 1<<10:
		return fmt.Sprintf("%.1f KB", v/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", v)
	}
}
