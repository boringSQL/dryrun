package diff

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

type (
	PlannerDelta struct {
		FromHash string        `json:"from_hash"`
		ToHash   string        `json:"to_hash"`
		Sizing   []SizingDelta `json:"sizing"`
		Stats    []StatDelta   `json:"stats,omitempty"`
	}

	SizingDelta struct {
		Identity ObjectRef `json:"identity"`
		Metric   string    `json:"metric"`
		ValueA   float64   `json:"value_a"`
		ValueB   float64   `json:"value_b"`
		Delta    float64   `json:"delta"`
		Pct      *float64  `json:"pct,omitempty"`
	}

	// column-stats deltas share the sizing shape; only the metric vocabulary differs.
	StatDelta = SizingDelta
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

	MetricNDistinct   = "n_distinct"
	MetricNullFrac    = "null_frac"
	MetricCorrelation = "correlation"
	MetricMCVChurn    = "mcv_churn"
)

func (d *PlannerDelta) IsEmpty() bool { return d == nil || (len(d.Sizing) == 0 && len(d.Stats) == 0) }

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
	stats := diffColumnStats(from, to, fromT, toT)
	return &PlannerDelta{FromHash: from.ContentHash, ToHash: to.ContentHash, Sizing: rows, Stats: stats}, nil
}

func diffColumnStats(from, to *snapshot.PlannerStatsSnapshot, fromT, toT map[string]*snapshot.TableSizingEntry) []StatDelta {
	key := func(e snapshot.ColumnStatsEntry) string { return e.Table.String() + "\x00" + e.Column }
	fromC := indexBy(from.Columns, key)
	toC := indexBy(to.Columns, key)
	var rows []StatDelta
	for _, k := range unionKeys(fromC, toC) {
		a, b := fromC[k], toC[k]
		if a == nil || b == nil {
			continue // a stat delta needs both endpoints; one-sided columns surface in sizing
		}
		s := a.Table.Schema
		ref := ObjectRef{Kind: "column", Schema: &s, Name: a.Table.Name + "." + a.Column}
		relA, relB := tableReltuples(fromT, a.Table), tableReltuples(toT, b.Table)
		rows = append(rows, columnStatRows(ref, a.Stats, b.Stats, relA, relB)...)
	}
	sortSizing(rows)
	return rows
}

func columnStatRows(ref ObjectRef, a, b snapshot.ColumnStats, relA, relB float64) []StatDelta {
	var rows []StatDelta
	if a.NDistinct != nil && b.NDistinct != nil {
		rows = append(rows, sizingRow(ref, MetricNDistinct, absNDistinct(*a.NDistinct, relA), absNDistinct(*b.NDistinct, relB)))
	}
	if a.NullFrac != nil && b.NullFrac != nil {
		rows = append(rows, sizingRow(ref, MetricNullFrac, *a.NullFrac, *b.NullFrac))
	}
	if a.Correlation != nil && b.Correlation != nil {
		rows = append(rows, sizingRow(ref, MetricCorrelation, *a.Correlation, *b.Correlation))
	}
	if churn, ok := mcvChurn(ref, a.MostCommonVals, b.MostCommonVals); ok {
		rows = append(rows, churn)
	}
	return rows
}

// negative n_distinct is a ratio of reltuples; resolve to a count.
func absNDistinct(nd, reltuples float64) float64 {
	if nd >= 0 {
		return nd
	}
	return -nd * reltuples
}

func tableReltuples(m map[string]*snapshot.TableSizingEntry, t snapshot.QualifiedName) float64 {
	if e := m[t.String()]; e != nil {
		return e.Sizing.Reltuples
	}
	return 0
}

// Delta carries MCV set turnover (members that entered or left), Pct the churn
// fraction; the most-common-values list has no meaningful B-A on its own.
func mcvChurn(ref ObjectRef, a, b *string) (StatDelta, bool) {
	if a == nil && b == nil {
		return StatDelta{}, false
	}
	sa, sb := parseMCV(a), parseMCV(b)
	union := make(map[string]bool, len(sa)+len(sb))
	for v := range sa {
		union[v] = true
	}
	for v := range sb {
		union[v] = true
	}
	if len(union) == 0 {
		return StatDelta{}, false
	}
	var inter int
	for v := range sa {
		if sb[v] {
			inter++
		}
	}
	turnover := float64(len(union) - inter)
	churn := turnover / float64(len(union)) * 100
	return StatDelta{
		Identity: ref, Metric: MetricMCVChurn,
		ValueA: float64(len(sa)), ValueB: float64(len(sb)),
		Delta: turnover, Pct: &churn,
	}, true
}

func parseMCV(p *string) map[string]bool {
	set := make(map[string]bool)
	if p == nil {
		return set
	}
	s := strings.TrimSpace(*p)
	s = strings.TrimSuffix(strings.TrimPrefix(s, "{"), "}")
	if s == "" {
		return set
	}
	var b strings.Builder
	inQuote := false
	flush := func() {
		set[b.String()] = true
		b.Reset()
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			if inQuote && i+1 < len(s) && s[i+1] == '"' {
				b.WriteByte('"')
				i++
			} else {
				inQuote = !inQuote
			}
		case c == ',' && !inQuote:
			flush()
		default:
			b.WriteByte(c)
		}
	}
	flush()
	return set
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
	MetricNDistinct: 7, MetricNullFrac: 8, MetricCorrelation: 9, MetricMCVChurn: 10,
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
	sizing := plannerMovers(env.Planner.Sizing, minPct)
	stats := plannerMovers(env.Planner.Stats, minPct)
	if len(sizing)+len(stats) == 0 {
		fmt.Fprintf(w, "  no movers ≥ %g%%\n", minPct)
		return
	}
	fmt.Fprintf(w, "  %s changed, top movers:\n\n", plural(len(sizing)+len(stats), "object", "objects"))
	renderSizingGroups(w, sizing)
	if len(stats) > 0 {
		fmt.Fprintln(w, "stats drift:")
		renderSizingGroups(w, stats)
	}
}

func renderSizingGroups(w io.Writer, groups []sizingGroup) {
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
	switch {
	case isByteMetric(r.Metric):
		h = humanizeBytes
	case isFracMetric(r.Metric):
		h = humanizeFrac
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

func isFracMetric(m string) bool {
	return m == MetricNullFrac || m == MetricCorrelation
}

func humanizeFrac(v float64) string { return fmt.Sprintf("%.3f", v) }

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
