package diff

import (
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/boringsql/dryrun/pkg/snapshot"
)

type (
	ActivityDelta struct {
		FromHash string         `json:"from_hash"`
		ToHash   string         `json:"to_hash"`
		Counters []CounterDelta `json:"counters"`
	}

	CounterDelta struct {
		Identity ObjectRef `json:"identity"`
		Metric   string    `json:"metric"`
		ValueA   float64   `json:"value_a"`
		ValueB   float64   `json:"value_b"`
		// nil when ResetBetween: a rolled-back cumulative counter has no honest delta.
		Delta        *float64 `json:"delta,omitempty"`
		Pct          *float64 `json:"pct,omitempty"`
		ResetBetween bool     `json:"reset_between,omitempty"`
	}

	// cumulative=false marks point-in-time gauges (live/dead tuples) that legitimately
	// fall, so a decrease there is not a reset signal.
	tableMetric struct {
		name       string
		get        func(snapshot.TableActivity) float64
		cumulative bool
	}

	indexMetric struct {
		name string
		get  func(snapshot.IndexActivity) float64
	}

	counterGroup struct {
		ref  ObjectRef
		rows []CounterDelta
		peak float64 // reset and newly-present objects sort first
	}
)

const (
	MetricSeqScan    = "seq_scan"
	MetricIdxScan    = "idx_scan"
	MetricNTupIns    = "n_tup_ins"
	MetricNTupUpd    = "n_tup_upd"
	MetricNTupDel    = "n_tup_del"
	MetricNTupHotUpd = "n_tup_hot_upd"
	MetricNLiveTup   = "n_live_tup"
	MetricNDeadTup   = "n_dead_tup"
	MetricIdxTupRead = "idx_tup_read"
	MetricIdxTupFch  = "idx_tup_fetch"
)

func (d *ActivityDelta) IsEmpty() bool { return d == nil || len(d.Counters) == 0 }

var (
	tableMetrics = []tableMetric{
		{MetricSeqScan, func(a snapshot.TableActivity) float64 { return float64(a.SeqScan) }, true},
		{MetricIdxScan, func(a snapshot.TableActivity) float64 { return float64(a.IdxScan) }, true},
		{MetricNTupIns, func(a snapshot.TableActivity) float64 { return float64(a.NTupIns) }, true},
		{MetricNTupUpd, func(a snapshot.TableActivity) float64 { return float64(a.NTupUpd) }, true},
		{MetricNTupDel, func(a snapshot.TableActivity) float64 { return float64(a.NTupDel) }, true},
		{MetricNTupHotUpd, func(a snapshot.TableActivity) float64 { return float64(a.NTupHotUpd) }, true},
		{MetricNLiveTup, func(a snapshot.TableActivity) float64 { return float64(a.NLiveTup) }, false},
		{MetricNDeadTup, func(a snapshot.TableActivity) float64 { return float64(a.NDeadTup) }, false},
	}

	indexMetrics = []indexMetric{
		{MetricIdxScan, func(a snapshot.IndexActivity) float64 { return float64(a.IdxScan) }},
		{MetricIdxTupRead, func(a snapshot.IndexActivity) float64 { return float64(a.IdxTupRead) }},
		{MetricIdxTupFch, func(a snapshot.IndexActivity) float64 { return float64(a.IdxTupFetch) }},
	}

	counterMetricOrder = map[string]int{
		MetricSeqScan: 0, MetricIdxScan: 1, MetricNTupIns: 2, MetricNTupUpd: 3,
		MetricNTupDel: 4, MetricNTupHotUpd: 5, MetricNLiveTup: 6, MetricNDeadTup: 7,
		MetricIdxTupRead: 8, MetricIdxTupFch: 9,
	}
)

func DiffActivity(from, to *snapshot.ActivityStatsSnapshot) (*ActivityDelta, error) {
	var rows []CounterDelta

	fromT := indexBy(from.Tables, func(e snapshot.TableActivityEntry) string { return e.Table.String() })
	toT := indexBy(to.Tables, func(e snapshot.TableActivityEntry) string { return e.Table.String() })
	for _, k := range unionKeys(fromT, toT) {
		a, b := fromT[k], toT[k]
		e := a
		if e == nil {
			e = b
		}
		s := e.Table.Schema
		ref := ObjectRef{Kind: "table", Schema: &s, Name: e.Table.Name}
		var av, bv snapshot.TableActivity
		if a != nil {
			av = a.Activity
		}
		if b != nil {
			bv = b.Activity
		}
		reset := a != nil && b != nil && tableReset(av, bv)
		for _, m := range tableMetrics {
			rows = append(rows, counterRow(ref, m.name, m.get(av), m.get(bv), reset && m.cumulative))
		}
	}

	fromI := indexBy(from.Indexes, func(e snapshot.IndexActivityEntry) string { return e.Table.String() + "\x00" + e.Index })
	toI := indexBy(to.Indexes, func(e snapshot.IndexActivityEntry) string { return e.Table.String() + "\x00" + e.Index })
	for _, k := range unionKeys(fromI, toI) {
		a, b := fromI[k], toI[k]
		e := a
		if e == nil {
			e = b
		}
		s := e.Table.Schema
		tn := e.Table.Name
		ref := ObjectRef{Kind: "index", Schema: &s, Name: e.Index, Table: &tn}
		var av, bv snapshot.IndexActivity
		if a != nil {
			av = a.Activity
		}
		if b != nil {
			bv = b.Activity
		}
		reset := a != nil && b != nil && indexReset(av, bv)
		for _, m := range indexMetrics {
			rows = append(rows, counterRow(ref, m.name, m.get(av), m.get(bv), reset))
		}
	}

	sortCounters(rows)
	return &ActivityDelta{FromHash: from.ContentHash, ToHash: to.ContentHash, Counters: rows}, nil
}

func counterRow(ref ObjectRef, metric string, a, b float64, reset bool) CounterDelta {
	d := CounterDelta{Identity: ref, Metric: metric, ValueA: a, ValueB: b}
	if reset {
		d.ResetBetween = true
		return d
	}
	delta := b - a
	d.Delta = &delta
	if a != 0 {
		p := (b - a) / a * 100
		d.Pct = &p
	}
	return d
}

// any rolled-back cumulative counter implies pg_stat_reset / failover between captures.
func tableReset(a, b snapshot.TableActivity) bool {
	return b.SeqScan < a.SeqScan || b.SeqTupRead < a.SeqTupRead ||
		b.IdxScan < a.IdxScan || b.IdxTupFetch < a.IdxTupFetch ||
		b.NTupIns < a.NTupIns || b.NTupUpd < a.NTupUpd ||
		b.NTupDel < a.NTupDel || b.NTupHotUpd < a.NTupHotUpd ||
		b.VacuumCount < a.VacuumCount || b.AutovacuumCount < a.AutovacuumCount ||
		b.AnalyzeCount < a.AnalyzeCount || b.AutoanalyzeCount < a.AutoanalyzeCount
}

func indexReset(a, b snapshot.IndexActivity) bool {
	return b.IdxScan < a.IdxScan || b.IdxTupRead < a.IdxTupRead || b.IdxTupFetch < a.IdxTupFetch
}

func sortCounters(rows []CounterDelta) {
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
		return counterMetricOrder[a.Metric] < counterMetricOrder[b.Metric]
	})
}

func RenderActivityConsole(w io.Writer, env *SnapshotDiff, minPct float64) {
	fmt.Fprintf(w, "activity diff  %s → %s\n", short(env.FromHash), short(env.ToHash))
	if env.Activity.IsEmpty() {
		fmt.Fprintln(w, "  no changes")
		return
	}
	groups := counterMovers(env.Activity.Counters, minPct)
	if len(groups) == 0 {
		fmt.Fprintf(w, "  no movers ≥ %g%%\n", minPct)
		return
	}
	fmt.Fprintf(w, "  %s changed, top movers:\n\n", plural(len(groups), "object", "objects"))
	for _, g := range groups {
		fmt.Fprintf(w, "~ %s %s\n", g.ref.Kind, g.ref.Qualified())
		for _, r := range g.rows {
			fmt.Fprintf(w, "    %s\n", describeCounter(r))
		}
	}
}

func counterMovers(rows []CounterDelta, minPct float64) []counterGroup {
	idx := make(map[string]int)
	var groups []counterGroup
	for _, r := range rows {
		if !r.ResetBetween {
			if r.Delta == nil || *r.Delta == 0 {
				continue
			}
			if r.Pct != nil && math.Abs(*r.Pct) < minPct {
				continue
			}
		}
		mag := math.Inf(1)
		if !r.ResetBetween && r.Pct != nil {
			mag = math.Abs(*r.Pct)
		}
		k := r.Identity.Kind + "\x00" + ptrStr(r.Identity.Schema) + "\x00" + r.Identity.Name
		i, ok := idx[k]
		if !ok {
			i = len(groups)
			idx[k] = i
			groups = append(groups, counterGroup{ref: r.Identity})
		}
		groups[i].rows = append(groups[i].rows, r)
		if mag > groups[i].peak {
			groups[i].peak = mag
		}
	}
	sort.SliceStable(groups, func(i, j int) bool { return groups[i].peak > groups[j].peak })
	return groups
}

func describeCounter(r CounterDelta) string {
	if r.ResetBetween {
		return fmt.Sprintf("%s  %s → %s  (reset between, delta omitted)", r.Metric, humanizeCount(r.ValueA), humanizeCount(r.ValueB))
	}
	if r.Pct == nil {
		return fmt.Sprintf("%s  %s → %s  (%s)", r.Metric, humanizeCount(r.ValueA), humanizeCount(r.ValueB), signed(*r.Delta, humanizeCount))
	}
	return fmt.Sprintf("%s  %s → %s  (%s, %+.1f%%)", r.Metric, humanizeCount(r.ValueA), humanizeCount(r.ValueB), signed(*r.Delta, humanizeCount), *r.Pct)
}
