package snapdiff

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/diff"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// one side of the diff
type moment struct {
	anchorKind history.SnapshotKind
	hash       string
	takenAt    time.Time

	schema   *snapshot.SchemaSnapshot
	planner  *snapshot.PlannerStatsSnapshot
	activity map[string]*snapshot.ActivityStatsSnapshot // node -> snapshot

	schemaMatch   *MatchInfo
	plannerMatch  *MatchInfo
	activityMatch []MatchInfo
}

func Build(ctx context.Context, store *history.Store, key history.SnapshotKey, opt Options) (*Result, error) {
	if opt.From == "" {
		opt.From = "latest~1"
	}
	if opt.To == "" {
		opt.To = "latest"
	}
	if opt.Window <= 0 {
		opt.Window = DefaultWindow
	}

	fromKind, fromRef, err := store.ResolveToken(ctx, key, opt.From, opt.Kind, opt.Node)
	if err != nil {
		return nil, err
	}
	toKind, toRef, err := store.ResolveToken(ctx, key, opt.To, opt.Kind, opt.Node)
	if err != nil {
		return nil, err
	}
	if fromKind.Tag != toKind.Tag {
		return nil, fmt.Errorf("not comparable: %s is a %s snapshot, %s is a %s snapshot; pass two of the same kind",
			opt.From, fromKind, opt.To, toKind)
	}

	fromAnchor, err := store.Get(ctx, key, fromKind, fromRef)
	if err != nil {
		return nil, fmt.Errorf("resolving %q: %w", opt.From, err)
	}
	toAnchor, err := store.Get(ctx, key, toKind, toRef)
	if err != nil {
		return nil, fmt.Errorf("resolving %q: %w", opt.To, err)
	}

	nodes := activityNodes(ctx, store, key, opt.Node)

	fromM := assembleMoment(ctx, store, key, fromAnchor, nodes, opt.Window)
	toM := assembleMoment(ctx, store, key, toAnchor, nodes, opt.Window)

	res := &Result{
		PrimaryKind:   fromKind.String(),
		FromHash:      fromM.hash,
		ToHash:        toM.hash,
		WindowMinutes: opt.Window.Minutes(),
	}

	if fromM.schema != nil && toM.schema != nil {
		res.SchemaDelta, _ = diff.DiffSchema(fromM.schema, toM.schema)
	}
	if fromM.planner != nil && toM.planner != nil {
		res.PlannerDelta, _ = diff.DiffPlanner(fromM.planner, toM.planner)
	}
	res.ActivityDelta = diffActivityByNode(fromM, toM)

	if opt.Schema != "" || opt.Table != "" {
		res.SchemaDelta = filterSchemaDelta(res.SchemaDelta, opt.Schema, opt.Table)
		res.PlannerDelta = filterPlannerDelta(res.PlannerDelta, opt.Schema, opt.Table)
		res.ActivityDelta = filterActivityDeltas(res.ActivityDelta, opt.Schema, opt.Table)
	}

	res.Objects = buildObjects(res.SchemaDelta, res.PlannerDelta, res.ActivityDelta)
	res.Summary = buildSummary(res)
	res.Correlation = buildCorrelation(opt.Window, fromKind, toKind, fromM, toM)

	return res, nil
}

// pull planner+activity captured against this schemaHash. want==nil means all nodes
func assembleBySchemaRef(ctx context.Context, store *history.Store, key history.SnapshotKey, m *moment, schemaHash string, want map[string]bool) {
	if p, err := store.GetPlanner(ctx, key, schemaHash); err == nil {
		m.planner = p
		m.plannerMatch = &MatchInfo{Hash: p.ContentHash, TakenAt: p.Timestamp, SkewSeconds: p.Timestamp.Sub(m.takenAt).Seconds(), Source: "schema_ref"}
	}
	acts, err := store.GetActivity(ctx, key, schemaHash)
	if err != nil {
		return
	}
	for i := range acts {
		a := &acts[i]
		if want != nil && !want[a.Node.Source] {
			continue
		}
		m.activity[a.Node.Source] = a
		m.activityMatch = append(m.activityMatch, MatchInfo{
			Node: a.Node.Source, Hash: a.ContentHash, TakenAt: a.Node.Timestamp,
			SkewSeconds: a.Node.Timestamp.Sub(m.takenAt).Seconds(), Source: "schema_ref",
		})
	}
}

func nodeSet(nodes []string) map[string]bool {
	if len(nodes) == 0 {
		return nil
	}
	set := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		set[n] = true
	}
	return set
}

// explicit node, else every node with activity
func activityNodes(ctx context.Context, store *history.Store, key history.SnapshotKey, node string) []string {
	if node != "" {
		return []string{node}
	}
	kinds, err := store.ListKinds(ctx, key)
	if err != nil {
		return nil
	}
	var out []string
	for _, k := range kinds {
		if k.Tag == history.KindActivity {
			out = append(out, k.NodeLabel)
		}
	}
	return out
}

func assembleMoment(ctx context.Context, store *history.Store, key history.SnapshotKey, anchor history.StoredSnapshot, nodes []string, window time.Duration) *moment {
	m := &moment{
		anchorKind: anchor.Kind(),
		hash:       anchor.ContentHash(),
		takenAt:    anchor.Timestamp(),
		activity:   map[string]*snapshot.ActivityStatsSnapshot{},
	}

	// schema anchor: exact schema_ref join, no window
	if s := anchor.AsSchema(); s != nil {
		m.schema = s
		m.schemaMatch = &MatchInfo{Hash: m.hash, TakenAt: m.takenAt, Source: "anchor"}
		assembleBySchemaRef(ctx, store, key, m, s.ContentHash, nodeSet(nodes))
		return m
	}

	// non-schema anchor: schema exact via ref; other kind by window.
	if ref := anchor.SchemaRefHash(); ref != "" {
		if got, err := store.GetSchemaByExactHash(ctx, key, ref); err == nil {
			m.schema = got
			m.schemaMatch = &MatchInfo{Hash: got.ContentHash, TakenAt: got.Timestamp, Source: "schema_ref"}
		} else {
			slog.Debug("activity rollup unavailable: schema_ref not resolved", "schema_ref_hash", ref, "err", err)
		}
	}

	// planner: anchor itself, else nearest within window
	if p := anchor.AsPlanner(); p != nil {
		m.planner = p
		m.plannerMatch = &MatchInfo{Hash: m.hash, TakenAt: m.takenAt, Source: "anchor"}
	} else {
		list, err := store.List(ctx, key, history.PlannerKind(), windowRange(m.takenAt, window))
		if sum := nearest(list, err, m.takenAt); sum != nil {
			if got, gerr := store.Get(ctx, key, history.PlannerKind(), history.NewRefHash(sum.ContentHash)); gerr == nil {
				m.planner = got.AsPlanner()
				m.plannerMatch = matchFrom(sum, m.takenAt, "window")
			}
		}
	}

	// activity: per node, anchor itself else nearest within window
	for _, node := range nodes {
		if a := anchor.AsActivity(); a != nil && a.Node.Source == node {
			m.activity[node] = a
			m.activityMatch = append(m.activityMatch, MatchInfo{Node: node, Hash: m.hash, TakenAt: m.takenAt, Source: "anchor"})
			continue
		}
		list, err := store.List(ctx, key, history.ActivityKind(node), windowRange(m.takenAt, window))
		sum := nearest(list, err, m.takenAt)
		if sum == nil {
			continue
		}
		got, gerr := store.Get(ctx, key, history.ActivityKind(node), history.NewRefHash(sum.ContentHash))
		if gerr != nil {
			continue
		}
		m.activity[node] = got.AsActivity()
		mi := matchFrom(sum, m.takenAt, "window")
		mi.Node = node
		m.activityMatch = append(m.activityMatch, *mi)
	}

	return m
}

// only nodes captured on both sides; rolled up all-or-nothing per pair so a
// partitioned parent never appears on one side and reads as zero on the other
func diffActivityByNode(from, to *moment) []NodeActivityDelta {
	rollUp := from.schema != nil && to.schema != nil

	var out []NodeActivityDelta
	for node, a := range from.activity {
		b, ok := to.activity[node]
		if !ok {
			continue
		}
		fromA, toA := a, b
		if rollUp {
			fromA, toA = snapshot.RollUpActivitySnapshot(a, from.schema), snapshot.RollUpActivitySnapshot(b, to.schema)
		}
		d, err := diff.DiffActivity(fromA, toA)
		if err != nil || d.IsEmpty() {
			continue
		}
		out = append(out, NodeActivityDelta{Node: node, Delta: d})
	}
	return out
}

func windowRange(t time.Time, window time.Duration) history.TimeRange {
	from := t.Add(-window)
	to := t.Add(window + time.Second) // TimeRange.To is exclusive
	return history.TimeRange{From: &from, To: &to}
}

// pick the capture closest to the anchor; the query already bounded the window
func nearest(list []history.SnapshotSummary, err error, anchor time.Time) *history.SnapshotSummary {
	if err != nil || len(list) == 0 {
		return nil
	}
	best := &list[0]
	bestSkew := absDur(best.Timestamp.Sub(anchor))
	for i := range list {
		if d := absDur(list[i].Timestamp.Sub(anchor)); d < bestSkew {
			best, bestSkew = &list[i], d
		}
	}
	return best
}

func matchFrom(sum *history.SnapshotSummary, anchor time.Time, source string) *MatchInfo {
	return &MatchInfo{
		Hash:        sum.ContentHash,
		TakenAt:     sum.Timestamp,
		SkewSeconds: sum.Timestamp.Sub(anchor).Seconds(),
		Source:      source,
	}
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
