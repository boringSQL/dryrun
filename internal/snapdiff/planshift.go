package snapdiff

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/pkg/diff"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// summary keeps this many flags per node, without SQL text
const summaryPlanShifts = 5

// PlanShiftsForPair looks for plan flips between two query captures of one node,
// reading the baseline from the store; a nil store or missing history lands in
// Suppressed, never as an empty list.
func PlanShiftsForPair(ctx context.Context, store *history.Store, key history.SnapshotKey, a, b *snapshot.QueryStatsSnapshot) *diff.PlanShiftResult {
	from, to := a, b
	if from.Node.Timestamp.After(to.Node.Timestamp) {
		from, to = to, from
	}
	if store == nil {
		return suppressedPlanShifts(to, "no history store to read the earlier captures from")
	}
	if from.Node.Source != to.Node.Source {
		return suppressedPlanShifts(to, "the captures come from different nodes")
	}

	// a rotating label pairs the newer capture with its own server's earlier one
	baseFrom, note, refusal, err := poolBaseline(ctx, store, key, history.WrapQueryStats(from), history.WrapQueryStats(to))
	if err != nil {
		return suppressedPlanShifts(to, err.Error())
	}
	if refusal != "" {
		return suppressedPlanShifts(to, refusal)
	}
	if baseFrom != nil {
		from = baseFrom.AsQueryStats()
	}

	started := to.Node.PostmasterStartTime
	if started == nil {
		return suppressedPlanShifts(to, "the newer capture carries no server start time, so earlier captures of the same server cannot be identified")
	}
	kind := history.QueryKind(to.Node.Source)
	prevAt := func(t time.Time) (*snapshot.QueryStatsSnapshot, bool, error) {
		sn, found, err := store.MemberBaseline(ctx, key, kind, *started, from.Node.Timestamp, t)
		if err != nil || !found {
			return nil, false, err
		}
		return sn.AsQueryStats(), true, nil
	}
	base, why, err := diff.PickPlanShiftBaseline(from, to, prevAt)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return suppressedPlanShifts(to, "the request ended before the earlier captures were read")
		}
		return suppressedPlanShifts(to, "could not read the earlier captures: "+err.Error())
	}
	if why != "" {
		return suppressedPlanShifts(to, why)
	}
	res := diff.PlanShifts(base, from, to)
	if note != "" {
		res.Notes = append(res.Notes, note)
	}
	return res
}

func suppressedPlanShifts(to *snapshot.QueryStatsSnapshot, why string) *diff.PlanShiftResult {
	return &diff.PlanShiftResult{Node: to.Node.Source, Shifts: []diff.PlanShift{}, Suppressed: why}
}

func planShiftsByNode(ctx context.Context, store *history.Store, key history.SnapshotKey, from, to *moment) []NodePlanShifts {
	var out []NodePlanShifts
	for node, a := range from.query {
		b, ok := to.query[node]
		if !ok {
			continue
		}
		out = append(out, NodePlanShifts{Node: node, Result: PlanShiftsForPair(ctx, store, key, a, b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

func planShiftCount(nps []NodePlanShifts) int {
	n := 0
	for _, np := range nps {
		n += len(np.Result.Shifts)
	}
	return n
}

// summary: the biggest few per node, with no SQL text
func summarizePlanShifts(nps []NodePlanShifts) []NodePlanShifts {
	if nps == nil {
		return nil
	}
	out := make([]NodePlanShifts, len(nps))
	for i, np := range nps {
		cp := *np.Result
		cp.Shifts = make([]diff.PlanShift, 0, min(len(np.Result.Shifts), summaryPlanShifts))
		for _, s := range np.Result.Shifts {
			if len(cp.Shifts) == summaryPlanShifts {
				break
			}
			s.Canonical = ""
			cp.Shifts = append(cp.Shifts, s)
		}
		out[i] = NodePlanShifts{Node: np.Node, Result: &cp}
	}
	return out
}

func capPlanShifts(nps []NodePlanShifts, limit int) ([]NodePlanShifts, int) {
	if nps == nil {
		return nil, 0
	}
	out := make([]NodePlanShifts, len(nps))
	total := 0
	for i, np := range nps {
		cp := *np.Result
		var om int
		cp.Shifts, om = capSlice(np.Result.Shifts, limit)
		out[i] = NodePlanShifts{Node: np.Node, Result: &cp}
		total += om
	}
	return out, total
}
