package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/internal/snapdiff"
	"github.com/boringsql/dryrun/pkg/diff"
)

// keeps one response readable by clients that cap or drop oversized tool results
const (
	maxSnapshotDiffBytes = 64 << 10
)

// snapshot-to-snapshot only; MCP has no live DB
func (s *Server) handleSnapshotDiff(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	opt := snapdiff.Options{
		From:   getArg(req, "from"),
		To:     getArg(req, "to"),
		Kind:   argOr(req, "kind", "schema"),
		Node:   getArg(req, "node"),
		Window: time.Duration(getFloatArg(req, "window_minutes", 30)) * time.Minute,
		Schema: getArg(req, "schema"),
		Table:  getArg(req, "table"),
	}
	return s.runSnapshotDiff(ctx, opt, argOr(req, "view", "summary"), limitArg(req), int(min(getFloatArg(req, "offset", 0), math.MaxInt32)))
}

func (s *Server) runSnapshotDiff(ctx context.Context, opt snapdiff.Options, view string, limit, offset int) (*mcp.CallToolResult, error) {
	s.mu.RLock()
	hist, key := s.history, s.snapshotKey
	s.mu.RUnlock()
	if hist == nil || key.ProjectID == "" {
		return errResult("no snapshot history available; capture with `dryrun init` or `dryrun snapshot capture` first"), nil
	}
	if note := s.historyNote(); note != nil {
		return errResult(*note), nil
	}

	res, err := snapdiff.Build(ctx, hist, key, opt)
	if err != nil {
		return errResult(err.Error()), nil
	}
	if res.IsEmpty() {
		return textResult(s.wrapText(res.Summary.Headline, "")), nil
	}

	payload, err := fitSnapshotDiff(res, view, limit, offset, maxSnapshotDiffBytes)
	if err != nil {
		narrow := narrowArgs(opt.Kind)
		if view == "full" {
			narrow += ", or use view=summary"
		}
		return errResult(fmt.Sprintf("%v; narrow with %s", err, narrow)), nil
	}
	hint, next := snapshotDiffFollowups(res, opt, view, limit, payload)
	return s.metaJSONResult(payload, "", hint, next), nil
}

// kind=query deltas ignore schema=/table=; only node= narrows them
func narrowArgs(kind string) string {
	if kind == "query" {
		return "node="
	}
	return "schema=/table="
}

// Pages are re-cut until the indented JSON (the larger of the two copies on the wire) fits budget.
// Limit shrinks by the overshoot ratio (always at least by one), so it ends within a few marshals.
func fitSnapshotDiff(res *snapdiff.Result, view string, limit, offset, budget int) (*snapdiff.Result, error) {
	// a limit past the longest list cuts nothing; clamping also keeps lim*budget from overflowing
	start := max(res.LongestList(), 1)
	if limit > 0 {
		start = min(limit, start)
	}
	for lim := start; ; {
		p := res.ForPage(view, lim, offset)
		b, err := json.MarshalIndent(p, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("serialization error: %w", err)
		}
		if len(b) <= budget {
			if p.Truncated {
				p.TruncatedBy, p.LimitApplied = "limit", lim
				if lim < start {
					p.TruncatedBy = "bytes"
				}
			}
			return p, nil
		}
		if lim <= 1 {
			return nil, fmt.Errorf("snapshot_diff result exceeds %d KB even at limit=1", budget>>10)
		}
		lim = max(lim*budget/len(b), 1)
	}
}

func snapshotDiffFollowups(res *snapdiff.Result, opt snapdiff.Options, view string, limit int, payload *snapdiff.Result) (string, []NextCall) {
	var next []NextCall
	if payload.Truncated {
		next = append(next, NextCall{Tool: "snapshot_diff", Args: nextPage(res, opt, view, limit, payload)})
	}
	for _, o := range res.Objects {
		if o.Kind == "table" {
			next = append(next, NextCall{Tool: "describe_table", Args: map[string]any{"table": o.Name, "schema": o.Schema}})
			break
		}
	}
	if res.Summary.PlannerMovers > 0 {
		next = append(next, NextCall{Tool: "detect", Args: map[string]any{"kind": "vacuum_health"}})
	}
	if res.Summary.ActivityMovers > 0 {
		next = append(next, NextCall{Tool: "detect", Args: map[string]any{"kind": "anomalies"}})
	}
	hint := "objects are ranked by significance; _meta.correlation shows how the planner/activity captures were matched to each anchor"
	if payload.Truncated {
		cut := fmt.Sprintf("capped at limit=%d", payload.LimitApplied)
		if payload.TruncatedBy == "bytes" {
			cut = fmt.Sprintf("cut to fit %d KB (limit_applied=%d)", maxSnapshotDiffBytes>>10, payload.LimitApplied)
		}
		hint += fmt.Sprintf(". Output %s; the next page is in _meta.next (offset=%d), or narrow with %s",
			cut, payload.Offset+payload.LimitApplied, narrowArgs(opt.Kind))
	}
	return hint, next
}

// Pins the anchor snapshots by hash, since "latest" moves when a capture lands between pages;
// planner/activity correlated to a schema anchor are still re-matched per page.
// The hash carries its own node, so node= is repeated only when the caller gave it.
func nextPage(res *snapdiff.Result, opt snapdiff.Options, view string, limit int, payload *snapdiff.Result) map[string]any {
	a := map[string]any{
		"from":   res.FromHash,
		"to":     res.ToHash,
		"limit":  limit,
		"offset": payload.Offset + payload.LimitApplied,
	}
	put := func(k, v string) {
		if v != "" {
			a[k] = v
		}
	}
	put("node", opt.Node)
	put("schema", opt.Schema)
	put("table", opt.Table)
	if opt.Kind != "" && opt.Kind != "schema" {
		a["kind"] = opt.Kind
	}
	if view != "" && view != "summary" {
		a["view"] = view
	}
	if opt.Window > 0 && opt.Window != snapdiff.DefaultWindow {
		a["window_minutes"] = opt.Window.Minutes()
	}
	return a
}

func (s *Server) handleCheckDrift(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pool, err := s.requirePool()
	if err != nil {
		return errResult(err.Error()), nil
	}
	liveSnap, err := schema.IntrospectSchema(ctx, pool)
	if err != nil {
		return errResult(fmt.Sprintf("introspection failed: %v", err)), nil
	}
	return s.driftAgainst(ctx, liveSnap), nil
}

// driftAgainst is pool-free so the baseline choice is testable without a database.
func (s *Server) driftAgainst(ctx context.Context, liveSnap *schema.SchemaSnapshot) *mcp.CallToolResult {

	savedSnap, baseline, err := s.driftBaseline(ctx)
	if err != nil {
		return errResult(err.Error())
	}
	if msg := databaseMismatch(savedSnap, liveSnap); msg != "" {
		return errResult(msg)
	}

	hint := ""
	if baseline == baselineLoaded {
		// "no drift" here only means nothing changed since startup
		hint = "No stored snapshot for this project and database, so the comparison is against the schema this server read at startup -- it cannot show a migration that ran before it. Run `dryrun snapshot capture`."
	}

	report := diff.ClassifyDrift(savedSnap, liveSnap)
	if report.Direction == diff.DriftIdentical {
		return textResult(s.wrapText(fmt.Sprintf("No drift detected against the %s baseline. Schema hash: %s", baseline, report.LiveHash), hint))
	}

	return s.metaJSONResult(driftResult{
		DriftReport:     report,
		Baseline:        baseline,
		BaselineTakenAt: stamp(savedSnap.Timestamp),
	}, "", hint, nil)
}

// databaseMismatch: the stored snapshot can be of another database entirely;
// reporting that as drift is worse than refusing.
func databaseMismatch(saved, live *schema.SchemaSnapshot) string {
	if saved.Database == "" || live.Database == "" || saved.Database == live.Database {
		return ""
	}
	return fmt.Sprintf("the stored snapshot is of database %q but the connection is to %q; a difference between two databases is not drift",
		saved.Database, live.Database)
}

type (
	// names the baseline the live database was compared against
	driftResult struct {
		*diff.DriftReport
		Baseline        string `json:"baseline"`
		BaselineTakenAt string `json:"baseline_taken_at,omitempty"`
	}
)
