package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/internal/snapdiff"
	"github.com/boringsql/dryrun/pkg/diff"
)

func (s *Server) handleReloadSchema(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if a, ok := s.loadAnnotatedFromHistory(ctx); ok && a.Schema != nil {
		s.mu.Lock()
		s.annotated = a
		s.uninitialized = false
		s.mu.Unlock()
		t, v, f := s.SchemaCounts()
		// what was just loaded, and when it was taken, is the whole question here
		return textResult(s.wrapText(
			fmt.Sprintf("Schema loaded from history.db: %d tables, %d views, %d functions", t, v, f), "")), nil
	}

	s.mu.RLock()
	key := s.snapshotKey
	s.mu.RUnlock()

	msg := fmt.Sprintf("no schema snapshot in history.db for project=%s database=%s", key.ProjectID, key.DatabaseID)
	if note := s.historyNote(); note != nil {
		msg += "\n\nNote: " + *note + "."
	}
	msg += "\n\nRun `dryrun init --db <DATABASE_URL>` or `dryrun snapshot take` first."
	return errResult(msg), nil
}

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
	return s.runSnapshotDiff(ctx, opt, argOr(req, "view", "summary"), limitArg(req))
}

func (s *Server) runSnapshotDiff(ctx context.Context, opt snapdiff.Options, view string, limit int) (*mcp.CallToolResult, error) {
	s.mu.RLock()
	hist, key := s.history, s.snapshotKey
	s.mu.RUnlock()
	if hist == nil || key.ProjectID == "" {
		return errResult("no snapshot history available; capture with `dryrun snapshot take` first"), nil
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

	payload := res.ForView(view, limit)
	hint, next := snapshotDiffFollowups(res, opt, view, payload.Truncated)
	return s.metaJSONResult(payload, "", hint, next), nil
}

func snapshotDiffFollowups(res *snapdiff.Result, opt snapdiff.Options, view string, truncated bool) (string, []NextCall) {
	var next []NextCall
	if truncated {
		next = append(next, NextCall{Tool: "snapshot_diff", Args: rerunUncapped(opt, view)})
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
	if truncated {
		hint += ". Output capped; narrow with schema=/table= or re-run with limit=0"
	}
	return hint, next
}

// pre-validated re-run with the cap lifted, carrying the same selection.
func rerunUncapped(opt snapdiff.Options, view string) map[string]any {
	a := map[string]any{"limit": 0}
	put := func(k, v string) {
		if v != "" {
			a[k] = v
		}
	}
	put("from", opt.From)
	put("to", opt.To)
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
		hint = "No stored snapshot for this project and database, so the comparison is against the schema this server read at startup -- it cannot show a migration that ran before it. Run `dryrun snapshot take`."
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
