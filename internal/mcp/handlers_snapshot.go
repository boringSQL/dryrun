package mcp

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/internal/snapdiff"
	"github.com/boringsql/dryrun/pkg/diff"
)

func (s *Server) handleReloadSchema(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// history.db wins — it carries planner/activity
	if a, ok := s.loadAnnotatedFromHistory(ctx); ok && a.Schema != nil {
		s.mu.Lock()
		s.annotated = a
		s.uninitialized = false
		s.mu.Unlock()
		return textResult(fmt.Sprintf("Schema loaded from history.db: %d tables, %d views, %d functions",
			len(a.Schema.Tables), len(a.Schema.Views), len(a.Schema.Functions))), nil
	}

	s.mu.RLock()
	candidates := append([]string(nil), s.schemaCandidates...)
	s.mu.RUnlock()

	for _, path := range candidates {
		if _, err := os.Stat(path); err != nil {
			continue
		}
		snap, err := schema.LoadSchemaFile(path)
		if err != nil {
			return errResult(fmt.Sprintf("failed to load %s: %v", path, err)), nil
		}
		s.mu.Lock()
		s.annotated = &schema.AnnotatedSchema{Schema: snap}
		s.uninitialized = false
		s.mu.Unlock()
		msg := fmt.Sprintf("Schema loaded from %s: %d tables, %d views, %d functions",
			path, len(snap.Tables), len(snap.Views), len(snap.Functions))
		// history.db would carry planner/activity stats but was skipped
		if note := s.historyNote(); note != nil {
			msg += "\n\nNote: " + *note + "."
		}
		return textResult(msg), nil
	}

	var lines []string
	for _, p := range candidates {
		lines = append(lines, "  - "+p)
	}
	msg := "no schema file found at any expected location"
	if len(lines) > 0 {
		msg += ":\n" + strings.Join(lines, "\n")
	}
	msg += "\n\nRun `dryrun dump-schema --db <DATABASE_URL>` first."
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
	return s.runSnapshotDiff(ctx, opt, argOr(req, "view", "summary"), capArg(req))
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

// limit: explicit 0 means all; absent falls back to the default cap.
func capArg(req mcp.CallToolRequest) int {
	if v, ok := req.GetArguments()["limit"]; ok {
		if f, ok := v.(float64); ok && f >= 0 {
			return int(f)
		}
	}
	return defaultMaxItems
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
		next = append(next, NextCall{Tool: "vacuum_health", Args: map[string]any{}})
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
	savedSnap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	liveSnap, err := schema.IntrospectSchema(ctx, pool)
	if err != nil {
		return errResult(fmt.Sprintf("introspection failed: %v", err)), nil
	}

	report := diff.ClassifyDrift(savedSnap, liveSnap)

	if report.Direction == diff.DriftIdentical {
		return textResult(s.wrapText(fmt.Sprintf("No drift detected. Schema hash: %s", report.LiveHash), "")), nil
	}

	return s.metaJSONResult(report, "", "", nil), nil
}
