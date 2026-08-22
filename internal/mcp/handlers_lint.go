package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/audit"
	"github.com/boringsql/dryrun/pkg/lint"
)

func (s *Server) handleLintSchema(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	a, err := s.getAnnotated()
	if err != nil {
		return errResult(err.Error()), nil
	}

	schemaF, tableF, filterNote := resolveTableFilter(a.Schema, schemaFilterArg(req), getArg(req, "table"))
	target := filterSnap(a.Schema, schemaF, tableF)

	// fields wins over scope when set
	var wantConventions, wantAudit bool
	if fields := getStringSliceArg(req, "fields"); fields != nil {
		known := map[string]bool{"conventions": true, "audit": true}
		for _, f := range fields {
			if !known[f] {
				return errResult(fmt.Sprintf("unknown field '%s'; valid: conventions, audit", f)), nil
			}
			if f == "conventions" {
				wantConventions = true
			}
			if f == "audit" {
				wantAudit = true
			}
		}
	} else {
		scope := argOr(req, "scope", "all")
		wantConventions = scope == "all" || scope == "conventions"
		wantAudit = scope == "all" || scope == "audit"
	}

	verbosity := argOr(req, "verbosity", "summary")
	if verbosity != "summary" && verbosity != "full" {
		return errResult(fmt.Sprintf("verbosity must be 'summary' or 'full', got '%s'", verbosity)), nil
	}
	fullMode := verbosity == "full"

	result := map[string]any{}

	if wantConventions {
		findings := lint.GateMinSeverity(lint.RunRules(target, &s.lintConfig), s.lintConfig.MinSeverity)
		report := lint.NewReport(findings, len(target.Tables), "conventions")
		compact := lint.CompactReportFromReportN(report, 5)
		if fullMode {
			result["conventions"] = compact
		} else {
			raw, _ := json.Marshal(compact)
			var asMap map[string]any
			_ = json.Unmarshal(raw, &asMap)
			if rg, ok := asMap["rule_groups"].([]any); ok {
				for _, e := range rg {
					if g, ok := e.(map[string]any); ok {
						delete(g, "items")
					}
				}
			}
			result["conventions"] = asMap
		}
	}

	var hasDDLFixes, hasAuditFindings bool
	var auditFindingsCount int
	if wantAudit {
		auditCfg := audit.DefaultConfig()
		// planner stays unfiltered; bloat rules iterate target.Tables and look up sizing by qual
		ta := &schema.AnnotatedSchema{Schema: target, Planner: a.Planner, Merged: a.Merged}
		findings := lint.GateMinSeverity(audit.RunRulesAnnotated(ta, &auditCfg), s.lintConfig.MinSeverity)
		for _, f := range findings {
			if f.DDLFix != nil {
				hasDDLFixes = true
				break
			}
		}
		auditFindingsCount = len(findings)
		hasAuditFindings = auditFindingsCount > 0

		if fullMode {
			result["audit"] = lint.NewReport(findings, len(target.Tables), "audit")
		} else {
			result["audit"] = summarizeAudit(findings, len(target.Tables))
		}
	}

	const fullNextThreshold = 50
	manyFindings := auditFindingsCount > fullNextThreshold

	hint := ""
	switch {
	case fullMode && hasDDLFixes:
		hint = "Some findings include ddl_fix fields. Run those through check_migration before applying to verify lock safety."
	case !fullMode && hasAuditFindings && manyFindings:
		hint = "Summary view; many findings. Narrow with schema=, table=, or scope= before re-running with verbosity=\"full\"."
	case !fullMode && hasAuditFindings:
		hint = "Summary view. Re-run with verbosity=\"full\" for findings, recommendations, and ddl_fix."
	}

	var next []NextCall
	if !fullMode && hasAuditFindings && !manyFindings {
		args := map[string]any{"verbosity": "full"}
		if schemaF != "" {
			args["schema"] = schemaF
		}
		if tableF != "" {
			args["table"] = tableF
		}
		if v := getArg(req, "scope"); v != "" {
			args["scope"] = v
		}
		if fields := getStringSliceArg(req, "fields"); fields != nil {
			args["fields"] = fields
		}
		next = []NextCall{{Tool: "lint_schema", Args: args}}
	}
	s.injectMeta(result, joinHints(filterNote, hint), next)
	return jsonResult(result), nil
}

// summarizeAudit collapses findings to per-rule counts; the full findings
// array and ddl_fix payloads are dropped.
func summarizeAudit(findings []lint.Finding, tablesAnalyzed int) map[string]any {
	type byRuleEntry struct {
		rule, category string
		severity       lint.Severity
		count          int
		tablesCount    int
	}
	groups := map[string]*byRuleEntry{}
	for _, f := range findings {
		cat := f.Rule
		for j, c := range f.Rule {
			if c == '_' || c == '/' {
				cat = f.Rule[:j]
				break
			}
		}
		g, ok := groups[f.Rule]
		if !ok {
			g = &byRuleEntry{rule: f.Rule, category: cat, severity: f.Severity}
			groups[f.Rule] = g
		}
		g.count++
		g.tablesCount += len(f.Tables)
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	list := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		g := groups[k]
		list = append(list, map[string]any{
			"rule":         g.rule,
			"category":     g.category,
			"severity":     g.severity,
			"count":        g.count,
			"tables_count": g.tablesCount,
		})
	}
	summary := lint.NewReport(findings, tablesAnalyzed, "audit").Summary
	return map[string]any{
		"by_rule":         list,
		"tables_analyzed": tablesAnalyzed,
		"summary":         summary,
	}
}
