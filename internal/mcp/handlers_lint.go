package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/audit"
	"github.com/boringsql/dryrun/internal/lint"
)

func (s *Server) handleLintSchema(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snap, err := s.getSchema()
	if err != nil {
		return errResult(err.Error()), nil
	}

	target := filterSnap(snap, getArg(req, "schema"), getArg(req, "table"))

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
		findings := lint.RunRules(target, &s.lintConfig)
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
	if wantAudit {
		auditCfg := audit.DefaultConfig()
		findings := audit.RunRules(target, &auditCfg)
		for _, f := range findings {
			if f.DDLFix != nil {
				hasDDLFixes = true
				break
			}
		}
		hasAuditFindings = len(findings) > 0

		if fullMode {
			result["audit"] = lint.NewReport(findings, len(target.Tables), "audit")
		} else {
			result["audit"] = summarizeAudit(findings, len(target.Tables))
		}
	}

	hint := ""
	switch {
	case fullMode && hasDDLFixes:
		hint = "Some findings include ddl_fix fields. Run those through check_migration before applying to verify lock safety."
	case !fullMode && hasAuditFindings:
		hint = "Summary view. Re-run with verbosity=\"full\" for findings, recommendations, and ddl_fix."
	}
	s.injectMeta(result, hint, nil)

	data, err := json.Marshal(result)
	if err != nil {
		return errResult(fmt.Sprintf("serialization error: %v", err)), nil
	}
	return mcp.NewToolResultText(string(data)), nil
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
