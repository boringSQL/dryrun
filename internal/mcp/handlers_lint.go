package mcp

import (
	"context"
	"encoding/json"
	"fmt"

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

	scope := argOr(req, "scope", "all")
	result := map[string]any{}

	if scope == "all" || scope == "conventions" {
		findings := lint.RunRules(target, &s.lintConfig)
		report := lint.NewReport(findings, len(target.Tables), "conventions")
		result["conventions"] = lint.CompactReportFromReportN(report, 5)
	}
	hasDDLFixes := false
	if scope == "all" || scope == "audit" {
		auditCfg := audit.DefaultConfig()
		findings := audit.RunRules(target, &auditCfg)
		for _, f := range findings {
			if f.DDLFix != nil {
				hasDDLFixes = true
				break
			}
		}
		result["audit"] = lint.NewReport(findings, len(target.Tables), "audit")
	}

	hint := ""
	if hasDDLFixes {
		hint = "Some findings include ddl_fix fields. Run those through check_migration before applying to verify lock safety."
	}
	s.injectMeta(result, hint)

	data, err := json.Marshal(result)
	if err != nil {
		return errResult(fmt.Sprintf("serialization error: %v", err)), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}
