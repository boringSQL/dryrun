package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/columnar"

	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleColumnarReport(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pool, err := s.requirePool()
	if err != nil {
		return errResult(err.Error()), nil
	}

	state, err := schema.CaptureColumnarState(ctx, pool)
	if err != nil {
		if errors.Is(err, schema.ErrNotAlloyDB) {
			return errResult("columnar_report requires an AlloyDB instance"), nil
		}
		return errResult(fmt.Sprintf("columnar capture failed: %v", err)), nil
	}

	findings := columnar.Analyze(state)
	if findings == nil {
		findings = []columnar.Finding{}
	}

	hint := ""
	if len(findings) > 0 {
		hint = "an empty store needs google_columnar_engine_add; stale blocks mean writes are outpacing refresh"
	}

	payload := struct {
		State    *columnar.State    `json:"state"`
		Findings []columnar.Finding `json:"findings"`
	}{state, findings}

	return s.metaJSONResult(payload, "", hint, nil), nil
}
