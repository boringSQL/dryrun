package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

const (
	// Well above the per-tool budgets, so it only catches what they miss.
	MaxResultBytes = 1 << 20
)

// CeilingOption applies the result ceiling to a server.
func CeilingOption() mcpserver.ServerOption {
	return mcpserver.WithToolHandlerMiddleware(ResultCeiling(MaxResultBytes))
}

// ResultCeiling replaces an oversized result with an error saying how to ask for less,
// since an arbitrary result cannot be cut meaningfully.
func ResultCeiling(maxBytes int) mcpserver.ToolHandlerMiddleware {
	return func(next mcpserver.ToolHandlerFunc) mcpserver.ToolHandlerFunc {
		return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			res, err := next(ctx, req)
			if err != nil || res == nil || res.IsError {
				return res, err
			}
			size, tooBig := resultSize(res, maxBytes)
			if !tooBig {
				return res, nil
			}
			slog.Warn("tool result over the size ceiling", "tool", req.Params.Name, "bytes", size, "ceiling", maxBytes)
			return errResult(fmt.Sprintf(
				"%s returned %d KB, over the %d KB limit for one response, so nothing was sent. "+
					"Ask for less: lower limit=, page with offset=, or narrow with schema=, table= or node= where the tool takes them.",
				req.Params.Name, size>>10, maxBytes>>10)), nil
		}
	}
}

// resultSize counts text plus the structured copy, which both ride the wire;
// the structured side is only marshalled when text alone fits.
func resultSize(res *mcp.CallToolResult, maxBytes int) (int, bool) {
	size := 0
	for _, c := range res.Content {
		switch v := c.(type) {
		case mcp.TextContent:
			size += len(v.Text)
		case mcp.ImageContent:
			size += len(v.Data)
		default:
			// rare types (audio, embedded resources) are measured by their encoding
			if b, err := json.Marshal(c); err == nil {
				size += len(b)
			}
		}
	}
	if size <= maxBytes && res.StructuredContent != nil {
		if b, err := json.Marshal(res.StructuredContent); err == nil {
			size += len(b)
		}
	}
	return size, size > maxBytes
}
