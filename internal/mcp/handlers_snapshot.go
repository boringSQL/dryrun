package mcp

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/diff"
	"github.com/boringsql/dryrun/internal/schema"
)

func (s *Server) handleRefreshSchema(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pool, err := s.requirePool()
	if err != nil {
		return errResult(err.Error()), nil
	}

	snap, err := schema.IntrospectSchema(ctx, pool)
	if err != nil {
		return errResult(fmt.Sprintf("introspection failed: %v", err)), nil
	}

	s.mu.Lock()
	s.snap = snap
	s.mu.Unlock()

	hash := snap.ContentHash
	if len(hash) > 16 {
		hash = hash[:16]
	}
	return textResult(fmt.Sprintf("Schema refreshed: %d tables, %d views, %d functions (hash: %s)",
		len(snap.Tables), len(snap.Views), len(snap.Functions), hash)), nil
}

func (s *Server) handleReloadSchema(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
		s.snap = snap
		s.uninitialized = false
		s.mu.Unlock()
		return textResult(fmt.Sprintf("Schema loaded from %s: %d tables, %d views, %d functions",
			path, len(snap.Tables), len(snap.Views), len(snap.Functions))), nil
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

	return s.metaJSONResult(report, "", ""), nil
}
