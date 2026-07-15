// Package mcp is the public entry point to dryrun's offline MCP tool surface for
// callers outside this module (a hosted MCP-over-HTTP endpoint). It exposes the
// schema-only path; the live-DB and history-backed tools stay in internal/mcp.
package mcp

import (
	internalmcp "github.com/boringsql/dryrun/internal/mcp"
	"github.com/boringsql/dryrun/pkg/lint"
	"github.com/boringsql/dryrun/pkg/snapshot"

	mcpserver "github.com/mark3labs/mcp-go/server"
)

// Server is dryrun's MCP handler set. Construct it with NewOfflineServer.
type Server = internalmcp.Server

// NewOfflineServer builds a server answering from the in-memory AnnotatedSchema
// (assemble with snapshot.AssembleAnnotated). Register with RegisterOffline, or
// use BuildOfflineMCPServer for the whole thing.
func NewOfflineServer(a *snapshot.AnnotatedSchema, lintCfg lint.Config) *Server {
	return internalmcp.NewOfflineServerAnnotated(a, lintCfg)
}

// BuildOfflineMCPServer returns a ready mark3labs server with the offline subset
// registered and dryrun's standard handler options. A hosted transport wraps it;
// build one per request from that request's assembled snapshot.
func BuildOfflineMCPServer(name, version string, a *snapshot.AnnotatedSchema, lintCfg lint.Config) *mcpserver.MCPServer {
	s := NewOfflineServer(a, lintCfg)
	srv := mcpserver.NewMCPServer(name, version,
		mcpserver.WithInstructions(s.Instructions()),
		// a handler panic (malformed plan_json, bad SQL) returns a tool error instead of a dead stream
		mcpserver.WithRecovery(),
		// declared Enum/Required/type constraints reject bad args before handlers run
		mcpserver.WithInputSchemaValidation(),
		// a payload that drifts from its output schema fails here, not in the client
		mcpserver.WithOutputSchemaValidation(),
	)
	s.RegisterOffline(srv)
	return srv
}
