// Package mcp is dryrun's public offline MCP surface for callers outside this
// module (a hosted endpoint). Live-DB and history tools stay in internal/mcp.
package mcp

import (
	internalmcp "github.com/boringsql/dryrun/internal/mcp"
	"github.com/boringsql/dryrun/pkg/lint"
	"github.com/boringsql/dryrun/pkg/snapshot"

	mcpserver "github.com/mark3labs/mcp-go/server"
)

// Server exposes only RegisterOffline, so the internal server's pool, history, and
// live tools never escape the module — a hosted caller cannot wire in a pool.
type Server struct {
	inner *internalmcp.Server
}

// NewOfflineServer builds a server answering from the in-memory AnnotatedSchema.
func NewOfflineServer(a *snapshot.AnnotatedSchema, lintCfg lint.Config) *Server {
	return &Server{inner: internalmcp.NewOfflineServerAnnotated(a, lintCfg)}
}

// RegisterOffline registers the schema-only subset (no snapshot_diff, reload_schema, live).
func (s *Server) RegisterOffline(srv *mcpserver.MCPServer) {
	s.inner.RegisterOffline(srv)
}

// BuildOfflineMCPServer returns a ready mark3labs server with the offline subset
// and dryrun's standard handler options; build one per request per snpshot
func BuildOfflineMCPServer(name, version string, a *snapshot.AnnotatedSchema, lintCfg lint.Config) *mcpserver.MCPServer {
	s := NewOfflineServer(a, lintCfg)
	srv := mcpserver.NewMCPServer(name, version,
		mcpserver.WithInstructions(s.inner.Instructions()),
		mcpserver.WithRecovery(),
		mcpserver.WithInputSchemaValidation(),
		mcpserver.WithOutputSchemaValidation(),
	)
	s.RegisterOffline(srv)
	return srv
}
