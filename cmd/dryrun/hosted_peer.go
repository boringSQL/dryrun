package main

import "github.com/boringsql/dryrun/internal/config"

// hasHTTPRemote reports whether this project is configured to push to a hosted
// endpoint. One predicate, used by the MCP server's peer note and by the repo
// directive: gating the directive on the --hindsight FLAG instead meant a later
// plain `dryrun setup` rewrote it back to naming one server while .mcp.json
// still held both, which is the state the note exists to remove.
func hasHTTPRemote(cfg *config.ProjectConfig) bool {
	if cfg == nil {
		return false
	}
	for _, r := range cfg.Remotes {
		if r.Type == "http" {
			return true
		}
	}
	return false
}
