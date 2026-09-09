package main

import (
	"testing"

	"github.com/boringsql/dryrun/internal/config"
)

// One predicate behind both the MCP peer note and the repo directive. Gating
// the directive on the --hindsight flag instead meant a later plain
// `dryrun setup` rewrote it to name one server while .mcp.json held two.
func TestHasHTTPRemote(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *config.ProjectConfig
		want bool
	}{
		{"no config", nil, false},
		{"no remotes", &config.ProjectConfig{}, false},
		{"oci only", &config.ProjectConfig{Remotes: []config.RemoteConfig{{Name: "gar", Type: "oci"}}}, false},
		{"untyped is oci", &config.ProjectConfig{Remotes: []config.RemoteConfig{{Name: "r"}}}, false},
		{"http", &config.ProjectConfig{Remotes: []config.RemoteConfig{{Name: "h", Type: "http"}}}, true},
		{"mixed", &config.ProjectConfig{Remotes: []config.RemoteConfig{
			{Name: "gar", Type: "oci"}, {Name: "h", Type: "http"},
		}}, true},
	} {
		if got := hasHTTPRemote(tc.cfg); got != tc.want {
			t.Errorf("%s: want %v, got %v", tc.name, tc.want, got)
		}
	}
}
