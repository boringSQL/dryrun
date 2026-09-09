package main

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/history"
)

const (
	// Distinct from mcpServerName so both can be registered at once.
	hindsightServerName = "hindsight"

	// shared with buildHTTPStore: a mismatch is a 401 nobody can see
	defaultTokenEnv = "DRYRUN_TOKEN"

	// written where a client expands nothing
	tokenPlaceholder = "<paste your token>"
)

type hindsightTarget struct {
	URL      string
	TokenEnv string
	// which profile named the database, so the reported URL can be checked
	Profile string
	// unset in this shell: still correct to write, it just will not
	// authenticate until the caller exports it
	TokenUnset bool
	// a bearer token over plaintext to somewhere other than this machine
	Insecure bool
}

// Derived from what push already knows (remote base + snapshot key); the cloud
// resolves /mcp/{project}/{database} by the same ids, so no second mapping.
func resolveHindsight(cfg *config.ProjectConfig, key history.SnapshotKey, remoteName, profile string) (hindsightTarget, error) {
	// unreachable from the CLI, which reports loadProjectConfig's error first
	if cfg == nil {
		return hindsightTarget{}, fmt.Errorf("no dryrun.toml found; run `dryrun init` first")
	}
	r, err := cfg.ResolveRemote(remoteName)
	if err != nil {
		if len(cfg.Remotes) == 0 {
			return hindsightTarget{}, fmt.Errorf("%w; add one with `dryrun remote add hindsight --type http --ref https://...`", err)
		}
		return hindsightTarget{}, err
	}
	if r.Type != "http" {
		return hindsightTarget{}, fmt.Errorf("remote %q is type %q; the hosted endpoint needs an http remote", r.Name, r.Type)
	}
	base, err := endpointBase(r.Name, r.Ref)
	if err != nil {
		return hindsightTarget{}, err
	}
	if key.ProjectID == "" || key.DatabaseID == "" {
		return hindsightTarget{}, fmt.Errorf("cannot tell which database this is (project=%q database=%q); set [project].id or a profile in dryrun.toml",
			key.ProjectID, key.DatabaseID)
	}
	for _, part := range []string{string(key.ProjectID), string(key.DatabaseID)} {
		if !addressable(part) {
			return hindsightTarget{}, fmt.Errorf("%q cannot be addressed as a url path segment; the hosted endpoint cannot reach it", part)
		}
	}

	tokenEnv := r.TokenEnv
	if tokenEnv == "" {
		tokenEnv = defaultTokenEnv
	}
	return hindsightTarget{
		URL: base + "/mcp/" +
			url.PathEscape(string(key.ProjectID)) + "/" + url.PathEscape(string(key.DatabaseID)),
		TokenEnv:   tokenEnv,
		Profile:    profile,
		TokenUnset: os.Getenv(tokenEnv) == "",
		Insecure:   insecureBase(base),
	}, nil
}

// Rejects segments where EscapedPath disagrees with PathEscape (/ ; ,): chi
// routes the escaped literal and the lookup 404s with nothing to see.
func addressable(part string) bool {
	return (&url.URL{Path: "/" + part}).EscapedPath() == "/"+url.PathEscape(part)
}

func endpointBase(name, ref string) (string, error) {
	if ref == "" {
		return "", fmt.Errorf("remote %q: http remote requires a url in ref", name)
	}
	u, err := url.Parse(ref)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", fmt.Errorf("remote %q: ref %q is not an http(s) url", name, ref)
	}
	return strings.TrimSuffix(ref, "/"), nil
}

// http to anywhere but this machine sends the bearer token in the clear.
func insecureBase(base string) bool {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "http" {
		return false
	}
	host := u.Hostname()
	return host != "localhost" && host != "127.0.0.1" && host != "::1"
}

func (t hindsightTarget) entryFor(a agentDef) map[string]any {
	entry := hindsightServerEntry(t.URL, t.TokenEnv, a.tokenRef)
	// only Codex sources the token from its own config; anything else written
	// needs an actionable placeholder
	if _, ok := entry["headers"]; !ok && (a.kind == agentJSON || a.tokenRef == tokenNoExpansion) {
		entry["headers"] = map[string]any{"Authorization": "Bearer " + tokenPlaceholder}
	}
	return entry
}

func (t hindsightTarget) report(w io.Writer) {
	fmt.Fprintf(w, "Hindsight endpoint: %s\n", t.URL)
	if t.Profile != "" {
		fmt.Fprintf(w, "  from profile %q — check this is the database you meant.\n", t.Profile)
	}
	// holds on every agent path: Zed gets a placeholder, not a reference
	fmt.Fprintf(w, "No token value is written: each agent gets a reference to %s that its client expands, or a placeholder to fill in where the client expands none.\n", t.TokenEnv)
	if t.TokenUnset {
		fmt.Fprintf(w, "  %s is not set in this shell; agents that read it need it exported.\n", t.TokenEnv)
	}
	fmt.Fprintln(w, "  The token must carry mcp:read, or be a workspace token with no scopes at all — a token scoped to pushing alone would not reach this endpoint.")
	if t.Insecure {
		fmt.Fprintln(w, "  WARNING: this remote is plain http, so the token crosses the network in the clear.")
	}
}
