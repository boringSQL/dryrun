package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mattn/go-isatty"
)

const (
	mcpServerName = "dryrun"
	npmPackage    = "@boringsql/dryrun"

	directiveStart = "<!-- dryrun:start -->"
	directiveEnd   = "<!-- dryrun:end -->"
)

func directiveBody(hosted bool) string {
	lines := []string{
		directiveStart,
		"## Database schema",
		"",
		"This repo uses Postgres; the schema is captured in `.dryrun/`. Do not guess",
		"columns, indexes, or types — call the `dryrun` MCP server to inspect the",
		"schema, validate queries, and check migrations before writing SQL.",
	}
	// naming only the first server steers agents away from the one just added
	if hosted {
		lines = append(lines,
			"",
			"The `hindsight` server answers the same questions from the history of every",
			"node's pushes, and answers ones this repo's snapshot cannot: start with",
			"`worklist` for what to fix first.")
	}
	return strings.Join(append(lines, directiveEnd), "\n")
}

func mcpServerEntry() map[string]any {
	return map[string]any{
		"command": "npx",
		"args":    []any{"-y", npmPackage, "mcp-serve"},
	}
}

// Per-client token syntax: a ${VAR} in a config that does not expand it
// authenticates nothing, so the form is encoded here, not left to the caller.
type tokenRef int

const (
	tokenBraceVar    tokenRef = iota // Claude Code: ${VAR}
	tokenEnvVar                      // Cursor: ${env:VAR}
	tokenBearerEnv                   // Codex: bearer_token_env_var, no header
	tokenNoExpansion                 // Zed: expands nothing; the value is pasted
)

// A reference wherever the client resolves one; never the token itself.
func hindsightServerEntry(url, tokenEnv string, ref tokenRef) map[string]any {
	entry := map[string]any{"type": "http", "url": url}
	if h := authHeaderValue(tokenEnv, ref); h != "" {
		entry["headers"] = map[string]any{"Authorization": h}
	}
	return entry
}

// Empty when the client cannot resolve a reference (Codex, Zed).
func authHeaderValue(tokenEnv string, ref tokenRef) string {
	switch ref {
	case tokenBraceVar:
		return "Bearer ${" + tokenEnv + "}"
	case tokenEnvVar:
		return "Bearer ${env:" + tokenEnv + "}"
	}
	return ""
}

type agentKind int

const (
	agentJSON agentKind = iota
	agentSnippet
)

type agentDef struct {
	name       string
	label      string
	configPath string
	jsonKey    string
	kind       agentKind
	tokenRef   tokenRef
	detect     func(cwd, home string) bool
}

func pathExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func agentRegistry() []agentDef {
	return []agentDef{
		{
			name: "claude", label: "Claude Code", configPath: ".mcp.json",
			jsonKey: "mcpServers", kind: agentJSON, tokenRef: tokenBraceVar,
			detect: func(cwd, home string) bool {
				return pathExists(filepath.Join(cwd, ".claude")) ||
					pathExists(filepath.Join(cwd, ".mcp.json"))
			},
		},
		{
			name: "cursor", label: "Cursor", configPath: ".cursor/mcp.json",
			jsonKey: "mcpServers", kind: agentJSON, tokenRef: tokenEnvVar,
			detect: func(cwd, home string) bool { return pathExists(filepath.Join(cwd, ".cursor")) },
		},
		{
			name: "codex", label: "Codex", configPath: "~/.codex/config.toml",
			kind: agentSnippet, tokenRef: tokenBearerEnv,
			detect: func(cwd, home string) bool {
				return home != "" && pathExists(filepath.Join(home, ".codex"))
			},
		},
		{
			name: "zed", label: "Zed", configPath: ".zed/settings.json",
			jsonKey: "context_servers", kind: agentSnippet, tokenRef: tokenNoExpansion,
			detect: func(cwd, home string) bool { return pathExists(filepath.Join(cwd, ".zed")) },
		},
	}
}

func detectAgents(reg []agentDef, cwd, home string) []agentDef {
	var out []agentDef
	for _, a := range reg {
		if a.detect(cwd, home) {
			out = append(out, a)
		}
	}
	return out
}

func resolveNamed(reg []agentDef, raw string) (selected []agentDef, unknown []string) {
	byName := map[string]agentDef{}
	for _, a := range reg {
		byName[a.name] = a
	}
	for _, tok := range strings.Split(raw, ",") {
		tok = strings.ToLower(strings.TrimSpace(tok))
		if tok == "" {
			continue
		}
		if a, ok := byName[tok]; ok {
			selected = append(selected, a)
		} else {
			unknown = append(unknown, tok)
		}
	}
	return selected, unknown
}

func agentLabels(as []agentDef) string {
	labels := make([]string, len(as))
	for i, a := range as {
		labels[i] = a.label
	}
	return strings.Join(labels, ", ")
}

func promptSelect(detected []agentDef) []agentDef {
	fmt.Fprintln(os.Stderr, "Detected agents in this repo:")
	for i, a := range detected {
		dest := a.configPath
		if a.kind == agentSnippet {
			dest += " (snippet only)"
		}
		fmt.Fprintf(os.Stderr, "  %d. %-12s → %s\n", i+1, a.label, dest)
	}
	fmt.Fprint(os.Stderr, "Write MCP config for all? [Y/n] or comma-numbers to choose: ")

	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	line = strings.TrimSpace(line)
	switch strings.ToLower(line) {
	case "", "y", "yes":
		return detected
	case "n", "no":
		return nil
	}

	var chosen []agentDef
	for _, tok := range strings.Split(line, ",") {
		tok = strings.TrimSpace(tok)
		var idx int
		if _, err := fmt.Sscanf(tok, "%d", &idx); err == nil && idx >= 1 && idx <= len(detected) {
			chosen = append(chosen, detected[idx-1])
		}
	}
	return chosen
}

func isTTY() bool { return isatty.IsTerminal(os.Stdin.Fd()) }

func writeAgentConfigs(cwd string, selected []agentDef, hosted *hindsightTarget) error {
	var (
		wrote    []string
		snippets []agentDef
	)
	for _, a := range selected {
		if a.kind == agentSnippet {
			snippets = append(snippets, a)
			continue
		}
		// per agent: the auth syntax is the client's, not ours
		entries := map[string]any{mcpServerName: mcpServerEntry()}
		if hosted != nil {
			entries[hindsightServerName] = hosted.entryFor(a)
		}
		abs := filepath.Join(cwd, filepath.FromSlash(a.configPath))
		changed, err := mergeMCPJSON(abs, a.jsonKey, entries, hosted != nil)
		if err != nil {
			return err
		}
		if changed {
			wrote = append(wrote, a.configPath)
		}
	}

	// repo state, not this invocation: a plain `dryrun setup` in a repo that
	// already registered the endpoint must not rewrite the directive back to
	// one server while the agent config still holds both
	peer := hosted != nil
	if !peer {
		if _, cfg, err := loadProjectConfig(); err == nil {
			peer = hasHTTPRemote(cfg)
		}
	}
	directives, err := writeDirective(cwd, peer)
	if err != nil {
		return err
	}

	if len(wrote) > 0 {
		fmt.Fprintf(os.Stderr, "Wrote MCP config: %s\n", strings.Join(wrote, ", "))
	}
	if len(directives) > 0 {
		fmt.Fprintf(os.Stderr, "Updated agent directive: %s\n", strings.Join(directives, ", "))
	}
	for _, a := range snippets {
		printSnippet(os.Stderr, a, hosted)
	}
	printCommitGuidance(wrote, directives, hosted != nil)
	return nil
}

func mergeMCPJSON(absPath, key string, entries map[string]any, hosted bool) (bool, error) {
	existing, err := os.ReadFile(absPath)
	if err != nil && !os.IsNotExist(err) {
		return false, err
	}

	root := map[string]any{}
	if len(strings.TrimSpace(string(existing))) > 0 {
		if err := json.Unmarshal(existing, &root); err != nil {
			return false, fmt.Errorf("%s: existing file is not valid JSON, refusing to overwrite: %w", absPath, err)
		}
	}

	servers, _ := root[key].(map[string]any)
	if servers == nil {
		servers = map[string]any{}
	}
	for name, entry := range entries {
		servers[name] = entry
	}
	root[key] = servers

	out, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return false, err
	}
	out = append(out, '\n')

	if string(out) == string(existing) {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return false, err
	}
	perm := os.FileMode(0o644)
	if hosted {
		perm = 0o600
	}
	if err := os.WriteFile(absPath, out, perm); err != nil {
		return false, err
	}
	// WriteFile's perm applies only on create; chmod so an existing config
	// does not hold the auth header world-readable.
	if hosted {
		if err := os.Chmod(absPath, perm); err != nil {
			return false, err
		}
	}
	return true, nil
}

func writeDirective(cwd string, hosted bool) ([]string, error) {
	var written []string

	agents := filepath.Join(cwd, "AGENTS.md")
	changed, err := upsertDirective(agents, hosted)
	if err != nil {
		return written, err
	}
	if changed {
		written = append(written, "AGENTS.md")
	}

	claude := filepath.Join(cwd, "CLAUDE.md")
	if pathExists(claude) {
		changed, err := upsertDirective(claude, hosted)
		if err != nil {
			return written, err
		}
		if changed {
			written = append(written, "CLAUDE.md")
		}
	}
	return written, nil
}

func upsertDirective(path string, hosted bool) (bool, error) {
	existing := ""
	if b, err := os.ReadFile(path); err == nil {
		existing = string(b)
	} else if !os.IsNotExist(err) {
		return false, err
	}

	var next string
	start := strings.Index(existing, directiveStart)
	end := strings.Index(existing, directiveEnd)
	if start >= 0 && end > start {
		next = existing[:start] + directiveBody(hosted) + existing[end+len(directiveEnd):]
	} else if existing == "" {
		next = directiveBody(hosted) + "\n"
	} else {
		sep := "\n\n"
		if strings.HasSuffix(existing, "\n\n") {
			sep = ""
		} else if strings.HasSuffix(existing, "\n") {
			sep = "\n"
		}
		next = existing + sep + directiveBody(hosted) + "\n"
	}

	if next == existing {
		return false, nil
	}
	return true, os.WriteFile(path, []byte(next), 0o644)
}

func printSnippet(w io.Writer, a agentDef, hosted *hindsightTarget) {
	fmt.Fprintf(w, "\n%s uses %s — add this entry yourself:\n", a.label, a.configPath)
	switch a.name {
	case "codex":
		fmt.Fprintln(w, "  [mcp_servers.dryrun]")
		fmt.Fprintln(w, "  command = \"npx\"")
		fmt.Fprintf(w, "  args = [\"-y\", %q, \"mcp-serve\"]\n", npmPackage)
		if hosted != nil {
			// Codex sources the token itself, so no header is formatted here
			fmt.Fprintf(w, "\n  [mcp_servers.%s]\n", hindsightServerName)
			fmt.Fprintf(w, "  url = %q\n", hosted.URL)
			fmt.Fprintf(w, "  bearer_token_env_var = %q\n", hosted.TokenEnv)
		}
	default:
		fmt.Fprintln(w, "  {")
		fmt.Fprintf(w, "    %q: {\n", a.jsonKey)
		fmt.Fprintf(w, "      \"dryrun\": { \"command\": \"npx\", \"args\": [\"-y\", %q, \"mcp-serve\"] }", npmPackage)
		if hosted != nil {
			// no HTML escaping: Marshal would mangle the placeholder's < >
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(hosted.entryFor(a)); err == nil {
				fmt.Fprintf(w, ",\n      %q: %s", hindsightServerName, strings.TrimSpace(buf.String()))
			}
		}
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "    }")
		fmt.Fprintln(w, "  }")
	}
	if hosted != nil && a.tokenRef == tokenNoExpansion {
		fmt.Fprintf(w, "  %s expands no variables, so paste the value of %s in place of the placeholder — and keep that file out of version control.\n",
			a.label, hosted.TokenEnv)
	}
}

func printCommitGuidance(wrote, directives []string, hosted bool) {
	if len(wrote) == 0 && len(directives) == 0 {
		return
	}
	files := append([]string{"dryrun.toml"}, wrote...)
	files = append(files, directives...)
	fmt.Fprintf(os.Stderr, "\nCommit so teammates' agents pick this up automatically:\n  git add %s\n",
		strings.Join(files, " "))
	// safe only while the header is a ${VAR} reference, not an inlined token
	if hosted {
		fmt.Fprintln(os.Stderr, "  (safe as written: the auth header is an environment-variable reference. If you inlined the token instead, do not commit that file — and note the next run replaces it with the reference.)")
	}
	fmt.Fprintln(os.Stderr, "Share the schema (.dryrun/) per your team's workflow: commit it, or use `dryrun snapshot push`/`pull`.")
}
