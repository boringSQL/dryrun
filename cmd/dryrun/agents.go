package main

import (
	"bufio"
	"encoding/json"
	"fmt"
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

var directiveBody = strings.Join([]string{
	directiveStart,
	"## Database schema",
	"",
	"This repo uses Postgres; the schema is captured in `.dryrun/`. Do not guess",
	"columns, indexes, or types — call the `dryrun` MCP server to inspect the",
	"schema, validate queries, and check migrations before writing SQL.",
	directiveEnd,
}, "\n")

func mcpServerEntry() map[string]any {
	return map[string]any{
		"command": "npx",
		"args":    []any{"-y", npmPackage, "mcp-serve"},
	}
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
			jsonKey: "mcpServers", kind: agentJSON,
			detect: func(cwd, home string) bool {
				return pathExists(filepath.Join(cwd, ".claude")) ||
					pathExists(filepath.Join(cwd, ".mcp.json"))
			},
		},
		{
			name: "cursor", label: "Cursor", configPath: ".cursor/mcp.json",
			jsonKey: "mcpServers", kind: agentJSON,
			detect: func(cwd, home string) bool { return pathExists(filepath.Join(cwd, ".cursor")) },
		},
		{
			name: "codex", label: "Codex", configPath: "~/.codex/config.toml",
			kind: agentSnippet,
			detect: func(cwd, home string) bool {
				return home != "" && pathExists(filepath.Join(home, ".codex"))
			},
		},
		{
			name: "zed", label: "Zed", configPath: ".zed/settings.json",
			jsonKey: "context_servers", kind: agentSnippet,
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

func writeAgentConfigs(cwd string, selected []agentDef) error {
	var (
		wrote    []string
		snippets []agentDef
	)
	for _, a := range selected {
		if a.kind == agentSnippet {
			snippets = append(snippets, a)
			continue
		}
		abs := filepath.Join(cwd, filepath.FromSlash(a.configPath))
		changed, err := mergeMCPJSON(abs, a.jsonKey)
		if err != nil {
			return err
		}
		if changed {
			wrote = append(wrote, a.configPath)
		}
	}

	directives, err := writeDirective(cwd)
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
		printSnippet(a)
	}
	printCommitGuidance(wrote, directives)
	return nil
}

func mergeMCPJSON(absPath, key string) (bool, error) {
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
	servers[mcpServerName] = mcpServerEntry()
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
	return true, os.WriteFile(absPath, out, 0o644)
}

func writeDirective(cwd string) ([]string, error) {
	var written []string

	agents := filepath.Join(cwd, "AGENTS.md")
	changed, err := upsertDirective(agents)
	if err != nil {
		return written, err
	}
	if changed {
		written = append(written, "AGENTS.md")
	}

	claude := filepath.Join(cwd, "CLAUDE.md")
	if pathExists(claude) {
		changed, err := upsertDirective(claude)
		if err != nil {
			return written, err
		}
		if changed {
			written = append(written, "CLAUDE.md")
		}
	}
	return written, nil
}

func upsertDirective(path string) (bool, error) {
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
		next = existing[:start] + directiveBody + existing[end+len(directiveEnd):]
	} else if existing == "" {
		next = directiveBody + "\n"
	} else {
		sep := "\n\n"
		if strings.HasSuffix(existing, "\n\n") {
			sep = ""
		} else if strings.HasSuffix(existing, "\n") {
			sep = "\n"
		}
		next = existing + sep + directiveBody + "\n"
	}

	if next == existing {
		return false, nil
	}
	return true, os.WriteFile(path, []byte(next), 0o644)
}

func printSnippet(a agentDef) {
	fmt.Fprintf(os.Stderr, "\n%s uses %s — add this entry yourself:\n", a.label, a.configPath)
	switch a.name {
	case "codex":
		fmt.Fprintln(os.Stderr, "  [mcp_servers.dryrun]")
		fmt.Fprintln(os.Stderr, "  command = \"npx\"")
		fmt.Fprintf(os.Stderr, "  args = [\"-y\", %q, \"mcp-serve\"]\n", npmPackage)
	default:
		fmt.Fprintln(os.Stderr, "  {")
		fmt.Fprintf(os.Stderr, "    %q: {\n", a.jsonKey)
		fmt.Fprintf(os.Stderr, "      \"dryrun\": { \"command\": \"npx\", \"args\": [\"-y\", %q, \"mcp-serve\"] }\n", npmPackage)
		fmt.Fprintln(os.Stderr, "    }")
		fmt.Fprintln(os.Stderr, "  }")
	}
}

func printCommitGuidance(wrote, directives []string) {
	if len(wrote) == 0 && len(directives) == 0 {
		return
	}
	files := append([]string{"dryrun.toml"}, wrote...)
	files = append(files, directives...)
	fmt.Fprintf(os.Stderr, "\nCommit so teammates' agents pick this up automatically:\n  git add %s\n",
		strings.Join(files, " "))
	fmt.Fprintln(os.Stderr, "Share the schema (.dryrun/) per your team's workflow: commit it, or use `dryrun snapshot push`/`pull`.")
}
