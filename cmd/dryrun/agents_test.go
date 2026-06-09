package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// mergeMCPJSON on a fresh path should produce a well-formed config with the
// dryrun server entry launched via npx.
func TestMergeMCPJSON_NewFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")

	changed, err := mergeMCPJSON(path, "mcpServers")
	if err != nil || !changed {
		t.Fatalf("mergeMCPJSON: changed=%v err=%v", changed, err)
	}

	var root map[string]any
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := json.Unmarshal(b, &root); err != nil {
		t.Fatalf("written file is not valid JSON: %v", err)
	}

	servers, ok := root["mcpServers"].(map[string]any)
	if !ok {
		t.Fatalf("expected mcpServers object, got %T", root["mcpServers"])
	}
	entry, ok := servers["dryrun"].(map[string]any)
	if !ok {
		t.Fatalf("expected dryrun entry, got %T", servers["dryrun"])
	}
	if entry["command"] != "npx" {
		t.Errorf("command = %v, want npx", entry["command"])
	}
	args, ok := entry["args"].([]any)
	if !ok || len(args) != 3 || args[1] != npmPackage || args[2] != "mcp-serve" {
		t.Errorf("args = %v, want [-y %s mcp-serve]", entry["args"], npmPackage)
	}
}

// Merging into an existing config must preserve every unrelated key and any
// sibling server already present — we only set servers[dryrun].
func TestMergeMCPJSON_PreservesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp.json")
	seed := `{
  "globalShortcut": "X",
  "mcpServers": { "other": { "command": "foo" } }
}`
	if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := mergeMCPJSON(path, "mcpServers"); err != nil {
		t.Fatalf("mergeMCPJSON: %v", err)
	}

	var root map[string]any
	b, _ := os.ReadFile(path)
	if err := json.Unmarshal(b, &root); err != nil {
		t.Fatal(err)
	}
	if root["globalShortcut"] != "X" {
		t.Errorf("globalShortcut lost: %v", root["globalShortcut"])
	}
	servers := root["mcpServers"].(map[string]any)
	if _, ok := servers["other"]; !ok {
		t.Errorf("sibling server 'other' was dropped")
	}
	if _, ok := servers["dryrun"]; !ok {
		t.Errorf("dryrun server not added")
	}
}

// A pre-existing file that is not valid JSON must error rather than clobber the
// user's hand-written config.
func TestMergeMCPJSON_RefusesInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp.json")
	if err := os.WriteFile(path, []byte("{ not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := mergeMCPJSON(path, "mcpServers"); err == nil {
		t.Fatalf("expected error on invalid JSON, got nil")
	}
}

// Re-running on an already-configured file must be a no-op (changed=false) and
// leave the bytes byte-for-byte identical — no diff churn in a committed config.
func TestMergeMCPJSON_NoOpOnUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")

	if _, err := mergeMCPJSON(path, "mcpServers"); err != nil {
		t.Fatal(err)
	}
	first, _ := os.ReadFile(path)

	changed, err := mergeMCPJSON(path, "mcpServers")
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Errorf("second run reported a change on identical content")
	}
	second, _ := os.ReadFile(path)
	if string(first) != string(second) {
		t.Errorf("file rewritten on no-op:\nbefore:\n%s\nafter:\n%s", first, second)
	}
}

// upsertDirective on a missing file creates it containing exactly the marked block.
func TestUpsertDirective_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENTS.md")

	changed, err := upsertDirective(path)
	if err != nil || !changed {
		t.Fatalf("upsertDirective: changed=%v err=%v", changed, err)
	}
	b, _ := os.ReadFile(path)
	got := string(b)
	if !strings.Contains(got, directiveStart) || !strings.Contains(got, directiveEnd) {
		t.Errorf("markers missing:\n%s", got)
	}
}

// Appending to a file with prior content keeps that content and adds the block
// after a separating blank line.
func TestUpsertDirective_AppendsToExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "CLAUDE.md")
	if err := os.WriteFile(path, []byte("# Title\n\nbody\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	changed, err := upsertDirective(path)
	if err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	b, _ := os.ReadFile(path)
	got := string(b)
	if !strings.HasPrefix(got, "# Title\n\nbody\n") {
		t.Errorf("original content not preserved:\n%s", got)
	}
	if strings.Contains(got, "body\n<!-- dryrun:start") {
		t.Errorf("block not separated from prior content:\n%s", got)
	}
}

// Re-running replaces the block in place (no duplication) and is idempotent on
// already-current content.
func TestUpsertDirective_Idempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENTS.md")

	if _, err := upsertDirective(path); err != nil {
		t.Fatal(err)
	}
	changed, err := upsertDirective(path)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Errorf("second run reported a change on unchanged content")
	}
	b, _ := os.ReadFile(path)
	if n := strings.Count(string(b), directiveStart); n != 1 {
		t.Errorf("block duplicated: %d start markers", n)
	}
}

// Editing the marked block out-of-band and re-running must restore the canonical
// block, not append a second one.
func TestUpsertDirective_ReplacesStaleBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "AGENTS.md")
	stale := "intro\n\n" + directiveStart + "\nOLD TEXT\n" + directiveEnd + "\n\noutro\n"
	if err := os.WriteFile(path, []byte(stale), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := upsertDirective(path); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	got := string(b)
	if strings.Contains(got, "OLD TEXT") {
		t.Errorf("stale block not replaced:\n%s", got)
	}
	if !strings.Contains(got, "intro") || !strings.Contains(got, "outro") {
		t.Errorf("surrounding content lost:\n%s", got)
	}
	if n := strings.Count(got, directiveStart); n != 1 {
		t.Errorf("expected 1 block, got %d", n)
	}
}

func TestResolveNamed(t *testing.T) {
	reg := agentRegistry()
	sel, unknown := resolveNamed(reg, "claude, CURSOR ,bogus")
	if len(sel) != 2 {
		t.Errorf("selected %d, want 2 (case-insensitive, trimmed)", len(sel))
	}
	if len(unknown) != 1 || unknown[0] != "bogus" {
		t.Errorf("unknown = %v, want [bogus]", unknown)
	}
}

// --agents= (changed flag, empty value) is an explicit opt-out: no files touched.
func TestConfigureAgents_ExplicitOptOut(t *testing.T) {
	dir := t.TempDir()
	mustMkdir(t, filepath.Join(dir, ".cursor"))

	if err := configureAgents(dir, true, ""); err != nil {
		t.Fatalf("configureAgents: %v", err)
	}
	if pathExists(filepath.Join(dir, ".cursor", "mcp.json")) {
		t.Errorf(".cursor/mcp.json written despite opt-out")
	}
	if pathExists(filepath.Join(dir, "AGENTS.md")) {
		t.Errorf("AGENTS.md written despite opt-out")
	}
}

// An explicit name list writes that agent's config plus the directive, with no
// prompt and regardless of detection.
func TestConfigureAgents_ExplicitListWrites(t *testing.T) {
	dir := t.TempDir()

	if err := configureAgents(dir, true, "cursor"); err != nil {
		t.Fatalf("configureAgents: %v", err)
	}
	if !pathExists(filepath.Join(dir, ".cursor", "mcp.json")) {
		t.Errorf(".cursor/mcp.json not written")
	}
	if !pathExists(filepath.Join(dir, "AGENTS.md")) {
		t.Errorf("AGENTS.md directive not written")
	}
}

func mustMkdir(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
}
