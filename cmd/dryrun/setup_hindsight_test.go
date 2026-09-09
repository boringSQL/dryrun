package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/history"
)

func httpRemoteConfig(tokenEnv string) *config.ProjectConfig {
	return &config.ProjectConfig{
		Remotes: []config.RemoteConfig{{
			Name: "hindsight", Type: "http", Ref: "https://hindsight.example.com/", TokenEnv: tokenEnv,
		}},
	}
}

var (
	testKey = history.SnapshotKey{ProjectID: "acme", DatabaseID: "billing"}

	claudeAgent = agentDef{
		name: "claude", label: "Claude Code", configPath: ".mcp.json",
		jsonKey: "mcpServers", kind: agentJSON, tokenRef: tokenBraceVar,
	}
)

// The route is /mcp/{project}/{database} and the cloud resolves it by the same
// project_id/database_id a push is keyed under, so the snapshot key IS the
// path. Nothing here should need a second mapping to keep in step.
func TestResolveHindsightBuildsTheRouteFromTheSnapshotKey(t *testing.T) {
	got, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	want := "https://hindsight.example.com/mcp/acme/billing"
	if got.URL != want {
		t.Errorf("url: want %s, got %s", want, got.URL)
	}
	if got.TokenEnv != defaultTokenEnv {
		t.Errorf("token env: want the same default push reads (%s), got %s", defaultTokenEnv, got.TokenEnv)
	}
}

// A space is escaped and still addresses the right route. What cannot be
// addressed at all is refused instead, in TestOnlyUnaddressableSegmentsAreRefused.
func TestResolveHindsightEscapesTheKey(t *testing.T) {
	got, err := resolveHindsight(httpRemoteConfig(""), history.SnapshotKey{ProjectID: "acme", DatabaseID: "c d"}, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got.URL, "/mcp/acme/c%20d") {
		t.Errorf("want an escaped path, got %s", got.URL)
	}
}

func TestResolveHindsightRefusesWhatItCannotAddress(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *config.ProjectConfig
		key  history.SnapshotKey
		want string
	}{
		{"no config", nil, testKey, "dryrun init"},
		{"no remotes", &config.ProjectConfig{}, testKey, "remote add"},
		{
			"an oci remote is not an endpoint",
			&config.ProjectConfig{Remotes: []config.RemoteConfig{{Name: "gar", Type: "oci", Ref: "ghcr.io/x/y"}}},
			testKey, "needs an http remote",
		},
		{
			"http remote with no url",
			&config.ProjectConfig{Remotes: []config.RemoteConfig{{Name: "h", Type: "http"}}},
			testKey, "requires a url",
		},
		{"no database identity", httpRemoteConfig(""), history.SnapshotKey{}, "which database this is"},
		{
			"an http remote whose ref is not a url",
			&config.ProjectConfig{Remotes: []config.RemoteConfig{{Name: "h", Type: "http", Ref: "ghcr.io/org/x"}}},
			testKey, "not an http(s) url",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveHindsight(tc.cfg, tc.key, "", "")
			if err == nil {
				t.Fatal("want a refusal, got none")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("want the message to name %q, got %q", tc.want, err)
			}
		})
	}
}

// The one thing this command must never do. `buildHTTPStore` states the rule
// for the push path ("never from disk"), and setup then tells you to COMMIT
// these files, so a token that reached one would reach the repo.
func TestTheTokenValueNeverReachesTheConfig(t *testing.T) {
	const secret = "prk_thisisthesecretvalue"
	t.Setenv("DRYRUN_TOKEN", secret)

	target, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if target.TokenUnset {
		t.Error("the var is set; the report would wrongly tell the caller to export it")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")
	if _, err := mergeMCPJSON(path, "mcpServers", map[string]any{hindsightServerName: target.entryFor(claudeAgent)}, true); err != nil {
		t.Fatal(err)
	}
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(blob), secret) {
		t.Fatalf("the token value was written to disk:\n%s", blob)
	}
	if !strings.Contains(string(blob), "${DRYRUN_TOKEN}") {
		t.Errorf("want a reference to the env var, got:\n%s", blob)
	}
}

// Both servers answer "what is the state of this database" — one from this
// machine's history.db, one from every node's pushes. Under one name a client
// would hold two different answers and show one.
func TestTheLocalAndHostedServersAreRegisteredApart(t *testing.T) {
	target, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := writeAgentConfigs(dir, []agentDef{claudeAgent}, &target); err != nil {
		t.Fatal(err)
	}

	blob, err := os.ReadFile(filepath.Join(dir, ".mcp.json"))
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Servers map[string]json.RawMessage `json:"mcpServers"`
	}
	if err := json.Unmarshal(blob, &root); err != nil {
		t.Fatal(err)
	}
	if _, ok := root.Servers[mcpServerName]; !ok {
		t.Error("--hindsight must not replace the local server")
	}
	if _, ok := root.Servers[hindsightServerName]; !ok {
		t.Error("the hosted server is missing")
	}
	if mcpServerName == hindsightServerName {
		t.Fatal("the two servers share a name; one silently wins")
	}
}

// An existing unrelated server in the file survives the merge.
func TestWritingTheHostedEntryPreservesOtherServers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")
	if err := os.WriteFile(path, []byte(`{"mcpServers":{"other":{"command":"x"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	target, err := resolveHindsight(httpRemoteConfig("HINDSIGHT_TOKEN"), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mergeMCPJSON(path, "mcpServers", map[string]any{hindsightServerName: target.entryFor(claudeAgent)}, true); err != nil {
		t.Fatal(err)
	}
	blob, _ := os.ReadFile(path)
	for _, want := range []string{`"other"`, `"hindsight"`, "${HINDSIGHT_TOKEN}"} {
		if !strings.Contains(string(blob), want) {
			t.Errorf("want %s in:\n%s", want, blob)
		}
	}
}

// The syntax is the CLIENT's. A ${VAR} written where the client does not expand
// it authenticates nothing, and the recovery — pasting the token in — is the
// failure this whole design exists to prevent.
func TestEachClientGetsItsOwnAuthSyntax(t *testing.T) {
	target, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]agentDef{}
	for _, a := range agentRegistry() {
		byName[a.name] = a
	}
	for _, tc := range []struct{ agent, want string }{
		{"claude", "Bearer ${DRYRUN_TOKEN}"},
		{"cursor", "Bearer ${env:DRYRUN_TOKEN}"},
		// Zed documents no expansion: a reference there would look like it works
		{"zed", "Bearer " + tokenPlaceholder},
	} {
		t.Run(tc.agent, func(t *testing.T) {
			entry := target.entryFor(byName[tc.agent])
			headers, ok := entry["headers"].(map[string]any)
			if !ok {
				t.Fatalf("no headers in %v", entry)
			}
			if headers["Authorization"] != tc.want {
				t.Errorf("want %q, got %q", tc.want, headers["Authorization"])
			}
		})
	}
	// Codex sources the token itself, so no header is written at all
	if _, ok := target.entryFor(byName["codex"])["headers"]; ok {
		t.Error("codex takes bearer_token_env_var; a hand-written header would be sent verbatim")
	}
}

// The snippet path formats its own output, so the secret-never-on-disk property
// has to be checked there too — it is the path that does not go through
// mergeMCPJSON.
func TestNoSnippetPrintsTheTokenValue(t *testing.T) {
	const secret = "prk_thisisthesecretvalue"
	t.Setenv("DRYRUN_TOKEN", secret)
	target, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range agentRegistry() {
		var buf bytes.Buffer
		printSnippet(&buf, a, &target)
		if strings.Contains(buf.String(), secret) {
			t.Errorf("%s snippet leaks the token:\n%s", a.name, buf.String())
		}
		if a.name == "codex" && !strings.Contains(buf.String(), `bearer_token_env_var = "DRYRUN_TOKEN"`) {
			t.Errorf("codex snippet must name the env var Codex reads:\n%s", buf.String())
		}
		if a.name == "zed" && !strings.Contains(buf.String(), "expands no variables") {
			t.Errorf("zed cannot expand a reference; the snippet must say so:\n%s", buf.String())
		}
	}
}

// Plain `dryrun setup` is unchanged: one server, no auth header anywhere.
func TestWithoutHindsightNothingHostedIsWritten(t *testing.T) {
	dir := t.TempDir()
	if err := writeAgentConfigs(dir, []agentDef{claudeAgent}, nil); err != nil {
		t.Fatal(err)
	}
	blob, err := os.ReadFile(filepath.Join(dir, ".mcp.json"))
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Servers map[string]json.RawMessage `json:"mcpServers"`
	}
	if err := json.Unmarshal(blob, &root); err != nil {
		t.Fatal(err)
	}
	if len(root.Servers) != 1 {
		t.Errorf("want only the local server, got %v", root.Servers)
	}
	if strings.Contains(string(blob), "Authorization") {
		t.Errorf("no --hindsight, so no auth header:\n%s", blob)
	}
}

// A file carrying an auth header, even as a reference, is not world-readable.
func TestTheHostedConfigIsWrittenPrivate(t *testing.T) {
	target, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := writeAgentConfigs(dir, []agentDef{claudeAgent}, &target); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, ".mcp.json"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("want 0600 on a file holding an auth header, got %04o", perm)
	}
}

// The modal path, and the one WriteFile's perm does not cover: the Claude
// detector fires BECAUSE .mcp.json already exists, and an existing file keeps
// its mode through O_CREATE. A test on a fresh temp dir certifies a property
// that does not hold in the field.
func TestAnExistingConfigIsTightenedWhenItGainsAnAuthHeader(t *testing.T) {
	target, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, ".mcp.json")
	if err := os.WriteFile(path, []byte(`{"mcpServers":{"other":{"command":"x"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeAgentConfigs(dir, []agentDef{claudeAgent}, &target); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("an existing file gained an auth header and kept %04o", perm)
	}
}

// The property is not "contains a slash": it is that escaping the segment
// disagrees with the server's own path encoding, so chi hands the lookup an
// escaped literal and it 404s with nothing to see. A directory named foo,bar
// reaches this, since [project].id defaults to the basename.
func TestOnlyUnaddressableSegmentsAreRefused(t *testing.T) {
	for _, tc := range []struct {
		id string
		ok bool
	}{
		{"acme", true},
		{"acme-staging", true},
		{"c d", true},
		{"a/b", false},
		{"foo,bar", false},
		{"foo;bar", false},
	} {
		_, err := resolveHindsight(httpRemoteConfig(""), history.SnapshotKey{ProjectID: history.ProjectId(tc.id), DatabaseID: "db"}, "", "")
		if tc.ok && err != nil {
			t.Errorf("%q is addressable, got %v", tc.id, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("%q cannot be addressed; want a refusal", tc.id)
		}
	}
}

func TestTokenUnsetIsReportedWhenTheVarIsMissing(t *testing.T) {
	t.Setenv("DRYRUN_TOKEN", "")
	target, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !target.TokenUnset {
		t.Error("the var is unset; the caller needs telling before the agent 401s")
	}
}

// A bearer token over plaintext to another host is worth saying out loud;
// localhost is the ordinary dev case and is not.
func TestPlaintextToAnotherHostIsFlagged(t *testing.T) {
	for _, tc := range []struct {
		ref  string
		want bool
	}{
		{"https://hindsight.example.com", false},
		{"http://localhost:8080", false},
		{"http://127.0.0.1:8080", false},
		{"http://hindsight.example.com", true},
	} {
		cfg := &config.ProjectConfig{Remotes: []config.RemoteConfig{{Name: "h", Type: "http", Ref: tc.ref}}}
		got, err := resolveHindsight(cfg, testKey, "", "")
		if err != nil {
			t.Fatalf("%s: %v", tc.ref, err)
		}
		if got.Insecure != tc.want {
			t.Errorf("%s: insecure=%v, want %v", tc.ref, got.Insecure, tc.want)
		}
	}
}

// The report runs before an agent is chosen and Zed gets a literal placeholder,
// so it must not claim a reference was written. It said so once.
func TestTheReportClaimsOnlyWhatHoldsForEveryAgent(t *testing.T) {
	target, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	target.report(&buf)
	got := buf.String()
	if strings.Contains(got, "is written as a reference") {
		t.Errorf("the Zed path writes a placeholder, not a reference:\n%s", got)
	}
	if !strings.Contains(got, "No token value is written") {
		t.Errorf("want the claim that holds on every path:\n%s", got)
	}
	// Pins the WORDING only: mcp:read is the cloud's literal (token.ScopeMCPRead,
	// checked by token.Grants) and nothing in this repo can see it, so a rename
	// there is caught by a human reading both, not by this.
	if !strings.Contains(got, "mcp:read") {
		t.Errorf("want the literal scope, not a description of it:\n%s", got)
	}
}

// setup registers a second server, and the repo directive is the only place
// that says so: with the MCP-side note cut, this text is what tells an agent
// `worklist` exists at all.
func TestTheRepoDirectiveNamesBothServersAfterHindsight(t *testing.T) {
	if strings.Contains(directiveBody(false), "hindsight") {
		t.Error("plain setup must not name an endpoint this repo may not have")
	}
	hosted := directiveBody(true)
	for _, want := range []string{"hindsight", "worklist"} {
		if !strings.Contains(hosted, want) {
			t.Errorf("the directive does not name %q:\n%s", want, hosted)
		}
	}

	target, err := resolveHindsight(httpRemoteConfig(""), testKey, "", "")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := writeAgentConfigs(dir, []agentDef{claudeAgent}, &target); err != nil {
		t.Fatal(err)
	}
	blob, err := os.ReadFile(filepath.Join(dir, "AGENTS.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(blob), "worklist") {
		t.Errorf("the written directive does not mention the server just registered:\n%s", blob)
	}
}

// The regression this gate exists for: `setup --hindsight` writes the
// two-server directive, then a later plain `dryrun setup` (adding an agent,
// re-running after a rename) rewrote it to name one server while .mcp.json
// still held both — mergeMCPJSON is additive, upsertDirective replaces.
func TestAPlainSetupKeepsTheDirectiveInARepoThatPushes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "dryrun.toml"), []byte(
		"[project]\nid = \"acme\"\n\n[[remote]]\nname = \"hindsight\"\ntype = \"http\"\nref = \"https://h.example.com\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)

	// no --hindsight on this run, but the repo is configured to push
	if err := writeAgentConfigs(dir, []agentDef{claudeAgent}, nil); err != nil {
		t.Fatal(err)
	}
	blob, err := os.ReadFile(filepath.Join(dir, "AGENTS.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(blob), "worklist") {
		t.Errorf("a plain setup stripped the two-server directive:\n%s", blob)
	}
}
