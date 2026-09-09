package mcp

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/boringsql/dryrun/pkg/lint"
)

// A pointer to a tool the endpoint does not serve is the defect this note was
// written to remove: the local server registers the history tools, pkg/mcp's
// offline surface does not, and both read the same Instructions().
func TestTheSelectionNoteOnlyNamesToolsThatAreServed(t *testing.T) {
	hist := historyStore(t)
	srv := serverWithHistory(t, minimalSnapshot(), hist)

	registered := map[string]bool{}
	mcpSrv := mcpserver.NewMCPServer("dryrun-test", "0.1.0")
	srv.Register(mcpSrv)
	for _, tool := range mcpSrv.ListTools() {
		registered[tool.Tool.Name] = true
	}
	if len(registered) == 0 {
		t.Fatal("no tools registered; this test would pass vacuously")
	}

	// Backticks in the note mean a tool or a shell command and nothing else:
	// a single-token backticked name with an underscore is a tool claim.
	note := selectionNote(true)
	matched := 0
	for _, m := range regexp.MustCompile("`([a-z_]+)`").FindAllStringSubmatch(note, -1) {
		name := m[1]
		if !strings.Contains(name, "_") {
			continue
		}
		matched++
		if !registered[name] {
			t.Errorf("the note names `%s`, which this server does not serve", name)
		}
	}
	// dropping the backticks, or a merge renaming a tool to one word, would
	// leave this sweeping nothing and passing
	if matched < 2 {
		t.Fatalf("the sweep matched %d tool names; it is not checking anything", matched)
	}
}

// pkg/mcp serves the schema-only subset with no history store, so the note must
// not appear there at all.
func TestNoSelectionNoteWithoutAHistoryStore(t *testing.T) {
	srv := NewOfflineServer(minimalSnapshot(), lint.DefaultConfig())
	got := srv.Instructions()
	for _, tool := range []string{"snapshot_diff", "list_top_queries", "_meta.history", "last_attempt"} {
		if strings.Contains(got, tool) {
			t.Errorf("offline instructions name %s, which that surface does not serve:\n%s", tool, got)
		}
	}
}

// The sentence is the intervention: measured, it moved snapshot_diff from
// never-chosen to chosen in every run. A server that holds history must carry it.
func TestInstructionsCarryTheSelectionNoteWithHistory(t *testing.T) {
	hist := historyStore(t)
	srv := serverWithHistory(t, minimalSnapshot(), hist)
	got := srv.Instructions()
	for _, want := range []string{"snapshot_diff", "list_top_queries", "_meta.history", "last_attempt"} {
		if !strings.Contains(got, want) {
			t.Errorf("instructions do not name %s:\n%s", want, got)
		}
	}
}

// The instructions and the tool output schemas describe one field, and they
// diverged inside a single change set: the schema said rows dedup on content
// with no exception while the note carried it. One const, used by both, is what
// keeps a reader who meets only one of them from a different answer.
func TestBothTextsStateTheFieldOnce(t *testing.T) {
	if !strings.Contains(metaProperty, historyFieldDoc) {
		t.Error("the output schema does not carry the shared statement of _meta.history")
	}
	if !strings.Contains(historyMetaNote(true), historyFieldDoc) {
		t.Error("the instructions do not carry the shared statement of _meta.history")
	}
	// the clauses a reader gets wrong without them. Self-referential by
	// construction: it catches a dropped clause and a re-inlined copy, not the
	// const drifting from inventory.go, which is what the store's own tests pin.
	for _, want := range []string{"activity does not", "SUCCESSFULLY", "corrupt_rows", "history_unavailable"} {
		if !strings.Contains(historyFieldDoc, want) {
			t.Errorf("the shared statement drops %q", want)
		}
	}
}

// The const is embedded in five raw-JSON schemas by string concatenation, so a
// quote, newline or control character in it breaks them — and the character
// check alone would not notice metaProperty falling out of a schema.
func TestEverySchemaEmbeddingTheFieldDocStillParses(t *testing.T) {
	schemas := map[string]json.RawMessage{
		"describe_table":  describeTableOutputSchema,
		"detect":          detectOutputSchema,
		"lint_schema":     lintSchemaOutputSchema,
		"check_migration": checkMigrationOutputSchema,
		"snapshot_diff":   snapshotDiffOutputSchema,
	}
	for name, raw := range schemas {
		if !json.Valid(raw) {
			t.Errorf("%s: schema is not valid JSON after embedding", name)
		}
		if !strings.Contains(string(raw), historyFieldDoc) {
			t.Errorf("%s: no longer carries the shared statement", name)
		}
	}
}
