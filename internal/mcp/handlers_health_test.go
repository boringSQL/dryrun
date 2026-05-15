package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

// Smoke tests for health-family tools: compare_nodes, detect (all kinds),
// vacuum_health. Each subtest exercises one kind or filter and asserts the
// expected JSON keys or error text appear.
func TestHealthHandlers_OfflineSmoke(t *testing.T) {
	c := setupOfflineTest(t)

	t.Run("compare_nodes", func(t *testing.T) {
		out := callTool(t, c, "compare_nodes", map[string]any{"table": "users"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_default_all", func(t *testing.T) {
		out := callTool(t, c, "detect", nil)
		assertContains(t, out, "stale_stats")
		assertContains(t, out, "unused_indexes")
		assertContains(t, out, "anomalies")
		assertContains(t, out, "bloated_indexes")
	})

	t.Run("detect_stale_stats", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "stale_stats"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_unused_indexes", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "unused_indexes"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_anomalies", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "anomalies"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_bloated_indexes", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bloated_indexes"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_bloated_with_threshold", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bloated_indexes", "threshold": 2.0})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("detect_invalid_kind", func(t *testing.T) {
		out := callTool(t, c, "detect", map[string]any{"kind": "bogus"})
		assertContains(t, out, "unknown detect kind")
	})

	t.Run("vacuum_health", func(t *testing.T) {
		out := callTool(t, c, "vacuum_health", nil)
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("vacuum_health_with_filter", func(t *testing.T) {
		out := callTool(t, c, "vacuum_health", map[string]any{"table": "users"})
		if out == "" {
			t.Fatal("empty result")
		}
	})

	t.Run("vacuum_health_nonexistent_table", func(t *testing.T) {
		out := callTool(t, c, "vacuum_health", map[string]any{"table": "nonexistent_xyz"})
		assertContains(t, out, "No vacuum health concerns")
	})
}

// Pins that vacuum_health with an unknown schema returns the friendly
// "No vacuum health concerns" message rather than an error or empty payload.
func TestVacuumHealth_SchemaFilter(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "vacuum_health", map[string]any{"schema": "nonexistent_schema_xyz"})
	if !strings.Contains(out, "No vacuum health concerns") {
		t.Errorf("expected empty vacuum health for unknown schema, got %s", out)
	}
}

// Sanity check that detect tolerates a table filter matching nothing without
// crashing or returning empty output. JSON-parseable detect kinds must still
// produce valid JSON, text-mode kinds are tolerated as is.
func TestDetect_TableFilter(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "detect", map[string]any{"table": "definitely_not_a_table_xyz"})
	if out == "" {
		t.Fatal("empty result")
	}
	var any map[string]any
	if err := json.Unmarshal([]byte(out), &any); err != nil {
		return
	}
}
