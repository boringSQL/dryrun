package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// A snapshot shaped like a real application: many tables, each with a
// secondary index that carries sizing and per-node scan counts, across two
// nodes. The index detectors iterate per index, so a fixture without indexes
// measures an empty loop.
func bigSnap(n int) *schema.AnnotatedSchema {
	snap := &schema.SchemaSnapshot{FormatVersion: schema.FormatVersion, Database: "bench"}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("t%04d", i)
		t := testTable("public", name, testCol("a", "text", true), testCol("b", "integer", false))
		t.Indexes = append(t.Indexes, schema.Index{
			Name: name + "_a_idx", Columns: []string{"a"}, IndexType: "btree",
			IsValid: true, IsReady: true,
			Definition: indexDef(false, name+"_a_idx", "public", name, "a"),
		})
		snap.Tables = append(snap.Tables, t)
	}

	a := annotate(snap, 100_000)
	var tables []schema.TableActivityEntry
	var indexes []schema.IndexActivityEntry
	for i := range snap.Tables {
		qual := snap.Tables[i].Qual()
		tables = append(tables, schema.TableActivityEntry{
			Table:    qual,
			Activity: schema.TableActivity{SeqScan: 200_000, IdxScan: 3, NDeadTup: 5000},
		})
		for _, idx := range snap.Tables[i].Indexes {
			indexes = append(indexes, schema.IndexActivityEntry{
				Table: qual, Index: idx.Name,
				Activity: schema.IndexActivity{IdxScan: 7},
			})
			a.Planner.Indexes = append(a.Planner.Indexes, schema.IndexSizingEntry{
				Table: qual, Index: idx.Name,
				Sizing: schema.IndexSizing{Relpages: 40, Reltuples: 100_000, Size: 40 * 8192},
			})
		}
	}
	node := func(source string) schema.NodeActivity {
		return schema.NodeActivity{
			Node:    schema.NodeIdentity{Source: source, Timestamp: fixtureActivityTime},
			Tables:  tables,
			Indexes: indexes,
		}
	}
	a.Merged = &schema.MergedActivity{Nodes: []schema.NodeActivity{node("primary"), node("replica1")}}
	return a
}

func tableIn(a *schema.AnnotatedSchema, name string) *schema.Table {
	for i := range a.Schema.Tables {
		if a.Schema.Tables[i].Name == name {
			return &a.Schema.Tables[i]
		}
	}
	return nil
}

func BenchmarkTableFindings(b *testing.B) {
	a := bigSnap(1000)
	t := tableIn(a, "t0500")
	for i := 0; i < b.N; i++ {
		tableFindings(a, t)
	}
}

// The number the scoping in tableFindings is justified by.
func BenchmarkDetectors(b *testing.B) {
	a := bigSnap(1000)
	b.Run("buildAnomalies", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buildAnomalies(a)
		}
	})
	b.Run("staleStatsWholeSnapshot", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			schema.DetectStaleStats(a, findingsStaleDays)
		}
	})
	b.Run("bloatedTables", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			schema.DetectBloatedTables(a, 4.0)
		}
	})
}

// tableFindings derives anomalies from one table instead of running
// buildAnomalies over the snapshot. The two must agree, or describe_table
// would advertise findings detect does not report, or stay silent on ones it
// does.
func TestTableFindingsAgreesWithBuildAnomalies(t *testing.T) {
	a := bigSnap(20)
	q := schema.QualifiedName{Schema: "public", Name: "t0007"}

	whole, _ := buildAnomalies(a)
	var wantFlagged bool
	for _, row := range whole {
		if row["schema"] == q.Schema && row["table"] == q.Name {
			wantFlagged = true
		}
	}
	if !wantFlagged {
		t.Fatal("fixture no longer produces an anomaly for the probed table")
	}

	var want []string
	for _, row := range whole {
		if row["schema"] == q.Schema && row["table"] == q.Name {
			want = anomalyFlags(row)
		}
	}
	var got []string
	for _, f := range tableFlags(a, q) {
		got = append(got, string(f))
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("scoped flags disagree with buildAnomalies:\n  want %v\n  got  %v", want, got)
	}

	// and a table the snapshot has no activity for must stay quiet
	quiet := schema.QualifiedName{Schema: "public", Name: "nosuch"}
	if got := tableFlags(a, quiet); len(got) != 0 {
		t.Errorf("want no flags for a table with no activity, got %v", got)
	}
}

// Every kind tableFindings names must be one detect would return for the same
// table, so the pointer never sends an agent somewhere empty.
func TestTableFindingsMatchesDetect(t *testing.T) {
	a := bigSnap(20)
	q := schema.QualifiedName{Schema: "public", Name: "t0007"}
	kinds := tableFindings(a, tableIn(a, q.Name))
	if len(kinds) == 0 {
		t.Fatal("fixture no longer produces findings for the probed table")
	}

	c := serveOffline(t, NewOfflineServerAnnotated(a, lint.DefaultConfig()))
	out := callTool(t, c, "detect", map[string]any{
		"kind": "all", "schema": q.Schema, "table": q.Name,
	})

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("detect not JSON: %v\n%.300s", err, out)
	}
	for _, kind := range kinds {
		block, ok := payload[kind].(map[string]any)
		if !ok {
			t.Errorf("detect has no %s block at all", kind)
			continue
		}
		entries, _ := block["entries"].([]any)
		if len(entries) == 0 {
			t.Errorf("tableFindings claims %s, detect reports none", kind)
		}
	}
}

// describe_table should hand the agent the next question, with the resolved
// name rather than whatever was typed.
func TestDescribeTablePointsAtDetect(t *testing.T) {
	a := bigSnap(20)
	c := serveOffline(t, NewOfflineServerAnnotated(a, lint.DefaultConfig()))

	var decoded struct {
		Meta struct {
			Hint string `json:"hint"`
			Next []struct {
				Tool string         `json:"tool"`
				Args map[string]any `json:"args"`
			} `json:"next"`
		} `json:"_meta"`
	}
	out := callTool(t, c, "describe_table", map[string]any{"table": "public.t0007"})
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("unmarshal: %v\n%.300s", err, out)
	}

	if !strings.Contains(decoded.Meta.Hint, "detect reports") {
		t.Errorf("want the findings hint, got %q", decoded.Meta.Hint)
	}
	var found bool
	for _, n := range decoded.Meta.Next {
		if n.Tool != "detect" {
			continue
		}
		found = true
		if n.Args["table"] != "t0007" || n.Args["schema"] != "public" {
			t.Errorf("want the resolved name, got %v", n.Args)
		}
	}
	if !found {
		t.Errorf("no detect follow-up: %+v", decoded.Meta.Next)
	}
}

// A clean table must not be told to go looking.
func TestDescribeTableQuietWithoutFindings(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig()))
	out := callTool(t, c, "describe_table", map[string]any{"table": "app.orders"})
	if strings.Contains(out, "detect reports") {
		t.Errorf("clean table advertised findings: %.400s", out)
	}
}

// A schema-only snapshot has no index scan counts, so every secondary index
// reads as unused. That is missing data, not a finding, and describe_table
// must not send the agent after it.
func TestTableFindingsQuietWithoutActivity(t *testing.T) {
	snap := multiSchemaSnap()
	a := &schema.AnnotatedSchema{Schema: snap}

	var withIndex *schema.Table
	for i := range snap.Tables {
		if len(snap.Tables[i].Indexes) > 0 {
			withIndex = &snap.Tables[i]
			break
		}
	}
	if withIndex == nil {
		t.Fatal("fixture has no indexed table, so this proves nothing")
	}

	if kinds := tableFindings(a, withIndex); len(kinds) > 0 {
		t.Errorf("schema-only snapshot reported %v", kinds)
	}
}
