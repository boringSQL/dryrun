package mcp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// A rotating label lists one breakdown entry per server, and only started_at
// tells them apart. A plain label keeps its entry exactly as before.
func TestDescribeTableNodeBreakdownNamesPoolServers(t *testing.T) {
	bootA := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	bootB := time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC)
	orders := schema.QualifiedName{Schema: "public", Name: "orders"}
	row := func(source string, boot time.Time, seq int64) schema.NodeActivity {
		b := boot
		return schema.NodeActivity{
			Node:   schema.NodeIdentity{Source: source, Timestamp: boot.Add(24 * time.Hour), PostmasterStartTime: &b},
			Tables: []schema.TableActivityEntry{{Table: orders, Activity: schema.TableActivity{SeqScan: seq}}},
		}
	}
	a := &schema.AnnotatedSchema{
		Schema: multiSchemaSnap(),
		Merged: &schema.MergedActivity{Nodes: []schema.NodeActivity{
			row("primary", bootA, 1), row("pool", bootB, 2), row("pool", bootA, 3),
		}},
	}

	var req mcp.CallToolRequest
	req.Params.Name = "describe_table"
	req.Params.Arguments = map[string]any{"table": "public.orders"}
	res, err := NewOfflineServerAnnotated(a, lint.DefaultConfig()).handleDescribeTable(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	text, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("want TextContent, got %T", res.Content[0])
	}
	if n := strings.Count(text.Text, `"started_at"`); n != 2 {
		t.Errorf("want started_at on the two pool entries only, got %d in:\n%.2000s", n, text.Text)
	}
	for _, want := range []string{`"started_at":"2026-07-02T08:00:00Z"`, `"started_at":"2026-07-01T08:00:00Z"`} {
		if !strings.Contains(strings.ReplaceAll(text.Text, " ", ""), want) {
			t.Errorf("missing %s", want)
		}
	}
}
