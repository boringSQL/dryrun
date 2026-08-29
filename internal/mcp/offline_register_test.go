package mcp

import (
	"bytes"
	"context"
	"io"
	"log"
	"sort"
	"testing"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

// The hosted endpoint serves RegisterOffline. This locks its surface: exactly
// the schema-only tools, and never the history-bound (snapshot_diff) or
// live-only (explain_query, check_drift, columnar_report)
// tools, which would emit "run dryrun ..." nonsense or need a pool the hosted
// caller does not have. A new tool added to registerSchemaTools must be added
// here deliberately.
func TestRegisterOffline_ExactSubset(t *testing.T) {
	c := setupOfflineRegisterTest(t)

	list, err := c.ListTools(context.Background(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	got := make([]string, len(list.Tools))
	for i, tl := range list.Tools {
		got[i] = tl.Name
	}
	sort.Strings(got)

	want := []string{
		"advise", "analyze_plan", "check_migration", "describe_table", "detect",
		"lint_schema", "list_tables", "search_schema",
		"validate_query",
	}
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("offline tool count = %d, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("offline tool set mismatch\n got: %v\nwant: %v", got, want)
		}
	}

	forbidden := map[string]bool{
		"snapshot_diff": true,
		"explain_query": true, "check_drift": true, "columnar_report": true,
	}
	for _, name := range got {
		if forbidden[name] {
			t.Errorf("offline surface leaked history/live tool %q", name)
		}
	}
}

// setupOfflineRegisterTest mirrors setupOfflineTest but registers only the
// offline subset (RegisterOffline), as a hosted transport would.
func setupOfflineRegisterTest(t *testing.T) *client.Client {
	t.Helper()

	snap := loadDemoSchema(t)

	srv := NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: snap}, lint.DefaultConfig())
	mcpSrv := mcpserver.NewMCPServer("dryrun-offline-test", "0.1.0",
		mcpserver.WithRecovery(),
		mcpserver.WithInputSchemaValidation(),
		mcpserver.WithOutputSchemaValidation(),
	)
	srv.RegisterOffline(mcpSrv)

	serverReader, clientWriter := io.Pipe()
	clientReader, serverWriter := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())

	stdioSrv := mcpserver.NewStdioServer(mcpSrv)
	stdioSrv.SetErrorLogger(log.New(io.Discard, "", 0))
	go stdioSrv.Listen(ctx, serverReader, serverWriter)

	var logBuf bytes.Buffer
	tr := transport.NewIO(clientReader, clientWriter, io.NopCloser(&logBuf))
	if err := tr.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}

	c := client.NewClient(tr)
	var initReq mcp.InitializeRequest
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	if _, err := c.Initialize(ctx, initReq); err != nil {
		cancel()
		t.Fatal(err)
	}

	t.Cleanup(func() {
		tr.Close()
		cancel()
		serverWriter.Close()
		serverReader.Close()
	})

	return c
}
