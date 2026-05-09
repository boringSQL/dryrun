package mcp

import (
	"bytes"
	"context"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/boringsql/dryrun/internal/lint"
	"github.com/boringsql/dryrun/internal/schema"
)

func setupOfflineTest(t *testing.T) *client.Client {
	t.Helper()

	snap, err := schema.LoadSchemaFile("../../examples/demo/.dryrun/schema.json")
	if err != nil {
		t.Fatal(err)
	}

	srv := NewOfflineServer(snap, lint.DefaultConfig())
	mcpSrv := mcpserver.NewMCPServer("dryrun-test", "0.1.0")
	srv.Register(mcpSrv)

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

func callTool(t *testing.T, c *client.Client, name string, args map[string]any) string {
	t.Helper()

	var req mcp.CallToolRequest
	req.Params.Name = name
	req.Params.Arguments = args

	result, err := c.CallTool(context.Background(), req)
	if err != nil {
		t.Fatalf("CallTool(%s): %v", name, err)
	}
	if result == nil || len(result.Content) == 0 {
		t.Fatalf("CallTool(%s): empty result", name)
	}

	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("CallTool(%s): expected TextContent, got %T", name, result.Content[0])
	}
	return text.Text
}

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected output to contain %q, got:\n%.500s", needle, haystack)
	}
}

// Pins the error message contract for getSchema when the server has no snap
// loaded; clients use the "no schema loaded" / "initialize first" substrings
// to surface actionable guidance back to the user.
func TestGetSchema_UninitializedError(t *testing.T) {
	srv := &Server{lintConfig: lint.DefaultConfig()}
	srv.SetUninitialized([]string{"/tmp/nonexistent"})
	_, err := srv.getSchema()
	if err == nil {
		t.Fatal("expected error when uninitialized")
	}
	if !strings.Contains(err.Error(), "no schema loaded") || !strings.Contains(err.Error(), "initialize first") {
		t.Errorf("unexpected error: %v", err)
	}
}
