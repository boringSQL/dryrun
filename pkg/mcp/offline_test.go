package mcp_test

import (
	"bytes"
	"context"
	"io"
	"log"
	"testing"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	mcpproto "github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/boringsql/dryrun/pkg/lint"
	drmcp "github.com/boringsql/dryrun/pkg/mcp"
	"github.com/boringsql/dryrun/pkg/snapshot"
)

// The path the hosted endpoint takes: assemble a snapshot from blobs, build the
// offline server, list its tools. Proves the public facade wires end to end and
// serves the schema-only subset (not the history/live tools).
func TestBuildOfflineMCPServer_ListsSchemaSubset(t *testing.T) {
	sch := &snapshot.SchemaSnapshot{
		Database: "app",
		Tables: []snapshot.Table{
			{Schema: "public", Name: "users"},
		},
	}
	a := snapshot.AssembleAnnotated(sch, nil, nil)

	mcpSrv := drmcp.BuildOfflineMCPServer("hindsight-test", "0.1.0", a, lint.DefaultConfig())

	c := inProcessClient(t, mcpSrv)
	list, err := c.ListTools(context.Background(), mcpproto.ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(list.Tools) != 9 {
		names := make([]string, len(list.Tools))
		for i, tl := range list.Tools {
			names[i] = tl.Name
		}
		t.Fatalf("offline server exposed %d tools, want 9: %v", len(list.Tools), names)
	}

	for _, tl := range list.Tools {
		switch tl.Name {
		case "snapshot_diff", "reload_schema", "explain_query", "check_drift", "columnar_report":
			t.Errorf("hosted surface leaked %q", tl.Name)
		}
	}
}

func inProcessClient(t *testing.T, mcpSrv *mcpserver.MCPServer) *client.Client {
	t.Helper()

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
	var initReq mcpproto.InitializeRequest
	initReq.Params.ProtocolVersion = mcpproto.LATEST_PROTOCOL_VERSION
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
