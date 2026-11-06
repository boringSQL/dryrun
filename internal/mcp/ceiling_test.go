package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
)

func ceilingCall(t *testing.T, max int, res *mcp.CallToolResult, herr error) (*mcp.CallToolResult, error) {
	t.Helper()
	var req mcp.CallToolRequest
	req.Params.Name = "snapshot_diff"
	h := ResultCeiling(max)(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) { return res, herr })
	return h(context.Background(), req)
}

func TestResultCeiling(t *testing.T) {
	small := mcp.NewToolResultText("ok")
	if got, err := ceilingCall(t, 1024, small, nil); err != nil || got != small {
		t.Errorf("a small result must pass through untouched: %v %v", got, err)
	}

	big := mcp.NewToolResultText(strings.Repeat("x", 5000))
	got, _ := ceilingCall(t, 1024, big, nil)
	text := got.Content[0].(mcp.TextContent).Text
	if !got.IsError {
		t.Fatal("an oversized result was not replaced by an error")
	}
	for _, want := range []string{"snapshot_diff", "limit=", "offset="} {
		if !strings.Contains(text, want) {
			t.Errorf("error %q does not mention %q", text, want)
		}
	}
	if len(text) > 1024 {
		t.Errorf("the replacement is itself %d bytes", len(text))
	}

	// a short text with a huge structured copy is just as big on the wire
	structured := mcp.NewToolResultStructured(map[string]any{"rows": strings.Repeat("y", 5000)}, "summary")
	if got, _ := ceilingCall(t, 1024, structured, nil); !got.IsError {
		t.Error("an oversized structuredContent slipped through")
	}

	errRes := mcp.NewToolResultError(strings.Repeat("e", 5000))
	if got, _ := ceilingCall(t, 1024, errRes, nil); got != errRes {
		t.Error("error results must not be rewritten")
	}
	boom := errors.New("boom")
	if _, err := ceilingCall(t, 1024, nil, boom); !errors.Is(err, boom) {
		t.Errorf("handler errors must pass through, got %v", err)
	}
}

// through a real server: the replacement passes output validation, and recovery still reports a panic
func TestResultCeiling_ThroughServer(t *testing.T) {
	srv := mcpserver.NewMCPServer("t", "1",
		mcpserver.WithToolHandlerMiddleware(ResultCeiling(1024)),
		mcpserver.WithRecovery(),
		mcpserver.WithOutputSchemaValidation(),
	)
	srv.AddTool(mcp.NewTool("big", mcp.WithRawOutputSchema(json.RawMessage(
		`{"type":"object","properties":{"rows":{"type":"string"}},"required":["rows"]}`))), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultStructured(map[string]any{"rows": strings.Repeat("x", 5000)}, "rows"), nil
	})
	srv.AddTool(mcp.NewTool("boom"), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		panic("bad plan_json")
	})

	call := func(name string) map[string]any {
		msg := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + name + `","arguments":{}}}`
		b, _ := json.Marshal(srv.HandleMessage(context.Background(), json.RawMessage(msg)))
		var out map[string]any
		_ = json.Unmarshal(b, &out)
		return out
	}

	res, _ := call("big")["result"].(map[string]any)
	if res["isError"] != true {
		t.Errorf("oversized tool result reached the client: %v", res)
	}
	if msg, _ := json.Marshal(call("boom")["error"]); !strings.Contains(string(msg), "panic recovered") {
		t.Errorf("a panic should surface as a recovered error, got %s", msg)
	}
}
