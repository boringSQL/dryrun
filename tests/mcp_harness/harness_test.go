package mcpharness

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

var binaryPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "dryrun-harness-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	binaryPath = filepath.Join(dir, "dryrun")
	cmd := exec.Command("go", "build", "-o", binaryPath, "../../cmd/dryrun")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// shared fixture content hash; any constant works as long as schema/planner/activity all use it
const refHash = "deadbeef0000000000000000000000000000000000000000000000000000beef"

type Fixture struct {
	ProjectDir     string
	Now            time.Time
	LastAutovacuum time.Time
	Schema         *schema.SchemaSnapshot
}

// buildFixture seeds history.db with the fixture schema — the only schema source
// mcp-serve reads. If withStats, also seeds matching planner+activity. A decoy
// .dryrun/schema.json carrying a different table is always written: every
// assertion against auth.oauth_token therefore also proves the file is ignored.
func buildFixture(t *testing.T, withStats bool) *Fixture {
	t.Helper()
	projectDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(projectDir, ".dryrun"), 0o755); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	autovac := now.Add(-6 * time.Hour)

	snap := &schema.SchemaSnapshot{
		PgVersion:   "16.13",
		Database:    "harness",
		Timestamp:   now,
		ContentHash: refHash,
		Tables: []schema.Table{{
			OID: 1, Schema: "auth", Name: "oauth_token",
			Columns: []schema.Column{
				{Name: "id", Ordinal: 1, TypeName: "bigint", Nullable: false},
				{Name: "user_id", Ordinal: 2, TypeName: "bigint", Nullable: false},
			},
			Constraints: []schema.Constraint{}, Indexes: []schema.Index{},
			Policies: []schema.RlsPolicy{}, Triggers: []schema.Trigger{},
		}},
	}
	decoy := &schema.SchemaSnapshot{
		PgVersion: "16.13", Database: "stale_file", Timestamp: now, ContentHash: "f11e" + refHash[4:],
		Tables: []schema.Table{{OID: 99, Schema: "auth", Name: "t_from_stale_file"}},
	}
	decoyBytes, _ := json.MarshalIndent(decoy, "", "  ")
	if err := os.WriteFile(filepath.Join(projectDir, ".dryrun", "schema.json"), decoyBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	key := (&config.ProjectConfig{}).ProjectID(projectDir)
	snapKey := history.SnapshotKey{ProjectID: key, DatabaseID: history.DatabaseId(string(key))}

	store, err := history.Open(filepath.Join(projectDir, ".dryrun", "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	if _, err := store.PutSchema(ctx, snapKey, snap); err != nil {
		t.Fatal(err)
	}

	if withStats {
		planner := &schema.PlannerStatsSnapshot{
			SchemaRefHash: refHash, ContentHash: refHash, Database: "harness", Timestamp: now,
			Tables: []schema.TableSizingEntry{{
				Table: schema.QualifiedName{Schema: "auth", Name: "oauth_token"},
				Sizing: schema.TableSizing{
					Reltuples: 1_636_991, Relpages: 251_292,
					TableSize: 2_058_584_064, TotalRelationSize: 3_150_209_024, IndexesSize: 1_091_026_944,
				},
			}},
		}
		activity := &schema.ActivityStatsSnapshot{
			SchemaRefHash: refHash, ContentHash: refHash,
			Node: schema.NodeIdentity{Source: "primary", PgVersion: "16.13", Timestamp: now},
			Tables: []schema.TableActivityEntry{{
				Table: schema.QualifiedName{Schema: "auth", Name: "oauth_token"},
				Activity: schema.TableActivity{
					NLiveTup: 1_636_991, NDeadTup: 13_912,
					LastAutovacuum: &autovac, AutovacuumCount: 4,
				},
			}},
		}

		if _, err := store.PutPlanner(ctx, snapKey, planner); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PutActivity(ctx, snapKey, activity); err != nil {
			t.Fatal(err)
		}
	}

	return &Fixture{ProjectDir: projectDir, Now: now, LastAutovacuum: autovac, Schema: snap}
}

// startMCP spawns bin/dryrun mcp-serve with cwd=projectDir and runs MCP initialize.
func startMCP(t *testing.T, projectDir string) *client.Client {
	t.Helper()
	cli, err := client.NewStdioMCPClientWithOptions(binaryPath, nil, []string{"mcp-serve"},
		transport.WithCommandFunc(func(ctx context.Context, cmd string, env []string, args []string) (*exec.Cmd, error) {
			c := exec.CommandContext(ctx, cmd, args...)
			c.Env = append(os.Environ(), env...)
			c.Dir = projectDir
			return c, nil
		}),
	)
	if err != nil {
		t.Fatalf("start mcp: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "harness", Version: "0"}
	if _, err := cli.Initialize(context.Background(), initReq); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	return cli
}

// callJSON invokes a tool and parses its text content as JSON into out.
// Fails the test if the tool errored or returned non-JSON text.
func callJSON(t *testing.T, cli *client.Client, name string, args map[string]any, out any) string {
	t.Helper()
	call := mcp.CallToolRequest{}
	call.Params.Name = name
	call.Params.Arguments = args
	res, err := cli.CallTool(context.Background(), call)
	if err != nil {
		t.Fatalf("call %s: %v", name, err)
	}
	if res.IsError {
		t.Fatalf("%s returned error: %+v", name, res.Content)
	}
	if len(res.Content) == 0 {
		t.Fatalf("%s returned no content", name)
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("%s: expected TextContent, got %T", name, res.Content[0])
	}
	if tc.Text == "" || tc.Text[0] != '{' {
		t.Fatalf("%s: expected JSON object, got plain text: %q", name, tc.Text)
	}
	if err := json.Unmarshal([]byte(tc.Text), out); err != nil {
		t.Fatalf("%s: parse: %v\n%s", name, err, tc.Text)
	}
	return tc.Text
}

// callMaybeError returns the first text content and the IsError flag without
// failing the test. Use for handlers that may legitimately return errResult.
func callMaybeError(t *testing.T, cli *client.Client, name string, args map[string]any) (string, bool) {
	t.Helper()
	call := mcp.CallToolRequest{}
	call.Params.Name = name
	call.Params.Arguments = args
	res, err := cli.CallTool(context.Background(), call)
	if err != nil {
		t.Fatalf("call %s: %v", name, err)
	}
	if len(res.Content) == 0 {
		return "", res.IsError
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("%s: expected TextContent, got %T", name, res.Content[0])
	}
	return tc.Text, res.IsError
}
