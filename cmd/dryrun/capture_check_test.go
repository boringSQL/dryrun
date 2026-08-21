package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/history"
)

func at(t *testing.T, s string) *time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return &ts
}

// two labels on one server: same database is fatal, different databases just a warning
func TestFleetIssues_DuplicateServer(t *testing.T) {
	boot := at(t, "2026-08-01T10:00:00.123456Z")
	results := []checkResult{
		{Label: "primary", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1"},
		{Label: "replica-1", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1"},
	}
	fatal, warn := fleetIssues(results)
	if len(fatal) != 1 {
		t.Fatalf("fatal %v, want one duplicate-server problem", fatal)
	}
	for _, want := range []string{"primary", "replica-1", "10.0.0.1"} {
		if !strings.Contains(fatal[0], want) {
			t.Errorf("message %q omits %q", fatal[0], want)
		}
	}
	if len(warn) != 0 {
		t.Errorf("warnings %v, want none", warn)
	}

	results[1].Database = "billing"
	fatal, warn = fleetIssues(results)
	if len(fatal) != 0 {
		t.Errorf("fatal %v: two databases on one server is legitimate", fatal)
	}
	if len(warn) != 2 {
		t.Errorf("warnings %v, want the same-server notice and the split-database one", warn)
	}
}

// distinct servers must not merge; no boot time or unreachable means no evidence
func TestFleetIssues_NoFalsePositives(t *testing.T) {
	results := []checkResult{
		{Label: "primary", Reached: true, Started: at(t, "2026-08-01T10:00:00Z"), Database: "app"},
		{Label: "replica-1", Reached: true, Started: at(t, "2026-08-01T10:00:01Z"), Database: "app"},
		{Label: "replica-2", Reached: false},
		{Label: "replica-3", Reached: true, Database: "app"},
	}
	fatal, warn := fleetIssues(results)
	if len(fatal) != 0 || len(warn) != 0 {
		t.Errorf("fatal %v warn %v, want silence", fatal, warn)
	}
}

// same instant in two zones is one server
func TestFleetIssues_TimezoneNormalised(t *testing.T) {
	results := []checkResult{
		{Label: "a", Reached: true, Started: at(t, "2026-08-01T12:00:00+02:00"), Database: "app"},
		{Label: "b", Reached: true, Started: at(t, "2026-08-01T10:00:00Z"), Database: "app"},
	}
	if fatal, _ := fleetIssues(results); len(fatal) != 1 {
		t.Errorf("fatal %v, want the duplicate caught across zones", fatal)
	}
}

func TestCheckNode_UnsetURLEnvNeverConnects(t *testing.T) {
	dir := t.TempDir()
	store := testStoreAt(t, dir)
	cfg := `[project]
id = "p"
[[node]]
name = "primary"
url_env = "DRYRUN_TEST_UNSET_URL"
`
	t.Setenv("DRYRUN_TEST_UNSET_URL", "")
	nodes := resolveNodesFromTOML(t, dir, cfg)

	r := checkNode(context.Background(), store, history.SnapshotKey{ProjectID: "p", DatabaseID: "p"},
		targetFromNode(nodes[0], nil), captureRunOptions{}, time.Second)
	if r.Fail == nil || !strings.Contains(r.Fail.Error(), "DRYRUN_TEST_UNSET_URL") {
		t.Fatalf("fail %v, want the unset variable named", r.Fail)
	}
	if r.Reached {
		t.Error("an unresolvable URL must not be reported as reached")
	}
}

// one unreachable node fails the run but still shows up named in the report
func TestRunCaptureCheck_UnreachableNodeFailsButReports(t *testing.T) {
	dir := t.TempDir()
	store := testStoreAt(t, dir)
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "p"}

	var out bytes.Buffer
	targets := []captureTarget{{
		Label:   "replica-1",
		URL:     "postgres://u:hunter2@127.0.0.1:1/app",
		Role:    "auto",
		Streams: []string{"query"},
	}}
	err := runCaptureCheck(context.Background(), store, key, targets, captureRunOptions{AllowOrphan: true}, 250*time.Millisecond, &out)
	if err == nil {
		t.Fatal("want a non-nil error so the shell sees a non-zero exit")
	}
	got := out.String()
	if !strings.Contains(got, "replica-1") || !strings.Contains(got, "FAIL") {
		t.Errorf("report does not show the failed node:\n%s", got)
	}
	if !strings.Contains(got, "0 of 1 node(s) ready") {
		t.Errorf("report does not summarise:\n%s", got)
	}
	if strings.Contains(got, "hunter2") {
		t.Errorf("password leaked into the report:\n%s", got)
	}
}

// planner needs a stored schema snapshot; fail here, not mid-capture
func TestApplySchemaBinding(t *testing.T) {
	missing := errors.New("no schema snapshot to bind to")

	// no snapshot and no --allow-orphan: every stream needs the ref
	got := applySchemaBinding(checkResult{Label: "n", Reached: true, Streams: []string{"activity"}}, missing, false)
	if got.Fail == nil {
		t.Error("without --allow-orphan a node with no schema snapshot must fail")
	}
	// --allow-orphan waives the ref, but planner annotates against the snapshot itself
	got = applySchemaBinding(checkResult{Label: "n", Reached: true, Streams: []string{"planner", "activity"}}, nil, false)
	if got.Fail == nil || !strings.Contains(got.Fail.Error(), "planner") {
		t.Errorf("fail %v, want the planner-needs-a-snapshot refusal", got.Fail)
	}
	if got := applySchemaBinding(checkResult{Streams: []string{"activity", "query"}}, nil, false); got.Fail != nil {
		t.Errorf("fail %v: with --allow-orphan these streams capture fine", got.Fail)
	}
	if got := applySchemaBinding(checkResult{Streams: []string{"planner"}}, nil, true); got.Fail != nil {
		t.Errorf("fail %v, want none when a schema snapshot exists", got.Fail)
	}
	// the node's own error is the more useful one
	conn := errors.New("connection failed")
	got = applySchemaBinding(checkResult{Label: "n", Fail: conn, Streams: []string{"planner"}}, missing, false)
	if !errors.Is(got.Fail, conn) {
		t.Errorf("fail %v, want the node's own error kept", got.Fail)
	}
}

// pool label lands on a member by design, so sharing a server isn't a misconfiguration
func TestFleetIssues_PoolLabelDowngradesToWarning(t *testing.T) {
	boot := at(t, "2026-08-01T10:00:00Z")
	results := []checkResult{
		{Label: "primary", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1"},
		{Label: "read-pool", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1", Pool: true},
	}
	fatal, warn := fleetIssues(results)
	if len(fatal) != 0 {
		t.Errorf("fatal %v: a pool member may legitimately be a declared node", fatal)
	}
	if len(warn) != 1 || !strings.Contains(warn[0], "pool") {
		t.Errorf("warnings %v, want one mentioning the pool", warn)
	}
}

// one server can answer on two addresses; the message must not name either
func TestFleetIssues_DisagreeingAddresses(t *testing.T) {
	boot := at(t, "2026-08-01T10:00:00Z")
	fatal, _ := fleetIssues([]checkResult{
		{Label: "a", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1"},
		{Label: "b", Reached: true, Started: boot, Database: "app", ServerAddr: "127.0.0.1"},
	})
	if len(fatal) != 1 || !strings.Contains(fatal[0], "two addresses") {
		t.Errorf("fatal %v, want the address disagreement acknowledged", fatal)
	}
}

// unknown --streams must fail before connecting, on both paths
func TestCaptureTargets_RejectsUnknownStreams(t *testing.T) {
	for _, bad := range []string{"schema", "plnner"} {
		_, err := captureTargets("", "postgres://u@h/db", "n", []string{bad}, false)
		if err == nil || !strings.Contains(err.Error(), bad) {
			t.Errorf("--streams %s: error %v, want a refusal naming it", bad, err)
		}
	}
	if _, err := captureTargets("", "postgres://u@h/db", "n", []string{"query", "activity"}, false); err != nil {
		t.Errorf("valid streams refused: %v", err)
	}
}

// --check captures nothing, so there is nothing to --push; refuse the combo
func TestCaptureCmd_CheckRefusesPush(t *testing.T) {
	db := filepath.Join(t.TempDir(), "history.db")
	cmd := snapshotCaptureCmd(&db)
	cmd.SetArgs([]string{"--check", "--push", "--from", "postgres://u@h/db", "--label", "n"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--push") {
		t.Fatalf("error %v, want the flag conflict refused", err)
	}
}

func TestPrintCheckTable(t *testing.T) {
	var out bytes.Buffer
	printCheckTable(&out, []checkResult{
		{Label: "primary", Reached: true, Role: "primary", PgVersion: "PostgreSQL 17.2 on aarch64", Database: "app",
			ServerAddr: "10.0.0.1", Streams: []string{"planner", "activity", "query"}},
		{Label: "replica-1", Reached: true, Role: "standby", Streams: []string{"activity"},
			Warnings: []string{"pg_stat_statements missing"}},
		{Label: "replica-2", Fail: errors.New("boom")},
	})
	got := out.String()
	for _, want := range []string{"NODE", "17.2", "planner,activity,query", "warn", "FAIL"} {
		if !strings.Contains(got, want) {
			t.Errorf("table omits %q:\n%s", want, got)
		}
	}
}

func resolveNodesFromTOML(t *testing.T, dir, body string) []config.ResolvedNode {
	t.Helper()
	path := filepath.Join(dir, "dryrun.toml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := cfg.ResolveNodes()
	if err != nil {
		t.Fatal(err)
	}
	return nodes
}
