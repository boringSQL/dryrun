package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/config"
	"github.com/boringsql/dryrun/internal/history"
	"github.com/boringsql/dryrun/internal/schema"
)

func at(t *testing.T, s string) *time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return &ts
}

// A project whose schema is settled and refreshed by this run, so fleetIssues
// reports only the server/database problems these tests are about.
func settledSchema() schemaState {
	return schemaState{
		prior:      &schema.SchemaSnapshot{ContentHash: "abc123def456", Timestamp: time.Now().UTC()},
		verified:   time.Now().UTC(),
		hasAttempt: true,
	}
}

// marks the first reachable node as capturing schema
func withSchemaNode(results []checkResult) []checkResult {
	results = slices.Clone(results) // applied twice to one slice in places
	for i := range results {
		if results[i].Reached && results[i].Fail == nil {
			results[i].Streams = append(results[i].Streams, "schema")
			return results
		}
	}
	return results
}

// two labels on one server: same database is fatal, different databases just a warning
func TestFleetIssues_DuplicateServer(t *testing.T) {
	boot := at(t, "2026-08-01T10:00:00.123456Z")
	results := []checkResult{
		{Label: "primary", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1"},
		{Label: "replica-1", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1"},
	}
	fatal, warn := fleetIssues(withSchemaNode(results), settledSchema(), false)
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
	fatal, warn = fleetIssues(withSchemaNode(results), settledSchema(), false)
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
	fatal, warn := fleetIssues(withSchemaNode(results), settledSchema(), false)
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
	if fatal, _ := fleetIssues(withSchemaNode(results), settledSchema(), false); len(fatal) != 1 {
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

func TestSchemaFleetIssues(t *testing.T) {
	stored := schemaState{prior: &schema.SchemaSnapshot{ContentHash: "abc", Timestamp: time.Now().UTC()}}
	empty := schemaState{}

	statsOnly := []checkResult{{Label: "n", Reached: true, Streams: []string{"activity"}}}
	withPlanner := []checkResult{{Label: "n", Reached: true, Streams: []string{"planner", "activity"}}}
	capturesSchema := []checkResult{{Label: "p", Reached: true, Streams: []string{"schema", "activity"}}}

	// nothing to bind to and nothing in the run creating one
	fatal, _ := schemaFleetIssues(statsOnly, empty, false)
	if len(fatal) != 1 || !strings.Contains(fatal[0], "no node in this run captures one") {
		t.Errorf("fatal %v, want one fleet-level refusal", fatal)
	}

	// §5.4: a run that captures schema closes the gap itself, so a fresh
	// project's first --check passes rather than failing a run that succeeds
	if fatal, warn := schemaFleetIssues(capturesSchema, empty, false); len(fatal) != 0 || len(warn) != 0 {
		t.Errorf("fatal %v warn %v, want silence when this run writes the schema", fatal, warn)
	}

	// --allow-orphan waives the binding, but planner annotates against the
	// snapshot itself and cannot be waived
	if fatal, _ := schemaFleetIssues(statsOnly, empty, true); len(fatal) != 0 {
		t.Errorf("fatal %v, want --allow-orphan to waive stats-only streams", fatal)
	}
	fatal, _ = schemaFleetIssues(withPlanner, empty, true)
	if len(fatal) != 1 || !strings.Contains(fatal[0], "planner") {
		t.Errorf("fatal %v, want planner's own requirement surfaced", fatal)
	}

	// a stored schema nobody refreshes is not fatal, but it only gets older
	fatal, warn := schemaFleetIssues(statsOnly, stored, false)
	if len(fatal) != 0 {
		t.Errorf("fatal %v, want a stored schema to satisfy the gate", fatal)
	}
	if len(warn) != 1 || !strings.Contains(warn[0], "only gets older") {
		t.Errorf("warn %v, want the stale-binding warning", warn)
	}
	if _, warn := schemaFleetIssues(capturesSchema, stored, false); len(warn) != 0 {
		t.Errorf("warn %v, want none when a node refreshes it", warn)
	}

	// an unreadable history is never waived
	fatal, _ = schemaFleetIssues(capturesSchema, schemaState{readErr: errors.New("boom")}, true)
	if len(fatal) != 1 || !strings.Contains(fatal[0], "cannot read") {
		t.Errorf("fatal %v, want the store error surfaced", fatal)
	}

	// an unreachable node cannot be relied on to write the schema
	downSchema := []checkResult{
		{Label: "p", Fail: errors.New("down"), Streams: []string{"schema"}},
		{Label: "r", Reached: true, Streams: []string{"activity"}},
	}
	if fatal, _ := schemaFleetIssues(downSchema, empty, false); len(fatal) != 1 {
		t.Errorf("fatal %v, want a failed node not to count as capturing schema", fatal)
	}

	// a standby returns n/a for schema, so declaring it proves nothing.
	// role = "auto" on a replica loads fine, so config cannot catch this and
	// --check would otherwise pass a run that captureStreams aborts.
	standby := []checkResult{
		{Label: "r", Reached: true, Role: history.NodeRoleStandby, Streams: []string{"schema", "activity"}},
	}
	if fatal, _ := schemaFleetIssues(standby, empty, false); len(fatal) != 1 {
		t.Errorf("fatal %v, want a standby's schema declaration not to count", fatal)
	}
	primary := []checkResult{
		{Label: "p", Reached: true, Role: history.NodeRolePrimary, Streams: []string{"schema"}},
	}
	if fatal, _ := schemaFleetIssues(primary, empty, false); len(fatal) != 0 {
		t.Errorf("fatal %v, want a primary's declaration to count", fatal)
	}

	// a subset run (--from, --node) does not refresh the binding, but the
	// fleet cron may have; the attempt clock is the evidence
	recent := schemaState{prior: stored.prior, verified: time.Now().UTC().Add(-time.Hour), hasAttempt: true}
	if _, warn := schemaFleetIssues(statsOnly, recent, false); len(warn) != 0 {
		t.Errorf("warn %v, want silence when this host captured schema recently", warn)
	}
	stale := schemaState{prior: stored.prior, verified: time.Now().UTC().Add(-90 * 24 * time.Hour), hasAttempt: true}
	if _, warn := schemaFleetIssues(statsOnly, stale, false); len(warn) != 1 {
		t.Errorf("warn %v, want the stale-binding warning", warn)
	}

	// nothing answered: every node already failed on its own row, and the run
	// produced no evidence about the fleet
	allDown := []checkResult{{Label: "p", Fail: errors.New("down")}}
	if fatal, warn := schemaFleetIssues(allDown, empty, false); len(fatal) != 0 || len(warn) != 0 {
		t.Errorf("fatal %v warn %v, want no fleet verdict when nothing was reached", fatal, warn)
	}
}

func TestRoundAge(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Minute, "30m"},
		{time.Hour, "1h"},
		{47 * time.Hour, "47h"},
		{48 * time.Hour, "2d"},
		{90 * 24 * time.Hour, "90d"},
	} {
		if got := roundAge(tc.d); got != tc.want {
			t.Errorf("roundAge(%s) = %q, want %q", tc.d, got, tc.want)
		}
	}

	// a future or zero timestamp is a skewed clock, not "just now"
	now := time.Now().UTC()
	for _, then := range []time.Time{now.Add(time.Hour), {}} {
		if got := ageAgo(now, then); !strings.Contains(got, "clock skew") {
			t.Errorf("ageAgo(%v) = %q, want it flagged as untrustworthy", then, got)
		}
	}
}

// pool label lands on a member by design, so sharing a server isn't a misconfiguration
func TestFleetIssues_PoolLabelDowngradesToWarning(t *testing.T) {
	boot := at(t, "2026-08-01T10:00:00Z")
	results := []checkResult{
		{Label: "primary", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1"},
		{Label: "read-pool", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1", Pool: true},
	}
	fatal, warn := fleetIssues(withSchemaNode(results), settledSchema(), false)
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
	fatal, _ := fleetIssues(withSchemaNode([]checkResult{
		{Label: "a", Reached: true, Started: boot, Database: "app", ServerAddr: "10.0.0.1"},
		{Label: "b", Reached: true, Started: boot, Database: "app", ServerAddr: "127.0.0.1"},
	}), settledSchema(), false)
	if len(fatal) != 1 || !strings.Contains(fatal[0], "two addresses") {
		t.Errorf("fatal %v, want the address disagreement acknowledged", fatal)
	}
}

// unknown --streams must fail before connecting, on both paths
func TestCaptureTargets_RejectsUnknownStreams(t *testing.T) {
	for _, bad := range []string{"vacuum", "plnner"} {
		_, err := captureTargets("", "postgres://u@h/db", "n", []string{bad}, false)
		if err == nil || !strings.Contains(err.Error(), bad) {
			t.Errorf("--streams %s: error %v, want a refusal naming it", bad, err)
		}
	}
	if _, err := captureTargets("", "postgres://u@h/db", "n", []string{"schema", "query", "activity"}, false); err != nil {
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

// PutSchema dedups, so the snapshot's own timestamp is when the DDL last
// changed, not when it was last confirmed current. A stable database has an
// old schema and a recent attempt; a stalled capture has an old both. The
// report has to show the two numbers or they cannot be told apart.
func TestPrintSchemaLine(t *testing.T) {
	now := time.Now().UTC()
	old := &schema.SchemaSnapshot{ContentHash: "abc123def4567890", Timestamp: now.Add(-90 * 24 * time.Hour)}

	var out bytes.Buffer
	printSchemaLine(&out, schemaState{prior: old, verified: now.Add(-3 * time.Hour), hasAttempt: true})
	got := out.String()
	for _, want := range []string{"abc123def456", "90d ago", "last confirmed current 3h ago"} {
		if !strings.Contains(got, want) {
			t.Errorf("line %q omits %q", got, want)
		}
	}
	// the hash is truncated for the report, never the whole digest
	if strings.Contains(got, "abc123def4567890") {
		t.Errorf("line %q prints the full hash", got)
	}

	// never captured here: say so rather than printing a misleading age
	out.Reset()
	printSchemaLine(&out, schemaState{prior: old})
	if !strings.Contains(out.String(), "no schema capture recorded on this host") {
		t.Errorf("line %q, want the missing attempt clock called out", out.String())
	}

	// A pulled row, or one written by `snapshot take` (which does not stamp
	// the attempt clock), is newer than anything captured locally. Calling
	// that "last confirmed 40d ago" would read as a stalled capture.
	out.Reset()
	fresh := &schema.SchemaSnapshot{ContentHash: "abc", Timestamp: now.Add(-5 * time.Minute)}
	printSchemaLine(&out, schemaState{prior: fresh, verified: now.Add(-40 * 24 * time.Hour), hasAttempt: true})
	if got := out.String(); !strings.Contains(got, "written elsewhere") || strings.Contains(got, "last confirmed") {
		t.Errorf("line %q, want a snapshot newer than the local clock called out", got)
	}

	// a future-dated row (a peer with a skewed clock) must not render as
	// "just captured", the most reassuring read of the least trustworthy input
	out.Reset()
	printSchemaLine(&out, schemaState{
		prior: &schema.SchemaSnapshot{ContentHash: "abc", Timestamp: now.Add(2 * time.Hour)}})
	if !strings.Contains(out.String(), "clock skew") {
		t.Errorf("line %q, want the skew flagged", out.String())
	}

	// nothing to report on a fresh project or an unreadable history; the
	// fleet gate speaks for those
	for _, st := range []schemaState{{}, {readErr: errors.New("boom")}} {
		out.Reset()
		printSchemaLine(&out, st)
		if out.String() != "" {
			t.Errorf("line %q, want nothing printed", out.String())
		}
	}
}

// The gate moved from a per-node FAIL to one fleet problem, so the summary now
// says every node is ready while the run still exits non-zero. That pairing is
// only honest because the fleet-problem line follows it.
func TestRunCaptureCheck_SchemaGateIsFleetLevel(t *testing.T) {
	store := testStoreAt(t, t.TempDir())
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "p"}

	// unreachable, so the node fails on its own row and the fleet gate stays
	// quiet -- the pairing this test is about needs a reachable node, which a
	// unit test has no way to provide. Assert on the wiring we can reach:
	// a fresh project's fleet fatal is printed apart and still exits non-zero.
	var out bytes.Buffer
	targets := []captureTarget{{
		Label: "replica-1", URL: "postgres://u@127.0.0.1:1/app", Role: "auto", Streams: []string{"query"},
	}}
	err := runCaptureCheck(context.Background(), store, key, targets,
		captureRunOptions{}, 250*time.Millisecond, &out)
	if err == nil {
		t.Fatal("want a non-zero exit")
	}
	got := out.String()
	// no node answered, so the run has no fleet verdict to give
	if strings.Contains(got, "fleet-level problem") {
		t.Errorf("report claims a fleet problem from a run that reached nothing:\n%s", got)
	}
	// and with no schema stored, the age line has nothing to say
	if strings.Contains(got, "schema:") {
		t.Errorf("report prints a schema line for a project with no schema:\n%s", got)
	}
}

// The age line is what tells a stable database apart from a stalled capture,
// so it has to survive the full report path, not just its own unit test.
func TestRunCaptureCheck_PrintsSchemaAge(t *testing.T) {
	store := testStoreAt(t, t.TempDir())
	key := history.SnapshotKey{ProjectID: "p", DatabaseID: "p"}
	if _, err := store.PutSchema(context.Background(), key, &schema.SchemaSnapshot{
		ContentHash: "deadbeefcafe1234", Timestamp: time.Now().UTC().Add(-72 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	targets := []captureTarget{{
		Label: "replica-1", URL: "postgres://u@127.0.0.1:1/app", Role: "auto", Streams: []string{"query"},
	}}
	_ = runCaptureCheck(context.Background(), store, key, targets,
		captureRunOptions{}, 250*time.Millisecond, &out)

	got := out.String()
	for _, want := range []string{"schema: deadbeefcafe", "3d ago", "no schema capture recorded on this host"} {
		if !strings.Contains(got, want) {
			t.Errorf("report omits %q:\n%s", want, got)
		}
	}
}
