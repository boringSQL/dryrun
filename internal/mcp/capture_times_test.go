package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

const (
	fixtureSchemaAt   = "2026-08-01T12:00:00Z"
	fixtureActivityAt = "2026-08-02T12:00:00Z"
)

func metaOf(t *testing.T, out string) map[string]any {
	t.Helper()
	var decoded struct {
		Meta map[string]any `json:"_meta"`
	}
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("unmarshal: %v\n%.300s", err, out)
	}
	return decoded.Meta
}

// The tool descriptions say the answer is as of the last capture. Without a
// timestamp an agent can only repeat that; with one it can decide.
func TestMetaCarriesCaptureTimes(t *testing.T) {
	a := withActivity(annotate(multiSchemaSnap(), 1000),
		schema.QualifiedName{Schema: "app", Name: "events"},
		schema.TableActivity{SeqScan: 500_000, NLiveTup: 500_000})
	c := serveOffline(t, NewOfflineServerAnnotated(a, lint.DefaultConfig()))

	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"describe_table", map[string]any{"table": "app.orders"}},
		{"detect", map[string]any{"kind": "anomalies"}},
		{"lint_schema", nil},
	} {
		meta := metaOf(t, callTool(t, c, tc.tool, tc.args))
		if meta["schema_captured_at"] != fixtureSchemaAt {
			t.Errorf("%s: want schema_captured_at %s, got %v", tc.tool, fixtureSchemaAt, meta["schema_captured_at"])
		}
		// the two are captured separately and must not be conflated
		if meta["activity_captured_at"] != fixtureActivityAt {
			t.Errorf("%s: want activity_captured_at %s, got %v", tc.tool, fixtureActivityAt, meta["activity_captured_at"])
		}
	}
}

// A snapshot with no activity rows must not claim an activity capture.
func TestMetaOmitsActivityWhenThereIsNone(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig()))
	meta := metaOf(t, callTool(t, c, "describe_table", map[string]any{"table": "app.orders"}))
	if meta["schema_captured_at"] != fixtureSchemaAt {
		t.Errorf("want the schema time, got %v", meta["schema_captured_at"])
	}
	if _, ok := meta["activity_captured_at"]; ok {
		t.Errorf("no activity rows, but meta claims %v", meta["activity_captured_at"])
	}
}

// find_objects answers through the typed toolMeta struct rather than
// injectMeta, so the field has to be on both paths.
func TestTypedMetaCarriesCaptureTime(t *testing.T) {
	srv := NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig())

	var req mcp.CallToolRequest
	req.Params.Name = "find_objects"
	res, err := srv.handleFindObjects(context.Background(), req)
	if err != nil {
		t.Fatalf("handleFindObjects: %v", err)
	}
	got, ok := res.StructuredContent.(findObjectsResult)
	if !ok {
		t.Fatalf("want findObjectsResult, got %T", res.StructuredContent)
	}
	if got.Meta == nil || got.Meta.SchemaCapturedAt != fixtureSchemaAt {
		t.Errorf("want schema_captured_at %s, got %+v", fixtureSchemaAt, got.Meta)
	}
}

// SetUninitialized means the snapshot in hand must not be dated: the check has
// to fire on a server that does hold one, or it proves nothing.
func TestCaptureTimesUninitialized(t *testing.T) {
	srv := NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig())
	if got := srv.captureTimes(); got.schema == "" {
		t.Fatal("fixture must carry a schema time for this test to mean anything")
	}
	srv.SetUninitialized()
	if got := srv.captureTimes(); got != (captureStamps{}) {
		t.Errorf("want no times once uninitialized, got %+v", got)
	}
}

// The oldest node is the honest staleness number: detect tells the agent to
// check index scans across every replica before dropping an index, and a fresh
// primary beside a stale replica must not read as fresh.
func TestCaptureTimesReportsTheOldestNode(t *testing.T) {
	a := annotate(multiSchemaSnap(), 1000)
	q := schema.QualifiedName{Schema: "app", Name: "events"}
	a = withActivityAt(a, "primary", fixtureActivityTime, q, schema.TableActivity{SeqScan: 1})
	stale := fixtureActivityTime.AddDate(0, 0, -21)
	a = withActivityAt(a, "replica1", stale, q, schema.TableActivity{IdxScan: 1})

	want := stale.UTC().Format(time.RFC3339)
	got := srvFor(a).captureTimes()
	if got.activity != want {
		t.Errorf("want the oldest node %s, got %s", want, got.activity)
	}
	// "three weeks stale" is not actionable without knowing whose capture it is
	if got.activityNode != "replica1" {
		t.Errorf("want the laggard named, got %q", got.activityNode)
	}
}

// An undated node must not win the oldest-node comparison: a zero timestamp is
// missing information, not a capture at the dawn of time.
func TestCaptureTimesIgnoresUndatedNodes(t *testing.T) {
	q := schema.QualifiedName{Schema: "app", Name: "events"}

	a := annotate(multiSchemaSnap(), 1000)
	a = withActivityAt(a, "primary", fixtureActivityTime, q, schema.TableActivity{SeqScan: 1})
	a = withActivityAt(a, "replica1", time.Time{}, q, schema.TableActivity{IdxScan: 1})
	if got := srvFor(a).captureTimes(); got.activity != fixtureActivityAt {
		t.Errorf("undated node erased the real time: want %s, got %q", fixtureActivityAt, got.activity)
	}

	// and with nothing dated at all there is no activity time to report
	b := annotate(multiSchemaSnap(), 1000)
	b = withActivityAt(b, "primary", time.Time{}, q, schema.TableActivity{SeqScan: 1})
	if got := srvFor(b).captureTimes(); got.activity != "" {
		t.Errorf("want no activity time, got %s", got.activity)
	}
}

// Planner rows are written by every snapshot take and joined by schema_ref_hash,
// so they can be far newer than DDL that has not changed in weeks.
func TestCaptureTimesDatesPlannerSeparately(t *testing.T) {
	a := annotate(multiSchemaSnap(), 1000)
	a.Planner.Timestamp = fixtureActivityTime
	meta := metaOf(t, callTool(t, serveOffline(t, srvFor(a)), "describe_table", map[string]any{"table": "app.orders"}))
	if meta["schema_captured_at"] != fixtureSchemaAt {
		t.Errorf("want schema %s, got %v", fixtureSchemaAt, meta["schema_captured_at"])
	}
	if meta["planner_captured_at"] != fixtureActivityAt {
		t.Errorf("want planner %s, got %v", fixtureActivityAt, meta["planner_captured_at"])
	}
}

func srvFor(a *schema.AnnotatedSchema) *Server {
	return NewOfflineServerAnnotated(a, lint.DefaultConfig())
}

// Thin clients read content[0].text and nothing else, so the tools that still
// answer in prose have to carry the capture times in the first line.
func TestTextHeaderCarriesCaptureTimes(t *testing.T) {
	a := annotate(multiSchemaSnap(), 1000)
	a.Planner.Timestamp = fixtureActivityTime
	a = withActivity(a, schema.QualifiedName{Schema: "app", Name: "events"}, schema.TableActivity{SeqScan: 1})

	header := strings.SplitN(srvFor(a).wrapText("body", ""), "\n", 2)[0]
	for _, want := range []string{"schema " + fixtureSchemaAt, "planner " + fixtureActivityAt, "activity " + fixtureActivityAt} {
		if !strings.Contains(header, want) {
			t.Errorf("header missing %q: %s", want, header)
		}
	}
}

// A loaded snapshot that carries no timestamps is the case worth pinning: a
// nil server returns early and never reaches the per-field logic.
func TestTextHeaderQuietWithoutTimes(t *testing.T) {
	snap := multiSchemaSnap()
	snap.Timestamp = time.Time{}
	srv := NewOfflineServerAnnotated(&schema.AnnotatedSchema{Schema: snap}, lint.DefaultConfig())
	if got := srv.wrapText("body", ""); strings.Contains(got, "captured:") {
		t.Errorf("want no capture clause, got %q", got)
	}
}
