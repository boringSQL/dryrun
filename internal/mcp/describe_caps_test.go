package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
	"github.com/boringsql/dryrun/pkg/lint"
)

func describeWide(t *testing.T, args map[string]any) string {
	t.Helper()
	a := withColumnStats(annotate(wideSnap(400, 60), 1000),
		schema.QualifiedName{Schema: "public", Name: "wide"})
	c := serveOffline(t, NewOfflineServerAnnotated(a, lint.DefaultConfig()))
	return callTool(t, c, "describe_table", args)
}

// column_profiles is a second full-width payload beside columns, so a wide
// table pays for its width twice unless the two are narrowed together.
func TestDescribeTableProfilesFollowColumns(t *testing.T) {
	for _, args := range []map[string]any{
		{"table": "wide"},
		{"table": "wide", "columns": []string{"id", "c0007"}},
		// limit=0 skips the cap entirely, so the columns argument has to narrow
		// the profiles by itself
		{"table": "wide", "columns": []string{"id", "c0007"}, "limit": 0},
	} {
		var got struct {
			Columns []struct {
				Name string `json:"name"`
			} `json:"columns"`
			Profiles []struct {
				Column string `json:"column"`
			} `json:"column_profiles"`
		}
		out := describeWide(t, args)
		if err := json.Unmarshal([]byte(out), &got); err != nil {
			t.Fatalf("args=%v unmarshal: %v\n%.300s", args, err, out)
		}
		if len(got.Profiles) == 0 {
			t.Fatalf("args=%v: fixture stopped producing profiles", args)
		}
		if len(got.Profiles) != len(got.Columns) {
			t.Errorf("args=%v: %d columns but %d profiles", args, len(got.Columns), len(got.Profiles))
		}
		shown := map[string]bool{}
		for _, c := range got.Columns {
			shown[c.Name] = true
		}
		for _, p := range got.Profiles {
			if !shown[p.Column] {
				t.Errorf("args=%v: profile for %s, which is not in columns", args, p.Column)
			}
		}
	}
}

// A 400-column table used to arrive whole. The cap is only safe if what it
// drops is never structure the reader needs to write correct SQL.
func TestDescribeTableCapsWideTable(t *testing.T) {
	out := describeWide(t, map[string]any{"table": "wide"})

	var got struct {
		Columns []struct {
			Name string `json:"name"`
		} `json:"columns"`
		ColumnsOmitted int `json:"columns_omitted"`
		Meta           struct {
			Hint string `json:"hint"`
			Next []struct {
				Tool string         `json:"tool"`
				Args map[string]any `json:"args"`
			} `json:"next"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%.300s", err, out)
	}

	if got.ColumnsOmitted != 350 {
		t.Errorf("want 350 columns omitted of 401, got %d", got.ColumnsOmitted)
	}
	if len(got.Columns) != 51 {
		t.Errorf("want 50 capped columns plus the retained one, got %d", len(got.Columns))
	}

	// c0400 carries the only secondary index and sits last, so an ordinal cap
	// drops it and a retaining cap does not
	var names []string
	for _, c := range got.Columns {
		names = append(names, c.Name)
	}
	if !strings.Contains(strings.Join(names, ","), "c0400") {
		t.Error("the indexed trailing column must survive the cap")
	}
	if names[0] != "id" {
		t.Errorf("want the key first, got %s", names[0])
	}

	if !strings.Contains(got.Meta.Hint, "limit=0") || !strings.Contains(got.Meta.Hint, "columns=") {
		t.Errorf("hint must name both ways out: %q", got.Meta.Hint)
	}
	var rerun map[string]any
	for _, n := range got.Meta.Next {
		if n.Tool == "describe_table" {
			rerun = n.Args
		}
	}
	if rerun == nil || rerun["limit"] != float64(0) {
		t.Errorf("want a limit=0 describe_table follow-up, got %+v", got.Meta.Next)
	}
}

func TestDescribeTableLimitZeroIsUncapped(t *testing.T) {
	out := describeWide(t, map[string]any{"table": "wide", "limit": 0})
	if strings.Contains(out, "columns_omitted") {
		t.Error("limit=0 must return every column")
	}
	if !strings.Contains(out, `"c0200"`) {
		t.Error("want a middle column that the cap would have dropped")
	}
}

func TestDescribeTableColumnsArgument(t *testing.T) {
	out := describeWide(t, map[string]any{"table": "wide", "columns": []string{"id", "c0007"}})

	var got struct {
		Columns []struct {
			Name string `json:"name"`
		} `json:"columns"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%.300s", err, out)
	}
	if len(got.Columns) != 2 {
		t.Fatalf("want the two named columns, got %d", len(got.Columns))
	}
	if strings.Contains(out, `"c0008"`) {
		t.Error("a column that was not asked for came back")
	}

	// silently returning fewer columns than asked for reads as "this table does
	// not have that column"
	bad := describeWide(t, map[string]any{"table": "wide", "columns": []string{"id", "nope"}})
	if !strings.Contains(bad, "no column named nope") {
		t.Errorf("want an error naming the unknown column, got %.200s", bad)
	}
}

// toCompactTable caps children at 20, but detail=full marshals the raw table
// and never met that cap.
func TestDescribeTableCapsPartitionChildren(t *testing.T) {
	out := describeWide(t, map[string]any{"table": "events", "detail": "full"})
	if !strings.Contains(out, `"partition_children_omitted": 10`) {
		t.Errorf("want 60 children capped to 50, got %.300s", out)
	}
	if strings.Contains(out, "events_p059") {
		t.Error("the last child is past the cap and must not be present")
	}
}

// The demo snapshot is small enough that nothing should be capped; a cap that
// fires there would be a silent change to every existing caller.
func TestDescribeTableLeavesSmallTablesAlone(t *testing.T) {
	c := setupOfflineTest(t)
	out := callTool(t, c, "describe_table", map[string]any{"table": "users"})
	for _, key := range []string{"columns_omitted", "indexes_omitted", "constraints_omitted"} {
		if strings.Contains(out, key) {
			t.Errorf("small table reported %s", key)
		}
	}
}

// capRetaining's corners, which the end-to-end tests only reach through one
// fixture shape.
func TestCapRetaining(t *testing.T) {
	rows := func(names ...string) []any {
		out := make([]any, len(names))
		for i, n := range names {
			out[i] = map[string]any{"name": n}
		}
		return out
	}
	keep := map[string]bool{"d": true}

	t.Run("under_the_cap_is_untouched", func(t *testing.T) {
		in := rows("a", "b")
		got, omitted, retained, dropped := capRetaining(in, "name", keep, 5)
		if len(got) != 2 || omitted != 0 || retained != 0 || dropped != nil {
			t.Errorf("got %d kept, %d omitted, %d retained, dropped %v", len(got), omitted, retained, dropped)
		}
	})

	t.Run("exactly_the_cap_is_untouched", func(t *testing.T) {
		if _, omitted, _, _ := capRetaining(rows("a", "b"), "name", keep, 2); omitted != 0 {
			t.Errorf("len == max must not truncate, omitted %d", omitted)
		}
	})

	t.Run("empty", func(t *testing.T) {
		if got, omitted, _, _ := capRetaining(nil, "name", keep, 2); got != nil || omitted != 0 {
			t.Errorf("got %v, omitted %d", got, omitted)
		}
	})

	t.Run("retains_past_the_cap_in_order", func(t *testing.T) {
		got, omitted, retained, dropped := capRetaining(rows("a", "b", "c", "d"), "name", keep, 2)
		if retained != 1 || omitted != 1 {
			t.Fatalf("want 1 retained and 1 omitted, got %d and %d", retained, omitted)
		}
		var names []string
		for _, r := range got {
			n, _ := fieldString(r, "name")
			names = append(names, n)
		}
		if strings.Join(names, ",") != "a,b,d" {
			t.Errorf("want a,b,d in original order, got %v", names)
		}
		if strings.Join(dropped, ",") != "c" {
			t.Errorf("want c reported as dropped, got %v", dropped)
		}
	})

	t.Run("no_field_retains_nothing", func(t *testing.T) {
		got, omitted, retained, dropped := capRetaining(rows("a", "b", "d"), "", keep, 1)
		if len(got) != 1 || omitted != 2 || retained != 0 || dropped != nil {
			t.Errorf("got %d kept, %d omitted, %d retained, dropped %v", len(got), omitted, retained, dropped)
		}
	})

	t.Run("rows_that_are_not_objects", func(t *testing.T) {
		in := []any{"a", 2, nil, map[string]any{"name": "d"}}
		got, omitted, retained, _ := capRetaining(in, "name", keep, 1)
		if len(got) != 2 || retained != 1 || omitted != 2 {
			t.Errorf("got %d kept, %d omitted, %d retained", len(got), omitted, retained)
		}
	})
}

// column_profiles is populated under detail=stats while columns is not, so the
// pair is a natural call -- and writing a nil into a key the output schema
// declares as an array is a hard failure on the hosted surface.
func TestDescribeTableColumnsWithStatsDetail(t *testing.T) {
	out := describeWide(t, map[string]any{"table": "wide", "detail": "stats", "columns": []string{"id"}})
	if strings.Contains(out, "validation failed") || strings.Contains(out, `"columns": null`) {
		t.Errorf("detail=stats with columns= must not emit a null section: %.300s", out)
	}
}

// An empty array is not a request for no columns.
func TestDescribeTableEmptyColumnsArgument(t *testing.T) {
	out := describeWide(t, map[string]any{"table": "wide", "columns": []string{}})
	if !strings.Contains(out, `"id"`) {
		t.Errorf("columns=[] must behave as absent, got %.300s", out)
	}
}

// Named columns are the escape from the cap, so the cap must not apply to them.
func TestDescribeTableNamedColumnsBypassTheCap(t *testing.T) {
	want := make([]string, 0, 60)
	for i := 1; i <= 60; i++ {
		want = append(want, fmt.Sprintf("c%04d", i))
	}
	out := describeWide(t, map[string]any{"table": "wide", "columns": want})

	var got struct {
		Columns []struct {
			Name string `json:"name"`
		} `json:"columns"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%.300s", err, out)
	}
	if len(got.Columns) != 60 {
		t.Errorf("want all 60 named columns past the default cap of 50, got %d", len(got.Columns))
	}
}

// The counts are the machine-readable half; fields= must not delete them while
// leaving the prose behind.
func TestDescribeTableCountsSurviveFieldsWhitelist(t *testing.T) {
	out := describeWide(t, map[string]any{"table": "wide", "fields": []string{"columns"}})
	if !strings.Contains(out, `"columns_omitted"`) {
		t.Errorf("fields= deleted the omitted count: %.400s", out)
	}
}

// A generated column is computed, never supplied. Reporting its expression as
// a default tells an agent it can INSERT into it.
func TestDescribeTableGeneratedColumn(t *testing.T) {
	c := serveOffline(t, NewOfflineServerAnnotated(annotate(multiSchemaSnap(), 1000), lint.DefaultConfig()))
	out := callTool(t, c, "describe_table", map[string]any{"table": "public.customers"})

	var got struct {
		Columns []struct {
			Name           string  `json:"name"`
			Default        *string `json:"default"`
			Generated      *string `json:"generated"`
			GenerationExpr *string `json:"generation_expr"`
		} `json:"columns"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%.300s", err, out)
	}

	var found bool
	for _, col := range got.Columns {
		if col.Name != "email_lower" {
			continue
		}
		found = true
		if col.Default != nil {
			t.Errorf("generated column reports a default of %q", *col.Default)
		}
		if col.Generated == nil || *col.Generated != "virtual" {
			t.Errorf("want generated=virtual, got %v", col.Generated)
		}
		if col.GenerationExpr == nil || *col.GenerationExpr != "lower(email)" {
			t.Errorf("want the expression kept, got %v", col.GenerationExpr)
		}
	}
	if !found {
		t.Fatalf("fixture no longer carries the generated column: %.300s", out)
	}
}
