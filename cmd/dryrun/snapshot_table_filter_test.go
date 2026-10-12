package main

import (
	"errors"
	"testing"

	"github.com/boringsql/dryrun/pkg/diff"
)

// --table is a name filter over history. It must not fold case or consult
// search_path, must try the literal name before splitting on the dot, must see
// every overload and every name a renamed table carried, and must not match on
// a prefix: a wrong match here silently adds or hides versions.
func TestTouches(t *testing.T) {
	ref := func(kind, schema, name string) diff.ObjectRef {
		r := diff.ObjectRef{Kind: kind, Name: name}
		if schema != "" {
			r.Schema = &schema
		}
		return r
	}
	obj := func(r diff.ObjectRef) diff.Change { return diff.Change{Type: diff.ObjectModified, Object: r} }
	renamed := diff.Change{Type: diff.TableRenamed, Object: ref("table", "archive", "t2"),
		Rename: &diff.RenameChange{FromName: "public.t", ToName: "archive.t2"}}

	cases := []struct {
		name string
		arg  string
		c    diff.Change
		want bool
	}{
		{"bare name in public", "time_entry", obj(ref("table", "public", "time_entry")), true},
		{"bare name in another schema", "time_entry", obj(ref("table", "billing", "time_entry")), true},
		{"qualified, right schema", "billing.time_entry", obj(ref("table", "billing", "time_entry")), true},
		{"qualified, wrong schema", "billing.time_entry", obj(ref("table", "public", "time_entry")), false},
		{"literally dotted name", "a.b", obj(ref("table", "public", "a.b")), true},
		{"no prefix match", "task", obj(ref("table", "public", "task_comments")), false},
		{"case-exact", "Users", obj(ref("table", "public", "users")), false},
		{"function overload by bare name", "f", obj(ref("function", "public", "f(integer, text)")), true},
		{"function qualified", "app.f", obj(ref("function", "app", "f()")), true},
		{"signature pasted from -p", "f(integer, text)", obj(ref("function", "public", "f(integer, text)")), true},
		{"enum carries schema.name in Name", "mood", obj(ref("enum", "", "app.mood")), true},
		{"enum qualified", "app.mood", obj(ref("enum", "", "app.mood")), true},
		{"enum in another schema", "public.mood", obj(ref("enum", "", "app.mood")), false},
		{"extension by name", "pg_repack", obj(ref("extension", "", "pg_repack")), true},
		{"rename matches the old name", "t", renamed, true},
		{"rename matches the old qualified name", "public.t", renamed, true},
		{"rename matches the new name", "archive.t2", renamed, true},
		{"rename does not match an unrelated name", "public.t2", renamed, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := touches(tc.arg, tc.c); got != tc.want {
				t.Errorf("touches(%q, %s %v) = %t, want %t", tc.arg, tc.c.Object.Kind, tc.c.Object.Qualified(), got, tc.want)
			}
		})
	}
}

// A DiffErr version stays (hiding it would claim it leaves the object alone),
// the oldest has no diff and drops out, and trimming must not reach back into
// the shared delta.
func TestFilterTable(t *testing.T) {
	pub := "public"
	change := func(name string) diff.Change {
		return diff.Change{Type: diff.TableAdded, Object: diff.ObjectRef{Kind: "table", Schema: &pub, Name: name}}
	}
	mixed := &diff.SchemaDelta{Changes: []diff.Change{change("a"), change("b")}}
	in := []schemaVersion{
		{Ref: 0, Delta: mixed},
		{Ref: 1, Delta: &diff.SchemaDelta{Changes: []diff.Change{change("b")}}},
		{Ref: 2, DiffErr: errors.New("corrupt")},
		{Ref: 3, Oldest: true},
	}
	out := filterTable(in, "a")
	if len(out) != 2 || out[0].Ref != 0 || out[1].Ref != 2 {
		t.Fatalf("kept refs %v, want [0 2]", refs(out))
	}
	if len(out[0].Delta.Changes) != 1 || len(mixed.Changes) != 2 {
		t.Errorf("trimmed to %d changes, shared delta now %d; want 1 and 2", len(out[0].Delta.Changes), len(mixed.Changes))
	}
}

func refs(vs []schemaVersion) []int {
	out := make([]int, len(vs))
	for i, v := range vs {
		out[i] = v.Ref
	}
	return out
}
