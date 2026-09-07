package main

import (
	"testing"

	"github.com/boringsql/dryrun/internal/history"
)

// The delete prompt's cascade line: it must count the rows, drop the kinds that
// have none, and say whether they go or stay.
func TestDescribeCascade(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   history.CascadeCounts
		want string
	}{
		{
			name: "nothing bound",
			in:   history.CascadeCounts{},
			want: "No planner/activity/query stats are bound to it.",
		},
		{
			name: "one row is singular and lists only its kind",
			in:   history.CascadeCounts{Planner: 1},
			want: "1 bound stats row goes with it: 1 planner.",
		},
		{
			name: "zero kinds are omitted",
			in:   history.CascadeCounts{Activity: 12043, QueryStats: 8},
			want: "12051 bound stats rows go with it: 12043 activity, 8 query.",
		},
		{
			name: "a content twin keeps them",
			in:   history.CascadeCounts{Planner: 2, Activity: 3, Blocked: true},
			want: "5 bound stats rows stay, kept by the identical snapshot: 2 planner, 3 activity.",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := describeCascade(tc.in); got != tc.want {
				t.Errorf("describeCascade(%+v)\n got %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}
