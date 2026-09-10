package main

import (
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/history"
)

func TestRegressionSpan(t *testing.T) {
	at := func(y, m, d int) *time.Time {
		v := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
		return &v
	}

	tests := []struct {
		name string
		n    history.NodeSummary
		want string
	}{
		{"no span when nothing regressed", history.NodeSummary{}, ""},
		{"single day", history.NodeSummary{RegressionFirst: at(2026, 7, 27), RegressionLast: at(2026, 7, 27)}, " (2026-07-27)"},
		{"range", history.NodeSummary{RegressionFirst: at(2026, 7, 27), RegressionLast: at(2026, 7, 31)}, " (2026-07-27 .. 2026-07-31)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := regressionSpan(tc.n); got != tc.want {
				t.Errorf("regressionSpan=%q, want %q", got, tc.want)
			}
		})
	}
}
