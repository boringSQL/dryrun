package schema

import (
	"strings"
	"testing"

	"github.com/boringsql/dryrun/internal/dryrun"
)

func TestBelowSupportedFloor(t *testing.T) {
	cases := map[int]bool{
		0:  false, // unparsed banner: unknown, not old
		8:  true,
		13: true, // EOL 2025-11-13, a previous floor
		14: true, // EOL 2026-11-12, the version this floor moved off
		15: false,
		19: false,
	}
	for major, want := range cases {
		if got := BelowSupportedFloor(dryrun.PgVersion{Major: major}); got != want {
			t.Errorf("BelowSupportedFloor(major=%d) = %v, want %v", major, got, want)
		}
	}
}

func TestFloorWarningNamesBothVersions(t *testing.T) {
	w := FloorWarning(dryrun.PgVersion{Major: 14})
	for _, want := range []string{"14", "15"} {
		if !strings.Contains(w, want) {
			t.Errorf("FloorWarning should name %q, got %q", want, w)
		}
	}
}
