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
		13: true, // EOL 2025-11-13, the version this floor moved off
		14: false,
		18: false,
	}
	for major, want := range cases {
		if got := BelowSupportedFloor(dryrun.PgVersion{Major: major}); got != want {
			t.Errorf("BelowSupportedFloor(major=%d) = %v, want %v", major, got, want)
		}
	}
}

func TestFloorWarningNamesBothVersions(t *testing.T) {
	w := FloorWarning(dryrun.PgVersion{Major: 13})
	for _, want := range []string{"13", "14"} {
		if !strings.Contains(w, want) {
			t.Errorf("FloorWarning should name %q, got %q", want, w)
		}
	}
}
