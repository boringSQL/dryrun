package schema

import "testing"

// Column remapping is covered live; these are the cases a live target can't
// easily produce.
func TestRemapExtendedStatsAttnums(t *testing.T) {
	src, dst := []int16{1, 2}, []int16{2, 3}

	// negative numbers are expression slots, not table attnums
	got, err := remapNDistinct(`[{"attributes":[-1,2],"ndistinct":1}]`, src, dst)
	if want := `[{"attributes":[-1,3],"ndistinct":1}]`; err != nil || got != want {
		t.Errorf("expression slot remap = %s, %v; want %s", got, err, want)
	}

	if _, err := remapDependencies(`[{"attributes":[1],"dependency":4,"degree":1}]`, src, dst); err == nil {
		t.Error("attnum outside the captured key list was accepted")
	}
}
