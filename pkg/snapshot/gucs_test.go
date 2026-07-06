package snapshot

import (
	"testing"
)

func ptr(s string) *string { return &s }

// CanonicalValue must produce values that are valid on the right-hand side of
// SET LOCAL. pg_settings reports settings as bare numbers in an internal unit
// (shared_buffers counts 8kB pages, work_mem counts kB), so the unit has to be
// re-attached — and for block-sized units like "8kB" the count has to be
// multiplied out, because "163848kB" (naive concatenation) would be garbage.
func TestCanonicalValue(t *testing.T) {
	tests := []struct {
		name string
		guc  GucSetting
		want string
	}{
		{
			name: "shared_buffers in 8kB pages multiplies out to kB",
			guc:  GucSetting{Name: "shared_buffers", Setting: "16384", Unit: ptr("8kB")},
			want: "131072kB",
		},
		{
			name: "work_mem in kB appends the unit directly",
			guc:  GucSetting{Name: "work_mem", Setting: "4096", Unit: ptr("kB")},
			want: "4096kB",
		},
		{
			name: "min_parallel_table_scan_size in 8kB pages",
			guc:  GucSetting{Name: "min_parallel_table_scan_size", Setting: "1024", Unit: ptr("8kB")},
			want: "8192kB",
		},
		{
			name: "time unit ms appends directly",
			guc:  GucSetting{Name: "autovacuum_vacuum_cost_delay", Setting: "2", Unit: ptr("ms")},
			want: "2ms",
		},
		{
			name: "unitless boolean passes through",
			guc:  GucSetting{Name: "jit", Setting: "on", Unit: nil},
			want: "on",
		},
		{
			name: "unitless float passes through",
			guc:  GucSetting{Name: "random_page_cost", Setting: "1.1", Unit: nil},
			want: "1.1",
		},
		{
			name: "empty-string unit treated as unitless",
			guc:  GucSetting{Name: "max_parallel_workers", Setting: "8", Unit: ptr("")},
			want: "8",
		},
		{
			name: "negative sentinel stays bare even with a unit",
			guc:  GucSetting{Name: "autovacuum_work_mem", Setting: "-1", Unit: ptr("kB")},
			want: "-1",
		},
		{
			name: "zero with unit keeps the unit",
			guc:  GucSetting{Name: "autovacuum_vacuum_cost_delay", Setting: "0", Unit: ptr("ms")},
			want: "0ms",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.guc.CanonicalValue(); got != tt.want {
				t.Errorf("CanonicalValue() = %q, want %q", got, tt.want)
			}
		})
	}
}
