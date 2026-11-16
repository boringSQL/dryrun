package schema

import "testing"

func TestSourceVersionNum(t *testing.T) {
	tests := []struct {
		name   string
		banner string
		want   int
		isNil  bool
	}{
		{name: "pg17 minor", banner: "PostgreSQL 17.2 on x86_64-pc-linux-gnu, compiled by gcc", want: 170002},
		{name: "pg18 point zero", banner: "PostgreSQL 18.0 on aarch64-apple-darwin", want: 180000},
		{name: "pg16 two digit minor", banner: "PostgreSQL 16.10 (Debian 16.10-1.pgdg120+1) on x86_64", want: 160010},
		{name: "beta with minor suffix", banner: "PostgreSQL 18.1beta1 on x86_64", want: 180001},
		// no dotted token: pg_restore_*_stats skips a NULL 'version' pair
		{name: "bare beta", banner: "PostgreSQL 18beta1 on x86_64", isNil: true},
		{name: "empty", banner: "", isNil: true},
		{name: "garbage", banner: "not a version banner", isNil: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sourceVersionNum(tt.banner)
			if tt.isNil {
				if got != nil {
					t.Fatalf("sourceVersionNum(%q) = %d, want nil", tt.banner, *got)
				}
				return
			}
			if got == nil {
				t.Fatalf("sourceVersionNum(%q) = nil, want %d", tt.banner, tt.want)
			}
			if *got != tt.want {
				t.Fatalf("sourceVersionNum(%q) = %d, want %d", tt.banner, *got, tt.want)
			}
		})
	}
}
