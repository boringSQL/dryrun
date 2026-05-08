package dryrun

import "testing"

func TestParsePgVersion(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    PgVersion
		wantErr bool
	}{
		{
			name:  "pg17",
			input: "PostgreSQL 17.2 on x86_64-pc-linux-gnu, compiled by gcc 12.2.0, 64-bit",
			want:  PgVersion{Major: 17, Minor: 2, Patch: 0},
		},
		{
			name:  "pg16 three part",
			input: "PostgreSQL 16.1.3 (Debian 16.1.3-1) on aarch64-unknown-linux-gnu",
			want:  PgVersion{Major: 16, Minor: 1, Patch: 3},
		},
		{
			name:  "pg14 beta",
			input: "PostgreSQL 14.0beta1 on x86_64",
			want:  PgVersion{Major: 14, Minor: 0, Patch: 0},
		},
		{
			name:  "pg12 minor only",
			input: "PostgreSQL 12.18 on aarch64",
			want:  PgVersion{Major: 12, Minor: 18, Patch: 0},
		},
		{
			name:    "garbage fails",
			input:   "not a version string",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePgVersion(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
