package query

import "testing"

func TestValidateTextColumnComparedWithNumber(t *testing.T) {
	snap := migrationTestAnnotated().Schema
	tests := []struct {
		sql       string
		valid     bool
		corrected bool // a name fix that leaves the type error must not come back as clean
	}{
		{"SELECT * FROM users WHERE email = 1", false, false},
		{"SELECT * FROM users u WHERE u.emial = 1", false, false},
		{"SELECT * FROM users WHERE email = '1'", true, false},
		{"SELECT * FROM users WHERE id = 1", true, false},
	}
	for _, tt := range tests {
		res, err := ValidateQuery(tt.sql, snap)
		if err != nil {
			t.Fatalf("%s: %v", tt.sql, err)
		}
		if res.Valid != tt.valid || (res.CorrectedSQL != "") != tt.corrected {
			t.Errorf("%s: valid=%v corrected=%q, want valid=%v (errors %v)", tt.sql, res.Valid, res.CorrectedSQL, tt.valid, res.Errors)
		}
	}
}
