package query

import (
	"strings"
	"testing"
)

// Every case also pins byte-length preservation: the correction pass relies on
// parser offsets addressing the caller's own SQL.
func TestRewriteNamedParams(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "simple",
			in:   "SELECT * FROM users WHERE id = :user_id",
			want: "SELECT * FROM users WHERE id = $1      ",
		},
		{
			name: "repeated name shares ordinal",
			in:   "SELECT * FROM t WHERE a = :x AND b = :x",
			want: "SELECT * FROM t WHERE a = $1 AND b = $1",
		},
		{
			name: "distinct names number in order",
			in:   "SELECT * FROM t WHERE a = :aa AND b = :bb",
			want: "SELECT * FROM t WHERE a = $1  AND b = $2 ",
		},
		{
			name: "cast left intact",
			in:   "SELECT * FROM t WHERE created_at::date > :since",
			want: "SELECT * FROM t WHERE created_at::date > $1    ",
		},
		{
			name: "cast after parameter",
			in:   "SELECT :user_id::bigint",
			want: "SELECT $1      ::bigint",
		},
		{
			name: "assignment operator skipped",
			in:   "SELECT fn(a := :v)",
			want: "SELECT fn(a := $1)",
		},
		{
			name: "psql var quoted skipped",
			in:   "SELECT * FROM t WHERE x = :'var' AND y = :real",
			want: "SELECT * FROM t WHERE x = :'var' AND y = $1   ",
		},
		{
			name: "time literal untouched",
			in:   "SELECT to_char(now(), 'HH24:MI') WHERE id = :id",
			want: "SELECT to_char(now(), 'HH24:MI') WHERE id = $1 ",
		},
		{
			name: "line comment untouched",
			in:   "-- :not_a_param\nSELECT :a",
			want: "-- :not_a_param\nSELECT $1",
		},
		{
			name: "block comment untouched",
			in:   "SELECT /* :not_a_param */ :a",
			want: "SELECT /* :not_a_param */ $1",
		},
		{
			name: "nested block comment",
			in:   "SELECT /* outer /* :inner */ :still */ :a",
			want: "SELECT /* outer /* :inner */ :still */ $1",
		},
		{
			name: "dollar body untouched",
			in:   "DO $$ BEGIN x := ':nope'; END $$; SELECT :a",
			want: "DO $$ BEGIN x := ':nope'; END $$; SELECT $1",
		},
		{
			name: "tagged dollar body untouched",
			in:   "DO $fn$ :nope $fn$; SELECT :a",
			want: "DO $fn$ :nope $fn$; SELECT $1",
		},
		{
			name: "array slice untouched",
			in:   "SELECT arr[a:b], :param",
			want: "SELECT arr[a:b], $1    ",
		},
		{
			name: "array slice with spaces",
			in:   "SELECT arr[a :b], :param",
			want: "SELECT arr[a :b], $1    ",
		},
		{
			name: "numeric slice untouched",
			in:   "SELECT arr[1:5], :param",
			want: "SELECT arr[1:5], $1    ",
		},
		{
			name: "slice after full expression untouched",
			in:   "SELECT arr[a + b :c], :param",
			want: "SELECT arr[a + b :c], $1    ",
		},
		{
			name: "slice from parameter with expression length",
			in:   "SELECT arr[:start + :len]",
			want: "SELECT arr[$1     + $2  ]",
		},
		{
			name: "array constructor with operator expression",
			in:   "SELECT ARRAY[1 + :x]",
			want: "SELECT ARRAY[1 + $1]",
		},
		{
			name: "array constructor with concatenation",
			in:   "SELECT ARRAY[a || :b]",
			want: "SELECT ARRAY[a || $1]",
		},
		{
			name: "array subscript parameter",
			in:   "SELECT arr[:idx]",
			want: "SELECT arr[$1  ]",
		},
		{
			name: "array constructor parameters",
			in:   "SELECT ARRAY[:a, :b]",
			want: "SELECT ARRAY[$1, $2]",
		},
		{
			name: "double quoted identifier untouched",
			in:   `SELECT "weird:name" FROM t WHERE x = :x`,
			want: `SELECT "weird:name" FROM t WHERE x = $1`,
		},
		{
			name: "quoted identifier with doubled quote",
			in:   `SELECT "a""b:c" FROM t WHERE x = :x`,
			want: `SELECT "a""b:c" FROM t WHERE x = $1`,
		},
		{
			name: "escape string backslash quote",
			in:   `SELECT E'don\'t :nope' WHERE x = :x`,
			want: `SELECT E'don\'t :nope' WHERE x = $1`,
		},
		{
			name: "escape string doubled quote",
			in:   `SELECT E'it''s :nope' WHERE x = :x`,
			want: `SELECT E'it''s :nope' WHERE x = $1`,
		},
		{
			name: "dollar positional param not a quote",
			in:   "SELECT * FROM t WHERE a = $1 AND b = :b",
			want: "SELECT * FROM t WHERE a = $1 AND b = $1",
		},
		{
			name: "short name at two digit ordinal clamps",
			in:   "SELECT :a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k",
			want: "SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$1,$1",
		},
		{
			name: "long name at two digit ordinal fits",
			in:   "SELECT :a,:b,:c,:d,:e,:f,:g,:h,:i,:long_name",
			want: "SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10       ",
		},
		{
			name: "parameter at start",
			in:   ":first AND x = :second",
			want: "$1     AND x = $2     ",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rewriteNamedParams(tc.in)
			if got != tc.want {
				t.Errorf("rewrite:\n got %q\nwant %q", got, tc.want)
			}
			if len(got) != len(tc.in) {
				t.Errorf("length changed: got %d want %d", len(got), len(tc.in))
			}
		})
	}
}

func TestRewriteNamedParamsLeavesPlainSQLAlone(t *testing.T) {
	for _, sql := range []string{
		"SELECT id, name FROM users WHERE id = 1",
		"SELECT ':' AS colon",
		"SELECT * FROM t WHERE ts > now()",
	} {
		if got := rewriteNamedParams(sql); got != sql {
			t.Errorf("rewrite(%q) = %q, want unchanged", sql, got)
		}
	}
}

func TestParseSQLAcceptsNamedParams(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM users WHERE id = :user_id AND created_at::date > :since",
		"SELECT :user_id::bigint",
		"SELECT :user_id ::bigint",
		"SELECT * FROM users WHERE id IN (:a, :b) LIMIT :n OFFSET :m",
		"INSERT INTO users (email) VALUES (:email)",
		"SELECT * FROM users WHERE id = $1 AND org_id = :org_id",
		"SELECT ARRAY[1 + :x], arr[:start + :len]",
		"SELECT * FROM users WHERE id = :user_id AND id = :user_id",
	} {
		if _, err := ParseSQL(sql); err != nil {
			t.Errorf("ParseSQL(%q): %v", sql, err)
		}
	}
}

func TestValidateQueryCorrectsNamedParamSQL(t *testing.T) {
	snap := testSchema()
	res, err := ValidateQuery("SELECT * FROM usres WHERE id = :user_id", snap)
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid {
		t.Fatal("expected the typo'd table to be invalid")
	}
	if res.CorrectedSQL != "SELECT * FROM users WHERE id = :user_id" {
		t.Errorf("corrected SQL lost the named parameter: %q", res.CorrectedSQL)
	}
}

func TestValidateQueryCorrectsAfterNamedParam(t *testing.T) {
	snap := testSchema()
	res, err := ValidateQuery("SELECT * FROM users u WHERE u.id = :user_id AND u.emial = 'x'", snap)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.CorrectedSQL, "u.email = 'x'") {
		t.Errorf("column after parameter not corrected: %q", res.CorrectedSQL)
	}
	if !strings.Contains(res.CorrectedSQL, ":user_id") {
		t.Errorf("corrected SQL lost the named parameter: %q", res.CorrectedSQL)
	}
}
