package query

import (
	"testing"
	"time"

	"github.com/boringsql/dryrun/internal/schema"
)

func correctSchema() *schema.SchemaSnapshot {
	return &schema.SchemaSnapshot{
		PgVersion:   "PostgreSQL 17.0",
		Database:    "test",
		Timestamp:   time.Now().UTC(),
		ContentHash: "test",
		Tables: []schema.Table{
			{
				OID: 1, Schema: "public", Name: "users",
				Columns: []schema.Column{
					{Name: "id", Ordinal: 1, TypeName: "bigint"},
					{Name: "email", Ordinal: 2, TypeName: "text"},
					{Name: "created_at", Ordinal: 3, TypeName: "timestamptz"},
				},
			},
			{
				OID: 2, Schema: "public", Name: "orders",
				Columns: []schema.Column{
					{Name: "id", Ordinal: 1, TypeName: "bigint"},
					{Name: "user_id", Ordinal: 2, TypeName: "bigint"},
				},
			},
			// reserved word: only reachable quoted
			{
				OID: 3, Schema: "public", Name: "order",
				Columns: []schema.Column{{Name: "id", Ordinal: 1, TypeName: "bigint"}},
			},
			// created with a quoted mixed-case column
			{
				OID: 4, Schema: "public", Name: "contacts",
				Columns: []schema.Column{
					{Name: "id", Ordinal: 1, TypeName: "bigint"},
					{Name: "Email", Ordinal: 2, TypeName: "text"},
				},
			},
			{
				OID: 5, Schema: "app", Name: "audit_log",
				Columns: []schema.Column{
					{Name: "id", Ordinal: 1, TypeName: "bigint"},
					{Name: "actor_id", Ordinal: 2, TypeName: "bigint"},
				},
			},
			// same name in two schemas, neither of them public
			{OID: 6, Schema: "app", Name: "sessions", Columns: []schema.Column{{Name: "id", Ordinal: 1, TypeName: "bigint"}}},
			{OID: 7, Schema: "billing", Name: "sessions", Columns: []schema.Column{{Name: "id", Ordinal: 1, TypeName: "bigint"}}},
		},
		Views: []schema.View{
			{Schema: "public", Name: "active_users"},
		},
	}
}

func TestCorrectedSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string // "" means no correction should be offered
	}{
		{
			name: "column typo in the select list",
			sql:  "SELECT u.emial FROM users u WHERE u.id = 1",
			want: "SELECT u.email FROM users u WHERE u.id = 1",
		},
		{
			name: "column typo in the where clause",
			sql:  "SELECT u.id FROM users u WHERE u.creaed_at > now()",
			want: "SELECT u.id FROM users u WHERE u.created_at > now()",
		},
		{
			name: "table typo",
			sql:  "SELECT * FROM userss",
			want: "SELECT * FROM users",
		},
		{
			name: "table typo resolving to a view",
			sql:  "SELECT * FROM active_user",
			want: "SELECT * FROM active_users",
		},
		{
			name: "unqualified name that lives in one other schema",
			sql:  "SELECT * FROM audit_log WHERE id = 1",
			want: "SELECT * FROM app.audit_log WHERE id = 1",
		},
		{
			name: "typo inside an explicit schema stays in that schema",
			sql:  "SELECT * FROM app.audit_logs",
			want: "SELECT * FROM app.audit_log",
		},
		{
			name: "every occurrence of the table is rewritten",
			sql:  "SELECT a.id FROM userss a JOIN userss b ON a.id = b.id",
			want: "SELECT a.id FROM users a JOIN users b ON a.id = b.id",
		},
		{
			name: "matching text in a string literal is left alone",
			sql:  "SELECT u.emial FROM users u WHERE u.email = 'emial'",
			want: "SELECT u.email FROM users u WHERE u.email = 'emial'",
		},
		{
			name: "whitespace and comments survive",
			sql:  "SELECT\n  u.emial  -- the address\nFROM users u",
			want: "SELECT\n  u.email  -- the address\nFROM users u",
		},
		{
			name: "candidate that only differs in case comes back quoted",
			sql:  "SELECT c.email FROM contacts c",
			want: `SELECT c."Email" FROM contacts c`,
		},
		{
			name: "candidate that is a reserved word comes back quoted",
			sql:  "SELECT * FROM ordr",
			want: `SELECT * FROM "order"`,
		},
		{
			name: "two typos are both fixed",
			sql:  "SELECT u.emial FROM userss u",
			want: "SELECT u.email FROM users u",
		},
		{
			// a bad table name hides the column errors under it, so the second
			// pass sees errors the first one could not
			name: "columns hidden behind a bad table name",
			sql:  "SELECT u.emial, u.creaed_at FROM userss u",
			want: "SELECT u.email, u.created_at FROM users u",
		},
		{
			name: "the same typo in two places",
			sql:  "SELECT u.emial FROM users u WHERE u.emial = 'x'",
			want: "SELECT u.email FROM users u WHERE u.email = 'x'",
		},
		{
			name: "quoted references in the source",
			sql:  `SELECT "u"."emial" FROM users "u"`,
			want: `SELECT "u".email FROM users "u"`,
		},
		{
			name: "insert target",
			sql:  "INSERT INTO userss (id) VALUES (1)",
			want: "INSERT INTO users (id) VALUES (1)",
		},
		{
			name: "every statement in the input",
			sql:  "SELECT * FROM userss; DELETE FROM userss WHERE id = 1",
			want: "SELECT * FROM users; DELETE FROM users WHERE id = 1",
		},

		{
			name: "no candidate close enough",
			sql:  "SELECT * FROM warehouses",
			want: "",
		},
		{
			name: "short names are not guessed at",
			sql:  "SELECT * FROM usr",
			want: "",
		},
		{
			name: "ambiguous across schemas",
			sql:  "SELECT * FROM sessions",
			want: "",
		},
		{
			name: "one fixable error and one that is not",
			sql:  "SELECT * FROM userss, warehouses",
			want: "",
		},
		{
			name: "valid query gets no correction",
			sql:  "SELECT u.email FROM users u",
			want: "",
		},
		{
			// the parser points at `u`, the scan reads `u` and stops; the run
			// does not match what the parser recorded, so nothing is rewritten
			name: "whitespace around the dot",
			sql:  "SELECT u . emial FROM users u",
			want: "",
		},
		{
			name: "a cte is not a missing table",
			sql:  "WITH ordr AS (SELECT 1 AS id) SELECT * FROM ordr",
			want: "",
		},
	}

	snap := correctSchema()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := ValidateQuery(tc.sql, snap)
			if err != nil {
				t.Fatalf("ValidateQuery: %v", err)
			}
			if res.CorrectedSQL != tc.want {
				t.Fatalf("corrected_sql:\n got: %q\nwant: %q\n(errors: %v)", res.CorrectedSQL, tc.want, res.Errors)
			}
			if tc.want == "" {
				if len(res.Fixes) != 0 {
					t.Fatalf("fixes reported without a corrected query: %+v", res.Fixes)
				}
				return
			}
			if len(res.Fixes) == 0 {
				t.Fatal("corrected_sql without any fixes listed")
			}
		})
	}
}

// The promise the hint makes: what comes back parses and resolves.
func TestCorrectedSQLAlwaysValidates(t *testing.T) {
	snap := correctSchema()
	queries := []string{
		"SELECT u.emial FROM users u WHERE u.id = 1",
		"SELECT * FROM userss",
		"SELECT * FROM audit_log WHERE id = 1",
		"SELECT c.email FROM contacts c",
		"SELECT * FROM ordr",
		"SELECT u.emial FROM userss u",
		"UPDATE userss SET email = 'x' WHERE id = 1",
		"DELETE FROM userss WHERE id = 1",
	}
	for _, sql := range queries {
		res, err := ValidateQuery(sql, snap)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if res.CorrectedSQL == "" {
			t.Fatalf("%s: expected a correction, got none (errors: %v)", sql, res.Errors)
		}
		again, err := ValidateQuery(res.CorrectedSQL, snap)
		if err != nil {
			t.Fatalf("%s -> %s: does not parse: %v", sql, res.CorrectedSQL, err)
		}
		if !again.Valid {
			t.Fatalf("%s -> %s: does not validate: %v", sql, res.CorrectedSQL, again.Errors)
		}
	}
}

func TestFixesDescribeTheEdits(t *testing.T) {
	res, err := ValidateQuery("SELECT u.emial FROM userss u", correctSchema())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]Fix{
		"emial":  {Kind: "column", From: "emial", To: "email"},
		"userss": {Kind: "table", From: "userss", To: "users"},
	}
	if len(res.Fixes) != len(want) {
		t.Fatalf("got %d fixes, want %d: %+v", len(res.Fixes), len(want), res.Fixes)
	}
	for _, f := range res.Fixes {
		if want[f.From] != f {
			t.Errorf("fix %+v does not match %+v", f, want[f.From])
		}
	}

	// one name, two places, one fix
	res, err = ValidateQuery("SELECT u.emial FROM users u WHERE u.emial = 'x'", correctSchema())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Fixes) != 1 {
		t.Fatalf("repeated typo listed %d times: %+v", len(res.Fixes), res.Fixes)
	}
}

func TestBestCandidate(t *testing.T) {
	tests := []struct {
		bad  string
		pool []string
		want string
	}{
		{"emial", []string{"email", "id"}, "email"},                  // transposition
		{"custmers", []string{"customers", "orders"}, "customers"},   // dropped letter
		{"organizatons", []string{"organizations"}, "organizations"}, // long name, one edit
		{"Email", []string{"email"}, "email"},                        // case only
		{"email", []string{"Email", "EMAIL"}, ""},                    // two case-only matches
		{"email", []string{"emails", "emailz"}, ""},                  // tie
		{"usr", []string{"users"}, ""},                               // too short to guess
		{"zzzzzzzz", []string{"users"}, ""},                          // nothing close
		{"created_by", []string{"created_at", "id"}, ""},             // two edits is a different word
	}
	for _, tc := range tests {
		got, ok := bestCandidate(tc.bad, tc.pool)
		if !ok {
			got = ""
		}
		if got != tc.want {
			t.Errorf("bestCandidate(%q, %v) = %q, want %q", tc.bad, tc.pool, got, tc.want)
		}
	}
}

func TestOSADistance(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"", "abc", 3},
		{"abc", "", 3},
		{"abc", "abc", 0},
		{"emial", "email", 1},
		{"kitten", "sitting", 3},
		{"ca", "abc", 3},
	}
	for _, tc := range tests {
		if got := osaDistance(tc.a, tc.b); got != tc.want {
			t.Errorf("osaDistance(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// A CTE shadows a real table of the same name; reporting it missing was already
// wrong, and rewriting it to the real table changes what the query reads.
func TestCTEIsNotAMissingTable(t *testing.T) {
	res, err := ValidateQuery("WITH ordr AS (SELECT 1 AS id) SELECT * FROM ordr", correctSchema())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid {
		t.Fatalf("expected valid, got errors: %v", res.Errors)
	}
	if res.CorrectedSQL != "" {
		t.Fatalf("rewrote a cte reference: %q", res.CorrectedSQL)
	}
}

// Two schemas holding the same table name are two references, not one.
func TestSameNameInTwoSchemas(t *testing.T) {
	res, err := ValidateQuery("SELECT a.id FROM app.sessions a, billing.sessions b WHERE a.id = b.id", correctSchema())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid {
		t.Fatalf("expected valid, got errors: %v", res.Errors)
	}
	if len(res.ReferencedObjects) != 2 {
		t.Fatalf("got %d referenced objects, want 2: %+v", len(res.ReferencedObjects), res.ReferencedObjects)
	}
}

func TestScanIdent(t *testing.T) {
	tests := []struct {
		in    string
		want  string
		end   int
		wantK bool
	}{
		{"users", "users", 5, true},
		{"Users x", "users", 5, true},
		{`"Users"`, "Users", 7, true},
		{`"a""b"`, `a"b`, 6, true},
		{"a1_$ ", "a1_$", 4, true},
		{"1abc", "", 0, false},
		{`"unterminated`, "", 0, false},
		{"", "", 0, false},
		{".x", "", 0, false},
	}
	for _, tc := range tests {
		got, ok := scanIdent(tc.in, 0)
		if ok != tc.wantK {
			t.Errorf("scanIdent(%q) ok = %v, want %v", tc.in, ok, tc.wantK)
			continue
		}
		if !ok {
			continue
		}
		if got.value != tc.want || got.end != tc.end {
			t.Errorf("scanIdent(%q) = %q end %d, want %q end %d", tc.in, got.value, got.end, tc.want, tc.end)
		}
	}
}

func TestScanQualifiedIdent(t *testing.T) {
	tests := []struct {
		in   string
		want []string
		ok   bool
	}{
		{"users", []string{"users"}, true},
		{"app.audit_log", []string{"app", "audit_log"}, true},
		{`app."Audit"`, []string{"app", "Audit"}, true},
		{"a.b.c ", []string{"a", "b", "c"}, true},
		{"u.* ", nil, false},
		{"u. ", nil, false},
	}
	for _, tc := range tests {
		parts, ok := scanQualifiedIdent(tc.in, 0)
		if ok != tc.ok {
			t.Errorf("scanQualifiedIdent(%q) ok = %v, want %v", tc.in, ok, tc.ok)
			continue
		}
		if !ok {
			continue
		}
		got := values(parts)
		if len(got) != len(tc.want) {
			t.Errorf("scanQualifiedIdent(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("scanQualifiedIdent(%q) = %v, want %v", tc.in, got, tc.want)
				break
			}
		}
	}
}

func TestNeedsQuote(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"users", false},
		{"a_1$", false},
		{"Users", true},
		{"1users", true},
		{"user name", true},
		{"", true},
	} {
		if got := needsQuote(tc.in); got != tc.want {
			t.Errorf("needsQuote(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
