package schema

import (
	"testing"

	"github.com/boringsql/qshape"
)

// COPY is the one utility statement the capture whitelist admits, and utility statements
// are stored VERBATIM by pg_stat_statements. Anything carrying a path, a command line or
// an inner query's constants must not reach the corpus, which every workspace member can
// read.
func TestDropUnsafeCopy(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		keep bool
	}{
		{"stdin", "COPY events (a, b) FROM STDIN", true},
		{"stdout", "COPY events TO STDOUT", true},
		{"stdin with options", "COPY events FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER)", true},
		{"lowercase and padded", "  copy events from stdin\n", true},

		{"file path", "COPY events FROM '/srv/data/customers.csv'", false},
		{"file path out", "COPY events TO '/tmp/dump.csv'", false},
		{"program", "COPY events FROM PROGRAM 'aws s3 cp s3://b/k -'", false},
		{"inner query carries constants", "COPY (SELECT * FROM users WHERE email = 'a@b.c') TO STDOUT", false},
		// pgss truncates at track_activity_query_size; half a COPY says nothing about the
		// half that is missing, so undecidable is dropped.
		{"truncated", "COPY events FROM ST", false},

		// Not a COPY at all: untouched, whatever it is.
		{"select", "SELECT * FROM events WHERE id = $1", true},
		{"cte", "WITH x AS (SELECT 1) SELECT * FROM x", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := dropUnsafeCopy([]qshape.Query{{QueryID: 1, Raw: tc.sql}})
			if keep := len(got) == 1; keep != tc.keep {
				t.Fatalf("kept=%v, want %v for %q", keep, tc.keep, tc.sql)
			}
		})
	}
}

// A multi-statement text is undecidable: one safe COPY does not vouch for what follows it.
func TestDropUnsafeCopyMultiStatement(t *testing.T) {
	sql := "COPY a FROM STDIN; COPY b FROM '/etc/passwd'"
	if got := dropUnsafeCopy([]qshape.Query{{Raw: sql}}); len(got) != 0 {
		t.Fatalf("kept %q", sql)
	}
}

// The filter reuses the input's backing array; the survivors must still be the right rows.
func TestDropUnsafeCopyKeepsOrderAndIdentity(t *testing.T) {
	in := []qshape.Query{
		{QueryID: 1, Raw: "SELECT 1"},
		{QueryID: 2, Raw: "COPY t FROM '/x.csv'"},
		{QueryID: 3, Raw: "COPY t FROM STDIN"},
		{QueryID: 4, Raw: "COPY (SELECT 'x') TO STDOUT"},
		{QueryID: 5, Raw: "UPDATE t SET a = $1"},
	}
	got := dropUnsafeCopy(in)
	want := []int64{1, 3, 5}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i, id := range want {
		if got[i].QueryID != id {
			t.Fatalf("row %d = queryid %d, want %d", i, got[i].QueryID, id)
		}
	}
}

// PG12+ accepts a WHERE on COPY FROM, and that expression is arbitrary user SQL stored
// verbatim like the rest of a utility statement.
func TestDropUnsafeCopyRefusesWhere(t *testing.T) {
	for _, sql := range []string{
		"COPY events FROM STDIN WHERE tag = 'internal-secret'",
		"COPY events (a, b) FROM STDIN WITH (FORMAT csv) WHERE a > 0",
	} {
		if got := dropUnsafeCopy([]qshape.Query{{Raw: sql}}); len(got) != 0 {
			t.Errorf("kept %q", sql)
		}
	}
}

// isCopy gates the parse, so it must be at least as permissive as the capture whitelist's
// `^\s*copy`. Postgres' \s includes \v and \f; a statement starting with one of those
// would otherwise be admitted by SQL and then skip the check entirely.
func TestDropUnsafeCopyWhitespaceCannotSkipTheParse(t *testing.T) {
	for _, ws := range []string{" ", "\t", "\n", "\r", "\v", "\f", " \v\n\t"} {
		sql := ws + "COPY events FROM '/srv/customers.csv'"
		if got := dropUnsafeCopy([]qshape.Query{{Raw: sql}}); len(got) != 0 {
			t.Errorf("a %q prefix skipped the parse: kept %q", ws, sql)
		}
	}
}

// The whole point of admitting COPY is that a bulk load becomes a shape like any other,
// so it has to survive qshape's fingerprinting rather than falling into the unparseable
// bucket where every COPY in the database would merge into one row.
func TestCopySurvivesQshapeGrouping(t *testing.T) {
	kept := dropUnsafeCopy([]qshape.Query{
		{QueryID: 1, Raw: "COPY events (a, b) FROM STDIN", Calls: 4, TotalExecTimeMs: 40, Rows: 4000},
	})
	clusters, err := qshape.Group(kept)
	if err != nil {
		t.Fatalf("group: %v", err)
	}
	if len(clusters) != 1 {
		t.Fatalf("clusters = %d", len(clusters))
	}
	if clusters[0].Fingerprint == "" {
		t.Fatal("COPY landed in the unparseable bucket; every COPY would merge into one shape")
	}
	if clusters[0].MeanExecTimeMs != 10 {
		t.Fatalf("mean = %v, want total/calls", clusters[0].MeanExecTimeMs)
	}
}

// The filter runs before RawRows is read, and RawRows is what tells a truncated capture
// (a head) from a complete one. Counting post-filter rows would make a capture that
// dropped a few COPYs look like it was never truncated.
func TestDropUnsafeCopyDoesNotChangeTheInputLength(t *testing.T) {
	in := []qshape.Query{
		{QueryID: 1, Raw: "SELECT 1"},
		{QueryID: 2, Raw: "COPY t FROM '/x.csv'"},
	}
	before := len(in)
	dropUnsafeCopy(in)
	if len(in) != before {
		t.Fatalf("input length changed: %d -> %d", before, len(in))
	}
}
