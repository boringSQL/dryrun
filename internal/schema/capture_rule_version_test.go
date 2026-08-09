package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// The Go-side half of row selection, dropUnsafeCopy, is covered by
// copy_filter_test.go; it is named in CaptureRuleVersion's contract for the
// same reason and a change there needs the same bump.

// captureRuleFingerprints pins the content of the file that decides which
// pg_stat_statements rows a capture keeps, per CaptureRuleVersion.
//
// Only the current version needs an entry — older ones are history, and their
// files are gone. Add a new entry when you bump.
var captureRuleFingerprints = map[int]string{
	2: "fb89850637e6110da444ce9ba0e960edacfb0ec85d66a8d31f8f85acb3433691",
}

// TestCaptureRuleVersionMatchesSQL is the coupling between the constant and the
// SQL it claims to describe. Without it, editing the WHERE clause silently
// produces captures that are not comparable with earlier ones and says nothing
// — which is exactly what happened with commit 6d134bc.
//
// It fires on any edit to the file, including comments. That is the correct
// failure direction: a false alarm costs one line to re-bless, a missed bump
// costs a wrong answer nobody can detect afterwards.
func TestCaptureRuleVersionMatchesSQL(t *testing.T) {
	body, err := sqlFS.ReadFile("sql/query_stats.sql")
	if err != nil {
		t.Fatalf("read embedded query_stats.sql: %v", err)
	}
	sum := sha256.Sum256(body)
	got := hex.EncodeToString(sum[:])

	want, ok := captureRuleFingerprints[CaptureRuleVersion]
	if !ok {
		t.Fatalf("CaptureRuleVersion is %d but captureRuleFingerprints has no entry for it.\n"+
			"Add:\n\t%d: %q,", CaptureRuleVersion, CaptureRuleVersion, got)
	}
	if got != want {
		t.Fatalf("sql/query_stats.sql changed but CaptureRuleVersion is still %d.\n"+
			"  have: %s\n  want: %s\n"+
			"If the edit can change WHICH statements are captured, bump\n"+
			"snapshot.CaptureRuleVersion and add its fingerprint. If it cannot\n"+
			"(a comment or formatting change), update the entry for %d to the new sum.",
			CaptureRuleVersion, got, want, CaptureRuleVersion)
	}
}
