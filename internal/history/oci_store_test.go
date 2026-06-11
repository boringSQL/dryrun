package history

import (
	"testing"
	"time"
)

// These are the registry-free unit tests for OCIStore: the pure string/tag
// plumbing and constructor wiring we can exercise without standing up an actual
// OCI registry. The full put/get/list conformance suite lives in
// oci_store_integration_test.go behind the `integration` build tag, because it
// genuinely needs a registry to talk to (OCIStore drives a *remote.Repository,
// not an injectable in-memory target). Keeping the fast, hermetic checks here
// means a plain `go test ./internal/history/` still covers the tag scheme and
// repo-path mapping that everything else is built on top of.

// The version tag is how a snapshot is named in the registry, and List/Latest
// reconstruct timestamps by parsing it back. So versionTag and parseVersionTag
// have to be exact inverses — encode a (timestamp, hash) pair and we must get
// the same pair back out, with the timestamp landing on the dot in UTC.
func TestOCIVersionTag_RoundTrip(t *testing.T) {
	ts, err := time.Parse(time.RFC3339, "2024-06-01T12:30:45Z")
	if err != nil {
		t.Fatalf("parse timestamp: %v", err)
	}
	hash := "abc123def456"

	tag := versionTag(ts, hash)

	gotTS, gotHash, ok := parseVersionTag(tag)
	if !ok {
		t.Fatalf("parseVersionTag(%q) failed to parse a tag we just produced", tag)
	}
	if !gotTS.Equal(ts) {
		t.Errorf("timestamp round-trip: got %v, want %v", gotTS, ts)
	}
	if gotHash != hash {
		t.Errorf("hash round-trip: got %q, want %q", gotHash, hash)
	}
}

// parseVersionTag is also the filter that decides which tags in a repo are real
// snapshots. A repo holds version tags AND the hash-only `ref-<hash>` pointer
// tags used to locate a schema bundle during planner/activity merges — plus
// whatever junk a registry might surface. Only well-formed `<ts>-<hash>` tags
// should parse; everything else must be rejected so it gets skipped during a
// list rather than crashing or inventing a zero-time snapshot.
func TestOCIParseVersionTag_RejectsNonVersionTags(t *testing.T) {
	cases := []struct {
		name string
		tag  string
	}{
		{"ref pointer tag", refTag("abc123")},      // ref-abc123: "ref" is not a timestamp
		{"moving latest tag", "latest"},            // no dash at all
		{"empty string", ""},                       // nothing to parse
		{"dash with no hash", "20240601T123000Z-"}, // valid ts but empty hash half
		{"garbage before dash", "notatime-abc"},    // has a dash, but the left half isn't a timestamp
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, ok := parseVersionTag(tc.tag); ok {
				t.Errorf("parseVersionTag(%q) parsed successfully; expected it to be rejected", tc.tag)
			}
		})
	}
}

// NewOCIStore is the wiring seam: it must reject a missing base outright (an
// empty registry reference is never recoverable) and, when the caller doesn't
// supply a stream mapper, default to StreamSuffix so the OCI repo path lines up
// with the on-disk BundleDir layout. It should also tolerate a trailing slash
// on the base rather than producing a `//` in the final ref.
func TestNewOCIStore_DefaultsAndValidation(t *testing.T) {
	// Empty base is a hard error — there's nothing sensible to point at.
	if _, err := NewOCIStore(OCIConfig{}); err == nil {
		t.Fatal("NewOCIStore with empty Base succeeded; expected an error")
	}

	// A base with a trailing slash should be normalized, not passed through.
	store, err := NewOCIStore(OCIConfig{Base: "reg.example.com/proj/dryrun/"})
	if err != nil {
		t.Fatalf("NewOCIStore: %v", err)
	}
	if store.base != "reg.example.com/proj/dryrun" {
		t.Errorf("base not trimmed: got %q", store.base)
	}

	// With no StreamFor supplied, the store must fall back to StreamSuffix so
	// the default remote layout matches the shared local definition exactly.
	k := key("acme", "primary")
	if got, want := store.streamFor(k), StreamSuffix(k); got != want {
		t.Errorf("default streamFor: got %q, want %q (should match StreamSuffix)", got, want)
	}
}

// repo() composes the full registry reference from base + stream. It builds a
// remote.Repository, which parses (but does not contact) the reference, so this
// stays hermetic. We assert the composed reference is what we expect and that a
// custom StreamFor flows through instead of the default — that override is what
// makes the shared-stream feature (two projects -> one repo) work.
func TestOCIStore_RepoReference(t *testing.T) {
	store, err := NewOCIStore(OCIConfig{
		Base:      "reg.example.com/proj/dryrun",
		StreamFor: func(SnapshotKey) string { return "shared/auth" },
	})
	if err != nil {
		t.Fatalf("NewOCIStore: %v", err)
	}

	repo, err := store.repo(key("ignored", "ignored"))
	if err != nil {
		t.Fatalf("repo(): %v", err)
	}
	if got, want := repo.Reference.String(), "reg.example.com/proj/dryrun/shared/auth"; got != want {
		t.Errorf("repo reference: got %q, want %q", got, want)
	}
}
