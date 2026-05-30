package history

import (
	"reflect"
	"testing"

	"github.com/boringsql/dryrun/internal/schema"
)

// The bundle codec is the single chokepoint where a Bundle becomes bytes and
// back again. Every backend (filesystem today, OCI next) leans on it to produce
// byte-identical wire output, so the contract we actually care about is simple
// but load-bearing: whatever goes into EncodeBundle must come back out of
// DecodeBundle structurally unchanged, and the same input must always produce
// the same bytes. These tests pin that contract down.

// fullBundle assembles a Bundle carrying all three snapshot kinds, with two
// distinct activity nodes so the map-valued Activity field is exercised with
// more than one entry — single-entry maps have a way of hiding ordering and
// key-handling bugs. We reuse the package's existing fixtures so the shapes
// stay in lockstep with the rest of the store tests.
func fullBundle() *Bundle {
	return &Bundle{
		Schema:  testSnapshot("schema-hash-abc123", "appdb"),
		Planner: plannerFixture("schema-hash-abc123", "planner-hash-def456", "appdb"),
		Activity: map[string]*schema.ActivityStatsSnapshot{
			"primary": activityFixture("schema-hash-abc123", "activity-hash-primary", "primary", false),
			"replica": activityFixture("schema-hash-abc123", "activity-hash-replica", "replica", true),
		},
	}
}

// Full round-trip: a Bundle carrying schema + planner + two activity nodes
// should survive marshal -> zstd -> unzstd -> unmarshal and come back
// deep-equal to what we started with. If JSON tags drift, a field stops being
// exported, or compression mangles something, this is the test that screams.
func TestBundleCodec_RoundTrip(t *testing.T) {
	original := fullBundle()

	raw, err := EncodeBundle(original)
	if err != nil {
		t.Fatalf("encode bundle: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("encode produced zero bytes")
	}

	decoded, err := DecodeBundle(raw)
	if err != nil {
		t.Fatalf("decode bundle: %v", err)
	}

	// reflect.DeepEqual is the right hammer here: the fixtures build their
	// time.Time values via time.Now().UTC(), which carries no monotonic clock
	// reading and lives in the UTC location, so a JSON round-trip (RFC3339Nano,
	// also UTC) reproduces them field-for-field. No timezone or monotonic skew
	// to trip the comparison.
	if !reflect.DeepEqual(original, decoded) {
		t.Errorf("round-trip mismatch:\n original = %+v\n decoded = %+v", original, decoded)
	}
}

// Encoding the same bundle twice must yield byte-identical output. This is the
// property that makes registry-side dedup work for OCIStore: the layer digest
// is computed over these bytes, so any nondeterminism (map iteration leaking
// into output, compression dictionary drift) would silently defeat dedup by
// producing a "new" blob for unchanged content. The two-activity-node bundle is
// the important case — Go randomizes map iteration, so an encoder that walked
// the map without a stable key order would diverge here.
func TestBundleCodec_Deterministic(t *testing.T) {
	b := fullBundle()

	first, err := EncodeBundle(b)
	if err != nil {
		t.Fatalf("first encode: %v", err)
	}
	second, err := EncodeBundle(b)
	if err != nil {
		t.Fatalf("second encode: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Errorf("encode is nondeterministic: two encodings of the same bundle differ (%d vs %d bytes)", len(first), len(second))
	}
}

// DecodeBundle promises that a nil Activity map comes back as an empty-but-
// non-nil map. Callers index Activity directly (b.Activity[node] = ...) during
// the planner/activity merge, so a nil map would panic on write. We encode a
// schema-only bundle (no activity at all) and assert the normalization kicked
// in on the way back out.
func TestBundleCodec_NilActivityNormalized(t *testing.T) {
	b := &Bundle{
		Schema: testSnapshot("schema-only", "appdb"),
		// Activity intentionally left nil.
	}

	raw, err := EncodeBundle(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeBundle(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Activity == nil {
		t.Fatal("decoded Activity is nil; expected an empty non-nil map so callers can index without a nil check")
	}
	if len(decoded.Activity) != 0 {
		t.Errorf("decoded Activity should be empty, got %d entries", len(decoded.Activity))
	}
}

// Garbage in must not look like success. DecodeBundle first runs the bytes
// through the zstd reader, so a payload that isn't a valid zstd frame should
// surface a clean decompression error rather than a panic or a zero-value
// Bundle masquerading as real data. This is the on-disk / on-registry
// corruption guard at the codec layer.
func TestBundleCodec_DecodeGarbage(t *testing.T) {
	if _, err := DecodeBundle([]byte("this is definitely not a zstd frame")); err == nil {
		t.Fatal("decoding garbage bytes succeeded; expected an error")
	}

	// An empty input is its own little edge case — no frame at all — and should
	// likewise error rather than yielding a usable bundle.
	if _, err := DecodeBundle(nil); err == nil {
		t.Fatal("decoding nil bytes succeeded; expected an error")
	}
}
