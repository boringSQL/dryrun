package snapshot

import "testing"

func TestDetectFlavor(t *testing.T) {
	cases := []struct {
		name string
		sig  FlavorSignals
		want Flavor
	}{
		{
			name: "vanilla postgres has no columnar engine",
			sig:  FlavorSignals{HasColumnarEngine: false, OmniMarker: false},
			want: FlavorPostgres,
		},
		{
			name: "columnar engine plus omni marker is omni",
			sig:  FlavorSignals{HasColumnarEngine: true, OmniMarker: true},
			want: FlavorAlloyDBOmni,
		},
		{
			name: "columnar engine without omni marker falls back to managed",
			sig:  FlavorSignals{HasColumnarEngine: true, OmniMarker: false},
			want: FlavorAlloyDBManaged,
		},
		{
			name: "omni marker without columnar engine is still postgres (marker alone insufficient)",
			sig:  FlavorSignals{HasColumnarEngine: false, OmniMarker: true},
			want: FlavorPostgres,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DetectFlavor(tc.sig); got != tc.want {
				t.Errorf("DetectFlavor(%+v) = %q, want %q", tc.sig, got, tc.want)
			}
		})
	}
}

// The managed fallback must stay locked down: mislabeling managed as Omni would
// let the catalog-write path run and fail, so the safe default is not-writable.
func TestCapabilitiesFailSafe(t *testing.T) {
	managed := FlavorAlloyDBManaged.Capabilities()
	if managed.CatalogWritable {
		t.Error("managed AlloyDB must not be catalog-writable")
	}
	if managed.StorageInspectable {
		t.Error("managed AlloyDB storage is opaque, must not be inspectable")
	}
	if managed.ConfigTunable {
		t.Error("managed AlloyDB has no ALTER SYSTEM, must not be config-tunable")
	}
	if !managed.AdaptiveAutovacuum {
		t.Error("managed AlloyDB runs adaptive autovacuum")
	}

	// Omni runs on standard PostgreSQL storage, so the stock capabilities hold;
	// only the autovacuum scheduler differs from vanilla.
	omni := FlavorAlloyDBOmni.Capabilities()
	if !omni.CatalogWritable || !omni.StorageInspectable || !omni.FullPageWritesTax || !omni.ConfigTunable {
		t.Errorf("Omni should retain stock storage/config capabilities, got %+v", omni)
	}
	if !omni.AdaptiveAutovacuum {
		t.Error("Omni ships adaptive autovacuum")
	}

	pg := FlavorPostgres.Capabilities()
	if pg.AdaptiveAutovacuum {
		t.Error("vanilla PostgreSQL does not have adaptive autovacuum")
	}
	if !pg.CatalogWritable || !pg.StorageInspectable {
		t.Errorf("vanilla PostgreSQL should be fully capable, got %+v", pg)
	}
}

// Flavor is advisory metadata, kept out of the content hash so it can't fork
// dedup. Two snapshots identical but for Flavor must hash equal; this guards the
// exclusion in ComputeContentHash against a future refactor folding it back in.
func TestFlavorExcludedFromContentHash(t *testing.T) {
	base := &SchemaSnapshot{
		PgVersion: "PostgreSQL 17.7",
		Database:  "app",
		Tables:    []Table{{Schema: "public", Name: "orders"}},
	}
	omni := *base
	omni.Flavor = FlavorAlloyDBOmni
	managed := *base
	managed.Flavor = FlavorAlloyDBManaged

	if ComputeContentHash(&omni) != ComputeContentHash(&managed) {
		t.Error("flavor must not affect the content hash")
	}
}
